import "server-only";

import { ZodError } from "zod";

import { dedupeJobs } from "@/lib/server/crawler/dedupe";
import {
  buildExperienceInferencePrompt,
  buildContentFingerprint,
  buildSourceLookupKey,
  createId,
  isValidationStale,
  matchesFilters,
  matchesFiltersWithoutExperience,
  normalizeComparableText,
  resolveJobExperienceLevel,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  toStoredValidation,
  validateJobLink,
  type LinkValidationDraft,
} from "@/lib/server/crawler/link-validation";
import { sortJobs } from "@/lib/server/crawler/sort";
import { getRepository, JobCrawlerRepository, type PersistableJob } from "@/lib/server/db/repository";
import { createDefaultProviders } from "@/lib/server/providers";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import { getEnv } from "@/lib/server/env";
import {
  crawlResponseSchema,
  crawlSourceResultSchema,
  searchFiltersSchema,
  type CrawlResponse,
  type CrawlRun,
  type CrawlRunStatus,
  type CrawlSourceResult,
  type JobListing,
  type LinkValidationResult,
  type SearchDocument,
} from "@/lib/types";

export class ResourceNotFoundError extends Error {}

type Runtime = {
  repository?: JobCrawlerRepository;
  providers?: CrawlProvider[];
  fetchImpl?: typeof fetch;
  now?: Date;
};

export async function runSearchFromFilters(
  rawFilters: unknown,
  runtime: Runtime = {},
) {
  const filters = searchFiltersSchema.parse(rawFilters);
  const repository = await resolveRepository(runtime.repository);
  const now = runtime.now ?? new Date();
  const search = await repository.createSearch(filters, now.toISOString());

  return executeCrawl({
    search,
    repository,
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now,
  });
}

export async function rerunSearch(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  return executeCrawl({
    search,
    repository,
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now ?? new Date(),
  });
}

export async function getSearchDetails(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  if (!search.latestCrawlRunId) {
    return crawlResponseSchema.parse({
      search,
      crawlRun: {
        _id: createId(),
        searchId: search._id,
        startedAt: search.createdAt,
        finishedAt: search.updatedAt,
        status: search.lastStatus ?? "completed",
        totalFetchedJobs: 0,
        totalMatchedJobs: 0,
        dedupedJobs: 0,
      },
      sourceResults: [],
      jobs: [],
    });
  }

  const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId);
  if (!crawlRun) {
    throw new ResourceNotFoundError(
      `Latest crawl run ${search.latestCrawlRunId} for search ${searchId} was not found.`,
    );
  }

  let jobs = await repository.getJobsByCrawlRun(crawlRun._id);
  jobs = await refreshStaleJobs(jobs, repository, runtime.fetchImpl ?? fetch, runtime.now ?? new Date());
  const sourceResults = await repository.getCrawlSourceResults(crawlRun._id);

  return crawlResponseSchema.parse({
    search,
    crawlRun,
    sourceResults,
    jobs: sortJobs(jobs.map(applyResolvedExperienceLevel), search.filters.title),
  });
}

export async function revalidateJob(jobId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const job = await repository.getJob(jobId);

  if (!job) {
    throw new ResourceNotFoundError(`Job ${jobId} was not found.`);
  }

  const validation = await validateJobLink(
    job.applyUrl,
    runtime.fetchImpl ?? fetch,
    runtime.now ?? new Date(),
  );

  const updatedJob: JobListing = {
    ...job,
    experienceLevel: resolveJobExperienceLevel(job),
    resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
    canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
    linkStatus: validation.status,
    lastValidatedAt: validation.checkedAt,
  };

  const latestRunId = job.crawlRunIds[job.crawlRunIds.length - 1] ?? job.crawlRunIds[0];
  const [savedJob] = await repository.persistJobs(latestRunId, [
    {
      ...updatedJob,
      sourceProvenance: updatedJob.sourceProvenance,
      sourceLookupKeys: updatedJob.sourceLookupKeys,
    },
  ]);

  await repository.saveLinkValidation(toStoredValidation(jobId, validation));

  return savedJob;
}

export async function listRecentSearches(runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  return repository.listRecentSearches();
}

export function isInputValidationError(error: unknown) {
  return error instanceof ZodError;
}

type ExecuteCrawlInput = {
  search: SearchDocument;
  repository: JobCrawlerRepository;
  providers: CrawlProvider[];
  fetchImpl: typeof fetch;
  now: Date;
};

async function executeCrawl(input: ExecuteCrawlInput): Promise<CrawlResponse> {
  const crawlRun = await input.repository.createCrawlRun(input.search._id, input.now.toISOString());

  try {
    const providerResults = await Promise.allSettled(
      input.providers.map((provider) =>
        provider.crawl({
          fetchImpl: input.fetchImpl,
          now: input.now,
          filters: input.search.filters,
        }),
      ),
    );

    const sourceResults: CrawlSourceResult[] = [];
    const matchedSeeds: Array<{ provider: CrawlSourceResult["provider"]; seed: NormalizedJobSeed }> = [];
    let totalFetchedJobs = 0;
    let totalMatchedJobs = 0;
    const selectedExperienceLevels = input.search.filters.experienceLevels;

    for (let index = 0; index < providerResults.length; index += 1) {
      const provider = input.providers[index];
      const startedAt = input.now.toISOString();
      const finishedAt = new Date().toISOString();
      const result = providerResults[index];

      if (result.status === "rejected") {
        sourceResults.push(
          crawlSourceResultSchema.parse({
            _id: createId(),
            crawlRunId: crawlRun._id,
            searchId: input.search._id,
            provider: provider.provider,
            status: "failed",
            fetchedCount: 0,
            matchedCount: 0,
            savedCount: 0,
            errorMessage:
              result.reason instanceof Error
                ? result.reason.message
                : `Provider ${provider.provider} failed unexpectedly.`,
            startedAt,
            finishedAt,
          }),
        );
        continue;
      }

      totalFetchedJobs += result.value.fetchedCount;
      const titleAndLocationMatchedSeeds = result.value.jobs.filter((job) =>
        matchesFiltersWithoutExperience(job, input.search.filters),
      );
      const seedsWithResolvedExperience = selectedExperienceLevels?.length
        ? await enrichSeedsForExperienceFilter(
            titleAndLocationMatchedSeeds,
            input.fetchImpl,
          )
        : titleAndLocationMatchedSeeds;
      const filteredSeeds = seedsWithResolvedExperience.filter((job) =>
        matchesFilters(job, input.search.filters),
      );
      totalMatchedJobs += filteredSeeds.length;

      filteredSeeds.forEach((seed) => {
        matchedSeeds.push({ provider: provider.provider, seed });
      });

      sourceResults.push(
        crawlSourceResultSchema.parse({
          _id: createId(),
          crawlRunId: crawlRun._id,
          searchId: input.search._id,
          provider: result.value.provider,
          status: result.value.status,
          fetchedCount: result.value.fetchedCount,
          matchedCount: filteredSeeds.length,
          savedCount: 0,
          errorMessage: result.value.errorMessage,
          startedAt,
          finishedAt,
        }),
      );
    }

    const hydratedJobs = await hydrateJobs(
      matchedSeeds.map((entry) => entry.seed),
      input.repository,
      input.fetchImpl,
      input.now,
    );

    const dedupedJobs = sortJobs(dedupeJobs(hydratedJobs), input.search.filters.title);
    const savedJobs = await input.repository.persistJobs(crawlRun._id, dedupedJobs);

    for (const sourceResult of sourceResults) {
      const savedCount = savedJobs.filter((job) =>
        job.sourceProvenance.some((record) => record.sourcePlatform === sourceResult.provider),
      ).length;
      sourceResult.savedCount = savedCount;
    }

    await input.repository.saveCrawlSourceResults(sourceResults);

    for (let index = 0; index < savedJobs.length; index += 1) {
      const validation = dedupedJobs[index]?.lastValidatedAt
        ? buildValidationFromJob(savedJobs[index], dedupedJobs[index])
        : null;

      if (validation) {
        await input.repository.saveLinkValidation(validation);
      }
    }

    const status = deriveRunStatus(sourceResults);

    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status,
      totalFetchedJobs,
      totalMatchedJobs,
      dedupedJobs: savedJobs.length,
      finishedAt: new Date().toISOString(),
    });

    await input.repository.updateSearchLatestRun(
      input.search._id,
      crawlRun._id,
      status,
      new Date().toISOString(),
    );

    const finalizedRun = (await input.repository.getCrawlRun(crawlRun._id)) as CrawlRun;

    return crawlResponseSchema.parse({
      search: {
        ...input.search,
        latestCrawlRunId: crawlRun._id,
        lastStatus: status,
        updatedAt: new Date().toISOString(),
      },
      crawlRun: finalizedRun,
      sourceResults,
      jobs: savedJobs,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Crawl failed unexpectedly.";
    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status: "failed",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      errorMessage: message,
      finishedAt: new Date().toISOString(),
    });
    await input.repository.updateSearchLatestRun(
      input.search._id,
      crawlRun._id,
      "failed",
      new Date().toISOString(),
    );
    throw error;
  }
}

async function hydrateJobs(
  seeds: NormalizedJobSeed[],
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const checkedAfter = new Date(
    now.getTime() - getEnv().LINK_VALIDATION_TTL_MINUTES * 60_000,
  ).toISOString();

  return runWithConcurrency(
    seeds,
    async (seed) => {
      const cachedValidation = await repository.getFreshValidation(seed.applyUrl, checkedAfter);
      const validationDraft: LinkValidationDraft = cachedValidation
        ? {
            applyUrl: seed.applyUrl,
            resolvedUrl: cachedValidation.resolvedUrl,
            canonicalUrl: cachedValidation.canonicalUrl ?? seed.canonicalUrl,
            status: cachedValidation.status,
            method: "CACHE",
            httpStatus: cachedValidation.httpStatus,
            checkedAt: now.toISOString(),
            errorMessage: cachedValidation.errorMessage,
            staleMarkers: cachedValidation.staleMarkers,
          }
        : await validateJobLink(seed.applyUrl, fetchImpl, now);

      return seedToPersistableJob(seed, validationDraft);
    },
    5,
  );
}

async function enrichSeedsForExperienceFilter(
  seeds: NormalizedJobSeed[],
  fetchImpl: typeof fetch,
) {
  return runWithConcurrency(
    seeds,
    async (seed) => {
      if (resolveJobExperienceLevel(seed)) {
        return seed;
      }

      const sourcePageExperiencePrompt = await fetchExperiencePromptFromSeed(seed, fetchImpl);
      if (!sourcePageExperiencePrompt) {
        return seed;
      }

      const rawSourceMetadata = {
        ...seed.rawSourceMetadata,
        sourcePageExperiencePrompt,
      };
      const experienceLevel = resolveJobExperienceLevel({
        ...seed,
        rawSourceMetadata,
      });

      return {
        ...seed,
        rawSourceMetadata,
        experienceLevel,
      };
    },
    4,
  );
}

async function fetchExperiencePromptFromSeed(
  seed: NormalizedJobSeed,
  fetchImpl: typeof fetch,
) {
  const urls = Array.from(new Set([seed.sourceUrl, seed.applyUrl].filter(Boolean)));

  for (const url of urls) {
    try {
      const response = await fetchImpl(url, {
        method: "GET",
        redirect: "follow",
        cache: "no-store",
        headers: {
          Accept: "text/html,application/xhtml+xml",
        },
      });

      if (!response.ok) {
        continue;
      }

      const body = await safeReadResponseText(response);
      const prompt = buildExperienceInferencePrompt(body);
      if (prompt) {
        return prompt;
      }
    } catch {
      continue;
    }
  }

  return undefined;
}

async function safeReadResponseText(response: Response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

function seedToPersistableJob(seed: NormalizedJobSeed, validation: LinkValidationDraft): PersistableJob {
  const locationNormalized = normalizeComparableText(
    `${seed.city ?? ""} ${seed.state ?? ""} ${seed.country ?? ""} ${seed.locationText}`,
  );

  return {
    title: seed.title,
    company: seed.company,
    country: seed.country,
    state: seed.state,
    city: seed.city,
    locationText: seed.locationText,
    experienceLevel: resolveJobExperienceLevel(seed),
    sourcePlatform: seed.sourcePlatform,
    sourceJobId: seed.sourceJobId,
    sourceUrl: seed.sourceUrl,
    applyUrl: seed.applyUrl,
    resolvedUrl: validation.resolvedUrl,
    canonicalUrl: validation.canonicalUrl ?? seed.canonicalUrl,
    postedAt: seed.postedAt,
    discoveredAt: seed.discoveredAt,
    linkStatus: validation.status,
    lastValidatedAt: validation.checkedAt,
    rawSourceMetadata: seed.rawSourceMetadata,
    sourceProvenance: [
      {
        sourcePlatform: seed.sourcePlatform,
        sourceJobId: seed.sourceJobId,
        sourceUrl: seed.sourceUrl,
        applyUrl: seed.applyUrl,
        resolvedUrl: validation.resolvedUrl,
        canonicalUrl: validation.canonicalUrl ?? seed.canonicalUrl,
        discoveredAt: seed.discoveredAt,
        rawSourceMetadata: seed.rawSourceMetadata,
      },
    ],
    sourceLookupKeys: [buildSourceLookupKey(seed.sourcePlatform, seed.sourceJobId)],
    companyNormalized: normalizeComparableText(seed.company),
    titleNormalized: normalizeComparableText(seed.title),
    locationNormalized,
    contentFingerprint: buildContentFingerprint({
      company: seed.company,
      title: seed.title,
      location: locationNormalized,
    }),
  };
}

async function refreshStaleJobs(
  jobs: JobListing[],
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const ttl = getEnv().LINK_VALIDATION_TTL_MINUTES;

  return runWithConcurrency(
    jobs,
    async (job) => {
      if (!isValidationStale(job.lastValidatedAt, ttl, now)) {
        return job;
      }

      const validation = await validateJobLink(job.applyUrl, fetchImpl, now);
      const refreshed: PersistableJob = {
        ...job,
        experienceLevel: resolveJobExperienceLevel(job),
        resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
        canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
        linkStatus: validation.status,
        lastValidatedAt: validation.checkedAt,
      };

      const latestRunId = job.crawlRunIds[job.crawlRunIds.length - 1] ?? job.crawlRunIds[0];
      const [savedJob] = await repository.persistJobs(latestRunId, [refreshed]);
      await repository.saveLinkValidation(toStoredValidation(savedJob._id, validation));
      return savedJob;
    },
    3,
  );
}

function buildValidationFromJob(job: JobListing, hydrated: PersistableJob): LinkValidationResult | null {
  if (!hydrated.lastValidatedAt) {
    return null;
  }

  return {
    _id: createId(),
    jobId: job._id,
    applyUrl: hydrated.applyUrl,
    resolvedUrl: hydrated.resolvedUrl,
    canonicalUrl: hydrated.canonicalUrl,
    status: hydrated.linkStatus,
    method: "CACHE",
    checkedAt: hydrated.lastValidatedAt,
    errorMessage: hydrated.linkStatus === "invalid" ? "Link validation marked this URL invalid." : undefined,
  };
}

function applyResolvedExperienceLevel(job: JobListing) {
  const experienceLevel = resolveJobExperienceLevel(job);
  return experienceLevel === job.experienceLevel
    ? job
    : {
        ...job,
        experienceLevel,
      };
}

function deriveRunStatus(sourceResults: CrawlSourceResult[]): CrawlRunStatus {
  const hasFailures = sourceResults.some((result) => result.status === "failed");
  const hasPartials = sourceResults.some((result) => result.status === "partial");
  const hasSupported = sourceResults.some(
    (result) => result.status === "success" || result.status === "partial",
  );

  if (hasFailures && !hasSupported) {
    return "failed";
  }

  if (hasFailures || hasPartials) {
    return "partial";
  }

  return "completed";
}

async function resolveRepository(repository?: JobCrawlerRepository) {
  return repository ?? getRepository();
}
