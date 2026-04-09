import "server-only";

import { dedupeJobs } from "@/lib/server/crawler/dedupe";
import {
  buildContentFingerprint,
  buildSourceLookupKey,
  createId,
  evaluateSearchFilters,
  isValidationStale,
  normalizeComparableText,
  resolveJobExperienceClassification,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  toStoredValidation,
  validateJobLink,
  type LinkValidationDraft,
} from "@/lib/server/crawler/link-validation";
import { sortJobs } from "@/lib/server/crawler/sort";
import { getEnv } from "@/lib/server/env";
import type { JobCrawlerRepository, PersistableJob } from "@/lib/server/db/repository";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import {
  activeCrawlerPlatforms,
  crawlResponseSchema,
  crawlSourceResultSchema,
  resolveOperationalCrawlerPlatforms,
  searchFiltersSchema,
  type ActiveCrawlerPlatform,
  type CrawlerPlatform,
  type CrawlProviderSummary,
  type CrawlResponse,
  type CrawlDiagnostics,
  type CrawlMode,
  type CrawlRun,
  type CrawlRunStatus,
  type CrawlSourceResult,
  type CrawlValidationMode,
  type JobListing,
  type SearchDocument,
} from "@/lib/types";

export type CrawlLinkValidationMode = CrawlValidationMode;

export const defaultCrawlLinkValidationMode: CrawlLinkValidationMode = "deferred";

const defaultInlineValidationTopN = 0;
const defaultBalancedInlineValidationTopN = 5;

type ExecuteCrawlPipelineInput = {
  search: SearchDocument;
  repository: JobCrawlerRepository;
  discovery: DiscoveryService;
  providers: CrawlProvider[];
  fetchImpl: typeof fetch;
  now: Date;
  deepExperienceInference?: boolean;
  linkValidationMode?: CrawlLinkValidationMode;
  inlineValidationTopN?: number;
};

export async function executeCrawlPipeline(
  input: ExecuteCrawlPipelineInput,
): Promise<CrawlResponse> {
  const normalizedFilters = searchFiltersSchema.parse(input.search.filters);
  const selectedProviders = selectProvidersForSearch(
    input.providers,
    normalizedFilters.platforms,
  );
  const search = {
    ...input.search,
    filters: normalizedFilters,
  };
  const validationStrategy = resolveValidationStrategy(
    input.linkValidationMode,
    input.inlineValidationTopN,
    normalizedFilters.crawlMode,
  );
  const crawlRun = await input.repository.createCrawlRun(search._id, input.now.toISOString(), {
    validationMode: validationStrategy.mode,
  });

  try {
    // Boundary: discovery is the only stage that knows where candidate sources come from.
    const discoveredSources = await input.discovery.discover({
      filters: normalizedFilters,
      now: input.now,
      fetchImpl: input.fetchImpl,
    });
    const providerSources = selectedProviders.map((provider) =>
      discoveredSources.filter((source) => provider.supportsSource(source)),
    );
    const providerRouting = selectedProviders.map((provider, index) => ({
      provider: provider.provider,
      sourceCount: providerSources[index].length,
    }));

    console.info("[crawl:provider-routing]", {
      searchId: search._id,
      normalizedFilters,
      discoveredSourceCount: discoveredSources.length,
      discoveredPlatformCounts: discoveredSources.reduce<Record<string, number>>((counts, source) => {
        counts[source.platform] = (counts[source.platform] ?? 0) + 1;
        return counts;
      }, {}),
      selectedProviders: selectedProviders.map((provider) => provider.provider),
      providerRouting,
    });

    if (discoveredSources.length === 0) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Discovery returned zero runnable sources before provider routing began.",
      });
    } else if (providerRouting.every((entry) => entry.sourceCount === 0)) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Sources were discovered, but no provider accepted them.",
      });
    }

    const diagnostics = createEmptyDiagnostics({
      discoveredSources: discoveredSources.length,
      crawledSources: providerSources.reduce((total, sources) => total + sources.length, 0),
    });

    // Boundary: providers only extract from already classified sources.
    const providerResults = await Promise.allSettled(
      selectedProviders.map((provider, index) =>
        provider.crawlSources(
          {
            fetchImpl: input.fetchImpl,
            now: input.now,
            filters: normalizedFilters,
          },
          providerSources[index],
        ),
      ),
    );

    const sourceResults: CrawlSourceResult[] = [];
    const matchedSeeds: Array<{ provider: CrawlSourceResult["provider"]; seed: NormalizedJobSeed }> = [];
    let totalFetchedJobs = 0;
    let totalMatchedJobs = 0;

    for (let index = 0; index < providerResults.length; index += 1) {
      const provider = selectedProviders[index];
      const sourcesForProvider = providerSources[index];
      const startedAt = input.now.toISOString();
      const finishedAt = new Date().toISOString();
      const result = providerResults[index];

      if (result.status === "rejected") {
        diagnostics.providerFailures += 1;
        sourceResults.push(
          crawlSourceResultSchema.parse({
            _id: createId(),
            crawlRunId: crawlRun._id,
            searchId: search._id,
            provider: provider.provider,
            status: "failed",
            sourceCount: sourcesForProvider.length,
            fetchedCount: 0,
            matchedCount: 0,
            savedCount: 0,
            warningCount: 1,
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
      diagnostics.excludedByTitle += result.value.excludedByTitle ?? 0;
      diagnostics.excludedByLocation += result.value.excludedByLocation ?? 0;
      if (result.value.status === "failed" || result.value.status === "partial") {
        diagnostics.providerFailures += 1;
      }

      const filteredSeeds = filterSeedsForSearch(result.value.jobs, normalizedFilters, {
        deepExperienceInference: input.deepExperienceInference ?? false,
      });
      // Counters are accumulated at the first stage that excludes a seed so title,
      // location, and experience buckets stay mutually exclusive and easy to explain.
      diagnostics.excludedByTitle += filteredSeeds.excludedByTitle;
      diagnostics.excludedByLocation += filteredSeeds.excludedByLocation;
      diagnostics.excludedByExperience += filteredSeeds.excludedByExperience;
      totalMatchedJobs += filteredSeeds.jobs.length;

      filteredSeeds.jobs.forEach((seed) => {
        matchedSeeds.push({ provider: provider.provider, seed });
      });

      sourceResults.push(
        crawlSourceResultSchema.parse({
          _id: createId(),
            crawlRunId: crawlRun._id,
            searchId: search._id,
            provider: result.value.provider,
            status: result.value.status,
            sourceCount: result.value.sourceCount ?? sourcesForProvider.length,
            fetchedCount: result.value.fetchedCount,
            matchedCount: filteredSeeds.jobs.length,
            savedCount: 0,
            warningCount: result.value.warningCount ?? 0,
            errorMessage: result.value.errorMessage,
            startedAt,
            finishedAt,
        }),
      );
    }

    // Boundary: hydration/dedupe/persistence work on one shared normalized model.
    //
    // Link validation is deferred by default because HEAD + GET + stale-page scanning
    // adds a lot of latency and network noise compared with normalizing and saving the
    // matched jobs themselves. We still support explicit inline validation strategies
    // for targeted use cases, but the standard crawl request keeps the hot path light.
    const hydratedJobs = hydrateJobs(
      matchedSeeds.map((entry) => entry.seed),
      input.now,
    );
    const dedupedJobs = sortJobs(dedupeJobs(hydratedJobs), normalizedFilters.title);
    // Dedupe happens after all provider/filter stages so this counter reflects true
    // overlap between otherwise matchable seeds, not filter drop-offs.
    diagnostics.dedupedOut = Math.max(0, hydratedJobs.length - dedupedJobs.length);
    const {
      jobs: jobsReadyToPersist,
      validations: inlineValidations,
      deferredCount,
    } = await applyInlineValidationStrategy(
      dedupedJobs,
      validationStrategy,
      input.repository,
      input.fetchImpl,
      input.now,
    );
    diagnostics.validationDeferred = deferredCount;
    const savedJobs = await input.repository.persistJobs(crawlRun._id, jobsReadyToPersist);

    console.info("[crawl:summary]", {
      searchId: search._id,
      fetchedCount: totalFetchedJobs,
      matchedCount: totalMatchedJobs,
      savedCount: savedJobs.length,
    });

    for (const sourceResult of sourceResults) {
      const savedCount = savedJobs.filter((job) =>
        job.sourceProvenance.some((record) => record.sourcePlatform === sourceResult.provider),
      ).length;
      sourceResult.savedCount = savedCount;
    }

    await input.repository.saveCrawlSourceResults(sourceResults);

    for (const inlineValidation of inlineValidations) {
      await input.repository.saveLinkValidation(
        toStoredValidation(
          savedJobs[inlineValidation.jobIndex]._id,
          inlineValidation.validation,
        ),
      );
    }

    const status = deriveRunStatus(sourceResults);
    const finishedAt = new Date().toISOString();

    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status,
      totalFetchedJobs,
      totalMatchedJobs,
      dedupedJobs: savedJobs.length,
      diagnostics,
      validationMode: validationStrategy.mode,
      providerSummary: buildProviderSummary(sourceResults),
      finishedAt,
    });
    await input.repository.updateSearchLatestRun(
      search._id,
      crawlRun._id,
      status,
      finishedAt,
    );

    const finalizedRun = (await input.repository.getCrawlRun(crawlRun._id)) as CrawlRun;

    return crawlResponseSchema.parse({
      search: {
        ...search,
        latestCrawlRunId: crawlRun._id,
        lastStatus: status,
        updatedAt: finishedAt,
      },
      crawlRun: finalizedRun,
      sourceResults,
      jobs: savedJobs,
      diagnostics: finalizedRun.diagnostics,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Crawl failed unexpectedly.";
    const finishedAt = new Date().toISOString();

    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status: "failed",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      diagnostics: createEmptyDiagnostics(),
      validationMode: validationStrategy.mode,
      errorMessage: message,
      finishedAt,
    });
    await input.repository.updateSearchLatestRun(
      search._id,
      crawlRun._id,
      "failed",
      finishedAt,
    );
    throw error;
  }
}

export async function refreshStaleJobs(
  jobs: JobListing[],
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const ttl = getEnv().LINK_VALIDATION_TTL_MINUTES;

  return runWithConcurrency(
    jobs,
    async (job) => {
      if (!job.lastValidatedAt) {
        // Deferred crawls intentionally leave fresh jobs unvalidated. Keep search loads
        // read-only for those records so validation happens only through a separate pass
        // such as manual revalidation or an explicit background job.
        return job;
      }

      if (!isValidationStale(job.lastValidatedAt, ttl, now)) {
        return job;
      }

      const validation = await validateJobLink(job.applyUrl, fetchImpl, now);
      const experienceClassification = resolveJobExperienceClassification(job);
      const refreshed: PersistableJob = {
        ...job,
        experienceLevel:
          experienceClassification.explicitLevel ??
          experienceClassification.inferredLevel,
        experienceClassification,
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

export function applyResolvedExperienceLevel(job: JobListing) {
  const experienceClassification = resolveJobExperienceClassification(job);
  const experienceLevel =
    experienceClassification.explicitLevel ??
    experienceClassification.inferredLevel;
  return (
    experienceLevel === job.experienceLevel &&
    sameExperienceClassification(job.experienceClassification, experienceClassification)
  )
    ? job
    : {
        ...job,
        experienceLevel,
        experienceClassification,
      };
}

function filterSeedsForSearch(
  seeds: NormalizedJobSeed[],
  filters: SearchDocument["filters"],
  options: { deepExperienceInference: boolean },
) {
  // Keep the request path fast: synchronous crawls only use provider payload fields,
  // title signals, and metadata already attached to the seed.
  //
  // If we ever reintroduce deep page-level inference, it should run behind an
  // explicit opt-in and ideally outside the main crawl request path.
  void options.deepExperienceInference;

  const matchedSeeds: NormalizedJobSeed[] = [];
  let excludedByTitle = 0;
  let excludedByLocation = 0;
  let excludedByExperience = 0;

  for (const seed of seeds) {
    const evaluation = evaluateSearchFilters(seed, filters, {
      includeExperience: true,
    });

    if (evaluation.matches) {
      matchedSeeds.push(seed);
      continue;
    }

    if (evaluation.reason === "title") {
      excludedByTitle += 1;
      continue;
    }

    if (evaluation.reason === "location") {
      excludedByLocation += 1;
      continue;
    }

    excludedByExperience += 1;
  }

  return {
    jobs: matchedSeeds,
    excludedByTitle,
    excludedByLocation,
    excludedByExperience,
  };
}

function hydrateJobs(
  seeds: NormalizedJobSeed[],
  now: Date,
) {
  return seeds.map((seed) => seedToPersistableJob(seed, now));
}

async function applyInlineValidationStrategy(
  jobs: PersistableJob[],
  strategy: ResolvedValidationStrategy,
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const jobsToPersist = [...jobs];
  const indexesToValidate = pickInlineValidationIndexes(jobsToPersist, strategy);

  if (indexesToValidate.length === 0) {
    return {
      jobs: jobsToPersist,
      validations: [] as InlineValidationResult[],
      deferredCount: jobsToPersist.length,
    };
  }

  const checkedAfter = new Date(
    now.getTime() - getEnv().LINK_VALIDATION_TTL_MINUTES * 60_000,
  ).toISOString();

  const validations = await runWithConcurrency(
    indexesToValidate,
    async (jobIndex) => {
      const job = jobsToPersist[jobIndex];
      const cachedValidation = await repository.getFreshValidation(job.applyUrl, checkedAfter);
      const validation = cachedValidation
        ? buildCachedValidationDraft(job.applyUrl, job.canonicalUrl, cachedValidation, now)
        : await validateJobLink(job.applyUrl, fetchImpl, now);

      jobsToPersist[jobIndex] = applyValidationDraft(job, validation);

      return {
        jobIndex,
        validation,
      };
    },
    5,
  );

  return {
    jobs: jobsToPersist,
    validations,
    deferredCount: Math.max(0, jobsToPersist.length - indexesToValidate.length),
  };
}

function seedToPersistableJob(seed: NormalizedJobSeed, now: Date): PersistableJob {
  const locationNormalized = normalizeComparableText(
    `${seed.city ?? ""} ${seed.state ?? ""} ${seed.country ?? ""} ${seed.locationText}`,
  );
  const experienceClassification = resolveJobExperienceClassification(seed);
  const discoveredAt = seed.discoveredAt || now.toISOString();

  return {
    title: seed.title,
    company: seed.company,
    country: seed.country,
    state: seed.state,
    city: seed.city,
    locationText: seed.locationText,
    experienceLevel:
      experienceClassification.explicitLevel ??
      experienceClassification.inferredLevel,
    experienceClassification,
    sourcePlatform: seed.sourcePlatform,
    sourceJobId: seed.sourceJobId,
    sourceUrl: seed.sourceUrl,
    applyUrl: seed.applyUrl,
    canonicalUrl: seed.canonicalUrl,
    postedAt: seed.postedAt,
    discoveredAt,
    linkStatus: "unknown",
    rawSourceMetadata: seed.rawSourceMetadata,
    sourceProvenance: [
      {
        sourcePlatform: seed.sourcePlatform,
        sourceJobId: seed.sourceJobId,
        sourceUrl: seed.sourceUrl,
        applyUrl: seed.applyUrl,
        canonicalUrl: seed.canonicalUrl,
        discoveredAt,
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

function applyValidationDraft(job: PersistableJob, validation: LinkValidationDraft): PersistableJob {
  return {
    ...job,
    resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
    canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
    linkStatus: validation.status,
    lastValidatedAt: validation.checkedAt,
    sourceProvenance: job.sourceProvenance.map((record) =>
      record.applyUrl === job.applyUrl
        ? {
            ...record,
            resolvedUrl: validation.resolvedUrl ?? record.resolvedUrl,
            canonicalUrl: validation.canonicalUrl ?? record.canonicalUrl,
          }
        : record,
    ),
  };
}

function buildCachedValidationDraft(
  applyUrl: string,
  canonicalUrl: string | undefined,
  cachedValidation: Awaited<ReturnType<JobCrawlerRepository["getFreshValidation"]>>,
  now: Date,
): LinkValidationDraft {
  if (!cachedValidation) {
    throw new Error("Cached validation is required to build a cached validation draft.");
  }

  return {
    applyUrl,
    resolvedUrl: cachedValidation.resolvedUrl,
    canonicalUrl: cachedValidation.canonicalUrl ?? canonicalUrl,
    status: cachedValidation.status,
    method: "CACHE",
    httpStatus: cachedValidation.httpStatus,
    checkedAt: now.toISOString(),
    errorMessage: cachedValidation.errorMessage,
    staleMarkers: cachedValidation.staleMarkers,
  };
}

function pickInlineValidationIndexes(
  jobs: PersistableJob[],
  strategy: ResolvedValidationStrategy,
) {
  if (strategy.mode === "deferred") {
    return [];
  }

  if (strategy.mode === "full_inline") {
    return jobs.map((_, index) => index);
  }

  if (strategy.inlineTopN <= 0) {
    return [];
  }

  return jobs
    .map((job, index) => ({
      index,
      recency: job.postedAt ?? job.discoveredAt,
    }))
    .sort(
      (left, right) =>
        compareRecency(left.recency, right.recency) ||
        left.index - right.index,
    )
    .slice(0, strategy.inlineTopN)
    .map((entry) => entry.index);
}

function compareRecency(left: string, right: string) {
  if (left === right) {
    return 0;
  }

  return left > right ? -1 : 1;
}

function resolveValidationStrategy(
  mode: CrawlLinkValidationMode | undefined,
  inlineTopN: number | undefined,
  crawlMode: CrawlMode | undefined,
): ResolvedValidationStrategy {
  if (mode) {
    return {
      mode,
      inlineTopN: Number.isFinite(inlineTopN)
        ? Math.max(0, Math.floor(inlineTopN ?? 0))
        : defaultInlineValidationTopN,
    };
  }

  if (crawlMode === "deep") {
    return {
      mode: "full_inline",
      inlineTopN: 0,
    };
  }

  if (crawlMode === "balanced") {
    return {
      mode: "inline_top_n",
      inlineTopN: defaultBalancedInlineValidationTopN,
    };
  }

  return {
    mode: defaultCrawlLinkValidationMode,
    inlineTopN: defaultInlineValidationTopN,
  };
}

function sameExperienceClassification(
  left: JobListing["experienceClassification"],
  right: JobListing["experienceClassification"],
) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
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

type ResolvedValidationStrategy = {
  mode: CrawlLinkValidationMode;
  inlineTopN: number;
};

type InlineValidationResult = {
  jobIndex: number;
  validation: LinkValidationDraft;
};

function createEmptyDiagnostics(
  overrides: Partial<CrawlDiagnostics> = {},
): CrawlDiagnostics {
  return {
    discoveredSources: 0,
    crawledSources: 0,
    providerFailures: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    dedupedOut: 0,
    validationDeferred: 0,
    ...overrides,
  };
}

function buildProviderSummary(
  sourceResults: CrawlSourceResult[],
): CrawlProviderSummary[] {
  return sourceResults.map((sourceResult) => ({
    provider: sourceResult.provider,
    status: sourceResult.status,
    sourceCount: sourceResult.sourceCount,
    fetchedCount: sourceResult.fetchedCount,
    matchedCount: sourceResult.matchedCount,
    savedCount: sourceResult.savedCount,
    warningCount: sourceResult.warningCount,
    errorMessage: sourceResult.errorMessage,
  }));
}

function selectProvidersForSearch(
  providers: CrawlProvider[],
  selectedPlatforms: CrawlerPlatform[] | undefined,
) {
  const allowedPlatforms = new Set(
    resolveOperationalCrawlerPlatforms(selectedPlatforms),
  );

  // Provider execution is limited to the implemented families that survived the
  // requested platform scope, so disabled platforms never reach crawlSources.
  return providers.filter(
    (provider) =>
      isOperationalProvider(provider.provider) && allowedPlatforms.has(provider.provider),
  );
}

function isOperationalProvider(
  provider: CrawlProvider["provider"],
): provider is ActiveCrawlerPlatform {
  return activeCrawlerPlatforms.includes(provider as ActiveCrawlerPlatform);
}
