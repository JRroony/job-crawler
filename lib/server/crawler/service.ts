import "server-only";

import { ZodError } from "zod";

import {
  applyResolvedExperienceLevel,
  defaultCrawlLinkValidationMode,
  executeCrawlPipeline,
  refreshStaleJobs,
  type CrawlLinkValidationMode,
} from "@/lib/server/crawler/pipeline";
import { dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { normalizeSearchIntentInput } from "@/lib/server/crawler/search-intent";
import {
  createId,
  resolveJobExperienceClassification,
  resolveJobExperienceLevel,
} from "@/lib/server/crawler/helpers";
import {
  toStoredValidation,
  validateJobLink,
} from "@/lib/server/crawler/link-validation";
import { sortJobs } from "@/lib/server/crawler/sort";
import { getRepository, JobCrawlerRepository } from "@/lib/server/db/repository";
import { defaultDiscoveryService } from "@/lib/server/discovery/service";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import { createDefaultProviders } from "@/lib/server/providers";
import type { CrawlProvider } from "@/lib/server/providers/types";
import {
  crawlResponseSchema,
  searchFiltersSchema,
  type JobListing,
} from "@/lib/types";

export class ResourceNotFoundError extends Error {}
export class InputValidationError extends Error {
  constructor(private readonly error: ZodError) {
    super("Invalid search filters.");
    this.name = "InputValidationError";
  }

  flatten() {
    return this.error.flatten();
  }
}

type Runtime = {
  repository?: JobCrawlerRepository;
  providers?: CrawlProvider[];
  discovery?: DiscoveryService;
  fetchImpl?: typeof fetch;
  now?: Date;
  // Reserved for future background or explicitly opt-in enrichment.
  // Standard crawl requests keep this disabled so experience filtering stays fast.
  deepExperienceInference?: boolean;
  // Default to deferred validation so crawling stays focused on fetch/filter/save work.
  // Inline validation remains available for narrow or explicitly opt-in workflows.
  linkValidationMode?: CrawlLinkValidationMode;
  inlineValidationTopN?: number;
};

export async function runSearchFromFilters(
  rawFilters: unknown,
  runtime: Runtime = {},
) {
  const normalizedInput = normalizeSearchIntentInput(rawFilters);
  const parsedFilters = searchFiltersSchema.safeParse(normalizedInput);

  if (!parsedFilters.success) {
    throw new InputValidationError(parsedFilters.error);
  }

  const filters = parsedFilters.data;
  console.info("[crawl:normalized-filters]", filters);
  const repository = await resolveRepository(runtime.repository);
  const now = runtime.now ?? new Date();
  const search = await repository.createSearch(filters, now.toISOString());

  return executeCrawl({
    search,
    repository,
    discovery: runtime.discovery ?? defaultDiscoveryService,
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now,
    deepExperienceInference: runtime.deepExperienceInference ?? false,
    linkValidationMode: runtime.linkValidationMode,
    inlineValidationTopN: runtime.inlineValidationTopN,
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
    discovery: runtime.discovery ?? defaultDiscoveryService,
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now ?? new Date(),
    deepExperienceInference: runtime.deepExperienceInference ?? false,
    linkValidationMode: runtime.linkValidationMode,
    inlineValidationTopN: runtime.inlineValidationTopN,
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
        discoveredSourcesCount: 0,
        crawledSourcesCount: 0,
        totalFetchedJobs: 0,
        totalMatchedJobs: 0,
        dedupedJobs: 0,
        validationMode: defaultCrawlLinkValidationMode,
        providerSummary: [],
        diagnostics: {
          discoveredSources: 0,
          crawledSources: 0,
          providerFailures: 0,
          excludedByTitle: 0,
          excludedByLocation: 0,
          excludedByExperience: 0,
          dedupedOut: 0,
          validationDeferred: 0,
        },
      },
      sourceResults: [],
      jobs: [],
      diagnostics: {
        discoveredSources: 0,
        crawledSources: 0,
        providerFailures: 0,
        excludedByTitle: 0,
        excludedByLocation: 0,
        excludedByExperience: 0,
        dedupedOut: 0,
        validationDeferred: 0,
      },
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
    jobs: sortJobs(
      dedupeStoredJobs(jobs).map(applyResolvedExperienceLevel),
      search.filters.title,
    ),
    diagnostics: crawlRun.diagnostics,
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
    experienceClassification: resolveJobExperienceClassification(job),
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

export function isInputValidationError(
  error: unknown,
): error is InputValidationError {
  return error instanceof InputValidationError;
}

type ExecuteCrawlInput = {
  search: Parameters<typeof executeCrawlPipeline>[0]["search"];
  repository: Parameters<typeof executeCrawlPipeline>[0]["repository"];
  discovery: Parameters<typeof executeCrawlPipeline>[0]["discovery"];
  providers: Parameters<typeof executeCrawlPipeline>[0]["providers"];
  fetchImpl: Parameters<typeof executeCrawlPipeline>[0]["fetchImpl"];
  now: Parameters<typeof executeCrawlPipeline>[0]["now"];
  deepExperienceInference?: Parameters<typeof executeCrawlPipeline>[0]["deepExperienceInference"];
  linkValidationMode?: Parameters<typeof executeCrawlPipeline>[0]["linkValidationMode"];
  inlineValidationTopN?: Parameters<typeof executeCrawlPipeline>[0]["inlineValidationTopN"];
};

async function executeCrawl(input: ExecuteCrawlInput) {
  return executeCrawlPipeline(input);
}

async function resolveRepository(repository?: JobCrawlerRepository) {
  return repository ?? getRepository();
}
