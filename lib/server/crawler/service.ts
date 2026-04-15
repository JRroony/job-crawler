import "server-only";

import { ZodError } from "zod";

import {
  applyResolvedExperienceLevel,
  defaultCrawlLinkValidationMode,
  executeCrawlPipeline,
  refreshStaleJobs,
  type CrawlLinkValidationMode,
} from "@/lib/server/crawler/pipeline";
import {
  abortOwnerSearchRun,
  abortSearchRun,
  isSearchRunPending,
  queueSearchRun,
} from "@/lib/server/crawler/background-runs";
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
import { sortJobsWithDiagnostics } from "@/lib/server/crawler/sort";
import { getRepository, JobCrawlerRepository } from "@/lib/server/db/repository";
import {
  createDiscoveryService,
  refreshSourceInventory,
} from "@/lib/server/discovery/service";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import { getEnv } from "@/lib/server/env";
import { createDefaultProviders } from "@/lib/server/providers";
import type { CrawlProvider } from "@/lib/server/providers/types";
import {
  crawlDeltaResponseSchema,
  crawlResponseSchema,
  searchFiltersSchema,
  type CrawlRun,
  type JobListing,
  type SearchDocument,
} from "@/lib/types";

export class ResourceNotFoundError extends Error {}
export class CrawlAbortedError extends Error {
  constructor(message = "The crawl was aborted.") {
    super(message);
    this.name = "CrawlAbortedError";
  }
}
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
  providerTimeoutMs?: number;
  progressUpdateIntervalMs?: number;
  earlyVisibleTarget?: number;
  initialVisibleWaitMs?: number;
  requestOwnerKey?: string;
  signal?: AbortSignal;
};

const initialVisiblePollIntervalMs = 75;

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
  const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
  });
  await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now.toISOString());

  return executeCrawl({
    search,
    crawlRun,
    repository,
    discovery: runtime.discovery ?? createDiscoveryService({ repository }),
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now,
    deepExperienceInference: runtime.deepExperienceInference ?? false,
    linkValidationMode: runtime.linkValidationMode,
    inlineValidationTopN: runtime.inlineValidationTopN,
    providerTimeoutMs: runtime.providerTimeoutMs,
    progressUpdateIntervalMs: runtime.progressUpdateIntervalMs,
    signal: runtime.signal,
  });
}

export async function rerunSearch(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  const now = runtime.now ?? new Date();
  const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
  });
  await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now.toISOString());

  return executeCrawl({
    search,
    crawlRun,
    repository,
    discovery: runtime.discovery ?? createDiscoveryService({ repository }),
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now,
    deepExperienceInference: runtime.deepExperienceInference ?? false,
    linkValidationMode: runtime.linkValidationMode,
    inlineValidationTopN: runtime.inlineValidationTopN,
    providerTimeoutMs: runtime.providerTimeoutMs,
    progressUpdateIntervalMs: runtime.progressUpdateIntervalMs,
    signal: runtime.signal,
  });
}

export async function startSearchFromFilters(
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
  const requestOwnerKey = normalizeRequestOwnerKey(runtime.requestOwnerKey);
  if (requestOwnerKey) {
    void abortOwnerSearchRun(requestOwnerKey, {
      reason: "The crawl was superseded by a newer search request.",
    });
  }
  const now = runtime.now ?? new Date();
  const search = await repository.createSearch(filters, now.toISOString());
  const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
  });
  await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now.toISOString());

  const queued =
    queueSearchRun(search._id, async (signal) => {
      await executeCrawl({
        search,
        crawlRun,
        repository,
        discovery: runtime.discovery ?? createDiscoveryService({ repository }),
        providers: runtime.providers ?? createDefaultProviders(),
        fetchImpl: runtime.fetchImpl ?? fetch,
        now,
        deepExperienceInference: runtime.deepExperienceInference ?? false,
        linkValidationMode: runtime.linkValidationMode,
        inlineValidationTopN: runtime.inlineValidationTopN,
        providerTimeoutMs: runtime.providerTimeoutMs,
        progressUpdateIntervalMs: runtime.progressUpdateIntervalMs,
        signal,
      });
    }, {
      ownerKey: requestOwnerKey,
      crawlRunId: crawlRun._id,
    }) || isSearchRunPending(search._id);

  // If the client disconnects before the background crawl finishes, abort it
  if (runtime.signal) {
    const onAbort = () => {
      void requestSearchCancellation(search._id, repository, {
        reason: "The client disconnected before the crawl completed.",
      });
    };
    runtime.signal.addEventListener("abort", onAbort, { once: true });
  }

  return {
    queued,
    result: await getInitialSearchResult(search._id, crawlRun._id, {
      repository,
      fetchImpl: runtime.fetchImpl ?? fetch,
      now,
      earlyVisibleTarget: runtime.earlyVisibleTarget,
      initialVisibleWaitMs: runtime.initialVisibleWaitMs,
      signal: runtime.signal,
    }),
  };
}

export async function startSearchRerun(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  const requestOwnerKey = normalizeRequestOwnerKey(runtime.requestOwnerKey);
  if (requestOwnerKey) {
    await abortOwnerSearchRun(requestOwnerKey, {
      reason: "The crawl was superseded by a newer search request.",
      awaitCompletion: true,
    });
  } else {
    await requestSearchCancellation(search._id, repository, {
      reason: "The crawl was superseded by a rerun request.",
      awaitCompletion: true,
    });
  }

  const now = runtime.now ?? new Date();
  const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
  });
  await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now.toISOString());
  const queued =
    queueSearchRun(search._id, async (signal) => {
      await executeCrawl({
        search,
        crawlRun,
        repository,
        discovery: runtime.discovery ?? createDiscoveryService({ repository }),
        providers: runtime.providers ?? createDefaultProviders(),
        fetchImpl: runtime.fetchImpl ?? fetch,
        now,
        deepExperienceInference: runtime.deepExperienceInference ?? false,
        linkValidationMode: runtime.linkValidationMode,
        inlineValidationTopN: runtime.inlineValidationTopN,
        providerTimeoutMs: runtime.providerTimeoutMs,
        progressUpdateIntervalMs: runtime.progressUpdateIntervalMs,
        signal,
      });
    }, {
      ownerKey: requestOwnerKey,
      crawlRunId: crawlRun._id,
    }) || isSearchRunPending(search._id);

  return {
    queued,
    result: await getInitialSearchResult(searchId, crawlRun._id, {
      repository,
      fetchImpl: runtime.fetchImpl ?? fetch,
      now,
      earlyVisibleTarget: runtime.earlyVisibleTarget,
      initialVisibleWaitMs: runtime.initialVisibleWaitMs,
      signal: runtime.signal,
    }),
  };
}

export async function getSearchDetails(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchRun(searchId, repository);

  if (!resolved.crawlRun) {
    return buildSyntheticCrawlResponse(
      resolved.search,
      isSearchRunPending(searchId) ? "running" : undefined,
    );
  }

  let jobs = await repository.getJobsByCrawlRun(resolved.crawlRun._id);
  if (resolved.crawlRun.status !== "running") {
    jobs = await refreshStaleJobs(jobs, repository, runtime.fetchImpl ?? fetch, runtime.now ?? new Date());
  }
  const sourceResults = await repository.getCrawlSourceResults(resolved.crawlRun._id);
  const deliveryCursor = await repository.getCrawlRunDeliveryCursor(resolved.crawlRun._id);

  return crawlResponseSchema.parse({
    search: resolved.search,
    crawlRun: resolved.crawlRun,
    sourceResults,
    jobs: sortJobsWithDiagnostics(
      jobs.map(applyResolvedExperienceLevel),
      resolved.search.filters.title,
      runtime.now ?? new Date(),
    ),
    diagnostics: resolved.crawlRun.diagnostics,
    delivery: {
      mode: "full",
      cursor: deliveryCursor,
    },
  });
}

export async function getSearchJobDeltas(
  searchId: string,
  afterCursor: number,
  runtime: Runtime = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchRun(searchId, repository);

  if (!resolved.crawlRun) {
    return crawlDeltaResponseSchema.parse({
      ...buildSyntheticCrawlResponse(
        resolved.search,
        isSearchRunPending(searchId) ? "running" : undefined,
      ),
      delivery: {
        mode: "delta",
        previousCursor: afterCursor,
        cursor: 0,
      },
    });
  }

  const [sourceResults, jobDelta] = await Promise.all([
    repository.getCrawlSourceResults(resolved.crawlRun._id),
    repository.getJobsByCrawlRunAfterSequence(resolved.crawlRun._id, afterCursor),
  ]);

  return crawlDeltaResponseSchema.parse({
    search: resolved.search,
    crawlRun: resolved.crawlRun,
    sourceResults,
    jobs: sortJobsWithDiagnostics(
      jobDelta.jobs.map(applyResolvedExperienceLevel),
      resolved.search.filters.title,
      runtime.now ?? new Date(),
    ),
    diagnostics: resolved.crawlRun.diagnostics,
    delivery: {
      mode: "delta",
      previousCursor: afterCursor,
      cursor: jobDelta.cursor,
    },
  });
}

export async function abortSearch(searchId: string, runtime: Runtime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  const aborted = await requestSearchCancellation(searchId, repository, {
    reason: "The crawl was stopped by the user.",
    awaitCompletion: true,
  });

  return {
    aborted,
    result: await getSearchDetails(searchId, runtime),
  };
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
  crawlRun?: CrawlRun;
  repository: Parameters<typeof executeCrawlPipeline>[0]["repository"];
  discovery: Parameters<typeof executeCrawlPipeline>[0]["discovery"];
  providers: Parameters<typeof executeCrawlPipeline>[0]["providers"];
  fetchImpl: Parameters<typeof executeCrawlPipeline>[0]["fetchImpl"];
  now: Parameters<typeof executeCrawlPipeline>[0]["now"];
  deepExperienceInference?: Parameters<typeof executeCrawlPipeline>[0]["deepExperienceInference"];
  linkValidationMode?: Parameters<typeof executeCrawlPipeline>[0]["linkValidationMode"];
  inlineValidationTopN?: Parameters<typeof executeCrawlPipeline>[0]["inlineValidationTopN"];
  providerTimeoutMs?: Parameters<typeof executeCrawlPipeline>[0]["providerTimeoutMs"];
  progressUpdateIntervalMs?: Parameters<typeof executeCrawlPipeline>[0]["progressUpdateIntervalMs"];
  requestOwnerKey?: string;
  signal?: AbortSignal;
};

async function executeCrawl(input: ExecuteCrawlInput) {
  return executeCrawlPipeline(input);
}

async function requestSearchCancellation(
  searchId: string,
  repository: JobCrawlerRepository,
  options: {
    reason: string;
    awaitCompletion?: boolean;
  },
) {
  const search = await repository.getSearch(searchId);
  if (!search?.latestCrawlRunId) {
    return false;
  }

  const runState = await repository.getCrawlRun(search.latestCrawlRunId);
  const shouldRequestPersistentCancel = runState?.status === "running" && !runState.finishedAt;

  if (shouldRequestPersistentCancel) {
    await repository.requestCrawlRunCancellation(search.latestCrawlRunId, {
      reason: options.reason,
    });
  }

  const abortedInMemory = await abortSearchRun(searchId, {
    reason: options.reason,
    awaitCompletion: options.awaitCompletion,
  });

  return shouldRequestPersistentCancel || abortedInMemory;
}

async function getInitialSearchResult(
  searchId: string,
  crawlRunId: string,
  runtime: Pick<
    Runtime,
    "repository" | "fetchImpl" | "now" | "earlyVisibleTarget" | "initialVisibleWaitMs" | "signal"
  >,
) {
  const repository = await resolveRepository(runtime.repository);
  const env = getEnv();
  const earlyTarget = Math.max(1, Math.floor(runtime.earlyVisibleTarget ?? env.CRAWL_EARLY_VISIBLE_TARGET));
  const maxWaitMs = Math.max(
    0,
    Math.floor(runtime.initialVisibleWaitMs ?? env.CRAWL_INITIAL_VISIBLE_WAIT_MS),
  );
  const deadline = Date.now() + maxWaitMs;

  while (true) {
    throwIfClientAborted(runtime.signal);

    const [crawlRun, deliveryCursor] = await Promise.all([
      repository.getCrawlRun(crawlRunId),
      repository.getCrawlRunDeliveryCursor(crawlRunId),
    ]);

    if (!crawlRun) {
      break;
    }

    const runFinished = crawlRun.status !== "running" || Boolean(crawlRun.finishedAt);
    if (deliveryCursor >= earlyTarget || runFinished || Date.now() >= deadline) {
      break;
    }

    await delay(initialVisiblePollIntervalMs, runtime.signal);
  }

  return getSearchDetails(searchId, {
    repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now,
  });
}

export async function refreshPersistentSourceInventory(runtime: {
  repository?: JobCrawlerRepository;
  now?: Date;
} = {}) {
  const repository = await resolveRepository(runtime.repository);
  return refreshSourceInventory({
    repository,
    now: runtime.now ?? new Date(),
  });
}

async function resolveRepository(repository?: JobCrawlerRepository) {
  return repository ?? getRepository();
}

async function loadSearchRun(
  searchId: string,
  repository: JobCrawlerRepository,
) {
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  if (!search.latestCrawlRunId) {
    return {
      search,
      crawlRun: null,
    };
  }

  const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId);
  if (!crawlRun) {
    throw new ResourceNotFoundError(
      `Latest crawl run ${search.latestCrawlRunId} for search ${searchId} was not found.`,
    );
  }

  return {
    search,
    crawlRun,
  };
}

function buildSyntheticCrawlResponse(
  search: SearchDocument,
  status?: "running" | "aborted",
) {
  const crawlStatus = status ?? search.lastStatus ?? "completed";
  const diagnostics = {
    discoveredSources: 0,
    crawledSources: 0,
    providerFailures: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    dedupedOut: 0,
    validationDeferred: 0,
  };

  return crawlResponseSchema.parse({
    search: {
      ...search,
      ...(status
        ? {
            lastStatus: status,
          }
        : {}),
    },
    crawlRun: {
      _id: createId(),
      searchId: search._id,
      startedAt: search.createdAt,
      finishedAt: status ? undefined : search.updatedAt,
      status: crawlStatus,
      discoveredSourcesCount: 0,
      crawledSourcesCount: 0,
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      validationMode: defaultCrawlLinkValidationMode,
      providerSummary: [],
      diagnostics,
    },
    sourceResults: [],
    jobs: [],
    diagnostics,
    delivery: {
      mode: "full",
      cursor: 0,
    },
  });
}

function throwIfClientAborted(signal?: AbortSignal) {
  if (!signal?.aborted) {
    return;
  }

  const error =
    signal.reason instanceof Error
      ? signal.reason
      : new Error("The request was aborted before the initial visible result batch was ready.");
  error.name = "AbortError";
  throw error;
}

async function delay(ms: number, signal?: AbortSignal) {
  if (ms <= 0) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      reject(
        signal?.reason instanceof Error
          ? signal.reason
          : Object.assign(new Error("The request was aborted."), { name: "AbortError" }),
      );
    };

    if (signal?.aborted) {
      onAbort();
      return;
    }

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function normalizeRequestOwnerKey(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
