import "server-only";

import {
  applyResolvedExperienceLevel,
  defaultCrawlLinkValidationMode,
  refreshStaleJobs,
} from "@/lib/server/crawler/pipeline";
import { evaluateSearchFilters } from "@/lib/server/crawler/helpers";
import { abortOwnerSearchRun, abortSearchRun, isSearchRunPending } from "@/lib/server/crawler/background-runs";
import { createId } from "@/lib/server/crawler/helpers";
import { sortJobsWithDiagnostics } from "@/lib/server/crawler/sort";
import {
  getIndexedJobDeltasForSearch,
  getIndexedJobsForSearch,
  mergeSearchResultJobs,
} from "@/lib/server/search/indexed-jobs";
import { getEnv } from "@/lib/server/env";
import { resolveRepository, type JobCrawlerRuntime } from "@/lib/server/services/runtime";
import {
  crawlDiagnosticsSchema,
  crawlDeltaResponseSchema,
  crawlResponseSchema,
  type CrawlDiagnostics,
  type JobListing,
  type SearchDocument,
} from "@/lib/types";

import { ResourceNotFoundError } from "@/lib/server/search/errors";

const initialVisiblePollIntervalMs = 75;
const defaultSearchResultsPageSize = 50;
const maxSearchResultsPageSize = 100;

type SearchReuseBaseline = {
  reusedExistingSearch: boolean;
  previousVisibleJobCount: number;
  previousRunStatus?: SearchDocument["lastStatus"];
  previousFinishedAt?: string;
};

export type SearchPaginationOptions = {
  cursor?: number;
  pageSize?: number;
  searchSessionId?: string;
};

type SearchDetailsRuntime = JobCrawlerRuntime & SearchPaginationOptions;

type SearchPage = {
  cursor: number;
  pageSize: number;
  totalMatchedCount: number;
  returnedCount: number;
  nextCursor: number | null;
  hasMore: boolean;
  jobs: JobListing[];
};

export async function createSearchSession(
  filters: SearchDocument["filters"],
  runtime: JobCrawlerRuntime = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const now = runtime.now ?? new Date();
  const nowIso = now.toISOString();
  const resolvedSearch = await resolveSearchForNewSession(filters, repository, nowIso);
  const search = resolvedSearch.search;
  const searchReuse = await loadSearchReuseBaseline(search, repository, resolvedSearch.reusedExistingSearch);
  const searchSession = await repository.createSearchSession(search._id, nowIso, {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, nowIso, {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
    searchSessionId: searchSession._id,
  });
  await Promise.all([
    repository.updateSearchLatestSession(search._id, searchSession._id, "running", nowIso),
    repository.updateSearchLatestRun(search._id, crawlRun._id, "running", nowIso),
    repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: nowIso,
    }),
  ]);

  return {
    repository,
    now,
    search,
    searchReuse,
    searchSession,
    crawlRun,
  };
}

export async function createSearchRerunSession(
  searchId: string,
  runtime: JobCrawlerRuntime = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  const now = runtime.now ?? new Date();
  const nowIso = now.toISOString();
  const searchSession = await repository.createSearchSession(search._id, nowIso, {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, nowIso, {
    validationMode: runtime.linkValidationMode ?? defaultCrawlLinkValidationMode,
    stage: "queued",
    searchSessionId: searchSession._id,
  });
  await Promise.all([
    repository.updateSearchLatestSession(search._id, searchSession._id, "running", nowIso),
    repository.updateSearchLatestRun(search._id, crawlRun._id, "running", nowIso),
    repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: nowIso,
    }),
  ]);

  return {
    repository,
    now,
    search,
    searchReuse: {
      reusedExistingSearch: false,
      previousVisibleJobCount: 0,
      previousRunStatus: search.lastStatus,
    } satisfies SearchReuseBaseline,
    searchSession,
    crawlRun,
  };
}

export async function abortSupersededSearch(
  searchId: string,
  runtime: JobCrawlerRuntime = {},
  options: {
    defaultReason: string;
  },
) {
  const repository = await resolveRepository(runtime.repository);
  const requestOwnerKey = normalizeRequestOwnerKey(runtime.requestOwnerKey);

  if (requestOwnerKey) {
    await abortOwnerSearchRun(requestOwnerKey, repository, {
      reason: "The crawl was superseded by a newer search request.",
      awaitCompletion: true,
    });
    return;
  }

  await requestSearchCancellation(searchId, repository, {
    reason: options.defaultReason,
    awaitCompletion: true,
  });
}

export async function monitorSearchRequestAbort(
  searchId: string,
  runtime: Pick<JobCrawlerRuntime, "repository" | "signal"> = {},
) {
  if (!runtime.signal) {
    return;
  }

  const repository = await resolveRepository(runtime.repository);
  const onAbort = () => {
    void requestSearchCancellation(searchId, repository, {
      reason: "The client disconnected before the crawl completed.",
    });
  };

  runtime.signal.addEventListener("abort", onAbort, { once: true });
}

export async function getInitialSearchResult(
  searchId: string,
  searchSessionId: string,
  runtime: Pick<
    JobCrawlerRuntime,
    "repository" | "fetchImpl" | "now" | "earlyVisibleTarget" | "initialVisibleWaitMs" | "signal"
  > &
    SearchPaginationOptions = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const initialResult = await getSearchDetails(searchId, {
    repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now,
    cursor: runtime.cursor,
    pageSize: runtime.pageSize,
    searchSessionId: runtime.searchSessionId,
  });

  if (
    initialResult.jobs.length > 0 ||
    initialResult.crawlRun.status !== "running" ||
    Boolean(initialResult.crawlRun.finishedAt)
  ) {
    return initialResult;
  }

  const env = getEnv();
  const earlyTarget = Math.max(1, Math.floor(runtime.earlyVisibleTarget ?? env.CRAWL_EARLY_VISIBLE_TARGET));
  const maxWaitMs = Math.max(
    0,
    Math.floor(runtime.initialVisibleWaitMs ?? env.CRAWL_INITIAL_VISIBLE_WAIT_MS),
  );
  const deadline = Date.now() + maxWaitMs;

  while (true) {
    throwIfClientAborted(runtime.signal);

    const [searchSession, deliveryCursor] = await Promise.all([
      repository.getSearchSession(searchSessionId),
      repository.getSearchSessionDeliveryCursor(searchSessionId),
    ]);

    if (!searchSession) {
      break;
    }

    const runFinished = searchSession.status !== "running" || Boolean(searchSession.finishedAt);
    if (deliveryCursor >= earlyTarget || runFinished || Date.now() >= deadline) {
      break;
    }

    await delay(initialVisiblePollIntervalMs, runtime.signal);
  }

  return getSearchDetails(searchId, {
    repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now,
    cursor: runtime.cursor,
    pageSize: runtime.pageSize,
    searchSessionId: runtime.searchSessionId,
  });
}

export async function getSearchDetails(searchId: string, runtime: SearchDetailsRuntime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchState(searchId, repository);
  const indexedCursor = await repository.getIndexedJobDeliveryCursor();

  if (!resolved.crawlRun) {
    return buildSyntheticCrawlResponse(
      resolved.search,
      await loadIndexedSearchJobs(resolved.search, repository),
      (await isSearchRunPending(searchId, repository)) ? "running" : undefined,
      indexedCursor,
      runtime,
    );
  }

  const indexedJobs = await loadIndexedSearchJobs(resolved.search, repository);
  let jobs = resolved.searchSession
    ? await repository.getJobsBySearchSession(resolved.searchSession._id)
    : await repository.getJobsByCrawlRun(resolved.crawlRun._id);
  if (resolved.crawlRun.status !== "running") {
    jobs = await refreshStaleJobs(jobs, repository, runtime.fetchImpl ?? fetch, runtime.now ?? new Date());
  }
  const filteredSessionJobs = filterAndDecorateSearchJobs(
    jobs.map(applyResolvedExperienceLevel),
    resolved.search.filters,
    "supplemental_session",
  );
  const sourceResults = await repository.getCrawlSourceResults(resolved.crawlRun._id);
  const deliveryCursor = resolved.searchSession
    ? await repository.getSearchSessionDeliveryCursor(resolved.searchSession._id)
    : await repository.getCrawlRunDeliveryCursor(resolved.crawlRun._id);
  const mergedJobs = filterAndDecorateSearchJobs(
    mergeSearchResultJobs(indexedJobs, filteredSessionJobs),
    resolved.search.filters,
    "response_guard",
  );
  const sortedJobs = sortJobsWithDiagnostics(
    mergedJobs,
    resolved.search.filters.title,
    runtime.now ?? new Date(),
  );
  const page = paginateSearchResults(sortedJobs, runtime);
  const candidateCount =
    resolved.crawlRun.diagnostics.session?.indexedCandidateCount ?? indexedJobs.length;
  const diagnostics = attachSessionDiagnostics(
    resolved.crawlRun.diagnostics,
    indexedJobs.length,
    mergedJobs.length,
    deliveryCursor,
    resolved.crawlRun,
    resolved.search,
    {
      candidateCount,
      matchedCount: page.totalMatchedCount,
      returnedCount: page.returnedCount,
      pageSize: page.pageSize,
      nextCursor: page.nextCursor,
      hasMore: page.hasMore,
      excludedByTitleCount: resolved.crawlRun.diagnostics.excludedByTitle,
      excludedByLocationCount:
        resolved.crawlRun.diagnostics.excludedByLocation +
        Math.max(0, jobs.length - filteredSessionJobs.length),
      excludedByExperienceCount: resolved.crawlRun.diagnostics.excludedByExperience,
    },
  );
  logSearchPagination({
    searchId: resolved.search._id,
    searchSessionId: resolved.searchSession?._id ?? resolved.search.latestSearchSessionId,
    totalMatchedCount: page.totalMatchedCount,
    returnedCount: page.returnedCount,
    pageSize: page.pageSize,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
  });

  return crawlResponseSchema.parse({
    searchId: resolved.search._id,
    searchSessionId: resolved.searchSession?._id ?? resolved.search.latestSearchSessionId,
    candidateCount,
    finalMatchedCount: page.totalMatchedCount,
    totalMatchedCount: page.totalMatchedCount,
    returnedCount: page.returnedCount,
    pageSize: page.pageSize,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
    search: resolved.search,
    ...(resolved.searchSession ? { searchSession: resolved.searchSession } : {}),
    crawlRun: resolved.crawlRun,
    sourceResults,
    jobs: page.jobs,
    diagnostics,
    delivery: {
      mode: "full",
      cursor: deliveryCursor,
      indexedCursor,
    },
  });
}

export async function getSearchJobDeltas(
  searchId: string,
  afterCursor: number,
  runtime: JobCrawlerRuntime & {
    afterIndexedCursor?: number;
  } = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchState(searchId, repository);
  const afterIndexedCursor = Math.max(0, Math.floor(runtime.afterIndexedCursor ?? afterCursor));
  const indexedDelta = await getIndexedJobDeltasForSearch(
    repository,
    resolved.search.filters,
    afterIndexedCursor,
  );

  if (!resolved.crawlRun) {
    return crawlDeltaResponseSchema.parse({
      ...buildSyntheticCrawlResponse(
        resolved.search,
        indexedDelta.jobs,
        (await isSearchRunPending(searchId, repository)) ? "running" : undefined,
        indexedDelta.cursor,
      ),
      delivery: {
        mode: "delta",
        previousCursor: afterCursor,
        cursor: 0,
        previousIndexedCursor: afterIndexedCursor,
        indexedCursor: indexedDelta.cursor,
      },
    });
  }

  const [sourceResults, jobDelta] = await Promise.all([
    repository.getCrawlSourceResults(resolved.crawlRun._id),
    resolved.searchSession
      ? repository.getJobsBySearchSessionAfterSequence(resolved.searchSession._id, afterCursor)
      : repository.getJobsByCrawlRunAfterSequence(resolved.crawlRun._id, afterCursor),
  ]);
  const mergedDeltaJobs = mergeSearchResultJobs(
    indexedDelta.jobs,
    filterAndDecorateSearchJobs(
      jobDelta.jobs.map(applyResolvedExperienceLevel),
      resolved.search.filters,
      "supplemental_delta",
    ),
  );
  const filteredDeltaJobs = filterAndDecorateSearchJobs(
    mergedDeltaJobs,
    resolved.search.filters,
    "delta_response_guard",
  );
  const diagnostics = attachSessionDiagnostics(
    resolved.crawlRun.diagnostics,
    (resolved.crawlRun.diagnostics.session?.indexedResultsCount ?? 0) + indexedDelta.jobs.length,
    (resolved.crawlRun.diagnostics.session?.totalVisibleResultsCount ?? 0) + filteredDeltaJobs.length,
    jobDelta.cursor,
    resolved.crawlRun,
    resolved.search,
    {
      candidateCount: resolved.crawlRun.diagnostics.session?.indexedCandidateCount ?? indexedDelta.jobs.length,
      matchedCount: filteredDeltaJobs.length,
      excludedByTitleCount: resolved.crawlRun.diagnostics.excludedByTitle,
      excludedByLocationCount:
        resolved.crawlRun.diagnostics.excludedByLocation +
        Math.max(0, mergedDeltaJobs.length - filteredDeltaJobs.length),
      excludedByExperienceCount: resolved.crawlRun.diagnostics.excludedByExperience,
    },
  );

  return crawlDeltaResponseSchema.parse({
    search: resolved.search,
    ...(resolved.searchSession ? { searchSession: resolved.searchSession } : {}),
    crawlRun: resolved.crawlRun,
    sourceResults,
    jobs: sortJobsWithDiagnostics(
      filteredDeltaJobs,
      resolved.search.filters.title,
      runtime.now ?? new Date(),
    ),
  diagnostics,
    delivery: {
      mode: "delta",
      previousCursor: afterCursor,
      cursor: jobDelta.cursor,
      previousIndexedCursor: afterIndexedCursor,
      indexedCursor: indexedDelta.cursor,
    },
  });
}

export async function abortSearch(searchId: string, runtime: JobCrawlerRuntime = {}) {
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

export async function listRecentSearches(runtime: JobCrawlerRuntime = {}) {
  const repository = await resolveRepository(runtime.repository);
  return repository.listRecentSearches();
}

export function normalizeSearchPaginationOptions(
  options: SearchPaginationOptions = {},
) {
  const cursor =
    Number.isFinite(options.cursor) && (options.cursor ?? 0) > 0
      ? Math.floor(options.cursor ?? 0)
      : 0;
  const requestedPageSize =
    Number.isFinite(options.pageSize) && (options.pageSize ?? 0) > 0
      ? Math.floor(options.pageSize ?? defaultSearchResultsPageSize)
      : defaultSearchResultsPageSize;

  return {
    cursor,
    pageSize: Math.min(Math.max(1, requestedPageSize), maxSearchResultsPageSize),
    searchSessionId: normalizeOptionalString(options.searchSessionId),
  };
}

function paginateSearchResults(
  jobs: JobListing[],
  options: SearchPaginationOptions = {},
): SearchPage {
  const pagination = normalizeSearchPaginationOptions(options);
  const totalMatchedCount = jobs.length;
  const pageJobs = jobs.slice(pagination.cursor, pagination.cursor + pagination.pageSize);
  const nextCursorValue = pagination.cursor + pageJobs.length;
  const hasMore = nextCursorValue < totalMatchedCount;

  return {
    cursor: pagination.cursor,
    pageSize: pagination.pageSize,
    totalMatchedCount,
    returnedCount: pageJobs.length,
    nextCursor: hasMore ? nextCursorValue : null,
    hasMore,
    jobs: pageJobs,
  };
}

function logSearchPagination(input: {
  searchId: string;
  searchSessionId?: string | null;
  totalMatchedCount: number;
  returnedCount: number;
  pageSize: number;
  nextCursor: number | null;
  hasMore: boolean;
}) {
  console.info("[search:pagination]", {
    searchId: input.searchId,
    searchSessionId: input.searchSessionId,
    totalMatchedCount: input.totalMatchedCount,
    returnedCount: input.returnedCount,
    pageSize: input.pageSize,
    nextCursor: input.nextCursor,
    hasMore: input.hasMore,
  });
}

function normalizeOptionalString(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

export async function requestSearchCancellation(
  searchId: string,
  repository: Awaited<ReturnType<typeof resolveRepository>>,
  options: {
    reason: string;
    awaitCompletion?: boolean;
  },
) {
  const search = await repository.getSearch(searchId);
  if (!search) {
    return false;
  }

  const resolved = await loadSearchState(searchId, repository);
  if (!resolved.crawlRun) {
    return false;
  }

  const runState = await repository.getCrawlRun(resolved.crawlRun._id);
  const shouldRequestPersistentCancel = runState?.status === "running" && !runState.finishedAt;

  if (shouldRequestPersistentCancel) {
    await repository.requestCrawlRunCancellation(resolved.crawlRun._id, {
      reason: options.reason,
    });
  }

  const abortedInMemory = await abortSearchRun(searchId, repository, {
    reason: options.reason,
    awaitCompletion: options.awaitCompletion,
  });

  return shouldRequestPersistentCancel || abortedInMemory;
}

async function loadSearchState(
  searchId: string,
  repository: Awaited<ReturnType<typeof resolveRepository>>,
) {
  const search = await repository.getSearch(searchId);

  if (!search) {
    throw new ResourceNotFoundError(`Search ${searchId} was not found.`);
  }

  if (!search.latestSearchSessionId && !search.latestCrawlRunId) {
    return {
      search,
      searchSession: null,
      crawlRun: null,
    };
  }

  const searchSession = search.latestSearchSessionId
    ? await repository.getSearchSession(search.latestSearchSessionId)
    : null;
  const crawlRunId = searchSession?.latestCrawlRunId ?? search.latestCrawlRunId;

  if (!crawlRunId) {
    return {
      search,
      searchSession,
      crawlRun: null,
    };
  }

  const crawlRun = await repository.getCrawlRun(crawlRunId);
  if (!crawlRun) {
    throw new ResourceNotFoundError(
      `Latest crawl run ${crawlRunId} for search ${searchId} was not found.`,
    );
  }

  return {
    search,
    searchSession,
    crawlRun,
  };
}

function buildSyntheticCrawlResponse(
  search: SearchDocument,
  jobs: JobListing[],
  status?: "running" | "aborted",
  indexedCursor = 0,
  paginationOptions: SearchPaginationOptions = {},
) {
  const crawlStatus = status ?? search.lastStatus ?? "completed";
  const sortedJobs = sortJobsWithDiagnostics(jobs, search.filters.title);
  const page = paginateSearchResults(sortedJobs, paginationOptions);
  const diagnostics = {
    discoveredSources: 0,
    crawledSources: 0,
    providerFailures: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    dedupedOut: 0,
    validationDeferred: 0,
    session: {
      indexedResultsCount: jobs.length,
      initialIndexedResultsCount: jobs.length,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: jobs.length,
      indexedCandidateCount: jobs.length,
      minimumIndexedCoverage: 0,
      targetJobCount: 0,
      supplementalQueued: false,
      supplementalRunning: status === "running",
    },
    searchResponse: {
      requestedFilters: search.filters,
      parsedFilters: search.filters,
      searchId: search._id,
      sessionId: search.latestSearchSessionId,
      candidateCount: jobs.length,
      matchedCount: page.totalMatchedCount,
      finalMatchedCount: page.totalMatchedCount,
      totalMatchedCount: page.totalMatchedCount,
      returnedCount: page.returnedCount,
      pageSize: page.pageSize,
      nextCursor: page.nextCursor,
      hasMore: page.hasMore,
      excludedByTitleCount: 0,
      excludedByLocationCount: 0,
      excludedByExperienceCount: 0,
    },
  };
  logSearchPagination({
    searchId: search._id,
    searchSessionId: search.latestSearchSessionId,
    totalMatchedCount: page.totalMatchedCount,
    returnedCount: page.returnedCount,
    pageSize: page.pageSize,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
  });

  return crawlResponseSchema.parse({
    searchId: search._id,
    searchSessionId: search.latestSearchSessionId,
    candidateCount: jobs.length,
    finalMatchedCount: page.totalMatchedCount,
    totalMatchedCount: page.totalMatchedCount,
    returnedCount: page.returnedCount,
    pageSize: page.pageSize,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
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
    jobs: page.jobs,
    diagnostics,
    delivery: {
      mode: "full",
      cursor: 0,
      indexedCursor,
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

async function loadIndexedSearchJobs(
  search: SearchDocument,
  repository: Awaited<ReturnType<typeof resolveRepository>>,
) {
  const indexedSearch = await getIndexedJobsForSearch(repository, search.filters);
  return indexedSearch.matches.map(({ job }) => job);
}

function filterAndDecorateSearchJobs(
  jobs: JobListing[],
  filters: SearchDocument["filters"],
  source: "supplemental_session" | "response_guard" | "supplemental_delta" | "delta_response_guard",
) {
  return jobs.flatMap((job) => {
    const evaluation = evaluateSearchFilters(job, filters, {
      includeExperience: true,
    });

    if (!evaluation.matches) {
      return [];
    }

    return [
      {
        ...job,
        rawSourceMetadata: {
          ...(job.rawSourceMetadata ?? {}),
          searchResponse: {
            source,
            titleMatch: {
              tier: evaluation.titleMatch.tier,
              score: evaluation.titleMatch.score,
              explanation: evaluation.titleMatch.explanation,
            },
            locationMatch: evaluation.locationMatch
              ? {
                  matches: evaluation.locationMatch.matches,
                  explanation: evaluation.locationMatch.explanation,
                  matchedTerms: evaluation.locationMatch.matchedTerms,
                  queryDiagnostics: evaluation.locationMatch.queryDiagnostics,
                  jobDiagnostics: evaluation.locationMatch.jobDiagnostics,
                }
              : undefined,
            experienceMatch: evaluation.experienceMatch
              ? {
                  matches: evaluation.experienceMatch.matches,
                  matchedLevel: evaluation.experienceMatch.matchedLevel,
                  explanation: evaluation.experienceMatch.explanation,
                }
              : undefined,
          },
        },
      },
    ] satisfies JobListing[];
  });
}

function attachSessionDiagnostics(
  diagnostics: CrawlDiagnostics | undefined,
  indexedResultsCount: number,
  totalVisibleResultsCount: number,
  searchSessionVisibleCount: number | undefined,
  crawlRun: {
    status: "running" | "completed" | "partial" | "failed" | "aborted";
    finishedAt?: string | null;
  },
  search?: SearchDocument,
  searchResponse?: {
    candidateCount: number;
    matchedCount: number;
    returnedCount?: number;
    pageSize?: number;
    nextCursor?: number | null;
    hasMore?: boolean;
    excludedByTitleCount: number;
    excludedByLocationCount: number;
    excludedByExperienceCount: number;
  },
): CrawlDiagnostics {
  const baseDiagnostics = crawlDiagnosticsSchema.parse(diagnostics ?? {});
  const safeTotalVisibleResultsCount = Math.max(totalVisibleResultsCount, indexedResultsCount);
  const initialIndexedResultsCount =
    baseDiagnostics.session?.initialIndexedResultsCount ?? indexedResultsCount;
  const sessionVisibleCount = Math.max(
    searchSessionVisibleCount ?? safeTotalVisibleResultsCount,
    initialIndexedResultsCount,
  );
  const supplementalResultsCount = Math.max(0, sessionVisibleCount - initialIndexedResultsCount);
  const visibleIndexedResultsCount = Math.max(
    0,
    safeTotalVisibleResultsCount - supplementalResultsCount,
  );

  return {
    ...baseDiagnostics,
    ...(search
      ? {
          searchResponse: {
            requestedFilters: search.filters,
            parsedFilters: search.filters,
            searchId: search._id,
            sessionId: search.latestSearchSessionId,
            candidateCount:
              searchResponse?.candidateCount ??
              baseDiagnostics.session?.indexedCandidateCount ??
              indexedResultsCount,
            matchedCount: searchResponse?.matchedCount ?? totalVisibleResultsCount,
            finalMatchedCount: searchResponse?.matchedCount ?? totalVisibleResultsCount,
            totalMatchedCount: searchResponse?.matchedCount ?? totalVisibleResultsCount,
            returnedCount:
              searchResponse?.returnedCount ??
              searchResponse?.matchedCount ??
              totalVisibleResultsCount,
            pageSize: searchResponse?.pageSize,
            nextCursor: searchResponse?.nextCursor,
            hasMore: searchResponse?.hasMore,
            excludedByTitleCount:
              searchResponse?.excludedByTitleCount ?? baseDiagnostics.excludedByTitle,
            excludedByLocationCount:
              searchResponse?.excludedByLocationCount ?? baseDiagnostics.excludedByLocation,
            excludedByExperienceCount:
              searchResponse?.excludedByExperienceCount ?? baseDiagnostics.excludedByExperience,
          },
        }
      : {}),
    session: {
      indexedResultsCount: visibleIndexedResultsCount,
      initialIndexedResultsCount,
      supplementalResultsCount,
      totalVisibleResultsCount: safeTotalVisibleResultsCount,
      indexedCandidateCount: baseDiagnostics.session?.indexedCandidateCount ?? indexedResultsCount,
      indexedRequestTimeEvaluationCount:
        baseDiagnostics.session?.indexedRequestTimeEvaluationCount ?? indexedResultsCount,
      indexedRequestTimeExcludedCount:
        baseDiagnostics.session?.indexedRequestTimeExcludedCount ?? 0,
      indexedSearchTimingsMs: baseDiagnostics.session?.indexedSearchTimingsMs,
      minimumIndexedCoverage: baseDiagnostics.session?.minimumIndexedCoverage ?? 0,
      coverageTarget: baseDiagnostics.session?.coverageTarget ?? 0,
      coveragePolicyReason: baseDiagnostics.session?.coveragePolicyReason,
      targetJobCount: baseDiagnostics.session?.targetJobCount ?? 0,
      supplementalQueued:
        baseDiagnostics.session?.supplementalQueued ??
        (crawlRun.status === "running" || supplementalResultsCount > 0),
      supplementalRunning: crawlRun.status === "running" && !crawlRun.finishedAt,
      targetedReplenishmentQueued:
        baseDiagnostics.session?.targetedReplenishmentQueued ?? false,
      targetedReplenishmentActive:
        baseDiagnostics.session?.targetedReplenishmentActive ?? false,
      activeQueueAlreadyExists:
        baseDiagnostics.session?.activeQueueAlreadyExists ?? false,
      triggerReason: baseDiagnostics.session?.triggerReason,
      triggerExplanation: baseDiagnostics.session?.triggerExplanation,
      reusedExistingSearch: baseDiagnostics.session?.reusedExistingSearch,
      previousVisibleJobCount: baseDiagnostics.session?.previousVisibleJobCount,
      previousRunStatus: baseDiagnostics.session?.previousRunStatus,
      previousFinishedAt: baseDiagnostics.session?.previousFinishedAt,
      latestIndexedJobAgeMs: baseDiagnostics.session?.latestIndexedJobAgeMs,
      backgroundIngestion: baseDiagnostics.session?.backgroundIngestion,
    },
  };
}

async function resolveSearchForNewSession(
  filters: SearchDocument["filters"],
  repository: Awaited<ReturnType<typeof resolveRepository>>,
  nowIso: string,
) {
  const existing = await repository.findMostRecentSearchByFilters(filters);
  if (!existing) {
    return {
      search: await repository.createSearch(filters, nowIso),
      reusedExistingSearch: false,
    };
  }

  const hasActiveQueueEntry = await repository.hasActiveCrawlQueueEntryForSearch(existing._id);
  if (hasActiveQueueEntry) {
    return {
      search: await repository.createSearch(filters, nowIso),
      reusedExistingSearch: false,
    };
  }

  return {
    search: existing,
    reusedExistingSearch: true,
  };
}

async function loadSearchReuseBaseline(
  search: SearchDocument,
  repository: Awaited<ReturnType<typeof resolveRepository>>,
  reusedExistingSearch: boolean,
): Promise<SearchReuseBaseline> {
  if (!reusedExistingSearch) {
    return {
      reusedExistingSearch: false,
      previousVisibleJobCount: 0,
      previousRunStatus: search.lastStatus,
    };
  }

  const previousSearchSession = search.latestSearchSessionId
    ? await repository.getSearchSession(search.latestSearchSessionId)
    : null;
  const previousCrawlRunId = previousSearchSession?.latestCrawlRunId ?? search.latestCrawlRunId;
  const previousCrawlRun = previousCrawlRunId
    ? await repository.getCrawlRun(previousCrawlRunId)
    : null;
  const indexedJobs = await loadIndexedSearchJobs(search, repository);
  const supplementalJobs = previousSearchSession
    ? await repository.getJobsBySearchSession(previousSearchSession._id)
    : previousCrawlRun
      ? await repository.getJobsByCrawlRun(previousCrawlRun._id)
      : [];
  const previousVisibleJobCount = mergeSearchResultJobs(indexedJobs, supplementalJobs).length;

  return {
    reusedExistingSearch,
    previousVisibleJobCount,
    previousRunStatus: previousCrawlRun?.status ?? search.lastStatus,
    previousFinishedAt: previousCrawlRun?.finishedAt,
  };
}
