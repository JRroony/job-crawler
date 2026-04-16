import "server-only";

import {
  applyResolvedExperienceLevel,
  defaultCrawlLinkValidationMode,
  refreshStaleJobs,
} from "@/lib/server/crawler/pipeline";
import { abortOwnerSearchRun, abortSearchRun, isSearchRunPending } from "@/lib/server/crawler/background-runs";
import { createId } from "@/lib/server/crawler/helpers";
import { sortJobsWithDiagnostics } from "@/lib/server/crawler/sort";
import {
  getIndexedJobsForSearch,
  mergeSearchResultJobs,
} from "@/lib/server/search/indexed-jobs";
import { getEnv } from "@/lib/server/env";
import { resolveRepository, type JobCrawlerRuntime } from "@/lib/server/services/runtime";
import {
  crawlDeltaResponseSchema,
  crawlResponseSchema,
  type JobListing,
  type SearchDocument,
} from "@/lib/types";

import { ResourceNotFoundError } from "@/lib/server/search/errors";

const initialVisiblePollIntervalMs = 75;

type SearchReuseBaseline = {
  reusedExistingSearch: boolean;
  previousVisibleJobCount: number;
  previousRunStatus?: SearchDocument["lastStatus"];
  previousFinishedAt?: string;
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
  > = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const initialResult = await getSearchDetails(searchId, {
    repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now,
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
  });
}

export async function getSearchDetails(searchId: string, runtime: JobCrawlerRuntime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchState(searchId, repository);

  if (!resolved.crawlRun) {
    return buildSyntheticCrawlResponse(
      resolved.search,
      await loadIndexedSearchJobs(resolved.search, repository),
      (await isSearchRunPending(searchId, repository)) ? "running" : undefined,
    );
  }

  const indexedJobs = await loadIndexedSearchJobs(resolved.search, repository);
  let jobs = resolved.searchSession
    ? await repository.getJobsBySearchSession(resolved.searchSession._id)
    : await repository.getJobsByCrawlRun(resolved.crawlRun._id);
  if (resolved.crawlRun.status !== "running") {
    jobs = await refreshStaleJobs(jobs, repository, runtime.fetchImpl ?? fetch, runtime.now ?? new Date());
  }
  const sourceResults = await repository.getCrawlSourceResults(resolved.crawlRun._id);
  const deliveryCursor = resolved.searchSession
    ? await repository.getSearchSessionDeliveryCursor(resolved.searchSession._id)
    : await repository.getCrawlRunDeliveryCursor(resolved.crawlRun._id);
  const mergedJobs = mergeSearchResultJobs(indexedJobs, jobs.map(applyResolvedExperienceLevel));

  return crawlResponseSchema.parse({
    search: resolved.search,
    ...(resolved.searchSession ? { searchSession: resolved.searchSession } : {}),
    crawlRun: resolved.crawlRun,
    sourceResults,
    jobs: sortJobsWithDiagnostics(
      mergedJobs,
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
  runtime: JobCrawlerRuntime = {},
) {
  const repository = await resolveRepository(runtime.repository);
  const resolved = await loadSearchState(searchId, repository);

  if (!resolved.crawlRun) {
    return crawlDeltaResponseSchema.parse({
      ...buildSyntheticCrawlResponse(
        resolved.search,
        [],
        (await isSearchRunPending(searchId, repository)) ? "running" : undefined,
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
    resolved.searchSession
      ? repository.getJobsBySearchSessionAfterSequence(resolved.searchSession._id, afterCursor)
      : repository.getJobsByCrawlRunAfterSequence(resolved.crawlRun._id, afterCursor),
  ]);

  return crawlDeltaResponseSchema.parse({
    search: resolved.search,
    ...(resolved.searchSession ? { searchSession: resolved.searchSession } : {}),
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
    jobs: sortJobsWithDiagnostics(jobs, search.filters.title),
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

async function loadIndexedSearchJobs(
  search: SearchDocument,
  repository: Awaited<ReturnType<typeof resolveRepository>>,
) {
  const indexedMatches = await getIndexedJobsForSearch(repository, search.filters);
  return indexedMatches.map(({ job }) => job);
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
  const previousVisibleJobCount = previousSearchSession
    ? previousSearchSession.lastEventSequence
    : previousCrawlRun
      ? await repository.getCrawlRunDeliveryCursor(previousCrawlRun._id)
      : 0;

  return {
    reusedExistingSearch,
    previousVisibleJobCount,
    previousRunStatus: previousCrawlRun?.status ?? search.lastStatus,
    previousFinishedAt: previousCrawlRun?.finishedAt,
  };
}
