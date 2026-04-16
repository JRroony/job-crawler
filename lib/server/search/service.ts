import "server-only";

import { normalizeSearchIntentInput } from "@/lib/server/crawler/search-intent";
import { getEnv } from "@/lib/server/env";
import { searchFiltersSchema } from "@/lib/types";
import { type JobCrawlerRuntime } from "@/lib/server/services/runtime";

import {
  executeSearchIngestion,
  queueSearchIngestion,
  runSearchIngestionFromSession,
} from "@/lib/server/ingestion/service";
import {
  createSearchRerunSession,
  createSearchSession,
  getInitialSearchResult,
  monitorSearchRequestAbort,
  abortSupersededSearch,
  listRecentSearches,
} from "@/lib/server/search/session-service";
import { getIndexedJobsForSearch } from "@/lib/server/search/indexed-jobs";
import { InputValidationError, isInputValidationError } from "@/lib/server/search/errors";

export async function runSearchFromFilters(
  rawFilters: unknown,
  runtime: JobCrawlerRuntime = {},
) {
  const filters = parseSearchFilters(rawFilters);
  console.info("[crawl:normalized-filters]", filters);

  const { result } = await startSearchFromFilters(filters, runtime);
  return result;
}

export async function rerunSearch(searchId: string, runtime: JobCrawlerRuntime = {}) {
  const { result } = await startSearchRerun(searchId, runtime);
  return result;
}

export async function startSearchFromFilters(
  rawFilters: unknown,
  runtime: JobCrawlerRuntime = {},
) {
  const filters = parseSearchFilters(rawFilters);
  console.info("[crawl:normalized-filters]", filters);

  await abortSupersededSearch("__new-search__", runtime, {
    defaultReason: "The crawl was superseded by a newer search request.",
  });

  const session = await createSearchSession(filters, runtime);
  const queued = await primeSearchSessionAndMaybeQueueSupplemental(session, runtime);

  if (queued) {
    await monitorSearchRequestAbort(session.search._id, {
      repository: session.repository,
      signal: runtime.signal,
    });
  }

  return {
    queued,
    result: await getInitialSearchResult(session.search._id, session.searchSession._id, {
      repository: session.repository,
      fetchImpl: runtime.fetchImpl ?? fetch,
      now: session.now,
      earlyVisibleTarget: runtime.earlyVisibleTarget,
      initialVisibleWaitMs: runtime.initialVisibleWaitMs,
      signal: runtime.signal,
    }),
  };
}

export async function startSearchRerun(searchId: string, runtime: JobCrawlerRuntime = {}) {
  await abortSupersededSearch(searchId, runtime, {
    defaultReason: "The crawl was superseded by a rerun request.",
  });

  const session = await createSearchRerunSession(searchId, runtime);
  const queued = await primeSearchSessionAndMaybeQueueSupplemental(session, runtime);

  return {
    queued,
    result: await getInitialSearchResult(searchId, session.searchSession._id, {
      repository: session.repository,
      fetchImpl: runtime.fetchImpl ?? fetch,
      now: session.now,
      earlyVisibleTarget: runtime.earlyVisibleTarget,
      initialVisibleWaitMs: runtime.initialVisibleWaitMs,
      signal: runtime.signal,
    }),
  };
}

export async function runSearchIngestionFromFilters(
  rawFilters: unknown,
  runtime: JobCrawlerRuntime = {},
) {
  const filters = parseSearchFilters(rawFilters);
  console.info("[crawl:normalized-filters]", filters);

  const session = await createSearchSession(filters, runtime);

  return runSearchIngestionFromSession(
    {
      search: session.search,
      searchSession: session.searchSession,
      crawlRun: session.crawlRun,
    },
    {
      ...runtime,
      repository: session.repository,
      now: session.now,
    },
  );
}

export async function runSearchRerunIngestion(
  searchId: string,
  runtime: JobCrawlerRuntime = {},
) {
  const session = await createSearchRerunSession(searchId, runtime);

  return executeSearchIngestion(
    {
      search: session.search,
      searchSession: session.searchSession,
      crawlRun: session.crawlRun,
    },
    {
      ...runtime,
      repository: session.repository,
      now: session.now,
    },
  );
}

export { isInputValidationError, listRecentSearches };

function parseSearchFilters(rawFilters: unknown) {
  const normalizedInput = normalizeSearchIntentInput(rawFilters);
  const parsedFilters = searchFiltersSchema.safeParse(normalizedInput);

  if (!parsedFilters.success) {
    throw new InputValidationError(parsedFilters.error);
  }

  return parsedFilters.data;
}

async function primeSearchSessionAndMaybeQueueSupplemental(
  session: Awaited<ReturnType<typeof createSearchSession>> | Awaited<ReturnType<typeof createSearchRerunSession>>,
  runtime: JobCrawlerRuntime,
) {
  const indexedMatches = await getIndexedJobsForSearch(session.repository, session.search.filters);
  const indexedJobs = indexedMatches.map(({ job }) => job);

  await session.repository.appendExistingJobsToSearchSession(
    session.searchSession._id,
    session.crawlRun._id,
    indexedJobs.map((job) => job._id),
  );

  const shouldQueueSupplemental = shouldEnqueueSupplementalCrawl(
    session.search.filters.crawlMode,
    indexedJobs.length,
    {
      reusedExistingSearch: session.searchReuse.reusedExistingSearch,
      previousVisibleJobCount: session.searchReuse.previousVisibleJobCount,
      previousRunStatus: session.searchReuse.previousRunStatus,
    },
  );

  console.info("[search:index-first]", {
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    indexedJobs: indexedJobs.length,
    supplementalQueued: shouldQueueSupplemental,
    crawlMode: session.search.filters.crawlMode ?? "balanced",
    reusedExistingSearch: session.searchReuse.reusedExistingSearch,
    previousVisibleJobCount: session.searchReuse.previousVisibleJobCount,
    previousRunStatus: session.searchReuse.previousRunStatus ?? "unknown",
  });

  if (!shouldQueueSupplemental) {
    const finishedAt = session.now.toISOString();
    await Promise.all([
      session.repository.finalizeCrawlRun(session.crawlRun._id, {
        status: "completed",
        stage: "finalizing",
        totalFetchedJobs: 0,
        totalMatchedJobs: indexedJobs.length,
        dedupedJobs: indexedJobs.length,
        diagnostics: {
          discoveredSources: 0,
          crawledSources: 0,
          providersEnqueued: 0,
          providerFailures: 0,
          directJobsHarvested: 0,
          jobsBeforeDedupe: indexedJobs.length,
          jobsAfterDedupe: indexedJobs.length,
          excludedByTitle: 0,
          excludedByLocation: 0,
          excludedByExperience: 0,
          dedupedOut: 0,
          validationDeferred: 0,
          dropReasonCounts: {},
          filterDecisionTraces: [],
          dedupeDecisionTraces: [],
          performance: {
            stageTimingsMs: {
              discovery: 0,
              providerExecution: 0,
              filtering: 0,
              dedupe: 0,
              persistence: 0,
              validation: 0,
              responseAssembly: 0,
              total: 0,
            },
            providerTimingsMs: [],
            progressUpdateCount: 0,
            persistenceBatchCount: 0,
          },
        },
        finishedAt,
      }),
      session.repository.updateSearchLatestRun(
        session.search._id,
        session.crawlRun._id,
        "completed",
        finishedAt,
      ),
      session.repository.updateSearchLatestSession(
        session.search._id,
        session.searchSession._id,
        "completed",
        finishedAt,
      ),
      session.repository.updateSearchSession(session.searchSession._id, {
        latestCrawlRunId: session.crawlRun._id,
        status: "completed",
        finishedAt,
        updatedAt: finishedAt,
      }),
    ]);

    return false;
  }

  return queueSearchIngestion(
    {
      search: session.search,
      searchSession: session.searchSession,
      crawlRun: session.crawlRun,
    },
    {
      ...runtime,
      repository: session.repository,
      now: session.now,
    },
  );
}

function shouldEnqueueSupplementalCrawl(
  crawlMode: ReturnType<typeof parseSearchFilters>["crawlMode"],
  indexedJobCount: number,
  previousCoverage: {
    reusedExistingSearch: boolean;
    previousVisibleJobCount: number;
    previousRunStatus?: "running" | "completed" | "partial" | "failed" | "aborted";
  },
) {
  const env = getEnv();
  const mode = crawlMode ?? "balanced";
  const minimumIndexedCoverageByMode = {
    fast: Math.min(3, env.CRAWL_TARGET_JOB_COUNT),
    balanced: Math.min(5, env.CRAWL_TARGET_JOB_COUNT),
    deep: Math.min(8, env.CRAWL_TARGET_JOB_COUNT),
  } as const;

  if (indexedJobCount >= minimumIndexedCoverageByMode[mode]) {
    return false;
  }

  if (
    previousCoverage.reusedExistingSearch &&
    previousCoverage.previousRunStatus === "completed" &&
    previousCoverage.previousVisibleJobCount > 0 &&
    indexedJobCount >= previousCoverage.previousVisibleJobCount
  ) {
    return false;
  }

  return indexedJobCount < minimumIndexedCoverageByMode[mode];
}
