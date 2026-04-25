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
import { triggerRecurringBackgroundIngestion } from "@/lib/server/background/recurring-ingestion";
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
import type { CrawlDiagnostics, SearchDocument } from "@/lib/types";

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
  const indexedSearch = await getIndexedJobsForSearch(session.repository, session.search.filters);
  const indexedJobs = indexedSearch.matches.map(({ job }) => job);

  await session.repository.appendExistingJobsToSearchSession(
    session.searchSession._id,
    session.crawlRun._id,
    indexedJobs.map((job) => job._id),
  );

  const supplementalDecision = resolveSupplementalCrawlDecision(
    session.search.filters,
    indexedSearch,
    session.searchReuse,
    session.now,
    {
      allowRequestTimeSupplementalCrawl:
        runtime.allowRequestTimeSupplementalCrawl === true,
      allowRequestTimeFreshnessRecovery:
        (runtime.providers?.length ?? 1) > 0 && Boolean(runtime.discovery ?? true),
    },
  );
  const backgroundIngestion = supplementalDecision.requestBackgroundIngestion
    ? await requestBackgroundIngestionForIndexGap(runtime, session.now)
    : { status: "not_requested" as const };
  const primedDiagnostics = buildPrimedSessionDiagnostics(
    supplementalDecision,
    indexedJobs.length,
    backgroundIngestion,
  );
  session.crawlRun = {
    ...session.crawlRun,
    diagnostics: primedDiagnostics,
  };

  await session.repository.updateCrawlRunProgress(session.crawlRun._id, {
    stage: supplementalDecision.shouldQueue ? "queued" : "finalizing",
    totalFetchedJobs: 0,
    totalMatchedJobs: indexedJobs.length,
    dedupedJobs: indexedJobs.length,
    diagnostics: primedDiagnostics,
  });

  console.info("[search:index-first]", {
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    indexedCandidates: indexedSearch.candidateCount,
    indexedJobs: indexedJobs.length,
    supplementalQueued: supplementalDecision.shouldQueue,
    supplementalReason: supplementalDecision.triggerReason,
    backgroundIngestion,
    crawlMode: session.search.filters.crawlMode ?? "balanced",
    reusedExistingSearch: session.searchReuse.reusedExistingSearch,
    previousVisibleJobCount: session.searchReuse.previousVisibleJobCount,
    previousRunStatus: session.searchReuse.previousRunStatus ?? "unknown",
    latestIndexedJobAgeMs: supplementalDecision.latestIndexedJobAgeMs,
    indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
    indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
    indexedSearchTimingsMs: indexedSearch.timingsMs,
  });

  if (!supplementalDecision.shouldQueue) {
    const finishedAt = session.now.toISOString();
    await Promise.all([
      session.repository.finalizeCrawlRun(session.crawlRun._id, {
        status: "completed",
        stage: "finalizing",
        totalFetchedJobs: 0,
        totalMatchedJobs: indexedJobs.length,
        dedupedJobs: indexedJobs.length,
        diagnostics: primedDiagnostics,
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

type SupplementalDecision = {
  shouldQueue: boolean;
  requestBackgroundIngestion: boolean;
  triggerReason: NonNullable<NonNullable<CrawlDiagnostics["session"]>["triggerReason"]>;
  triggerExplanation: string;
  minimumIndexedCoverage: number;
  targetJobCount: number;
  indexedCandidateCount: number;
  indexedRequestTimeEvaluationCount: number;
  indexedRequestTimeExcludedCount: number;
  indexedExcludedByTitleCount: number;
  indexedExcludedByLocationCount: number;
  indexedExcludedByExperienceCount: number;
  indexedSearchTimingsMs: Awaited<ReturnType<typeof getIndexedJobsForSearch>>["timingsMs"];
  indexedJobCount: number;
  reusedExistingSearch: boolean;
  previousVisibleJobCount: number;
  previousRunStatus?: SearchDocument["lastStatus"];
  previousFinishedAt?: string;
  latestIndexedJobAgeMs?: number;
};

type BackgroundIngestionRequestDiagnostics = NonNullable<
  NonNullable<CrawlDiagnostics["session"]>["backgroundIngestion"]
>;

function resolveSupplementalCrawlDecision(
  filters: ReturnType<typeof parseSearchFilters>,
  indexedSearch: Awaited<ReturnType<typeof getIndexedJobsForSearch>>,
  previousCoverage: {
    reusedExistingSearch: boolean;
    previousVisibleJobCount: number;
    previousRunStatus?: "running" | "completed" | "partial" | "failed" | "aborted";
    previousFinishedAt?: string;
  },
  now: Date,
  options: {
    allowRequestTimeSupplementalCrawl?: boolean;
    allowRequestTimeFreshnessRecovery?: boolean;
  } = {},
): SupplementalDecision {
  const env = getEnv();
  const mode = filters.crawlMode ?? "balanced";
  const minimumIndexedCoverageByMode = {
    fast: Math.min(3, env.CRAWL_TARGET_JOB_COUNT),
    balanced: Math.min(5, env.CRAWL_TARGET_JOB_COUNT),
    deep: Math.min(8, env.CRAWL_TARGET_JOB_COUNT),
  } as const;
  const minimumIndexedCoverage = minimumIndexedCoverageByMode[mode];
  const indexedJobCount = indexedSearch.matches.length;
  const latestIndexedJobAgeMs = resolveLatestIndexedJobAgeMs(
    indexedSearch.matches.map(({ job }) => job),
    now,
  );

  if (options.allowRequestTimeSupplementalCrawl && indexedJobCount < minimumIndexedCoverage) {
    return {
      shouldQueue: true,
      requestBackgroundIngestion: false,
      triggerReason: "explicit_request_time_recovery",
      triggerExplanation: `An explicit request-time supplemental recovery opt-in was provided and indexed coverage returned ${indexedJobCount} visible jobs, below the ${minimumIndexedCoverage}-job ${mode} threshold.`,
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  if (indexedJobCount === 0) {
    return {
      shouldQueue: false,
      requestBackgroundIngestion: true,
      triggerReason: "indexed_empty_background_requested",
      triggerExplanation: `Indexed coverage returned zero visible jobs for ${mode} mode, so the request path stays index-first and an independent background ingestion cycle was requested.`,
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  if (indexedJobCount >= minimumIndexedCoverage) {
    return {
      shouldQueue: false,
      requestBackgroundIngestion: false,
      triggerReason: "indexed_coverage_sufficient",
      triggerExplanation: `Indexed results already meet the ${minimumIndexedCoverage}-job ${mode} coverage threshold, so request-time crawl stays off.`,
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  if (
    previousCoverage.reusedExistingSearch &&
    previousCoverage.previousRunStatus === "completed" &&
    previousCoverage.previousVisibleJobCount > 0 &&
    indexedJobCount >= previousCoverage.previousVisibleJobCount
  ) {
    return {
      shouldQueue: false,
      requestBackgroundIngestion: false,
      triggerReason: "reused_completed_coverage",
      triggerExplanation: "A completed identical search was reused and the indexed session already preserves its previously visible coverage.",
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  const staleAfterMs = env.BACKGROUND_INGESTION_STALE_AFTER_MS;
  const freshnessRecoveryEligible =
    indexedJobCount > 0 &&
    typeof latestIndexedJobAgeMs === "number" &&
    latestIndexedJobAgeMs >= staleAfterMs;

  if (freshnessRecoveryEligible) {
    if (mode !== "deep" || !options.allowRequestTimeFreshnessRecovery) {
      return {
        shouldQueue: false,
        requestBackgroundIngestion: true,
        triggerReason: "stale_indexed_coverage_background_requested",
        triggerExplanation: `Indexed coverage is below the ${minimumIndexedCoverage}-job ${mode} threshold and the newest indexed match is stale, so background ingestion was requested instead of making the search request crawl.`,
        minimumIndexedCoverage,
        targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
        indexedCandidateCount: indexedSearch.candidateCount,
        indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
        indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
        indexedSearchTimingsMs: indexedSearch.timingsMs,
        indexedJobCount,
        reusedExistingSearch: previousCoverage.reusedExistingSearch,
        previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
        previousRunStatus: previousCoverage.previousRunStatus,
        previousFinishedAt: previousCoverage.previousFinishedAt,
        latestIndexedJobAgeMs,
      };
    }

    return {
      shouldQueue: true,
      requestBackgroundIngestion: false,
      triggerReason: "freshness_recovery",
      triggerExplanation: `Deep mode explicitly requested broader retrieval and indexed coverage is below the ${minimumIndexedCoverage}-job threshold with stale matches, so a bounded supplemental freshness recovery was queued.`,
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  if (
    previousCoverage.reusedExistingSearch &&
    previousCoverage.previousRunStatus &&
    previousCoverage.previousRunStatus !== "completed"
  ) {
    return {
      shouldQueue: false,
      requestBackgroundIngestion: true,
      triggerReason: "incomplete_previous_run_background_requested",
      triggerExplanation: "The previous identical session did not complete cleanly and indexed coverage is still below threshold, so background ingestion was requested instead of retrying crawl work in the search request.",
      minimumIndexedCoverage,
      targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
      indexedSearchTimingsMs: indexedSearch.timingsMs,
      indexedJobCount,
      reusedExistingSearch: previousCoverage.reusedExistingSearch,
      previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
      previousRunStatus: previousCoverage.previousRunStatus,
      previousFinishedAt: previousCoverage.previousFinishedAt,
      latestIndexedJobAgeMs,
    };
  }

  return {
    shouldQueue: false,
    requestBackgroundIngestion: true,
    triggerReason: "insufficient_indexed_coverage_background_requested",
    triggerExplanation: `Indexed coverage returned ${indexedJobCount} visible jobs, below the ${minimumIndexedCoverage}-job ${mode} threshold, so background ingestion was requested and the search request stayed DB-first.`,
    minimumIndexedCoverage,
    targetJobCount: env.CRAWL_TARGET_JOB_COUNT,
    indexedCandidateCount: indexedSearch.candidateCount,
    indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
    indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
    indexedSearchTimingsMs: indexedSearch.timingsMs,
    indexedJobCount,
    reusedExistingSearch: previousCoverage.reusedExistingSearch,
    previousVisibleJobCount: previousCoverage.previousVisibleJobCount,
    previousRunStatus: previousCoverage.previousRunStatus,
    previousFinishedAt: previousCoverage.previousFinishedAt,
    latestIndexedJobAgeMs,
  };
}

function buildPrimedSessionDiagnostics(
  decision: SupplementalDecision,
  indexedJobCount: number,
  backgroundIngestion: BackgroundIngestionRequestDiagnostics,
): CrawlDiagnostics {
  return {
    discoveredSources: 0,
    crawledSources: 0,
    providersEnqueued: 0,
    providerFailures: 0,
    directJobsHarvested: 0,
    jobsBeforeDedupe: indexedJobCount,
    jobsAfterDedupe: indexedJobCount,
    excludedByTitle: decision.indexedExcludedByTitleCount,
    excludedByLocation: decision.indexedExcludedByLocationCount,
    excludedByExperience: decision.indexedExcludedByExperienceCount,
    dedupedOut: 0,
    validationDeferred: 0,
    session: {
      indexedResultsCount: indexedJobCount,
      initialIndexedResultsCount: indexedJobCount,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: indexedJobCount,
      indexedCandidateCount: decision.indexedCandidateCount,
      indexedRequestTimeEvaluationCount: decision.indexedRequestTimeEvaluationCount,
      indexedRequestTimeExcludedCount: decision.indexedRequestTimeExcludedCount,
      indexedSearchTimingsMs: decision.indexedSearchTimingsMs,
      minimumIndexedCoverage: decision.minimumIndexedCoverage,
      targetJobCount: decision.targetJobCount,
      supplementalQueued: decision.shouldQueue,
      supplementalRunning: decision.shouldQueue,
      triggerReason: decision.triggerReason,
      triggerExplanation: decision.triggerExplanation,
      reusedExistingSearch: decision.reusedExistingSearch,
      previousVisibleJobCount: decision.previousVisibleJobCount,
      previousRunStatus: decision.previousRunStatus,
      previousFinishedAt: decision.previousFinishedAt,
      latestIndexedJobAgeMs: decision.latestIndexedJobAgeMs,
      backgroundIngestion,
    },
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
  };
}

async function requestBackgroundIngestionForIndexGap(
  runtime: JobCrawlerRuntime,
  now: Date,
): Promise<BackgroundIngestionRequestDiagnostics> {
  try {
    const result = await triggerRecurringBackgroundIngestion({
      repository: runtime.repository,
      providers: runtime.providers,
      fetchImpl: runtime.fetchImpl,
      now,
    });
    const mapped = mapBackgroundIngestionTriggerResult(result);

    console.info("[search:background-ingestion-request]", {
      status: mapped.status,
      searchId: mapped.searchId,
      crawlRunId: mapped.crawlRunId,
      systemProfileId: mapped.systemProfileId,
      reason: mapped.reason,
      message: mapped.message,
    });

    return mapped;
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Background ingestion could not be requested for the indexed coverage gap.";
    console.warn("[search:background-ingestion-request]", {
      status: "failed",
      message,
    });

    return {
      status: "failed",
      message,
    };
  }
}

function mapBackgroundIngestionTriggerResult(
  result: Awaited<ReturnType<typeof triggerRecurringBackgroundIngestion>>,
): BackgroundIngestionRequestDiagnostics {
  if (result.status === "started") {
    return {
      status: "started",
      searchId: result.searchId,
      crawlRunId: result.crawlRunId,
      systemProfileId: result.systemProfileId,
    };
  }

  if (result.status === "skipped-active") {
    return {
      status: "already_active",
      searchId: result.searchId,
      systemProfileId: result.systemProfileId,
    };
  }

  if (result.status === "skipped-disabled") {
    return {
      status: "disabled",
      message: result.message,
    };
  }

  if (result.status === "skipped-bootstrap-failed") {
    return {
      status: "bootstrap_failed",
      reason: result.reason,
      message: result.message,
    };
  }

  return {
    status: "mongo_unavailable",
    reason: result.reason,
    message: result.message,
  };
}

function resolveLatestIndexedJobAgeMs(
  indexedJobs: Array<{ indexedAt: string; crawledAt: string; discoveredAt: string }>,
  now: Date,
) {
  if (indexedJobs.length === 0) {
    return undefined;
  }

  const latestIndexedMs = indexedJobs.reduce((latest, job) => {
    const timestamp = Date.parse(job.indexedAt ?? job.crawledAt ?? job.discoveredAt);
    return Number.isFinite(timestamp) ? Math.max(latest, timestamp) : latest;
  }, Number.NEGATIVE_INFINITY);

  if (!Number.isFinite(latestIndexedMs)) {
    return undefined;
  }

  return Math.max(0, now.getTime() - latestIndexedMs);
}
