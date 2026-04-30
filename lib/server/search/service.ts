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
  resolveIndexedCoveragePolicy,
  type IndexedCoveragePolicy,
} from "@/lib/server/search/coverage-policy";
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
import { parseGeoIntentFromFilters } from "@/lib/server/geo/parse";
import { analyzeTitle } from "@/lib/server/title-retrieval";
import {
  attachSearchTraceStage,
  buildSearchIntentTracePayload,
  createEmptySearchTrace,
  createSearchTraceId,
  emitSearchTraceStage,
  type SearchTraceDiagnostics,
} from "@/lib/server/search/search-trace";
import type { CrawlDiagnostics, SearchDocument } from "@/lib/types";

type ParsedSearchFilters = ReturnType<typeof parseSearchFilters>;

export async function runSearchFromFilters(
  rawFilters: unknown,
  runtime: JobCrawlerRuntime = {},
) {
  const { result } = await startSearchFromFilters(rawFilters, runtime);
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
  const startedAt = Date.now();
  const traceId = createSearchTraceId();
  const filters = parseSearchFilters(rawFilters);
  console.info("[crawl:normalized-filters]", filters);
  console.info("[search:db-only-start]", {
    traceId,
    rawFilters,
    normalizedFilters: filters,
  });

  await abortSupersededSearch("__new-search__", runtime, {
    defaultReason: "The crawl was superseded by a newer search request.",
    awaitCompletion: runtime.allowRequestTimeSupplementalCrawl === true,
  });

  const session = await createSearchSession(filters, runtime);
  const searchTrace = initializeSearchTrace({
    traceId,
    rawFilters,
    normalizedFilters: filters,
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    timestamp: session.now.toISOString(),
  });
  const queued = await primeSearchSessionAndMaybeQueueSupplemental(
    session,
    runtime,
    searchTrace,
  );

  if (queued) {
    await monitorSearchRequestAbort(session.search._id, {
      repository: session.repository,
      signal: runtime.signal,
    });
  }

  const result = await getInitialSearchResult(session.search._id, session.searchSession._id, {
    repository: session.repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: session.now,
    earlyVisibleTarget: runtime.earlyVisibleTarget,
    initialVisibleWaitMs: runtime.initialVisibleWaitMs,
    signal: runtime.signal,
  });

  return {
    queued,
    result: attachSearchTraceResponse(result, traceId, Date.now() - startedAt),
  };
}

export async function startSearchRerun(searchId: string, runtime: JobCrawlerRuntime = {}) {
  const startedAt = Date.now();
  const traceId = createSearchTraceId();
  await abortSupersededSearch(searchId, runtime, {
    defaultReason: "The crawl was superseded by a rerun request.",
    awaitCompletion: runtime.allowRequestTimeSupplementalCrawl === true,
  });

  const session = await createSearchRerunSession(searchId, runtime);
  const searchTrace = initializeSearchTrace({
    traceId,
    rawFilters: session.search.filters,
    normalizedFilters: session.search.filters,
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    timestamp: session.now.toISOString(),
  });
  const queued = await primeSearchSessionAndMaybeQueueSupplemental(
    session,
    runtime,
    searchTrace,
  );
  const result = await getInitialSearchResult(searchId, session.searchSession._id, {
    repository: session.repository,
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: session.now,
    earlyVisibleTarget: runtime.earlyVisibleTarget,
    initialVisibleWaitMs: runtime.initialVisibleWaitMs,
    signal: runtime.signal,
  });

  return {
    queued,
    result: attachSearchTraceResponse(result, traceId, Date.now() - startedAt),
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

function initializeSearchTrace(input: {
  traceId: string;
  rawFilters: unknown;
  normalizedFilters: ReturnType<typeof parseSearchFilters>;
  searchId: string;
  searchSessionId: string;
  timestamp: string;
}): SearchTraceDiagnostics {
  const searchTrace = createEmptySearchTrace(input.traceId);
  const startTrace = emitSearchTraceStage("start", {
    traceId: input.traceId,
    searchId: input.searchId,
    searchSessionId: input.searchSessionId,
    rawFilters: input.rawFilters,
    normalizedFilters: input.normalizedFilters,
    timestamp: input.timestamp,
  });
  const titleAnalysis = analyzeTitle(input.normalizedFilters.title);
  const geoIntent = parseGeoIntentFromFilters(input.normalizedFilters);
  const intentDiagnostics = {
    traceId: input.traceId,
    titleIntent: {
      normalized: titleAnalysis.normalized,
      strippedNormalized: titleAnalysis.strippedNormalized,
      canonicalTitle: titleAnalysis.canonicalTitle,
      family: titleAnalysis.family,
      roleGroup: titleAnalysis.roleGroup,
      primaryConceptId: titleAnalysis.primaryConceptId,
      matchedConceptIds: titleAnalysis.matchedConceptIds,
      candidateConceptIds: titleAnalysis.candidateConceptIds.slice(0, 10),
      meaningfulTokens: titleAnalysis.meaningfulTokens,
    },
    geoIntent: {
      rawInput: geoIntent.rawInput,
      normalizedInput: geoIntent.normalizedInput,
      scope: geoIntent.scope,
      country: geoIntent.country,
      region: geoIntent.region,
      city: geoIntent.city,
      isRemote: geoIntent.isRemote,
      isCountryWide: geoIntent.isCountryWide,
      confidence: geoIntent.confidence,
      searchKeys: geoIntent.searchKeys,
    },
    hasTitleFilter: titleAnalysis.normalized.length > 0,
    hasLocationFilter: geoIntent.scope !== "none",
  };
  console.info("[search:intent]", intentDiagnostics);
  const intentTrace = emitSearchTraceStage("intent", {
    ...buildSearchIntentTracePayload({
      traceId: input.traceId,
      filters: input.normalizedFilters,
    }),
    ...intentDiagnostics,
    searchId: input.searchId,
    searchSessionId: input.searchSessionId,
  });

  return attachSearchTraceStage(
    attachSearchTraceStage(searchTrace, "start", startTrace),
    "intent",
    intentTrace,
  );
}

function attachSearchTraceResponse<
  TResult extends {
    search: { _id: string };
    searchSession?: { _id: string };
    jobs: unknown[];
    totalMatchedCount?: number;
    returnedCount?: number;
    pageSize?: number;
    nextCursor?: number | null;
    hasMore?: boolean;
    diagnostics: CrawlDiagnostics;
    crawlRun: { diagnostics: CrawlDiagnostics };
  },
>(result: TResult, traceId: string, durationMs?: number): TResult {
  const responsePayload = {
    traceId,
    returnedCount: result.returnedCount ?? result.jobs.length,
    totalMatchedCount:
      result.totalMatchedCount ??
      result.diagnostics.searchResponse?.matchedCount ??
      result.diagnostics.session?.totalVisibleResultsCount,
    pageSize: result.pageSize,
    nextCursor: result.nextCursor,
    hasMore: result.hasMore,
    durationMs,
    searchId: result.search._id,
    searchSessionId: result.searchSession?._id,
  };
  const responseTrace = emitSearchTraceStage("response", {
    ...responsePayload,
  });
  console.info("[search:response]", responsePayload);
  const diagnostics = {
    ...result.diagnostics,
    searchTrace: attachSearchTraceStage(
      (result.diagnostics.searchTrace as SearchTraceDiagnostics | undefined) ??
        createEmptySearchTrace(traceId),
      "response",
      responseTrace,
    ),
  };

  return {
    ...result,
    diagnostics,
    crawlRun: {
      ...result.crawlRun,
      diagnostics: {
        ...result.crawlRun.diagnostics,
        searchTrace: diagnostics.searchTrace,
      },
    },
  };
}

async function primeSearchSessionAndMaybeQueueSupplemental(
  session: Awaited<ReturnType<typeof createSearchSession>> | Awaited<ReturnType<typeof createSearchRerunSession>>,
  runtime: JobCrawlerRuntime,
  searchTrace: SearchTraceDiagnostics,
) {
  const indexedSearch = await getIndexedJobsForSearch(
    session.repository,
    session.search.filters,
    { traceId: searchTrace.traceId },
  );
  const indexedJobs = indexedSearch.matches.map(({ job }) => job);

  await session.repository.appendExistingJobsToSearchSession(
    session.searchSession._id,
    session.crawlRun._id,
    indexedJobs.map((job) => job._id),
  );

  const allowRequestTimeSupplementalCrawl =
    runtime.allowRequestTimeSupplementalCrawl === true;
  const allowRequestTimeFreshnessRecovery = allowRequestTimeSupplementalCrawl;
  const targetedReplenishmentOwnerKey = buildTargetedReplenishmentOwnerKey(
    session.search.filters,
  );
  const supplementalDecision = resolveSupplementalCrawlDecision(
    session.search.filters,
    indexedSearch,
    session.searchReuse,
    session.now,
    {
      allowRequestTimeSupplementalCrawl,
      allowRequestTimeFreshnessRecovery,
    },
  );
  console.info("[search:coverage-policy]", {
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    title: session.search.filters.title,
    country: session.search.filters.country ?? null,
    state: session.search.filters.state ?? null,
    city: session.search.filters.city ?? null,
    crawlMode: session.search.filters.crawlMode ?? "balanced",
    indexedCandidateCount: supplementalDecision.indexedCandidateCount,
    indexedMatchedCount: supplementalDecision.indexedJobCount,
    coverageTarget: supplementalDecision.coverageTarget,
    coveragePolicyReason: supplementalDecision.coveragePolicyReason,
    isCoverageSufficient: supplementalDecision.coveragePolicy.isCoverageSufficient,
    latestIndexedJobAgeMs: supplementalDecision.latestIndexedJobAgeMs ?? null,
    titleBroadness: supplementalDecision.coveragePolicy.titleBroadness,
    locationBroadness: supplementalDecision.coveragePolicy.locationBroadness,
    highDemandRole: supplementalDecision.coveragePolicy.highDemandRole,
    targetJobCount: supplementalDecision.targetJobCount,
  });
  const activeTargetedQueueEntry =
    supplementalDecision.shouldQueueTargetedReplenishment
      ? await session.repository.getActiveCrawlQueueEntryForOwner(
          targetedReplenishmentOwnerKey,
        )
      : null;
  const activeQueueAlreadyExists = Boolean(
    activeTargetedQueueEntry &&
      activeTargetedQueueEntry.crawlRunId !== session.crawlRun._id,
  );
  const shouldQueueCurrentRun =
    supplementalDecision.shouldQueue && !activeQueueAlreadyExists;
  const shouldRequestGenericBackgroundIngestion = false;
  const backgroundRefreshSuggested =
    supplementalDecision.requestBackgroundIngestion ||
    supplementalDecision.shouldQueueTargetedReplenishment ||
    supplementalDecision.shouldQueue;
  const indexedCoverageLowOrEmpty =
    supplementalDecision.triggerReason !== "indexed_coverage_sufficient" &&
    supplementalDecision.triggerReason !== "reused_completed_coverage";
  if (indexedCoverageLowOrEmpty) {
    console.info("[search:empty-or-low-index-fallback]", {
      searchId: session.search._id,
      searchSessionId: session.searchSession._id,
      crawlRunId: session.crawlRun._id,
      willReturnImmediately: true,
      willQueueTargetedReplenishment:
        supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
      reason: supplementalDecision.triggerReason,
      indexedCandidateCount: indexedSearch.candidateCount,
      indexedMatchedCount: indexedJobs.length,
      coverageTarget: supplementalDecision.coverageTarget,
      activeQueueAlreadyExists,
    });
  }
  const backgroundIngestion = { status: "not_requested" as const };
  const ingestionDecisionLog = {
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    crawlRunId: session.crawlRun._id,
    title: session.search.filters.title,
    country: session.search.filters.country ?? null,
    indexedCandidateCount: indexedSearch.candidateCount,
    indexedMatchedCount: indexedJobs.length,
    coverageTarget: supplementalDecision.coverageTarget,
    coveragePolicyReason: supplementalDecision.coveragePolicyReason,
    targetJobCount: supplementalDecision.targetJobCount,
    latestIndexedJobAgeMs: supplementalDecision.latestIndexedJobAgeMs ?? null,
    triggerReason: supplementalDecision.triggerReason,
    shouldQueueTargetedReplenishment:
      supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
    shouldRequestGenericBackgroundIngestion,
    backgroundRefreshSuggested,
    backgroundRefreshQueued: shouldQueueCurrentRun,
    shouldRunRequestTimeCrawl: supplementalDecision.shouldRunRequestTimeCrawl,
    activeQueueAlreadyExists,
    shouldQueue: shouldQueueCurrentRun,
    requestBackgroundIngestion: shouldRequestGenericBackgroundIngestion,
    backgroundIngestionStatus: backgroundIngestion.status,
    allowRequestTimeSupplementalCrawl,
    allowRequestTimeFreshnessRecovery,
  };
  console.info("[ingestion:decision]", ingestionDecisionLog);
  console.info("[ingestion:trace:decision]", {
    searchId: session.search._id,
    searchSessionId: session.searchSession._id,
    crawlRunId: session.crawlRun._id,
    indexedCandidateCount: indexedSearch.candidateCount,
    indexedMatchedCount: indexedJobs.length,
    coverageTarget: supplementalDecision.coverageTarget,
    coveragePolicyReason: supplementalDecision.coveragePolicyReason,
    triggerReason: supplementalDecision.triggerReason,
    shouldQueue: shouldQueueCurrentRun,
    shouldQueueTargetedReplenishment:
      supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
    requestBackgroundIngestion: shouldRequestGenericBackgroundIngestion,
    backgroundIngestionStatus: backgroundIngestion.status,
    backgroundIngestion,
    backgroundRefreshSuggested,
    backgroundRefreshQueued: shouldQueueCurrentRun,
    activeQueueAlreadyExists,
    crawlMode: session.search.filters.crawlMode ?? "balanced",
  });
  const indexFirstDecisionTrace = emitSearchTraceStage("index-first-decision", {
    traceId: searchTrace.traceId,
    indexedCandidateCount: indexedSearch.candidateCount,
    indexedMatchedCount: indexedJobs.length,
    minimumIndexedCoverage: supplementalDecision.minimumIndexedCoverage,
    coverageTarget: supplementalDecision.coverageTarget,
    coveragePolicyReason: supplementalDecision.coveragePolicyReason,
    triggerReason: supplementalDecision.triggerReason,
    backgroundIngestionRequested: shouldRequestGenericBackgroundIngestion,
    shouldQueueSupplemental: shouldQueueCurrentRun,
    shouldQueueTargetedReplenishment:
      supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
    activeQueueAlreadyExists,
  });
  const indexedSearchTrace = indexedSearch.searchTrace ?? { traceId: searchTrace.traceId };
  const completedSearchTrace = attachSearchTraceStage(
    {
      ...searchTrace,
      candidateQuery: indexedSearchTrace.candidateQuery,
      candidateDbResult: indexedSearchTrace.candidateDbResult,
      candidateChannelBreakdown: indexedSearchTrace.candidateChannelBreakdown,
      finalFilter: indexedSearchTrace.finalFilter,
    },
    "index-first-decision",
    indexFirstDecisionTrace,
  );
  const primedDiagnostics = buildPrimedSessionDiagnostics(
    supplementalDecision,
    indexedJobs.length,
    backgroundIngestion,
    completedSearchTrace,
    {
      shouldQueueCurrentRun,
      targetedReplenishmentQueued:
        supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
      targetedReplenishmentActive:
        supplementalDecision.shouldQueueTargetedReplenishment &&
        (activeQueueAlreadyExists || shouldQueueCurrentRun),
      activeQueueAlreadyExists,
      backgroundRefreshSuggested,
      backgroundRefreshQueued: shouldQueueCurrentRun,
    },
  );
  session.crawlRun = {
    ...session.crawlRun,
    diagnostics: primedDiagnostics,
  };

  await session.repository.updateCrawlRunProgress(session.crawlRun._id, {
    stage: shouldQueueCurrentRun ? "queued" : "finalizing",
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
    supplementalQueued: shouldQueueCurrentRun,
    targetedReplenishmentQueued:
      supplementalDecision.shouldQueueTargetedReplenishment && !activeQueueAlreadyExists,
    supplementalReason: supplementalDecision.triggerReason,
    backgroundIngestion,
    activeQueueAlreadyExists,
    crawlMode: session.search.filters.crawlMode ?? "balanced",
    reusedExistingSearch: session.searchReuse.reusedExistingSearch,
    previousVisibleJobCount: session.searchReuse.previousVisibleJobCount,
    previousRunStatus: session.searchReuse.previousRunStatus ?? "unknown",
    latestIndexedJobAgeMs: supplementalDecision.latestIndexedJobAgeMs,
    indexedRequestTimeEvaluationCount: indexedSearch.requestTimeEvaluationCount,
    indexedRequestTimeExcludedCount: indexedSearch.requestTimeExcludedCount,
    indexedCandidateChannelBreakdown: indexedSearch.candidateChannelBreakdown,
      indexedExcludedByTitleCount: indexedSearch.excludedByTitleCount,
      indexedExcludedByLocationCount: indexedSearch.excludedByLocationCount,
      indexedExcludedByExperienceCount: indexedSearch.excludedByExperienceCount,
    indexedSearchTimingsMs: indexedSearch.timingsMs,
  });

  if (!shouldQueueCurrentRun) {
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
      requestOwnerKey: supplementalDecision.shouldQueueTargetedReplenishment
        ? targetedReplenishmentOwnerKey
        : runtime.requestOwnerKey,
      ingestionQueueReason: supplementalDecision.triggerReason,
    },
  );
}

type SupplementalDecision = {
  shouldQueue: boolean;
  shouldQueueTargetedReplenishment: boolean;
  shouldRunRequestTimeCrawl: boolean;
  requestBackgroundIngestion: boolean;
  triggerReason: NonNullable<NonNullable<CrawlDiagnostics["session"]>["triggerReason"]>;
  triggerExplanation: string;
  minimumIndexedCoverage: number;
  coverageTarget: number;
  coveragePolicyReason: string;
  coveragePolicy: IndexedCoveragePolicy;
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
  filters: ParsedSearchFilters,
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
  const indexedJobCount = indexedSearch.matches.length;
  const latestIndexedJobAgeMs = resolveLatestIndexedJobAgeMs(
    indexedSearch.matches.map(({ job }) => job),
    now,
  );
  const coveragePolicy = resolveIndexedCoveragePolicy(
    filters,
    indexedSearch,
    env,
    { latestIndexedJobAgeMs },
  );
  const minimumIndexedCoverage = coveragePolicy.coverageTarget;
  const baseDecision = {
    minimumIndexedCoverage,
    coverageTarget: coveragePolicy.coverageTarget,
    coveragePolicyReason: coveragePolicy.reason,
    coveragePolicy,
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

  if (coveragePolicy.isCoverageSufficient) {
    return {
      ...baseDecision,
      shouldQueue: false,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: false,
      requestBackgroundIngestion: false,
      triggerReason: "indexed_coverage_sufficient",
      triggerExplanation: `Indexed results meet the ${minimumIndexedCoverage}-job ${coveragePolicy.reason} target, so request-time crawl stays off.`,
    };
  }

  if (
    previousCoverage.reusedExistingSearch &&
    previousCoverage.previousRunStatus === "completed" &&
    previousCoverage.previousVisibleJobCount > 0 &&
    indexedJobCount >= previousCoverage.previousVisibleJobCount &&
    previousCoverage.previousVisibleJobCount >= minimumIndexedCoverage
  ) {
    return {
      ...baseDecision,
      shouldQueue: false,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: false,
      requestBackgroundIngestion: false,
      triggerReason: "reused_completed_coverage",
      triggerExplanation: "A completed identical search was reused and the indexed session already preserves its previously visible coverage.",
    };
  }

  const staleAfterMs = env.BACKGROUND_INGESTION_STALE_AFTER_MS;
  const freshnessRecoveryEligible =
    indexedJobCount > 0 &&
    typeof latestIndexedJobAgeMs === "number" &&
    latestIndexedJobAgeMs >= staleAfterMs;

  if (
    previousCoverage.reusedExistingSearch &&
    previousCoverage.previousRunStatus &&
    previousCoverage.previousRunStatus !== "completed" &&
    !options.allowRequestTimeFreshnessRecovery
  ) {
    return {
      ...baseDecision,
      shouldQueue: false,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: false,
      requestBackgroundIngestion: true,
      triggerReason: "incomplete_previous_run_background_requested",
      triggerExplanation: "The previous identical session did not complete cleanly and indexed coverage is still below target, so generic background ingestion was requested because targeted providers are unavailable.",
    };
  }

  if (freshnessRecoveryEligible && mode === "deep" && options.allowRequestTimeFreshnessRecovery) {
    return {
      ...baseDecision,
      shouldQueue: true,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: true,
      requestBackgroundIngestion: false,
      triggerReason: "freshness_recovery",
      triggerExplanation: `Deep mode explicitly requested broader retrieval and indexed coverage is below the ${minimumIndexedCoverage}-job target with stale matches, so a bounded supplemental freshness recovery was queued.`,
    };
  }

  if (options.allowRequestTimeSupplementalCrawl && indexedJobCount < minimumIndexedCoverage) {
    return {
      ...baseDecision,
      shouldQueue: true,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: true,
      requestBackgroundIngestion: false,
      triggerReason: "explicit_request_time_recovery",
      triggerExplanation: `An explicit request-time supplemental recovery opt-in was provided and indexed coverage returned ${indexedJobCount} visible jobs, below the ${minimumIndexedCoverage}-job ${coveragePolicy.reason} target.`,
    };
  }

  if (!options.allowRequestTimeFreshnessRecovery) {
    return {
      ...baseDecision,
      shouldQueue: false,
      shouldQueueTargetedReplenishment: false,
      shouldRunRequestTimeCrawl: false,
      requestBackgroundIngestion: true,
      triggerReason:
        indexedJobCount === 0
          ? "indexed_empty_background_requested"
          : freshnessRecoveryEligible
            ? "stale_indexed_coverage_background_requested"
            : "insufficient_indexed_coverage_background_requested",
      triggerExplanation: `Indexed coverage returned ${indexedJobCount} visible jobs, below the ${minimumIndexedCoverage}-job ${coveragePolicy.reason} target, but targeted providers are unavailable so generic background ingestion was requested.`,
    };
  }

  return {
    ...baseDecision,
    shouldQueue: true,
    shouldQueueTargetedReplenishment: true,
    shouldRunRequestTimeCrawl: false,
    requestBackgroundIngestion: false,
    triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
    triggerExplanation: `Indexed coverage returned ${indexedJobCount} visible jobs, below the ${minimumIndexedCoverage}-job ${coveragePolicy.reason} target, so a targeted replenishment run was queued for the current search filters.`,
  };
}

function buildTargetedReplenishmentOwnerKey(filters: ParsedSearchFilters) {
  return `targeted-replenishment:${stableSerializeSearchFilters({
    title: filters.title,
    country: filters.country,
    state: filters.state,
    city: filters.city,
    platforms: [...(filters.platforms ?? [])].sort(),
    crawlMode: filters.crawlMode ?? "balanced",
    experienceLevels: [...(filters.experienceLevels ?? [])].sort(),
  })}`;
}

function stableSerializeSearchFilters(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerializeSearchFilters(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableSerializeSearchFilters(record[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

function normalizeRequestOwnerKey(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function buildPrimedSessionDiagnostics(
  decision: SupplementalDecision,
  indexedJobCount: number,
  backgroundIngestion: BackgroundIngestionRequestDiagnostics,
  searchTrace: SearchTraceDiagnostics,
  queueState: {
    shouldQueueCurrentRun: boolean;
    targetedReplenishmentQueued: boolean;
    targetedReplenishmentActive: boolean;
    activeQueueAlreadyExists: boolean;
    backgroundRefreshSuggested: boolean;
    backgroundRefreshQueued: boolean;
  },
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
      coverageTarget: decision.coverageTarget,
      coveragePolicyReason: decision.coveragePolicyReason,
      targetJobCount: decision.targetJobCount,
      supplementalQueued:
        queueState.shouldQueueCurrentRun || queueState.targetedReplenishmentActive,
      supplementalRunning: queueState.shouldQueueCurrentRun,
      targetedReplenishmentQueued: queueState.targetedReplenishmentQueued,
      targetedReplenishmentActive: queueState.targetedReplenishmentActive,
      activeQueueAlreadyExists: queueState.activeQueueAlreadyExists,
      backgroundRefreshSuggested: queueState.backgroundRefreshSuggested,
      backgroundRefreshQueued: queueState.backgroundRefreshQueued,
      triggerReason: decision.triggerReason,
      triggerExplanation: decision.triggerExplanation,
      reusedExistingSearch: decision.reusedExistingSearch,
      previousVisibleJobCount: decision.previousVisibleJobCount,
      previousRunStatus: decision.previousRunStatus,
      previousFinishedAt: decision.previousFinishedAt,
      latestIndexedJobAgeMs: decision.latestIndexedJobAgeMs,
      backgroundIngestion,
    },
    searchTrace,
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

  if (result.status === "skipped-bootstrap-running") {
    return {
      status: "bootstrap_running",
      reason: result.reason,
      message: result.message,
    };
  }

  if (result.status === "skipped-mongo-transient") {
    return {
      status: "mongo_transient",
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
