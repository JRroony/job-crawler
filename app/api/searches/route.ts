import { NextResponse } from "next/server";

import { queueSearchRun } from "@/lib/server/crawler/background-runs";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { getMongoDb } from "@/lib/server/mongodb";
import {
  sanitizeSearchFiltersInput,
  searchFiltersSchema,
  type CrawlRun,
  type JobListing,
  type SearchDocument,
  type SearchFilters,
  type SearchSessionDocument,
} from "@/lib/types";

const searchRequestLogPrefix = "[searches:request]";
const searchValidationLogPrefix = "[searches:validation]";
const searchErrorLogPrefix = "[searches:error]";
const lowCoverageBackgroundRefreshThreshold = 30;
const defaultSearchPageSize = 50;

type FlattenedValidationErrors = {
  formErrors: string[];
  fieldErrors: Record<string, string[] | undefined>;
};

export async function GET() {
  try {
    const { listRecentSearchesForApi } = await import("@/lib/server/search/recent-searches");
    const searches = await listRecentSearchesForApi();
    return NextResponse.json({ searches });
  } catch (error) {
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to load recent searches.",
      },
      { status: 500 },
    );
  }
}

export async function POST(request: Request) {
  try {
    const payload = sanitizeSearchFiltersInput(await request.json());
    const requestOwnerKey = request.headers.get("x-job-crawler-client-id")?.trim() || undefined;
    console.info(`${searchRequestLogPrefix} payload:`, payload);

    const filters = searchFiltersSchema.parse(payload);
    const totalStartedAt = Date.now();
    const db = await getMongoDb({ ensureIndexes: true, requireIndexes: true });
    const repository = new JobCrawlerRepository(db as never);
    const now = new Date().toISOString();
    const search = await repository.createSearch(filters, now);
    const searchSession = await repository.createSearchSession(search._id, now, {
      status: "running",
    });
    const crawlRun = await repository.createCrawlRun(search._id, now, {
      searchSessionId: searchSession._id,
      stage: "queued",
    });

    const dbStartedAt = Date.now();
    const candidateResult = await repository.getIndexedJobCandidatesForSearch(filters);
    const dbSearchMs = Date.now() - dbStartedAt;
    const matchedJobs = candidateResult.jobs;
    const pageJobs = matchedJobs.slice(0, defaultSearchPageSize);
    const indexedCursor = await repository.getIndexedJobDeliveryCursor();
    const updatedSearchSession =
      await repository.appendExistingJobsToSearchSession(
        searchSession._id,
        crawlRun._id,
        pageJobs.map((job) => job._id),
      ) ?? searchSession;
    const deliveryCursor = await repository.getSearchSessionDeliveryCursor(searchSession._id);
    const shouldQueueBackgroundRefresh =
      matchedJobs.length < lowCoverageBackgroundRefreshThreshold;
    const queuedBackgroundRefresh = shouldQueueBackgroundRefresh
      ? await queueDbFirstBackgroundRefresh({
          repository,
          search,
          searchSession: updatedSearchSession,
          crawlRun,
          filters,
          requestOwnerKey,
          queuedAt: now,
        })
      : false;

    const finalCrawlRun = queuedBackgroundRefresh
      ? crawlRun
      : await finalizeDbOnlySearchRun({
          repository,
          search,
          searchSession: updatedSearchSession,
          crawlRun,
          matchedCount: matchedJobs.length,
          finishedAt: new Date().toISOString(),
        });
    const finalSearch = await repository.getSearch(search._id) ?? {
      ...search,
      latestCrawlRunId: finalCrawlRun._id,
      latestSearchSessionId: updatedSearchSession._id,
      lastStatus: finalCrawlRun.status,
      updatedAt: new Date().toISOString(),
    };
    const finalSearchSession =
      await repository.getSearchSession(updatedSearchSession._id) ?? updatedSearchSession;
    const totalSearchMs = Date.now() - totalStartedAt;
    const timing = {
      dbSearchMs,
      providerCrawlMs: 0,
      totalSearchMs,
      returnedCount: pageJobs.length,
      queuedBackgroundRefresh,
    };

    console.info("[searches:db-first-response]", {
      searchId: search._id,
      searchSessionId: finalSearchSession._id,
      crawlRunId: crawlRun._id,
      candidateCount: matchedJobs.length,
      ...timing,
    });

    return NextResponse.json(
      buildDbFirstSearchResponse({
        search: finalSearch,
        searchSession: finalSearchSession,
        crawlRun: finalCrawlRun,
        jobs: pageJobs,
        candidateCount: matchedJobs.length,
        totalMatchedCount: matchedJobs.length,
        indexedCursor,
        deliveryCursor,
        hasMore: matchedJobs.length > pageJobs.length,
        nextCursor: matchedJobs.length > pageJobs.length ? pageJobs.length : null,
        timing,
      }),
      { status: 201 },
    );
  } catch (error) {
    if (isInputValidationError(error)) {
      const details = (error as { flatten(): FlattenedValidationErrors }).flatten();
      const readableErrors = buildReadableErrors(details);

      console.error(searchValidationLogPrefix, details);

      return NextResponse.json(
        {
          error: "Invalid search filters.",
          details,
          readableErrors,
        },
        { status: 400 },
      );
    }

    console.error(searchErrorLogPrefix, error);

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to run the crawl.",
      },
      { status: 500 },
    );
  }
}

async function queueDbFirstBackgroundRefresh(input: {
  repository: JobCrawlerRepository;
  search: SearchDocument;
  searchSession: SearchSessionDocument;
  crawlRun: CrawlRun;
  filters: SearchFilters;
  requestOwnerKey?: string;
  queuedAt: string;
}) {
  await input.repository.updateSearchLatestRun(
    input.search._id,
    input.crawlRun._id,
    "running",
    input.queuedAt,
  );
  await input.repository.updateSearchLatestSession(
    input.search._id,
    input.searchSession._id,
    "running",
    input.queuedAt,
  );

  return queueSearchRun(
    input.search._id,
    input.repository,
    async (signal) => {
      await runBackgroundRefreshWithoutBlockingSearch({
        repository: input.repository,
        search: input.search,
        searchSession: input.searchSession,
        crawlRun: input.crawlRun,
        filters: input.filters,
        signal,
      });
    },
    {
      ownerKey: input.requestOwnerKey,
      crawlRunId: input.crawlRun._id,
      searchSessionId: input.searchSession._id,
      queuedAt: input.queuedAt,
      deferStart: true,
    },
  );
}

async function runBackgroundRefreshWithoutBlockingSearch(input: {
  repository: JobCrawlerRepository;
  search: SearchDocument;
  searchSession: SearchSessionDocument;
  crawlRun: CrawlRun;
  filters: SearchFilters;
  signal: AbortSignal;
}) {
  let status: CrawlRun["status"] = "completed";
  let errorMessage: string | undefined;

  try {
    const { startSearchFromFilters } = await import("@/lib/server/search/service");
    await startSearchFromFilters(input.filters, {
      signal: input.signal,
    });
  } catch (error) {
    status = input.signal.aborted ? "aborted" : "partial";
    errorMessage =
      error instanceof Error
        ? error.message
        : "Background refresh failed after DB-first search response.";
    console.error("[searches:background-refresh-error]", {
      searchId: input.search._id,
      crawlRunId: input.crawlRun._id,
      errorMessage,
    });
  } finally {
    const finishedAt = new Date().toISOString();
    await input.repository.finalizeCrawlRun(input.crawlRun._id, {
      status,
      stage: "finalizing",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      diagnostics: input.crawlRun.diagnostics,
      validationMode: "deferred",
      providerSummary: [],
      errorMessage,
      finishedAt,
    });
    await input.repository.updateSearchSession(input.searchSession._id, {
      status,
      latestCrawlRunId: input.crawlRun._id,
      finishedAt,
      updatedAt: finishedAt,
    });
    await input.repository.updateSearchLatestRun(
      input.search._id,
      input.crawlRun._id,
      status,
      finishedAt,
    );
    await input.repository.updateSearchLatestSession(
      input.search._id,
      input.searchSession._id,
      status,
      finishedAt,
    );
  }
}

async function finalizeDbOnlySearchRun(input: {
  repository: JobCrawlerRepository;
  search: SearchDocument;
  searchSession: SearchSessionDocument;
  crawlRun: CrawlRun;
  matchedCount: number;
  finishedAt: string;
}) {
  await input.repository.finalizeCrawlRun(input.crawlRun._id, {
    status: "completed",
    stage: "finalizing",
    totalFetchedJobs: 0,
    totalMatchedJobs: input.matchedCount,
    dedupedJobs: input.matchedCount,
    diagnostics: input.crawlRun.diagnostics,
    validationMode: "deferred",
    providerSummary: [],
    finishedAt: input.finishedAt,
  });
  await input.repository.updateSearchSession(input.searchSession._id, {
    status: "completed",
    latestCrawlRunId: input.crawlRun._id,
    finishedAt: input.finishedAt,
    updatedAt: input.finishedAt,
  });
  await input.repository.updateSearchLatestRun(
    input.search._id,
    input.crawlRun._id,
    "completed",
    input.finishedAt,
  );
  await input.repository.updateSearchLatestSession(
    input.search._id,
    input.searchSession._id,
    "completed",
    input.finishedAt,
  );

  return input.repository.getCrawlRun(input.crawlRun._id).then((run) => run ?? input.crawlRun);
}

function buildDbFirstSearchResponse(input: {
  search: SearchDocument;
  searchSession: SearchSessionDocument;
  crawlRun: CrawlRun;
  jobs: JobListing[];
  candidateCount: number;
  totalMatchedCount: number;
  indexedCursor: number;
  deliveryCursor: number;
  hasMore: boolean;
  nextCursor: number | null;
  timing: {
    dbSearchMs: number;
    providerCrawlMs: number;
    totalSearchMs: number;
    returnedCount: number;
    queuedBackgroundRefresh: boolean;
  };
}) {
  const diagnostics = {
    ...input.crawlRun.diagnostics,
    jobsBeforeDedupe: input.totalMatchedCount,
    jobsAfterDedupe: input.totalMatchedCount,
    searchResponse: {
      source: "mongodb",
      candidateCount: input.candidateCount,
      finalMatchedCount: input.totalMatchedCount,
      totalMatchedCount: input.totalMatchedCount,
      returnedCount: input.timing.returnedCount,
      dbSearchMs: input.timing.dbSearchMs,
      providerCrawlMs: input.timing.providerCrawlMs,
      totalSearchMs: input.timing.totalSearchMs,
      queuedBackgroundRefresh: input.timing.queuedBackgroundRefresh,
    },
    performance: {
      ...input.crawlRun.diagnostics.performance,
      timeToFirstVisibleResultMs:
        input.jobs.length > 0 ? input.timing.totalSearchMs : undefined,
      stageTimingsMs: {
        ...input.crawlRun.diagnostics.performance.stageTimingsMs,
        providerExecution: 0,
        responseAssembly: input.timing.totalSearchMs,
        total: input.timing.totalSearchMs,
      },
      providerTimingsMs: [],
    },
  };

  return {
    searchId: input.search._id,
    searchSessionId: input.searchSession._id,
    candidateCount: input.candidateCount,
    finalMatchedCount: input.totalMatchedCount,
    totalMatchedCount: input.totalMatchedCount,
    returnedCount: input.timing.returnedCount,
    pageSize: defaultSearchPageSize,
    nextCursor: input.nextCursor,
    hasMore: input.hasMore,
    search: input.search,
    searchSession: input.searchSession,
    crawlRun: {
      ...input.crawlRun,
      diagnostics,
    },
    sourceResults: [],
    jobs: input.jobs,
    diagnostics,
    delivery: {
      mode: "full",
      cursor: input.deliveryCursor,
      indexedCursor: input.indexedCursor,
    },
    queued: input.timing.queuedBackgroundRefresh,
    queuedBackgroundRefresh: input.timing.queuedBackgroundRefresh,
    dbSearchMs: input.timing.dbSearchMs,
    providerCrawlMs: input.timing.providerCrawlMs,
    totalSearchMs: input.timing.totalSearchMs,
    timing: input.timing,
  };
}

function isInputValidationError(error: unknown) {
  return Boolean(
    error &&
      typeof error === "object" &&
      "flatten" in error &&
      typeof (error as { flatten?: unknown }).flatten === "function",
  );
}

function buildReadableErrors(details: FlattenedValidationErrors) {
  const fieldErrors = Object.entries(details.fieldErrors).flatMap(([field, messages]) =>
    (messages ?? [])
      .filter((message) => message.trim().length > 0)
      .map((message) => `${field}: ${message}`),
  );

  const formErrors = details.formErrors.filter((message) => message.trim().length > 0);

  return [...fieldErrors, ...formErrors];
}
