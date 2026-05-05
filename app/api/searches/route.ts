import { NextResponse } from "next/server";

import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { getMongoDb } from "@/lib/server/mongodb";
import {
  isInputValidationError,
  startSearchFromFilters,
  validateSearchFiltersInput,
} from "@/lib/server/search/service";
import { sanitizeSearchFiltersInput } from "@/lib/types";

const searchRequestLogPrefix = "[searches:request]";
const searchValidationLogPrefix = "[searches:validation]";
const searchErrorLogPrefix = "[searches:error]";

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
    validateSearchFiltersInput(payload);
    const requestOwnerKey = request.headers.get("x-job-crawler-client-id")?.trim() || undefined;
    const startedAt = Date.now();
    console.info(`${searchRequestLogPrefix} payload:`, payload);

    const db = await getMongoDb({ ensureIndexes: true, requireIndexes: true });
    const repository = new JobCrawlerRepository(db as never);
    const { result, queued } = await startSearchFromFilters(payload, {
      repository,
      requestOwnerKey,
      ensureIndexes: false,
    });
    const totalSearchMs = Date.now() - startedAt;
    const indexedSearchTimings = result.diagnostics.session?.indexedSearchTimingsMs;
    const providerCrawlMs =
      result.diagnostics.performance?.stageTimingsMs?.providerExecution ?? 0;
    const queuedBackgroundRefresh =
      result.diagnostics.session?.backgroundRefreshQueued === true ||
      result.diagnostics.session?.targetedReplenishmentQueued === true ||
      result.diagnostics.session?.targetedReplenishmentActive === true ||
      queued;
    const timing = {
      dbSearchMs: indexedSearchTimings?.candidateQuery ?? 0,
      providerCrawlMs,
      totalSearchMs,
      returnedCount: result.returnedCount ?? result.jobs.length,
      queuedBackgroundRefresh,
    };

    console.info("[searches:index-first-response]", {
      searchId: result.search._id,
      searchSessionId: result.searchSession?._id ?? result.searchSessionId,
      crawlRunId: result.crawlRun._id,
      candidateCount: result.candidateCount,
      totalMatchedCount: result.totalMatchedCount,
      ...timing,
    });

    return NextResponse.json(
      {
        ...result,
        queued,
        queuedBackgroundRefresh,
        dbSearchMs: timing.dbSearchMs,
        providerCrawlMs,
        totalSearchMs,
        timing,
      },
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

function buildReadableErrors(details: FlattenedValidationErrors) {
  const fieldErrors = Object.entries(details.fieldErrors).flatMap(([field, messages]) =>
    (messages ?? [])
      .filter((message) => message.trim().length > 0)
      .map((message) => `${field}: ${message}`),
  );

  const formErrors = details.formErrors.filter((message) => message.trim().length > 0);

  return [...fieldErrors, ...formErrors];
}
