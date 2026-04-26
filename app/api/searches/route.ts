import { NextResponse } from "next/server";

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
  let isInputValidationError: ((error: unknown) => boolean) | undefined;

  try {
    const payload = sanitizeSearchFiltersInput(await request.json());
    const requestOwnerKey = request.headers.get("x-job-crawler-client-id")?.trim() || undefined;
    console.info(`${searchRequestLogPrefix} payload:`, payload);

    const searchService = await import("@/lib/server/search/service");
    isInputValidationError = searchService.isInputValidationError;

    const { result, queued } = await searchService.startSearchFromFilters(payload, {
      requestOwnerKey,
      signal: request.signal,
    });
    return NextResponse.json(
      {
        ...result,
        queued,
      },
      { status: 201 },
    );
  } catch (error) {
    if (isInputValidationError?.(error)) {
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
