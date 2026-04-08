import { NextResponse } from "next/server";

import {
  isInputValidationError,
  listRecentSearches,
  runSearchFromFilters,
} from "@/lib/server/crawler/service";

const postSearchLogPrefix = "[api/searches][POST]";

type FlattenedValidationErrors = {
  formErrors: string[];
  fieldErrors: Record<string, string[] | undefined>;
};

export async function GET() {
  try {
    const searches = await listRecentSearches();
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
    const payload: unknown = await request.json();
    console.info(`${postSearchLogPrefix} incoming payload`, payload);

    const result = await runSearchFromFilters(payload);
    return NextResponse.json(result, { status: 201 });
  } catch (error) {
    if (isInputValidationError(error)) {
      const details = error.flatten();
      const readableErrors = buildReadableErrors(details);

      console.error(
        `${postSearchLogPrefix} validation failure`,
        details,
      );

      return NextResponse.json(
        {
          error: "Invalid search filters.",
          details,
          readableErrors,
        },
        { status: 400 },
      );
    }

    console.error(`${postSearchLogPrefix} unexpected failure`, error);

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
