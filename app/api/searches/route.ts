import { NextResponse } from "next/server";

import {
  isInputValidationError,
  listRecentSearches,
  runSearchFromFilters,
} from "@/lib/server/crawler/service";

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
    const payload = await request.json();
    const result = await runSearchFromFilters(payload);
    return NextResponse.json(result, { status: 201 });
  } catch (error) {
    if (isInputValidationError(error)) {
      return NextResponse.json(
        {
          error: "Invalid search filters.",
          details: error.flatten(),
        },
        { status: 400 },
      );
    }

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to run the crawl.",
      },
      { status: 500 },
    );
  }
}
