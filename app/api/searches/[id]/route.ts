import { NextResponse } from "next/server";

import {
  abortSearch,
  getSearchDetails,
  getSearchJobDeltas,
} from "@/lib/server/search/session-service";
import { ResourceNotFoundError } from "@/lib/server/search/errors";

export async function GET(
  request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    const { searchParams } = new URL(request.url);
    const mode = searchParams.get("mode");
    const afterParam = searchParams.get("after");
    const indexedAfterParam = searchParams.get("indexedAfter");
    const afterCursor =
      typeof afterParam === "string" && afterParam.trim().length > 0
        ? Number.parseInt(afterParam, 10)
        : 0;
    const afterIndexedCursor =
      typeof indexedAfterParam === "string" && indexedAfterParam.trim().length > 0
        ? Number.parseInt(indexedAfterParam, 10)
        : undefined;
    const result =
      mode === "delta"
        ? await getSearchJobDeltas(
            params.id,
            Number.isFinite(afterCursor) && afterCursor >= 0 ? afterCursor : 0,
            {
              afterIndexedCursor:
                Number.isFinite(afterIndexedCursor ?? Number.NaN) && (afterIndexedCursor ?? -1) >= 0
                  ? afterIndexedCursor
                  : undefined,
            },
          )
        : await getSearchDetails(params.id);
    return NextResponse.json(result);
  } catch (error) {
    if (error instanceof ResourceNotFoundError) {
      return NextResponse.json({ error: error.message }, { status: 404 });
    }

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to load the search.",
      },
      { status: 500 },
    );
  }
}

export async function DELETE(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    const { result, aborted } = await abortSearch(params.id);
    return NextResponse.json({
      ...result,
      aborted,
    });
  } catch (error) {
    if (error instanceof ResourceNotFoundError) {
      return NextResponse.json({ error: error.message }, { status: 404 });
    }

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to stop the search.",
      },
      { status: 500 },
    );
  }
}
