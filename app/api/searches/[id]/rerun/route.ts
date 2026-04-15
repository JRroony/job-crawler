import { NextResponse } from "next/server";

import { startSearchRerun } from "@/lib/server/search/service";
import { ResourceNotFoundError } from "@/lib/server/search/errors";

export async function POST(
  request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    const requestOwnerKey = request.headers.get("x-job-crawler-client-id")?.trim() || undefined;
    const { result, queued } = await startSearchRerun(params.id, {
      requestOwnerKey,
    });
    return NextResponse.json(
      {
        ...result,
        queued,
      },
      { status: 201 },
    );
  } catch (error) {
    if (error instanceof ResourceNotFoundError) {
      return NextResponse.json({ error: error.message }, { status: 404 });
    }

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to rerun the crawl.",
      },
      { status: 500 },
    );
  }
}
