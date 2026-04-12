import { NextResponse } from "next/server";

import { startSearchRerun, ResourceNotFoundError } from "@/lib/server/crawler/service";

export async function POST(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    const { result, queued } = await startSearchRerun(params.id);
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
