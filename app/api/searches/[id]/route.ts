import { NextResponse } from "next/server";

import { getSearchDetails, ResourceNotFoundError } from "@/lib/server/crawler/service";

export async function GET(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    const result = await getSearchDetails(params.id);
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
