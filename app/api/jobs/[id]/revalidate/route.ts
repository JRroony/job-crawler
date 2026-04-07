import { NextResponse } from "next/server";

import { revalidateJob, ResourceNotFoundError } from "@/lib/server/crawler/service";

export async function POST(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const params = await context.params;
    if (!params.id?.trim()) {
      return NextResponse.json({ error: "Job id is required." }, { status: 400 });
    }

    const job = await revalidateJob(params.id);
    return NextResponse.json({ job });
  } catch (error) {
    if (error instanceof ResourceNotFoundError) {
      return NextResponse.json({ error: error.message }, { status: 404 });
    }

    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to revalidate the job link.",
      },
      { status: 500 },
    );
  }
}
