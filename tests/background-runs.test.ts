import { afterEach, describe, expect, it, vi } from "vitest";

import { queueSearchRun } from "@/lib/server/crawler/background-runs";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("durable background run queue", () => {
  it("does not swallow task failure without crawlRun diagnostics", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = "2026-05-01T12:00:00.000Z";
    const search = await repository.createSearch({ title: "Software Engineer" }, now);
    const searchSession = await repository.createSearchSession(search._id, now, {
      status: "running",
    });
    const crawlRun = await repository.createCrawlRun(search._id, now, {
      searchSessionId: searchSession._id,
      stage: "queued",
    });
    await repository.updateSearchLatestSession(search._id, searchSession._id, "running", now);
    await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now);
    await repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: now,
    });

    const queued = await queueSearchRun(
      search._id,
      repository,
      async () => {
        throw new Error("controlled background task failure");
      },
      {
        crawlRunId: crawlRun._id,
        searchSessionId: searchSession._id,
        ownerKey: "test:background-runs",
        queuedAt: now,
        deferStart: true,
      },
    );

    expect(queued).toBe(true);
    const failedRun = await waitForRunCompletion(repository, crawlRun._id);
    const queueEntry = await repository.getCrawlQueueEntryByRunId(crawlRun._id);
    const failedSession = await repository.getSearchSession(searchSession._id);

    expect(failedRun).toMatchObject({
      status: "failed",
      errorMessage: "controlled background task failure",
    });
    expect(failedRun.diagnostics.backgroundPersistence).toMatchObject({
      failedBatches: 1,
      failureSamples: ["background-task: controlled background task failure"],
    });
    expect(queueEntry).toMatchObject({
      status: "failed",
      finishedAt: expect.any(String),
    });
    expect(failedSession).toMatchObject({
      status: "failed",
      finishedAt: expect.any(String),
    });
    expect(errorSpy).toHaveBeenCalledWith(
      "[crawl:background-run]",
      expect.objectContaining({
        crawlRunId: crawlRun._id,
        message: "controlled background task failure",
      }),
    );
  });
});

async function waitForRunCompletion(
  repository: JobCrawlerRepository,
  crawlRunId: string,
  timeoutMs = 2_000,
) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const crawlRun = await repository.getCrawlRun(crawlRunId);
    if (crawlRun && crawlRun.finishedAt && crawlRun.status !== "running") {
      return crawlRun;
    }

    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  throw new Error(`Timed out waiting for crawl run ${crawlRunId} to finish.`);
}
