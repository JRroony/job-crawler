import { beforeEach, describe, expect, it } from "vitest";

import {
  abortSearch,
  getSearchDetails,
  getSearchJobDeltas,
  startSearchFromFilters,
} from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";

import { FakeDb } from "@/tests/helpers/fake-db";

function createStubProvider(
  provider: CrawlProvider["provider"],
  crawlSources: CrawlProvider["crawlSources"],
): CrawlProvider {
  return {
    provider,
    supportsSource(source: DiscoveredSource): source is DiscoveredSource {
      return source.platform === provider;
    },
    crawlSources,
  };
}

function createDiscovery(): DiscoveryService {
  return {
    async discover() {
      return [
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/acme",
          token: "acme",
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
      ];
    },
  };
}

function createProviderJob(title: string, sourceToken: string, now: Date) {
  return {
    title,
    company: "Acme",
    locationText: "Seattle, WA",
    sourcePlatform: "greenhouse" as const,
    sourceJobId: `${sourceToken}-${title}`,
    sourceUrl: `https://example.com/jobs/${sourceToken}-${title}`,
    applyUrl: `https://example.com/jobs/${sourceToken}-${title}/apply`,
    canonicalUrl: `https://example.com/jobs/${sourceToken}-${title}`,
    discoveredAt: now.toISOString(),
    rawSourceMetadata: {},
  };
}

async function waitForBackgroundRunToSettle() {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

describe("abortable crawl runs", () => {
  beforeEach(() => {
    (globalThis as { __jobCrawlerPendingRuns?: Map<string, unknown> }).__jobCrawlerPendingRuns =
      undefined;
    (globalThis as { __jobCrawlerPendingRunOwners?: Map<string, unknown> }).__jobCrawlerPendingRunOwners =
      undefined;
  });

  it("aborts the previous in-flight crawl when the same client starts a new search", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-13T12:00:00.000Z");
    const provider = createStubProvider("greenhouse", async (context, sources) => {
      if (context.filters.title === "Software Engineer") {
        await waitForAbort(context.signal);
      }

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length,
        matchedCount: sources.length,
        warningCount: 0,
        jobs: sources.map((source) => ({
          title: context.filters.title,
          company: source.companyHint ?? "Acme",
          locationText: "Seattle, WA",
          sourcePlatform: "greenhouse" as const,
          sourceJobId: `${context.filters.title}-${source.token}`,
          sourceUrl: `https://example.com/jobs/${context.filters.title}-${source.token}`,
          applyUrl: `https://example.com/jobs/${context.filters.title}-${source.token}/apply`,
          canonicalUrl: `https://example.com/jobs/${context.filters.title}-${source.token}`,
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        })),
      };
    });

    const first = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now,
        requestOwnerKey: "client-1",
        progressUpdateIntervalMs: 1,
      },
    );

    const second = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now: new Date("2026-04-13T12:00:01.000Z"),
        requestOwnerKey: "client-1",
        progressUpdateIntervalMs: 1,
      },
    );

    await waitForBackgroundRunToSettle();

    const firstResult = await getSearchDetails(first.result.search._id, { repository });
    const secondResult = await getSearchDetails(second.result.search._id, { repository });

    expect(firstResult.crawlRun.status).toBe("aborted");
    expect(firstResult.jobs).toHaveLength(0);
    expect(secondResult.crawlRun.status).toBe("completed");
    expect(secondResult.jobs.map((job) => job.title)).toEqual(["Data Analyst"]);
  });

  it("stops the active crawl and prevents any further jobs from being saved", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-13T13:00:00.000Z");
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      await waitForAbort(_context.signal);

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length,
        matchedCount: sources.length,
        warningCount: 0,
        jobs: sources.map((source) => ({
          title: "Software Engineer",
          company: source.companyHint ?? "Acme",
          locationText: "Austin, TX",
          sourcePlatform: "greenhouse" as const,
          sourceJobId: `${source.token}-job`,
          sourceUrl: `https://example.com/jobs/${source.token}-job`,
          applyUrl: `https://example.com/jobs/${source.token}-job/apply`,
          canonicalUrl: `https://example.com/jobs/${source.token}-job`,
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        })),
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now,
        requestOwnerKey: "client-stop",
        progressUpdateIntervalMs: 1,
      },
    );

    const stopped = await abortSearch(started.result.search._id, { repository });
    await waitForBackgroundRunToSettle();

    expect(stopped.aborted).toBe(true);
    expect(stopped.result.crawlRun.status).toBe("aborted");
    expect(stopped.result.jobs).toHaveLength(0);

    const latest = await getSearchDetails(started.result.search._id, { repository });
    expect(latest.jobs).toHaveLength(0);
    expect(latest.crawlRun.status).toBe("aborted");
  });

  it("delivers newly saved jobs incrementally before the crawl completes", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-13T14:00:00.000Z");
    let releaseFinalBatch: (() => void) | undefined;
    const finalBatchGate = new Promise<void>((resolve) => {
      releaseFinalBatch = resolve;
    });
    const provider = createStubProvider("greenhouse", async (context, sources) => {
      await context.onBatch?.({
        provider: "greenhouse",
        fetchedCount: 1,
        sourceCount: sources.length,
        jobs: [createProviderJob("Software Engineer", "batch-1", now)],
      });

      await finalBatchGate;

      await context.onBatch?.({
        provider: "greenhouse",
        fetchedCount: 1,
        sourceCount: sources.length,
        jobs: [createProviderJob("Software Engineer II", "batch-2", now)],
      });

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now,
        requestOwnerKey: "client-incremental",
        progressUpdateIntervalMs: 1,
      },
    );

    const firstDelta = await waitForDeltaJobs(started.result.search._id, repository, 0, 1);

    expect(firstDelta.crawlRun.status).toBe("running");
    expect(firstDelta.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(firstDelta.delivery.cursor).toBe(1);

    releaseFinalBatch?.();
    await waitForBackgroundRunToSettle();

    const finalResult = await getSearchDetails(started.result.search._id, { repository });

    expect(finalResult.crawlRun.status).toBe("completed");
    expect(finalResult.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Software Engineer II",
    ]);
  });

  it("stops incremental delivery cleanly after aborting a crawl", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-13T15:00:00.000Z");
    const provider = createStubProvider("greenhouse", async (context, sources) => {
      await context.onBatch?.({
        provider: "greenhouse",
        fetchedCount: 1,
        sourceCount: sources.length,
        jobs: [createProviderJob("Software Engineer", "abort-batch", now)],
      });

      await waitForAbort(context.signal);

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [createProviderJob("Software Engineer II", "should-not-save", now)],
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now,
        requestOwnerKey: "client-abort-delta",
        progressUpdateIntervalMs: 1,
      },
    );

    const firstDelta = await waitForDeltaJobs(started.result.search._id, repository, 0, 1);
    const stopped = await abortSearch(started.result.search._id, { repository });
    await waitForBackgroundRunToSettle();
    const afterAbortDelta = await getSearchJobDeltas(started.result.search._id, firstDelta.delivery.cursor, {
      repository,
    });

    expect(stopped.result.crawlRun.status).toBe("aborted");
    expect(afterAbortDelta.crawlRun.status).toBe("aborted");
    expect(afterAbortDelta.jobs).toHaveLength(0);
    expect(afterAbortDelta.delivery.cursor).toBe(firstDelta.delivery.cursor);

    const latest = await getSearchDetails(started.result.search._id, { repository });
    expect(latest.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
  });

  it("honors persistent crawl-run cancellation and stops without saving later work", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-13T16:00:00.000Z");
    const provider = createStubProvider("greenhouse", async (context, sources) => {
      await context.onBatch?.({
        provider: "greenhouse",
        fetchedCount: 1,
        sourceCount: sources.length,
        jobs: [createProviderJob("Software Engineer", "persistent-batch", now)],
      });

      await waitForAbort(context.signal);

      await context.onBatch?.({
        provider: "greenhouse",
        fetchedCount: 1,
        sourceCount: sources.length,
        jobs: [createProviderJob("Software Engineer II", "should-never-save", now)],
      });

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 2,
        matchedCount: 2,
        warningCount: 0,
        jobs: [],
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        now,
        requestOwnerKey: "client-persistent-stop",
        progressUpdateIntervalMs: 1,
      },
    );

    const firstDelta = await waitForDeltaJobs(started.result.search._id, repository, 0, 1);
    const latestRunId = started.result.search.latestCrawlRunId;
    expect(latestRunId).toBeTruthy();

    await repository.requestCrawlRunCancellation(latestRunId!, {
      reason: "Persistently canceled for test coverage.",
      requestedAt: "2026-04-13T16:00:01.000Z",
    });
    await waitForSearchStatus(started.result.search._id, repository, "aborted");

    const afterCancelDelta = await getSearchJobDeltas(
      started.result.search._id,
      firstDelta.delivery.cursor,
      { repository },
    );
    const latest = await getSearchDetails(started.result.search._id, { repository });

    expect(afterCancelDelta.jobs).toHaveLength(0);
    expect(afterCancelDelta.delivery.cursor).toBe(firstDelta.delivery.cursor);
    expect(latest.crawlRun.status).toBe("aborted");
    expect(latest.crawlRun.cancelReason).toBe("Persistently canceled for test coverage.");
    expect(latest.crawlRun.cancelRequestedAt).toBe("2026-04-13T16:00:01.000Z");
    expect(latest.crawlRun.lastHeartbeatAt).toBeTruthy();
    expect(latest.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
  });
});

async function waitForDeltaJobs(
  searchId: string,
  repository: JobCrawlerRepository,
  afterCursor: number,
  expectedCount: number,
) {
  const deadline = Date.now() + 2_000;

  while (Date.now() < deadline) {
    const delta = await getSearchJobDeltas(searchId, afterCursor, { repository });
    if (delta.jobs.length >= expectedCount) {
      return delta;
    }

    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  throw new Error(`Timed out waiting for ${expectedCount} incremental job(s) after cursor ${afterCursor}.`);
}

async function waitForSearchStatus(
  searchId: string,
  repository: JobCrawlerRepository,
  expectedStatus: "aborted" | "completed" | "partial" | "failed",
) {
  const deadline = Date.now() + 2_000;

  while (Date.now() < deadline) {
    const details = await getSearchDetails(searchId, { repository });
    if (details.crawlRun.status === expectedStatus) {
      return details;
    }

    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  throw new Error(`Timed out waiting for crawl ${searchId} to reach status ${expectedStatus}.`);
}

async function waitForAbort(signal?: AbortSignal) {
  if (signal?.aborted) {
    throw toAbortError(signal.reason);
  }

  await new Promise<never>((_, reject) => {
    const onAbort = () => {
      reject(toAbortError(signal?.reason));
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function toAbortError(reason: unknown) {
  if (reason instanceof Error) {
    return reason;
  }

  const error = new Error("The crawl was aborted.");
  error.name = "AbortError";
  return error;
}
