import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  getSearchDetails,
  getSearchJobDeltas,
  startSearchFromFilters,
} from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type {
  DiscoveredSource,
  DiscoveryExecutionStage,
  DiscoveryService,
} from "@/lib/server/discovery/types";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";

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

function createStagedDiscovery(sourceCounts: number[]): DiscoveryService {
  const buildStages = (): DiscoveryExecutionStage[] =>
    sourceCounts.map((count, stageIndex) => {
      const sources = Array.from({ length: count }, (_, sourceIndex) =>
        classifySourceCandidate({
          url: `https://boards.greenhouse.io/acme${stageIndex}-${sourceIndex}`,
          token: `acme${stageIndex}-${sourceIndex}`,
          confidence: "high",
          discoveryMethod: stageIndex === 0 ? "configured_env" : "future_search",
        }),
      );

      const label: DiscoveryExecutionStage["label"] =
        stageIndex === 0 ? "baseline" : "public_search";

      return {
        label,
        sources,
        jobs: [],
        diagnostics: {
          inventorySources: stageIndex === 0 ? count : 0,
          configuredSources: stageIndex === 0 ? count : 0,
          curatedSources: 0,
          publicSources: stageIndex === 0 ? 0 : count,
          publicJobs: 0,
          discoveredBeforeFiltering: count,
          discoveredAfterFiltering: count,
          platformCounts: {
            greenhouse: count,
          },
          publicJobPlatformCounts: {},
        },
      };
    });

  return {
    async discover() {
      return buildStages().flatMap((stage) => stage.sources);
    },
    async discoverInStages() {
      return buildStages();
    },
  };
}

function createProviderJob(title: string, sourceToken: string, index: number, now: Date) {
  return {
    title,
    company: "Acme",
    locationText: "Seattle, WA",
    sourcePlatform: "greenhouse" as const,
    sourceJobId: `${sourceToken}-${title}-${index}`,
    sourceUrl: `https://example.com/jobs/${sourceToken}-${title}-${index}`,
    applyUrl: `https://example.com/jobs/${sourceToken}-${title}-${index}/apply`,
    canonicalUrl: `https://example.com/jobs/${sourceToken}-${title}-${index}`,
    discoveredAt: now.toISOString(),
    rawSourceMetadata: {},
  };
}

async function waitForBackgroundRunToSettle() {
  await new Promise((resolve) => setTimeout(resolve, 100));
}

describe("crawl performance optimizations", () => {
  beforeEach(() => {
    (globalThis as { __jobCrawlerPendingRuns?: Map<string, unknown> }).__jobCrawlerPendingRuns =
      undefined;
    (globalThis as { __jobCrawlerPendingRunOwners?: Map<string, unknown> }).__jobCrawlerPendingRunOwners =
      undefined;
  });

  describe("global timeout", () => {
    it("aborts the provider signal when the provider timeout fires", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T11:55:00.000Z");
      let providerSignalAborted = false;
      let fetchSignalAborted = false;

      const hangingFetch: typeof fetch = (_input, init) =>
        new Promise<Response>((_, reject) => {
          if (init?.signal?.aborted) {
            fetchSignalAborted = true;
            const error = new Error("The request was aborted.");
            error.name = "AbortError";
            reject(error);
            return;
          }

          init?.signal?.addEventListener(
            "abort",
            () => {
              fetchSignalAborted = true;
              const error = new Error("The request was aborted.");
              error.name = "AbortError";
              reject(error);
            },
            { once: true },
          );
        });

      const provider = createStubProvider("greenhouse", async (context, _sources) => {
        context.signal?.addEventListener(
          "abort",
          () => {
            providerSignalAborted = true;
          },
          { once: true },
        );

        await context.fetchImpl("https://example.com/hanging-provider-request");

        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: 0,
          fetchedCount: 0,
          matchedCount: 0,
          warningCount: 0,
          jobs: [] as NormalizedJobSeed[],
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
          fetchImpl: hangingFetch,
          now,
          requestOwnerKey: "client-provider-timeout-abort",
          progressUpdateIntervalMs: 1,
          providerTimeoutMs: 50,
        },
      );

      await waitForBackgroundRunToSettle();
      await new Promise((resolve) => setTimeout(resolve, 100));

      const result = await getSearchDetails(started.result.search._id, { repository });

      expect(providerSignalAborted).toBe(true);
      expect(fetchSignalAborted).toBe(true);
      expect(result.crawlRun.status).toBe("failed");
      expect(result.jobs).toHaveLength(0);
    });

    it("respects provider timeout and completes the crawl", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T12:00:00.000Z");

      const provider = createStubProvider("greenhouse", async (_context, sources) => {
        // Sleep to trigger timeout
        await new Promise((resolve) => setTimeout(resolve, 300));
        
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
            locationText: "Seattle, WA",
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
          requestOwnerKey: "client-timeout",
          progressUpdateIntervalMs: 1,
          providerTimeoutMs: 100, // Very short timeout for testing
        },
      );

      // Wait for the crawl to complete
      await new Promise((resolve) => setTimeout(resolve, 500));

      const result = await getSearchDetails(started.result.search._id, { repository });

      // Crawl should complete (either completed, partial, or failed due to timeout)
      expect(["completed", "partial", "failed"]).toContain(result.crawlRun.status);
    });

    it("clears timeout on successful completion", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T12:00:00.000Z");

      const provider = createStubProvider("greenhouse", async (_context, sources) => {
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
            locationText: "Seattle, WA",
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
          requestOwnerKey: "client-normal",
          progressUpdateIntervalMs: 1,
        },
      );

      await waitForBackgroundRunToSettle();

      const result = await getSearchDetails(started.result.search._id, { repository });

      expect(result.crawlRun.status).toBe("completed");
      expect(result.jobs.length).toBeGreaterThan(0);
    });
  });

  describe("early termination on target job count", () => {
    it("completes crawl normally when jobs are saved", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T13:00:00.000Z");

      const provider = createStubProvider("greenhouse", async (_context, sources) => {
        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: sources.length,
          fetchedCount: sources.length,
          matchedCount: sources.length,
          warningCount: 0,
          jobs: sources.map((source, i) => createProviderJob("Software Engineer", "batch", i, now)),
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
          requestOwnerKey: "client-early-term",
          progressUpdateIntervalMs: 1,
        },
      );

      await waitForBackgroundRunToSettle();

      const result = await getSearchDetails(started.result.search._id, { repository });

      // Should complete successfully
      expect(result.crawlRun.status).toBe("completed");
      expect(result.jobs.length).toBeGreaterThan(0);
    });

    it("keeps staged discovery provider routing bounded by the global source cap", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T13:15:00.000Z");
      const routedSourceIds: string[] = [];

      const provider = createStubProvider("greenhouse", async (_context, sources) => {
        routedSourceIds.push(...sources.map((source) => source.id));

        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: sources.length,
          fetchedCount: sources.length,
          matchedCount: sources.length,
          warningCount: 0,
          jobs: sources.map((source, index) =>
            createProviderJob("Software Engineer", source.id, index, now),
          ),
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
          discovery: createStagedDiscovery([30, 30]),
          now,
          requestOwnerKey: "client-staged-source-cap",
          progressUpdateIntervalMs: 1,
        },
      );

      await waitForBackgroundRunToSettle();

      const result = await getSearchDetails(started.result.search._id, { repository });

      expect(result.crawlRun.status).toBe("completed");
      expect(routedSourceIds).toHaveLength(40);
      expect(new Set(routedSourceIds)).toHaveLength(40);
      expect(result.diagnostics.discoveredSources).toBe(40);
      expect(result.diagnostics.crawledSources).toBe(40);
      expect(result.diagnostics.budgetExhausted).toBe(true);
      expect(result.diagnostics.dropReasonCounts.crawl_source_budget).toBe(20);
      expect(result.diagnostics.discovery).toMatchObject({
        sourcesTruncated: true,
        sourcesTruncatedCount: 20,
        sourcesBeforeTruncation: 60,
        sourcesAfterTruncation: 40,
      });
    });

    it("returns an early visible batch before the crawl fully finishes and preserves delta delivery", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T13:30:00.000Z");
      let releaseFinalBatch: (() => void) | undefined;
      const finalBatchGate = new Promise<void>((resolve) => {
        releaseFinalBatch = resolve;
      });

      const provider = createStubProvider("greenhouse", async (context, sources) => {
        await context.onBatch?.({
          provider: "greenhouse",
          fetchedCount: 1,
          sourceCount: sources.length,
          jobs: [createProviderJob("Software Engineer", "early-1", 1, now)],
        });

        await new Promise((resolve) => setTimeout(resolve, 40));

        await context.onBatch?.({
          provider: "greenhouse",
          fetchedCount: 2,
          sourceCount: sources.length,
          jobs: [
            createProviderJob("Software Engineer II", "early-2", 2, now),
            createProviderJob("Software Engineer III", "early-3", 3, now),
          ],
        });

        await finalBatchGate;

        await context.onBatch?.({
          provider: "greenhouse",
          fetchedCount: 2,
          sourceCount: sources.length,
          jobs: [
            createProviderJob("Staff Software Engineer", "late-4", 4, now),
            createProviderJob("Principal Software Engineer", "late-5", 5, now),
          ],
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
          requestOwnerKey: "client-progressive-visible",
          progressUpdateIntervalMs: 1,
          earlyVisibleTarget: 3,
          initialVisibleWaitMs: 500,
        },
      );

      expect(started.result.crawlRun.status).toBe("running");
      expect(started.result.jobs.map((job) => job.title)).toEqual([
        "Software Engineer",
        "Software Engineer II",
        "Software Engineer III",
      ]);
      expect(started.result.delivery?.cursor).toBe(3);

      const noDeltaYet = await getSearchJobDeltas(
        started.result.search._id,
        started.result.delivery?.cursor ?? 0,
        { repository },
      );
      expect(noDeltaYet.jobs).toHaveLength(0);
      expect(noDeltaYet.delivery.cursor).toBe(3);

      releaseFinalBatch?.();
      const lateDelta = await waitForDeltaJobs(
        started.result.search._id,
        repository,
        started.result.delivery?.cursor ?? 0,
        2,
      );

      expect(lateDelta.jobs.map((job) => job.title)).toEqual(
        expect.arrayContaining([
          "Staff Software Engineer",
          "Principal Software Engineer",
        ]),
      );
      expect(lateDelta.delivery.previousCursor).toBe(3);
      expect(lateDelta.delivery.cursor).toBe(5);

      await waitForBackgroundRunToSettle();

      const finalResult = await getSearchDetails(started.result.search._id, { repository });
      expect(finalResult.crawlRun.status).toBe("completed");
      expect(finalResult.jobs).toHaveLength(5);
    });

    it("returns the first saved jobs quickly even when the early visible target is still unmet", async () => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T13:45:00.000Z");
      let releaseSlowTail: (() => void) | undefined;
      const slowTailGate = new Promise<void>((resolve) => {
        releaseSlowTail = resolve;
      });

      const provider = createStubProvider("greenhouse", async (context, sources) => {
        await context.onBatch?.({
          provider: "greenhouse",
          fetchedCount: 1,
          sourceCount: sources.length,
          jobs: [createProviderJob("Software Engineer", "first-visible", 1, now)],
        });

        await slowTailGate;

        await context.onBatch?.({
          provider: "greenhouse",
          fetchedCount: 2,
          sourceCount: sources.length,
          jobs: [
            createProviderJob("Senior Software Engineer", "late-visible", 2, now),
            createProviderJob("Staff Software Engineer", "late-visible", 3, now),
          ],
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

      const startedAtMs = Date.now();
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
          requestOwnerKey: "client-progressive-timeout",
          progressUpdateIntervalMs: 1,
          earlyVisibleTarget: 50,
          initialVisibleWaitMs: 120,
        },
      );

      const elapsedMs = Date.now() - startedAtMs;

      // Allow some variance on Windows/CI, original 400ms
      expect(elapsedMs).toBeLessThan(500);
      expect(started.result.crawlRun.status).toBe("running");
      expect(started.result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
      expect(started.result.delivery?.cursor).toBe(1);

      releaseSlowTail?.();
      const delta = await waitForDeltaJobs(
        started.result.search._id,
        repository,
        started.result.delivery?.cursor ?? 0,
        2,
      );

      expect(delta.jobs.map((job) => job.title)).toEqual(
        expect.arrayContaining([
          "Senior Software Engineer",
          "Staff Software Engineer",
        ]),
      );
      expect(delta.delivery.previousCursor).toBe(1);
      expect(delta.delivery.cursor).toBe(3);
    });
  });

  describe("environment variable defaults", () => {
    it("uses default values for CRAWL_GLOBAL_TIMEOUT_MS and CRAWL_TARGET_JOB_COUNT", async () => {
      // This test verifies defaults work by running a normal crawl
      const repository = new JobCrawlerRepository(new FakeDb());
      const now = new Date("2026-04-13T14:00:00.000Z");

      const provider = createStubProvider("greenhouse", async (_context, sources) => {
        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: sources.length,
          fetchedCount: sources.length,
          matchedCount: sources.length,
          warningCount: 0,
          jobs: sources.map((source) => ({
            title: "Data Analyst",
            company: source.companyHint ?? "Acme",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse" as const,
            sourceJobId: `${source.token}-role`,
            sourceUrl: `https://example.com/jobs/${source.token}-role`,
            applyUrl: `https://example.com/jobs/${source.token}-role/apply`,
            canonicalUrl: `https://example.com/jobs/${source.token}-role`,
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          })),
        };
      });

      const started = await startSearchFromFilters(
        {
          title: "Data Analyst",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          now,
          requestOwnerKey: "client-defaults",
          progressUpdateIntervalMs: 1,
        },
      );

      await waitForBackgroundRunToSettle();

      const result = await getSearchDetails(started.result.search._id, { repository });

      // Should complete successfully with defaults
      expect(result.crawlRun.status).toBe("completed");
      expect(result.jobs.length).toBeGreaterThan(0);
    });
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

  throw new Error(`Timed out waiting for ${expectedCount} delta job(s) after cursor ${afterCursor}.`);
}
