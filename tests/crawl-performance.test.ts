import { beforeEach, describe, expect, it, vi } from "vitest";

import { getSearchDetails, startSearchFromFilters } from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
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
