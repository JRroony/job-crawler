import { afterEach, describe, expect, it, vi } from "vitest";

import { JobCrawlerRepository } from "@/lib/server/db/repository";
import {
  resetBackgroundIngestionSchedulerForTests,
  startRecurringBackgroundIngestionScheduler,
  stopRecurringBackgroundIngestionScheduler,
  triggerRecurringBackgroundIngestion,
} from "@/lib/server/background/recurring-ingestion";
import { collectionNames } from "@/lib/server/db/indexes";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { toSourceInventoryRecord } from "@/lib/server/discovery/inventory";
import type { DiscoveryService, DiscoveredSource } from "@/lib/server/discovery/types";
import { runSearchFromFilters } from "@/lib/server/search/service";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

afterEach(() => {
  stopRecurringBackgroundIngestionScheduler();
  resetBackgroundIngestionSchedulerForTests();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("recurring background ingestion", () => {
  it("starts immediately and repeats on the configured interval", async () => {
    vi.useFakeTimers();
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    let providerCalls = 0;
    const provider = createStubProvider("greenhouse", async () => {
      providerCalls += 1;
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 0,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });

    startRecurringBackgroundIngestionScheduler({
      repository,
      providers: [provider],
      intervalMs: 600_000,
      now: new Date("2026-04-15T12:00:00.000Z"),
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(providerCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(600_000);
    expect(providerCalls).toBe(2);
  });

  it("uses durable queue/control state and prevents duplicate recurring runs", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/openai",
          token: "openai",
          companyHint: "OpenAI",
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "greenhouse_registry",
          inventoryRank: 0,
        },
      ),
    ]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      await new Promise((resolve) => setTimeout(resolve, 80));
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          createProviderJob({
            title: "Software Engineer",
            company: "OpenAI",
            sourcePlatform: "greenhouse",
            sourceJobId: "sw-1",
          }),
        ],
      };
    });

    const first = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      runTimeoutMs: 2_000,
    });
    expect(first.status).toBe("started");

    const second = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      runTimeoutMs: 2_000,
    });
    expect(second.status).toBe("skipped-active");

    if (first.status !== "started") {
      throw new Error("Expected the first recurring run to start.");
    }

    await waitForRunCompletion(repository, first.crawlRunId);

    const crawlRun = await repository.getCrawlRun(first.crawlRunId);
    const queueEntry = await repository.getCrawlQueueEntryByRunId(first.crawlRunId);
    const control = await repository.getCrawlRunControlState(first.crawlRunId);
    const jobs = await repository.getJobsByCrawlRun(first.crawlRunId);

    expect(crawlRun?.status).toBe("completed");
    expect(queueEntry?.status).toBe("completed");
    expect(control?.finishedAt).toBeTruthy();
    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "OpenAI",
      sourcePlatform: "greenhouse",
    });
  });

  it(
    "persists Mongo-style jobs from recurring inventory refresh and returns indexed retrieval results for representative US queries",
    async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/openai",
          token: "openai",
          companyHint: "OpenAI",
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "greenhouse_registry",
          inventoryRank: 0,
        },
      ),
    ]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: 12,
      matchedCount: 12,
      warningCount: 0,
      jobs: [
        createProviderJob({ title: "Software Engineer", sourceJobId: "se-1" }),
        createProviderJob({ title: "Backend Engineer", sourceJobId: "se-2" }),
        createProviderJob({ title: "Full Stack Engineer", sourceJobId: "se-3" }),
        createProviderJob({ title: "Data Analyst", sourceJobId: "da-1" }),
        createProviderJob({ title: "Senior Data Analyst", sourceJobId: "da-2" }),
        createProviderJob({ title: "Product Data Analyst", sourceJobId: "da-3" }),
        createProviderJob({ title: "Business Analyst", sourceJobId: "ba-1" }),
        createProviderJob({ title: "Senior Business Analyst", sourceJobId: "ba-2" }),
        createProviderJob({ title: "Business Systems Analyst", sourceJobId: "ba-3" }),
        createProviderJob({ title: "Product Manager", sourceJobId: "pm-1" }),
        createProviderJob({ title: "Senior Product Manager", sourceJobId: "pm-2" }),
        createProviderJob({ title: "Technical Product Manager", sourceJobId: "pm-3" }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      runTimeoutMs: 2_000,
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);

    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);
    expect(storedJobs).toHaveLength(12);

    const inventory = await repository.listSourceInventory(["greenhouse"]);
    expect(inventory[0]?.lastCrawledAt).toBeTruthy();
    expect(inventory[0]?.health).toBe("healthy");

    const discovery = createEmptyDiscovery();
    const scenarios = [
      "Software Engineer",
      "Data Analyst",
      "Business Analyst",
      "Product Manager",
    ] as const;

    for (const title of scenarios) {
      const result = await runSearchFromFilters(
        {
          title,
          country: "United States",
          crawlMode: "fast",
        },
        {
          repository,
          providers: [],
          discovery,
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now,
          requestOwnerKey: `background-${title}`,
        },
      );

      expect(result.jobs.length).toBeGreaterThanOrEqual(3);
      expect(result.jobs.every((job) => job.resolvedLocation?.isUnitedStates ?? job.country === "United States")).toBe(true);
    }
    },
    15_000,
  );
});

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

function createProviderJob(overrides: {
  title: string;
  sourceJobId: string;
  company?: string;
  sourcePlatform?: "greenhouse";
}) {
  const company = overrides.company ?? "OpenAI";
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceUrl = `https://boards.greenhouse.io/openai/jobs/${overrides.sourceJobId}`;

  return {
    title: overrides.title,
    company,
    country: "United States",
    locationText: "Remote - United States",
    resolvedLocation: {
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
      confidence: "high" as const,
      evidence: [
        {
          source: "remote_hint" as const,
          value: "Remote - United States",
        },
      ],
    },
    sourcePlatform,
    sourceCompanySlug: "openai",
    sourceJobId: overrides.sourceJobId,
    sourceUrl,
    applyUrl: `${sourceUrl}/apply`,
    canonicalUrl: sourceUrl,
    discoveredAt: "2026-04-15T12:00:00.000Z",
    rawSourceMetadata: {
      source: "background-ingestion-test",
      greenhouseBoardToken: "openai",
    },
  };
}

function createEmptyDiscovery(): DiscoveryService {
  return {
    async discover() {
      return [];
    },
  };
}

async function waitForRunCompletion(
  repository: JobCrawlerRepository,
  crawlRunId: string,
  timeoutMs = 5_000,
) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const crawlRun = await repository.getCrawlRun(crawlRunId);
    if (crawlRun && crawlRun.finishedAt && crawlRun.status !== "running") {
      return crawlRun;
    }

    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  throw new Error(`Timed out waiting for crawl run ${crawlRunId} to finish.`);
}
