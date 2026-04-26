import { describe, expect, it, vi } from "vitest";

import { triggerRecurringBackgroundIngestion } from "@/lib/server/background/recurring-ingestion";
import { runSearchIngestionFromFilters } from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import type { ProviderPlatform } from "@/lib/types";
import { FakeDb } from "@/tests/helpers/fake-db";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

describe("provider tiering and timeout policy", () => {
  it("Test A: request-time crawl skips slow providers by default", async () => {
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-20T12:00:00.000Z");
    const calls: ProviderPlatform[] = [];
    const providers = [
      createProvider("greenhouse", async (_context, sources) => {
        calls.push("greenhouse");
        return providerResult("greenhouse", sources.length, [
          createProviderJob({
            title: "Software Engineer",
            sourcePlatform: "greenhouse",
            sourceJobId: "fast-job",
            now,
          }),
        ]);
      }),
      createProvider("workday", async (_context, sources) => {
        calls.push("workday");
        return providerResult("workday", sources.length, []);
      }),
      createProvider("company_page", async (_context, sources) => {
        calls.push("company_page");
        return providerResult("company_page", sources.length, []);
      }),
    ];

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse", "workday", "company_page"],
        crawlMode: "balanced",
      },
      {
        repository,
        providers,
        discovery: createDiscovery([
          createSource("greenhouse"),
          createSource("workday"),
          createSource("company_page"),
        ]),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(calls).toEqual(["greenhouse"]);
    expect(result.sourceResults.map((sourceResult) => sourceResult.provider)).toEqual([
      "greenhouse",
    ]);
    expect(infoSpy).toHaveBeenCalledWith(
      "[provider-tiering:selection]",
      expect.objectContaining({
        crawlMode: "balanced",
        selectedFastProviders: ["greenhouse"],
        selectedSlowProviders: [],
        skippedSlowProviders: ["workday", "company_page"],
        reason: "request_time_crawl_defaults_to_fast_providers",
      }),
    );
  });

  it("Test B: background crawl includes slow providers and uses background timeout values", async () => {
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-20T12:00:00.000Z");
    const calls: ProviderPlatform[] = [];
    await repository.upsertSourceInventory([
      createInventoryRecord("workday", now),
      createInventoryRecord("company_page", now),
    ]);

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [
        createProvider("workday", async (_context, sources) => {
          calls.push("workday");
          return providerResult("workday", sources.length, []);
        }),
        createProvider("company_page", async (_context, sources) => {
          calls.push("company_page");
          return providerResult("company_page", sources.length, []);
        }),
      ],
      now,
      maxSources: 2,
      runTimeoutMs: 123_456,
      providerTimeoutMs: 2_345,
      sourceTimeoutMs: 1_234,
      schedulingIntervalMs: 60_000,
      refreshInventory: () => repository.listSourceInventory(["workday", "company_page"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected background ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);

    expect(calls.sort()).toEqual(["company_page", "workday"]);
    expect(infoSpy).toHaveBeenCalledWith(
      "[crawl:timeout-policy]",
      expect.objectContaining({
        crawlRunId: triggered.crawlRunId,
        crawlMode: "deep",
        globalTimeoutMs: 123_456,
        providerTimeoutMs: 2_345,
        sourceTimeoutMs: 1_234,
        isBackgroundRun: true,
        isRequestTimeRun: false,
      }),
    );
    expect(infoSpy).toHaveBeenCalledWith(
      "[provider-tiering:selection]",
      expect.objectContaining({
        crawlRunId: triggered.crawlRunId,
        selectedSlowProviders: ["workday", "company_page"],
        skippedSlowProviders: [],
      }),
    );
  });

  it("Test C: slow provider timeout preserves fast provider persistence and finalizes partial", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-20T12:00:00.000Z");

    const result = await runDeepTimeoutScenario(repository, now);

    expect(result.crawlRun.status).toBe("partial");
    expect(result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(result.sourceResults).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          provider: "greenhouse",
          status: "success",
          savedCount: 1,
        }),
        expect.objectContaining({
          provider: "workday",
          status: "timed_out",
          sourceCount: 1,
          fetchedCount: 0,
          savedCount: 0,
        }),
      ]),
    );
  });

  it("Test D: provider timeout writes degraded inventory observations with backoff", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-20T12:00:00.000Z");

    await runDeepTimeoutScenario(repository, now);
    const inventory = await repository.listSourceInventory(["workday"]);
    const workday = inventory.find((record) => record.platform === "workday");

    expect(workday).toMatchObject({
      health: "degraded",
      failureCount: 1,
      consecutiveFailures: 1,
      lastCrawledAt: expect.any(String),
      lastFailedAt: expect.any(String),
    });
    expect(workday?.lastFailureReason).toContain("crawl budget");
    expect(Date.parse(workday?.nextEligibleAt ?? "")).toBeGreaterThan(now.getTime());
  });

  it("Test E: observed zero-fetch slow timeout does not erase successful persisted jobs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-20T12:00:00.000Z");

    const result = await runDeepTimeoutScenario(repository, now);
    const persistedJobs = await repository.getJobsByCrawlRun(result.crawlRun._id);
    const sourceResults = await repository.getCrawlSourceResults(result.crawlRun._id);
    const workdayResult = sourceResults.find((sourceResult) => sourceResult.provider === "workday");

    expect(workdayResult).toMatchObject({
      status: "timed_out",
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      savedCount: 0,
    });
    expect(persistedJobs).toHaveLength(1);
    expect(persistedJobs[0]).toMatchObject({
      title: "Software Engineer",
      sourcePlatform: "greenhouse",
    });
    expect(result.crawlRun.providerSummary).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ provider: "greenhouse", savedCount: 1 }),
        expect.objectContaining({ provider: "workday", status: "timed_out" }),
      ]),
    );
  });
});

async function runDeepTimeoutScenario(
  repository: JobCrawlerRepository,
  now: Date,
) {
  return runSearchIngestionFromFilters(
    {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse", "workday"],
      crawlMode: "deep",
    },
    {
      repository,
      providers: [
        createProvider("greenhouse", async (_context, sources) =>
          providerResult("greenhouse", sources.length, [
            createProviderJob({
              title: "Software Engineer",
              sourcePlatform: "greenhouse",
              sourceJobId: "greenhouse-fast-job",
              now,
            }),
          ]),
        ),
        createProvider(
          "workday",
          async () => new Promise<never>(() => undefined),
        ),
      ],
      discovery: createDiscovery([
        createSource("greenhouse"),
        createSource("workday"),
      ]),
      fetchImpl: vi.fn() as unknown as typeof fetch,
      now,
      providerTimeoutMs: 10,
    },
  );
}

function createProvider(
  provider: ProviderPlatform,
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

function providerResult(
  provider: ProviderPlatform,
  sourceCount: number,
  jobs: NormalizedJobSeed[],
) {
  return {
    provider,
    status: "success" as const,
    sourceCount,
    fetchedCount: jobs.length,
    matchedCount: jobs.length,
    warningCount: 0,
    jobs,
  };
}

function createProviderJob(input: {
  title: string;
  sourcePlatform: ProviderPlatform;
  sourceJobId: string;
  now: Date;
}): NormalizedJobSeed {
  const sourceUrl = `https://example.com/${input.sourcePlatform}/jobs/${input.sourceJobId}`;
  return {
    title: input.title,
    company: "OpenAI",
    country: "United States",
    locationText: "Remote - United States",
    resolvedLocation: {
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
      confidence: "high",
      evidence: [
        {
          source: "location_text",
          value: "Remote - United States",
        },
      ],
    },
    sourcePlatform: input.sourcePlatform,
    sourceCompanySlug: "openai",
    sourceJobId: input.sourceJobId,
    sourceUrl,
    applyUrl: `${sourceUrl}/apply`,
    canonicalUrl: sourceUrl,
    discoveredAt: input.now.toISOString(),
    rawSourceMetadata: {},
  };
}

function createDiscovery(sources: DiscoveredSource[]): DiscoveryService {
  return {
    async discover() {
      return sources;
    },
  };
}

function createInventoryRecord(
  platform: "workday" | "company_page",
  now: Date,
): SourceInventoryRecord {
  return toSourceInventoryRecord(createSource(platform), {
    now: now.toISOString(),
    inventoryOrigin: "public_search",
    inventoryRank: platform === "workday" ? 0 : 1,
  });
}

function createSource(platform: "greenhouse" | "workday" | "company_page") {
  if (platform === "greenhouse") {
    return classifySourceCandidate({
      url: "https://boards.greenhouse.io/openai",
      token: "openai",
      companyHint: "OpenAI",
      confidence: "high",
      discoveryMethod: "platform_registry",
    });
  }

  if (platform === "workday") {
    return classifySourceCandidate({
      url: "https://openai.wd1.myworkdayjobs.com/en-US/OpenAI",
      token: "openai",
      companyHint: "OpenAI",
      confidence: "high",
      discoveryMethod: "future_search",
    });
  }

  return classifySourceCandidate({
    url: "https://example.com/careers",
    token: "example",
    companyHint: "Example",
    confidence: "medium",
    discoveryMethod: "future_search",
    pageType: "html_page",
  });
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
