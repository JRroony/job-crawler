import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MongoClient } from "mongodb";

import { triggerRecurringBackgroundIngestion } from "@/lib/server/background/recurring-ingestion";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { toSourceInventoryRecord } from "@/lib/server/discovery/inventory";
import { runSearchFromFilters } from "@/lib/server/search/service";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";

const runRealMongo = process.env.RUN_REAL_MONGO === "true";
const mongoUri = process.env.MONGODB_URI ?? "mongodb://127.0.0.1:27017/job_crawler";
const databaseName = "job_crawler_recurring_validation";

describe("recurring background ingestion with real MongoDB", () => {
  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: 1500,
  });
  let repository: JobCrawlerRepository;

  beforeAll(async () => {
    if (!runRealMongo) {
      return;
    }

    await client.connect();
    const db = client.db(databaseName);
    await db.dropDatabase();
    repository = new JobCrawlerRepository(db as never);
  });

  afterAll(async () => {
    if (!runRealMongo) {
      return;
    }

    await client.db(databaseName).dropDatabase().catch(() => undefined);
    await client.close();
  });

  it.skipIf(!runRealMongo)("persists multi-platform recurring ingestion output into MongoDB and serves indexed results from it", async () => {
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
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://jobs.lever.co/plaid",
          token: "plaid",
          companyHint: "Plaid",
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "platform_registry",
          inventoryRank: 10_000,
        },
      ),
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://jobs.ashbyhq.com/notion",
          token: "notion",
          companyHint: "Notion",
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "platform_registry",
          inventoryRank: 20_000,
        },
      ),
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://workday.wd5.myworkdayjobs.com/Workday",
          token: "workday:workday",
          companyHint: "Workday",
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "platform_registry",
          inventoryRank: 30_000,
        },
      ),
    ]);

    const providers: CrawlProvider[] = ([
      ["greenhouse", "OpenAI"],
      ["lever", "Plaid"],
      ["ashby", "Notion"],
      ["workday", "Workday"],
    ] as const).map(([platform, company]) => ({
      provider: platform,
      supportsSource(source: DiscoveredSource): source is DiscoveredSource {
        return source.platform === platform;
      },
      async crawlSources(_context, sources) {
        const jobs = [
          ["Software Engineer", "se-1"],
          ["Backend Engineer", "se-2"],
          ["Full Stack Engineer", "se-3"],
          ["Data Analyst", "da-1"],
          ["Senior Data Analyst", "da-2"],
          ["Product Manager", "pm-1"],
          ["Senior Product Manager", "pm-2"],
          ["Technical Product Manager", "pm-3"],
          ["Business Analyst", "ba-1"],
        ].map(([title, sourceJobId]) => ({
          title,
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
          sourcePlatform: platform,
          sourceCompanySlug: company.toLowerCase(),
          sourceJobId: `${platform}-${sourceJobId}`,
          sourceUrl: `https://example.com/${platform}/jobs/${sourceJobId}`,
          applyUrl: `https://example.com/${platform}/jobs/${sourceJobId}/apply`,
          canonicalUrl: `https://example.com/${platform}/jobs/${sourceJobId}`,
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {
            sourceCount: sources.length,
            source: "real-mongo-validation",
          },
        }));

        return {
          provider: platform,
          status: "success" as const,
          sourceCount: sources.length,
          fetchedCount: jobs.length,
          matchedCount: jobs.length,
          warningCount: 0,
          jobs,
        };
      },
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers,
      now,
      maxSources: 4,
      refreshInventory: () =>
        repository.listSourceInventory(["greenhouse", "lever", "ashby", "workday"]),
      runTimeoutMs: 5000,
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error(`Unexpected trigger status: ${triggered.status}`);
    }

    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const run = await repository.getCrawlRun(triggered.crawlRunId);
      if (run?.finishedAt && run.status !== "running") {
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    const crawlRun = await repository.getCrawlRun(triggered.crawlRunId);
    const inventory = await repository.listSourceInventory(["greenhouse", "lever", "ashby", "workday"]);
    const storedJobs = await client.db(databaseName).collection("jobs").countDocuments();
    const platformCounts = await client
      .db(databaseName)
      .collection("jobs")
      .aggregate<{ _id: string; count: number }>([
        { $group: { _id: "$sourcePlatform", count: { $sum: 1 } } },
      ])
      .toArray();
    const productResult = await runSearchFromFilters(
      {
        title: "Product Manager",
        country: "United States",
        crawlMode: "fast",
      },
      {
        repository,
        providers: [],
        discovery: {
          async discover() {
            return [];
          },
        },
        fetchImpl: fetch,
        now,
        requestOwnerKey: "real-mongo-validation",
      },
    );

    expect(crawlRun?.status).toBe("completed");
    expect(crawlRun?.diagnostics.inventoryScheduling?.selectedByPlatform).toEqual({
      greenhouse: 1,
      lever: 1,
      ashby: 1,
      workday: 1,
    });
    expect(storedJobs).toBe(36);
    expect(new Set(platformCounts.map((entry) => entry._id))).toEqual(
      new Set(["greenhouse", "lever", "ashby", "workday"]),
    );
    expect(inventory.every((record) => record.lastCrawledAt)).toBe(true);
    expect(inventory.every((record) => record.health === "healthy")).toBe(true);
    expect(productResult.jobs.map((job) => job.title)).toContain("Product Manager");
  }, 15_000);
});
