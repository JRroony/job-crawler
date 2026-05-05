import { afterEach, describe, expect, it, vi } from "vitest";

import {
  backgroundIngestionOwnerKey,
  selectBackgroundSystemSearchProfiles,
} from "@/lib/server/background/constants";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import {
  resetBackgroundIngestionSchedulerForTests,
  classifyBackgroundRepositoryFailure,
  startRecurringBackgroundIngestionScheduler,
  stopRecurringBackgroundIngestionScheduler,
  triggerRecurringBackgroundIngestion,
} from "@/lib/server/background/recurring-ingestion";
import {
  collectionNames,
  ensureDatabaseIndexes,
  resetDatabaseIndexesForTests,
} from "@/lib/server/db/indexes";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { expandSourceInventory } from "@/lib/server/discovery/service";
import {
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import type { DiscoveryService, DiscoveredSource } from "@/lib/server/discovery/types";
import { runSearchFromFilters } from "@/lib/server/search/service";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { createCompanyPageProvider } from "@/lib/server/providers/company-page";
import { createLeverProvider } from "@/lib/server/providers/lever";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

afterEach(() => {
  stopRecurringBackgroundIngestionScheduler();
  resetBackgroundIngestionSchedulerForTests();
  resetDatabaseIndexesForTests();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("recurring background ingestion", () => {
  it("expands inventory with newly discovered sources before selecting crawl sources", async () => {
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "knownco",
        companyHint: "Known Co",
        lastCrawledAt: "2026-04-15T11:58:00.000Z",
        nextEligibleAt: "2026-04-15T12:45:00.000Z",
        health: "healthy",
      }),
    ]);

    const seenTokens: string[] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(...sources.map((source) => source.token ?? source.id));
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          createProviderJob({
            title: "Data Analyst",
            company: "Expansion Co",
            sourceJobId: "expanded-job",
          }),
        ],
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 2,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      expandInventory: async ({ repository: expansionRepository }) => {
        const before = await expansionRepository.listSourceInventory(["greenhouse"]);
        await expansionRepository.upsertSourceInventory([
          createInventoryRecord({
            token: "expansionco",
            companyHint: "Expansion Co",
            inventoryRank: 60_000,
            health: "unknown",
          }),
        ]);
        const inventory = await expansionRepository.listSourceInventory(["greenhouse"]);

        return createExpansionResult({
          inventory,
          beforeCount: before.length,
          afterExpansionCount: inventory.length,
          newSourceIds: ["greenhouse:expansionco"],
        });
      },
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const inventory = await repository.listSourceInventory(["greenhouse"]);

    expect(inventory.some((record) => record._id === "greenhouse:expansionco")).toBe(true);
    expect(seenTokens).toContain("expansionco");
    expect(seenTokens).not.toContain("knownco");
    expect(crawlRun.diagnostics.inventoryExpansion).toMatchObject({
      beforeCount: 1,
      afterExpansionCount: 2,
      newSourcesAdded: 1,
      newSourceIds: ["greenhouse:expansionco"],
    });
    expect(infoSpy).toHaveBeenCalledWith(
      "[background-ingestion:inventory-expansion]",
      expect.objectContaining({
        selectedSearchTitles: ["test expansion"],
        selectedSearchFilters: [
          {
            title: "test expansion",
            crawlMode: "balanced",
          },
        ],
        searchDiagnostics: [],
      }),
    );
    expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 1,
      jobsUpdated: 0,
      jobsLinkedToRun: 1,
      indexedEventsEmitted: 1,
      failedBatches: 0,
      providerStats: [
        expect.objectContaining({
          provider: "greenhouse",
          insertedCount: 1,
          updatedCount: 0,
          linkedToRunCount: 1,
          indexedEventCount: 1,
        }),
      ],
    });
    expect(db.snapshot(collectionNames.jobs)).toHaveLength(1);
    expect(db.snapshot(collectionNames.indexedJobEvents)).toHaveLength(1);
    expect(db.snapshot(collectionNames.crawlRunJobEvents)).toHaveLength(1);
    expect(db.snapshot(collectionNames.searchSessionJobEvents)).toHaveLength(1);
    expect(await repository.getIndexedJobDeliveryCursor()).toBe(1);
  });

  it("continues recurring Greenhouse ingestion when a mixed batch contains malformed seeds", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "openai",
        companyHint: "OpenAI",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "   ",
          normalizedTitle: "",
          titleNormalized: "",
          sourceJobId: "malformed-title",
        }),
        createProviderJob({
          title: "Software Engineer",
          sourceJobId: "valid-title",
        }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const sourceResults = await repository.getCrawlSourceResults(triggered.crawlRunId);
    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);

    expect(crawlRun.status).toBe("completed");
    expect(storedJobs).toEqual([
      expect.objectContaining({
        title: "Software Engineer",
        sourceJobId: "valid-title",
      }),
    ]);
    expect(crawlRun.diagnostics.dropReasonCounts).toMatchObject({
      seed_invalid_empty_title: 1,
    });
    expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 1,
      failedBatches: 0,
      providerStats: [
        expect.objectContaining({
          provider: "greenhouse",
          fetchedCount: 2,
          matchedCount: 1,
          seedCount: 2,
          insertedCount: 1,
          failedBatches: 0,
          warningCount: 1,
        }),
      ],
    });
    expect(sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      status: "partial",
      fetchedCount: 2,
      matchedCount: 1,
      savedCount: 1,
      warningCount: 1,
    });
  });

  it("does not mark source inventory failing when event sequence persistence retries succeed", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const db = new MongoLikeNullDb();
    await ensureDatabaseIndexes(db);
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "retryco",
        companyHint: "Retry Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
        health: "healthy",
      }),
    ]);
    await db.collection(collectionNames.indexedJobEvents).insertOne({
      _id: "existing-indexed-event",
      jobId: "existing-job",
      crawlRunId: "other-run",
      sequence: 1,
      createdAt: "2026-04-15T11:00:00.000Z",
    });
    await db.collection(collectionNames.counters).insertOne({
      _id: "indexedJobEvents",
      sequence: 0,
      updatedAt: "2026-04-15T11:00:00.000Z",
    });

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          company: "Retry Co",
          sourceJobId: "retry-sequence-job",
        }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);
    const [inventoryRecord] = await repository.listSourceInventory(["greenhouse"]);
    const indexedEventSequences = db
      .snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents)
      .map((event) => Number(event.sequence))
      .sort((left, right) => left - right);

    expect(indexedEventSequences).toEqual([1, 2]);
    expect(inventoryRecord).toMatchObject({
      health: "healthy",
      consecutiveFailures: 0,
    });
    expect(inventoryRecord?.lastFailureReason).toBeUndefined();
    expect(warnSpy).toHaveBeenCalledWith(
      "[db:event-sequence-duplicate-retry]",
      expect.objectContaining({
        eventCollection: "indexedJobEvents",
      }),
    );
    warnSpy.mockRestore();
  });

  it("persists and logs structured Canada expansion filters from the recurring inventory path", async () => {
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("1970-01-01T00:10:00.000Z");
    const requestedQueries: string[] = [];

    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      const query = new URL(url).searchParams.get("q") ?? "";
      requestedQueries.push(query);

      if (
        url.startsWith("https://www.bing.com/search") &&
        query.includes("software engineer")
      ) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://boards.greenhouse.io/canadaco/jobs/123-software-engineer-canada</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `<?xml version="1.0" encoding="utf-8" ?><rss version="2.0"><channel></channel></rss>`,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      return new Response("<html><body></body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const seenTokens: string[] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(...sources.map((source) => source.token ?? source.id));
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length * 3,
        matchedCount: sources.length * 3,
        warningCount: 0,
        jobs: sources.flatMap((source) =>
          ["Software Engineer", "Backend Engineer", "Full Stack Engineer"].map((title) => ({
            ...createProviderJob({
              title,
              company: "Canada Co",
              sourceJobId: `${source.token ?? source.id}-${title.toLowerCase().replace(/\s+/g, "-")}`,
            }),
            country: "Canada",
            locationText: "Remote - Canada",
            resolvedLocation: {
              country: "Canada",
              isRemote: true,
              isUnitedStates: false,
              confidence: "high" as const,
              evidence: [
                {
                  source: "location_text" as const,
                  value: "Remote - Canada",
                },
              ],
            },
          })),
        ),
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 600_000,
      runTimeoutMs: 5_000,
      fetchImpl,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      expandInventory: async ({ repository: expansionRepository, expansionFilters }) => {
        await expansionRepository.upsertSourceInventory([
          createInventoryRecord({
            token: "canadaco",
            companyHint: "Canada Co",
            inventoryRank: 60_000,
            health: "unknown",
            nextEligibleAt: now.toISOString(),
          }),
        ]);
        const inventory = await expansionRepository.listSourceInventory(["greenhouse"]);

        return {
          inventory,
          diagnostics: {
            beforeCount: 0,
            afterRefreshCount: 0,
            afterExpansionCount: inventory.length,
            selectedSearches: expansionFilters.length,
            candidateSources: 1,
            newSourcesAdded: 1,
            selectedSearchTitles: expansionFilters.map((filters) =>
              [filters.title, filters.city, filters.state, filters.country].filter(Boolean).join(" / "),
            ),
            selectedSearchFilters: expansionFilters,
            selectedSourceIds: ["greenhouse:canadaco"],
            newSourceIds: ["greenhouse:canadaco"],
            platformCountsBefore: {},
            platformCountsAfter: { greenhouse: inventory.length },
            searchDiagnostics: expansionFilters.map((filters) => ({
              title: filters.title,
              country: filters.country,
              state: filters.state,
              city: filters.city,
              discoveredSources: 1,
              publicSources: 1,
              publicJobs: 0,
            })),
          },
        };
      },
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const persistedJobs = await repository.getJobsByCrawlRun(triggered.crawlRunId);

    expect(requestedQueries).toEqual([]);
    expect(seenTokens).toEqual(["canadaco"]);
    expect(persistedJobs).toHaveLength(3);
    expect(persistedJobs.map((job) => job.title)).toEqual(
      expect.arrayContaining(["Software Engineer", "Backend Engineer", "Full Stack Engineer"]),
    );
    expect(persistedJobs.every((job) => job.company === "Canada Co")).toBe(true);
    expect(persistedJobs.every((job) => job.country === "Canada")).toBe(true);
    expect(
      persistedJobs.every(
        (job) =>
          job.resolvedLocation?.country === "Canada" &&
          job.resolvedLocation.isUnitedStates === false,
      ),
    ).toBe(true);
    expect(crawlRun.diagnostics.inventoryExpansion).toMatchObject({
      selectedSearchFilters: [
        expect.objectContaining({
          title: "software engineer",
          country: "Canada",
          crawlMode: "balanced",
        }),
      ],
      searchDiagnostics: expect.arrayContaining([
        expect.objectContaining({
          title: "software engineer",
          country: "Canada",
          publicSources: 1,
        }),
      ]),
      newSourceIds: expect.arrayContaining(["greenhouse:canadaco"]),
    });
    expect(infoSpy).toHaveBeenCalledWith(
      "[background-ingestion:inventory-expansion]",
      expect.objectContaining({
        selectedSearchFilters: expect.arrayContaining([
          expect.objectContaining({
            title: "software engineer",
            country: "Canada",
          }),
        ]),
        searchDiagnostics: expect.arrayContaining([
          expect.objectContaining({
            title: "software engineer",
            country: "Canada",
            publicSources: 1,
          }),
        ]),
      }),
    );

    const indexedResult = await runSearchFromFilters(
      {
        title: "software engineer canada",
        crawlMode: "fast",
      },
      {
        repository,
        providers: [],
        discovery: createEmptyDiscovery(),
        fetchImpl,
        now,
        requestOwnerKey: "background-canada-software",
      },
    );

    expect(indexedResult.search.filters).toMatchObject({
      title: "software engineer",
      country: "Canada",
    });
    expect(indexedResult.jobs.map((job) => job.title)).toContain("Software Engineer");
    expect(indexedResult.jobs.every((job) => job.resolvedLocation?.country === "Canada")).toBe(
      true,
    );
    expect(indexedResult.diagnostics.session).toMatchObject({
      indexedResultsCount: indexedResult.jobs.length,
      supplementalQueued: true,
      targetedReplenishmentQueued: true,
      backgroundRefreshSuggested: true,
      backgroundRefreshQueued: true,
      triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      backgroundIngestion: expect.objectContaining({
        status: "not_requested",
      }),
    });
    await waitForRunCompletion(repository, indexedResult.crawlRun._id);
  });

  it("starts immediately and repeats on the configured interval", async () => {
    vi.useFakeTimers();
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "schedulerco",
        companyHint: "Scheduler Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);
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
      maxProfiles: 1,
      now: new Date("2026-04-15T12:00:00.000Z"),
      refreshInventory: async () => [
        createInventoryRecord({
          token: "schedulerco",
          companyHint: "Scheduler Co",
          nextEligibleAt: "2026-04-15T11:00:00.000Z",
        }),
      ],
    });
    expect(infoSpy).toHaveBeenCalledWith(
      "[background-ingestion:scheduler-start]",
      expect.objectContaining({
        started: true,
        reason: "started",
        intervalMs: 600_000,
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(providerCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(600_000);
    expect(providerCalls).toBe(2);
  });

  it("defaults the recurring scheduler interval to five minutes", async () => {
    vi.useFakeTimers();
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "defaultintervalco",
        companyHint: "Default Interval Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);
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
      maxProfiles: 1,
      now: new Date("2026-04-15T12:00:00.000Z"),
      refreshInventory: async () => [
        createInventoryRecord({
          token: "defaultintervalco",
          companyHint: "Default Interval Co",
          nextEligibleAt: "2026-04-15T11:00:00.000Z",
        }),
      ],
    });

    expect(infoSpy).toHaveBeenCalledWith(
      "[background-ingestion:scheduler-start]",
      expect.objectContaining({
        started: true,
        intervalMs: 300_000,
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(providerCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(299_999);
    expect(providerCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(1);
    expect(providerCalls).toBe(2);
  });

  it("honors an initial scheduler delay before the first recurring run", async () => {
    vi.useFakeTimers();
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "delayedco",
        companyHint: "Delayed Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);
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
      initialDelayMs: 120_000,
      maxProfiles: 1,
      now: new Date("2026-04-15T12:00:00.000Z"),
      refreshInventory: async () => [
        createInventoryRecord({
          token: "delayedco",
          companyHint: "Delayed Co",
          nextEligibleAt: "2026-04-15T11:00:00.000Z",
        }),
      ],
    });

    await vi.advanceTimersByTimeAsync(119_999);
    expect(providerCalls).toBe(0);

    await vi.advanceTimersByTimeAsync(1);
    expect(providerCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(600_000);
    expect(providerCalls).toBe(2);
  });

  it("queues multiple profile runs per cycle and divides the cycle source budget across them", async () => {
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");
    const inventory = Array.from({ length: 9 }, (_, index) =>
      createInventoryRecord({
        token: `throughputco${index}`,
        companyHint: `Throughput Co ${index}`,
        inventoryRank: index,
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
        health: "healthy",
      }),
    );

    await repository.upsertSourceInventory(inventory);

    const providerSourceCounts: number[] = [];
    const provider = createStubProvider("greenhouse", async (context, sources) => {
      providerSourceCounts.push(sources.length);
      const titleSlug = String(context.filters.title ?? "profile").replace(/\s+/g, "-");
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length * 2,
        matchedCount: sources.length * 2,
        warningCount: 0,
        jobs: sources.flatMap((source) => [
          createProviderJob({
            title: "Software Engineer",
            company: source.companyHint ?? "Throughput Co",
            sourceJobId: `${source.token}-${titleSlug}-software-engineer`,
          }),
          createProviderJob({
            title: "Data Analyst",
            company: source.companyHint ?? "Throughput Co",
            sourceJobId: `${source.token}-${titleSlug}-data-analyst`,
          }),
        ]),
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxProfiles: 3,
      maxSources: 9,
      schedulingIntervalMs: 60_000,
      providerTimeoutMs: 1_000,
      runTimeoutMs: 5_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    expect(triggered.startedRuns).toHaveLength(3);
    expect(triggered.selectedProfileIds).toHaveLength(3);
    expect(triggered.sourceBudgetPerCycle).toBe(9);
    expect(triggered.sourceBudgetPerProfile).toBe(3);

    const completedRuns = await Promise.all(
      triggered.startedRuns.map((run) => waitForRunCompletion(repository, run.crawlRunId)),
    );

    expect(providerSourceCounts).toEqual([3, 3, 3]);
    expect(db.snapshot(collectionNames.jobs)).toHaveLength(18);
    expect(infoSpy).toHaveBeenCalledWith(
      "[background-ingestion:profiles-selected]",
      expect.objectContaining({
        selectedProfiles: 3,
        selectedProfileIds: triggered.selectedProfileIds,
        sourceBudgetPerCycle: 9,
        sourceBudgetPerProfile: 3,
        providerTimeoutMs: 1_000,
      }),
    );

    for (const crawlRun of completedRuns) {
      expect(crawlRun.diagnostics.backgroundCycle).toMatchObject({
        selectedProfiles: 3,
        selectedProfileIds: triggered.selectedProfileIds,
        startedRuns: 1,
        sourceBudgetPerCycle: 9,
        sourceBudgetPerProfile: 3,
        providerTimeoutMs: 1_000,
      });
      expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
        selectedSources: 3,
      });
      expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
        jobsInserted: 6,
        jobsLinkedToRun: 6,
        providerStats: [
          expect.objectContaining({
            provider: "greenhouse",
            sourceCount: 3,
            fetchedCount: 6,
            matchedCount: 6,
            seedCount: 6,
            insertedCount: 6,
            linkedToRunCount: 6,
          }),
        ],
      });
    }
  });

  it("fails slow providers fast and records throughput diagnostics without consuming the whole run timeout", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "slowproviderco",
        companyHint: "Slow Provider Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);

    const provider = createStubProvider(
      "greenhouse",
      async () =>
        new Promise((resolve) => {
          setTimeout(() => {
            resolve({
              provider: "greenhouse",
              status: "success",
              sourceCount: 1,
              fetchedCount: 1,
              matchedCount: 1,
              warningCount: 0,
              jobs: [
                createProviderJob({
                  title: "Software Engineer",
                  company: "Slow Provider Co",
                  sourceJobId: "slow-provider-job",
                }),
              ],
            });
          }, 1_000);
        }),
    );

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      providerTimeoutMs: 10,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);

    expect(crawlRun.status).toBe("failed");
    expect(crawlRun.diagnostics.providerFailures).toBe(1);
    expect(crawlRun.diagnostics.performance.providerTimingsMs).toEqual([
      expect.objectContaining({
        provider: "greenhouse",
        sourceCount: 1,
        timedOut: true,
      }),
    ]);
    expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 0,
      jobsLinkedToRun: 0,
      providerStats: [
        expect.objectContaining({
          provider: "greenhouse",
          sourceCount: 1,
          fetchedCount: 0,
          matchedCount: 0,
          seedCount: 0,
          failedBatches: 1,
        }),
      ],
    });
  });

  it("persists partial Lever background ingestion when one source times out", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createPlatformInventoryRecord({
        platform: "lever",
        url: "https://jobs.lever.co/fastco",
        token: "fastco",
        companyHint: "Fast Co",
      }),
      createPlatformInventoryRecord({
        platform: "lever",
        url: "https://jobs.lever.co/slowco",
        token: "slowco",
        companyHint: "Slow Co",
      }),
    ]);

    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.includes("/slowco?")) {
        return new Promise<Response>(() => undefined);
      }

      return new Response(
        JSON.stringify([
          {
            id: "lever-background-job",
            text: "Software Engineer",
            hostedUrl: "https://jobs.lever.co/fastco/lever-background-job",
            applyUrl: "https://jobs.lever.co/fastco/lever-background-job/apply",
            categories: {
              location: "Remote, United States",
              commitment: "Full-time",
            },
          },
        ]),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [createLeverProvider()],
      now,
      maxSources: 2,
      maxSourcesPerProvider: 2,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      providerTimeoutMs: 100,
      sourceTimeoutMs: 25,
      fetchImpl,
      refreshInventory: () => repository.listSourceInventory(["lever"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const sourceResults = await repository.getCrawlSourceResults(triggered.crawlRunId);
    const leverResult = sourceResults.find((sourceResult) => sourceResult.provider === "lever");
    const persistedJobs = await repository.getJobsByCrawlRun(triggered.crawlRunId);

    expect(crawlRun.status).not.toBe("failed");
    expect(persistedJobs).toHaveLength(1);
    expect(db.snapshot(collectionNames.jobs)).toHaveLength(1);
    expect(persistedJobs[0]).toMatchObject({
      title: "Software Engineer",
      sourcePlatform: "lever",
      sourceJobId: "lever-background-job",
    });
    expect(leverResult).toMatchObject({
      provider: "lever",
      status: "partial",
      sourceCount: 2,
      fetchedCount: 1,
      matchedCount: 1,
      savedCount: 1,
    });
    expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 1,
      jobsLinkedToRun: 1,
      providerStats: [
        expect.objectContaining({
          provider: "lever",
          fetchedCount: 1,
          matchedCount: 1,
          savedCount: 1,
          failedBatches: 0,
        }),
      ],
    });
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
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      runTimeoutMs: 2_000,
    });
    expect(first.status).toBe("started");

    const second = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
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

  it("allows an isolated controlled recurring run while the normal scheduler owner is active", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    let releaseProvider: () => void = () => undefined;
    const providerGate = new Promise<void>((resolve) => {
      releaseProvider = resolve;
    });
    let providerCalls = 0;

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
      providerCalls += 1;
      const sourceJobId = `isolated-${providerCalls}`;
      await providerGate;
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
            sourceJobId,
          }),
        ],
      };
    });

    const normalRun = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      runTimeoutMs: 2_000,
    });
    expect(normalRun.status).toBe("started");

    const isolatedRun = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      isolationKey: "diagnostic-test",
      runTimeoutMs: 2_000,
    });
    expect(isolatedRun.status).toBe("started");

    releaseProvider();

    if (normalRun.status !== "started" || isolatedRun.status !== "started") {
      throw new Error("Expected both recurring runs to start.");
    }

    await Promise.all([
      waitForRunCompletion(repository, normalRun.crawlRunId),
      waitForRunCompletion(repository, isolatedRun.crawlRunId),
    ]);

    const isolatedSearch = await repository.getSearch(isolatedRun.searchId);
    const isolatedQueueEntry = await repository.getCrawlQueueEntryByRunId(
      isolatedRun.crawlRunId,
    );

    expect(isolatedSearch?.systemProfileId).toContain(":isolated:diagnostic-test");
    expect(isolatedQueueEntry?.ownerKey).toContain(":isolated:");
  });

  it("reports bootstrap/index initialization failures separately from generic Mongo unavailability", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const bootstrapError = new Error(
      "Mongo bootstrap failed during index initialization for jobs_canonical_job_key.",
    );
    const transientBootstrapError = Object.assign(
      new Error(
        'MongoDB bootstrap failed during migration/index initialization: Connection pool for 127.0.0.1:27017 was cleared because another operation failed with: "connection <monitor> to 127.0.0.1:27017 timed out"',
      ),
      { name: "MongoBootstrapError", reason: "mongo_transient" },
    );
    const runningBootstrapError = Object.assign(
      new Error("MongoDB bootstrap/index initialization is already running."),
      { name: "MongoBootstrapError", reason: "bootstrap_running" },
    );
    const genericError = new Error("MongoServerSelectionError: connection refused");

    expect(classifyBackgroundRepositoryFailure(bootstrapError)).toMatchObject({
      reason: "bootstrap_failed",
      phase: "index_initialization",
    });
    expect(classifyBackgroundRepositoryFailure(transientBootstrapError)).toMatchObject({
      reason: "mongo_transient",
      phase: "index_initialization",
    });
    expect(classifyBackgroundRepositoryFailure(runningBootstrapError)).toMatchObject({
      reason: "bootstrap_running",
      phase: "index_initialization",
    });
    expect(classifyBackgroundRepositoryFailure(genericError)).toMatchObject({
      reason: "mongo_unavailable",
      phase: "repository_resolution",
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      resolveRepository: async () => {
        throw bootstrapError;
      },
    });

    expect(triggered).toMatchObject({
      status: "skipped-bootstrap-failed",
      reason: "bootstrap_failed",
      phase: "index_initialization",
      message: bootstrapError.message,
    });
    expect(warnSpy).toHaveBeenCalledWith(
      "[background-ingestion:trigger]",
      expect.objectContaining({
        status: "skipped-bootstrap-failed",
        reason: "bootstrap_failed",
        phase: "index_initialization",
        message: bootstrapError.message,
      }),
    );
  });

  it("skips transient Mongo bootstrap timeouts without permanently poisoning later ingestion", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const transientBootstrapError = Object.assign(
      new Error(
        'MongoDB bootstrap failed during migration/index initialization: Connection pool for 127.0.0.1:27017 was cleared because another operation failed with: "connection <monitor> to 127.0.0.1:27017 timed out"',
      ),
      { name: "MongoBootstrapError", reason: "mongo_transient" },
    );

    const skipped = await triggerRecurringBackgroundIngestion({
      resolveRepository: async () => {
        throw transientBootstrapError;
      },
    });

    expect(skipped).toMatchObject({
      status: "skipped-mongo-transient",
      reason: "mongo_transient",
      phase: "index_initialization",
    });
    expect(warnSpy).toHaveBeenCalledWith(
      "[background-ingestion:trigger]",
      expect.objectContaining({
        status: "skipped-mongo-transient",
        reason: "mongo_transient",
      }),
    );

    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");
    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "acme",
        companyHint: "Acme",
        lastCrawledAt: undefined,
      }),
    ]);

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          sourceJobId: "post-bootstrap-role",
          company: "Acme",
        }),
      ],
    }));

    const started = await triggerRecurringBackgroundIngestion({
      resolveRepository: async () => repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(started.status).toBe("started");
    if (started.status !== "started") {
      throw new Error("Expected background ingestion to start after a later successful bootstrap.");
    }

    const crawlRun = await waitForRunCompletion(repository, started.crawlRunId);
    const jobs = await repository.getJobsByCrawlRun(started.crawlRunId);

    expect(crawlRun.status).toBe("completed");
    expect(jobs.map((job) => job.sourceJobId)).toContain("post-bootstrap-role");
  });

  it("repairs legacy canonicalJobKey data during bootstrap and then starts background ingestion", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await db.collection(collectionNames.jobs).insertOne({
      _id: "legacy-bootstrap-a",
      title: "Software Engineer",
      company: "Acme",
      country: "United States",
      locationText: "Remote - United States",
      sourcePlatform: "greenhouse",
      sourceCompanySlug: "acme",
      sourceJobId: "Role A",
      sourceUrl: "https://example.com/jobs/role-a",
      applyUrl: "https://example.com/jobs/role-a/apply",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      locationNormalized: "remote united states",
      contentFingerprint: "legacy-bootstrap-a",
      canonicalJobKey: "legacy:duplicate",
      sourceLookupKeys: ["greenhouse:acme:role a"],
      crawlRunIds: ["run-bootstrap-a"],
      sourceProvenance: [],
      rawSourceMetadata: {},
    });
    await db.collection(collectionNames.jobs).insertOne({
      _id: "legacy-bootstrap-b",
      title: "Software Engineer",
      company: "Acme",
      country: "United States",
      locationText: "Remote - United States",
      sourcePlatform: "greenhouse",
      sourceCompanySlug: "acme",
      sourceJobId: "Role B",
      sourceUrl: "https://example.com/jobs/role-b",
      applyUrl: "https://example.com/jobs/role-b/apply",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      locationNormalized: "remote united states",
      contentFingerprint: "legacy-bootstrap-b",
      canonicalJobKey: "legacy:duplicate",
      sourceLookupKeys: ["greenhouse:acme:role b"],
      crawlRunIds: ["run-bootstrap-b"],
      sourceProvenance: [],
      rawSourceMetadata: {},
    });
    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "acme",
        companyHint: "Acme",
        lastCrawledAt: undefined,
      }),
    ]);

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          sourceJobId: "bootstrap-fresh-role",
          company: "Acme",
        }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      resolveRepository: async () => {
        await ensureDatabaseIndexes(db);
        return repository;
      },
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected background ingestion to start after bootstrap repair.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const canonicalKeys = db
      .snapshot<Record<string, unknown>>(collectionNames.jobs)
      .map((job) => job.canonicalJobKey);

    expect(crawlRun.status).toBe("completed");
    expect(canonicalKeys).toEqual(
      expect.arrayContaining([
        "platform:greenhouse:acme:role a",
        "platform:greenhouse:acme:role b",
        "platform:greenhouse:openai:bootstrap fresh role",
      ]),
    );
    expect(
      db.collection(collectionNames.jobs).indexes.find(
        (index) => index.name === "jobs_canonical_job_key",
      ),
    ).toMatchObject({
      unique: true,
      partialFilterExpression: {
        canonicalJobKey: {
          $type: "string",
          $gt: "",
        },
      },
    });
  });

  it("selects stale inventory sources before fresh ones and reports freshness skips", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const stale = createInventoryRecord({
      token: "staleco",
      companyHint: "Stale Co",
      inventoryRank: 20,
      lastCrawledAt: "2026-04-14T10:00:00.000Z",
      nextEligibleAt: "2026-04-15T11:00:00.000Z",
      health: "healthy",
    });
    const fresh = createInventoryRecord({
      token: "freshco",
      companyHint: "Fresh Co",
      inventoryRank: 0,
      lastCrawledAt: "2026-04-15T11:58:00.000Z",
      nextEligibleAt: "2026-04-15T12:20:00.000Z",
      health: "healthy",
    });

    await repository.upsertSourceInventory([stale, fresh]);

    const seenTokens: string[] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(...sources.map((source) => source.token ?? source.id));
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
            sourceJobId: "stale-source-job",
          }),
        ],
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);

    expect(seenTokens).toEqual(["staleco"]);
    expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
      eligibleSources: 1,
      selectedSources: 1,
      skippedByReason: {
        freshness_cooldown: 1,
      },
      selectedSourceIds: ["greenhouse:staleco"],
    });
  });

  it("backs off failing sources, deprioritizes degraded sources, and records health-aware diagnostics", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const healthy = createInventoryRecord({
      token: "healthyco",
      companyHint: "Healthy Co",
      lastCrawledAt: "2026-04-14T08:00:00.000Z",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      health: "healthy",
      inventoryRank: 1,
    });
    const degraded = createInventoryRecord({
      token: "degradedco",
      companyHint: "Degraded Co",
      lastCrawledAt: "2026-04-14T08:00:00.000Z",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      health: "degraded",
      consecutiveFailures: 1,
      failureCount: 1,
      inventoryRank: 0,
    });
    const failing = createInventoryRecord({
      token: "failingco",
      companyHint: "Failing Co",
      lastCrawledAt: "2026-04-15T11:55:00.000Z",
      nextEligibleAt: "2026-04-15T13:30:00.000Z",
      health: "failing",
      consecutiveFailures: 3,
      failureCount: 3,
      lastFailedAt: "2026-04-15T11:55:00.000Z",
      inventoryRank: 2,
    });

    await repository.upsertSourceInventory([healthy, degraded, failing]);

    const seenTokens: string[] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(...sources.map((source) => source.token ?? source.id));
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          createProviderJob({
            title: "Platform Engineer",
            sourceJobId: "healthy-job",
          }),
        ],
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);

    expect(seenTokens).toEqual(["healthyco"]);
    expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
      eligibleSources: 2,
      selectedSources: 1,
      skippedByReason: {
        capacity_deprioritized: 1,
        health_backoff: 1,
      },
      selectedByHealth: {
        healthy: 1,
      },
    });
  });

  it("records an OpenAI 404 only on the OpenAI source inventory record", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const openai = createInventoryRecord({
      token: "openai",
      companyHint: "OpenAI",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      inventoryRank: 0,
    });
    const stripe = createInventoryRecord({
      token: "stripe",
      companyHint: "Stripe",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      inventoryRank: 1,
    });

    await repository.upsertSourceInventory([openai, stripe]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "partial",
      sourceCount: sources.length,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 1,
      errorMessage:
        "Greenhouse returned 404 for openai, coinbase, affirm, airbnb, benchling.",
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          company: "Stripe",
          sourceJobId: "stripe-software-engineer",
        }),
      ],
      diagnostics: {
        provider: "greenhouse",
        discoveryCount: sources.length,
        sourceCount: sources.length,
        sourceSucceededCount: 1,
        sourceFailedCount: 1,
        sourceTimedOutCount: 0,
        sourceSkippedCount: 0,
        fetchCount: sources.length,
        fetchedCount: 1,
        parseSuccessCount: 1,
        parseFailureCount: 1,
        rawFetchedCount: 1,
        parsedSeedCount: 1,
        validSeedCount: 1,
        invalidSeedCount: 0,
        dropReasonCounts: {
          board_fetch_failed: 1,
        },
        sampleDropReasons: ["board_fetch_failed"],
        sampleInvalidSeeds: [],
        sourceObservations: sources.map((source) =>
          source.id === openai._id
            ? {
                sourceId: source.id,
                succeeded: false,
                errorType: "source_failed" as const,
                failureReason: "Greenhouse returned 404 for openai.",
              }
            : {
                sourceId: source.id,
                succeeded: true,
                errorType: "none" as const,
              },
        ),
      },
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 2,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);
    const inventoryById = new Map(
      (await repository.listSourceInventory(["greenhouse"])).map((record) => [
        record._id,
        record,
      ]),
    );

    expect(inventoryById.get(openai._id)).toMatchObject({
      consecutiveFailures: 1,
      lastFailureReason: "Greenhouse returned 404 for openai.",
    });
    expect(inventoryById.get(stripe._id)).toMatchObject({
      consecutiveFailures: 0,
    });
    expect(inventoryById.get(stripe._id)?.lastFailureReason).toBeUndefined();
  });

  it("records a timed-out source only on that source inventory record", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const openai = createInventoryRecord({
      token: "openai",
      companyHint: "OpenAI",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      inventoryRank: 0,
    });
    const stripe = createInventoryRecord({
      token: "stripe",
      companyHint: "Stripe",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      inventoryRank: 1,
    });
    const affirm = createInventoryRecord({
      token: "affirm",
      companyHint: "Affirm",
      nextEligibleAt: "2026-04-15T10:00:00.000Z",
      inventoryRank: 2,
    });

    await repository.upsertSourceInventory([openai, stripe, affirm]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "partial",
      sourceCount: sources.length,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 1,
      errorMessage:
        "Provider greenhouse completed with partial source results: 1 source(s) timed out.",
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          company: "OpenAI",
          sourceJobId: "openai-software-engineer",
        }),
        createProviderJob({
          title: "Software Engineer",
          company: "Affirm",
          sourceJobId: "affirm-software-engineer",
        }),
      ],
      diagnostics: {
        provider: "greenhouse",
        discoveryCount: sources.length,
        sourceCount: sources.length,
        sourceSucceededCount: 2,
        sourceFailedCount: 0,
        sourceTimedOutCount: 1,
        sourceSkippedCount: 0,
        fetchCount: sources.length,
        fetchedCount: 2,
        parseSuccessCount: 2,
        parseFailureCount: 1,
        rawFetchedCount: 2,
        parsedSeedCount: 2,
        validSeedCount: 2,
        invalidSeedCount: 0,
        dropReasonCounts: {
          source_timeout: 1,
        },
        sampleDropReasons: ["source_timeout"],
        sampleInvalidSeeds: [],
        sourceObservations: sources.map((source) =>
          source.id === stripe._id
            ? {
                sourceId: source.id,
                succeeded: false,
                errorType: "source_timeout" as const,
                failureReason:
                  "Provider greenhouse source greenhouse:stripe exceeded the 25ms source crawl budget.",
              }
            : {
                sourceId: source.id,
                succeeded: true,
                errorType: "none" as const,
              },
        ),
      },
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 3,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);
    const inventoryById = new Map(
      (await repository.listSourceInventory(["greenhouse"])).map((record) => [
        record._id,
        record,
      ]),
    );

    expect(inventoryById.get(stripe._id)?.lastFailureReason).toContain("greenhouse:stripe");
    expect(inventoryById.get(stripe._id)?.lastFailureReason).not.toContain("openai");
    expect(inventoryById.get(stripe._id)?.lastFailureReason).not.toContain("affirm");
    expect(inventoryById.get(openai._id)?.lastFailureReason).toBeUndefined();
    expect(inventoryById.get(affirm._id)?.lastFailureReason).toBeUndefined();
  });

  it("finalizes a selected provider with zero sources as unsupported", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "openai",
        companyHint: "OpenAI",
        nextEligibleAt: "2026-04-15T10:00:00.000Z",
      }),
    ]);

    const greenhouseProvider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Software Engineer",
          sourceJobId: "greenhouse-openai-software-engineer",
        }),
      ],
    }));
    const leverProvider = createStubProvider("lever", async () => {
      throw new Error("Lever should not run without sources.");
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [greenhouseProvider, leverProvider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);
    const sourceResults = await repository.getCrawlSourceResults(triggered.crawlRunId);

    expect(sourceResults).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          provider: "greenhouse",
          status: "success",
          savedCount: 1,
        }),
        expect.objectContaining({
          provider: "lever",
          status: "unsupported",
          sourceCount: 0,
          errorMessage: "no_sources_for_provider",
        }),
      ]),
    );
    expect(sourceResults.some((result) => result.status === "running")).toBe(false);
  });

  it("keeps background live-batch persistence when a later provider failure occurs", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const persistSpy = vi.spyOn(repository, "persistJobsWithStats");
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "openai",
        companyHint: "OpenAI",
        nextEligibleAt: "2026-04-15T10:00:00.000Z",
        inventoryRank: 0,
      }),
      createInventoryRecord({
        token: "stripe",
        companyHint: "Stripe",
        nextEligibleAt: "2026-04-15T10:00:00.000Z",
        inventoryRank: 1,
      }),
    ]);

    const provider = createStubProvider("greenhouse", async (context, sources) => {
      await context.onBatch?.({
        provider: "greenhouse",
        sourceCount: 1,
        fetchedCount: 1,
        jobs: [
          createProviderJob({
            title: "Software Engineer",
            company: "OpenAI",
            sourceJobId: "openai-live-batch",
          }),
        ],
      });

      expect(sources.map((source) => source.token)).toEqual(["openai", "stripe"]);
      throw new Error("stripe source failed after the live batch was persisted");
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 2,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const sourceResults = await repository.getCrawlSourceResults(triggered.crawlRunId);
    const persistedJobs = await repository.getJobsByCrawlRun(triggered.crawlRunId);

    expect(persistSpy).toHaveBeenCalledWith(
      triggered.crawlRunId,
      expect.arrayContaining([
        expect.objectContaining({
          title: "Software Engineer",
          sourceJobId: "openai-live-batch",
        }),
      ]),
      expect.objectContaining({
        searchSessionId: expect.any(String),
      }),
    );
    expect(crawlRun.status).toBe("partial");
    expect(persistedJobs).toHaveLength(1);
    expect(sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      status: "partial",
      sourceCount: 2,
      fetchedCount: 1,
      matchedCount: 1,
      savedCount: 1,
    });
  });

  it("reserves crawl capacity for never-crawled expansion sources so old inventory cannot starve growth", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const overdueKnownSource = createInventoryRecord({
      token: "overdueco",
      companyHint: "Overdue Co",
      lastCrawledAt: "2026-04-14T08:00:00.000Z",
      nextEligibleAt: "2026-04-15T08:00:00.000Z",
      health: "healthy",
      inventoryRank: 0,
    });

    await repository.upsertSourceInventory([overdueKnownSource]);

    const seenTokens: string[] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(...sources.map((source) => source.token ?? source.id));
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          createProviderJob({
            title: "Product Manager",
            company: "New Coverage Co",
            sourceJobId: "new-coverage-job",
          }),
        ],
      };
    });

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      expandInventory: async ({ repository: expansionRepository }) => {
        const before = await expansionRepository.listSourceInventory(["greenhouse"]);
        await expansionRepository.upsertSourceInventory([
          createInventoryRecord({
            token: "newcoverageco",
            companyHint: "New Coverage Co",
            inventoryRank: 60_000,
            health: "unknown",
          }),
        ]);
        const inventory = await expansionRepository.listSourceInventory(["greenhouse"]);

        return createExpansionResult({
          inventory,
          beforeCount: before.length,
          afterExpansionCount: inventory.length,
          newSourceIds: ["greenhouse:newcoverageco"],
        });
      },
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);

    expect(seenTokens).toEqual(["newcoverageco"]);
    expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
      eligibleSources: 2,
      selectedSources: 1,
      skippedByReason: {
        capacity_deprioritized: 1,
      },
      selectedSourceIds: ["greenhouse:newcoverageco"],
    });
    expect(crawlRun.diagnostics.inventoryExpansion?.newSourcesAdded).toBe(1);
  });

  it("repeated recurring runs grow into new inventory instead of looping only old sources", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const firstRunAt = new Date("2026-04-15T12:00:00.000Z");
    const secondRunAt = new Date("2026-04-15T12:02:00.000Z");

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "oldco",
        companyHint: "Old Co",
        lastCrawledAt: "2026-04-14T08:00:00.000Z",
        nextEligibleAt: "2026-04-15T08:00:00.000Z",
        health: "healthy",
      }),
    ]);

    const seenRuns: string[][] = [];
    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      const tokens = sources.map((source) => source.token ?? source.id);
      seenRuns.push(tokens);

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length,
        matchedCount: sources.length,
        warningCount: 0,
        jobs: tokens.map((token) =>
          createProviderJob({
            title: "Software Engineer",
            company: token === "newco" ? "New Co" : "Old Co",
            sourceJobId: `${token}-role`,
          }),
        ),
      };
    });

    const first = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now: firstRunAt,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });
    expect(first.status).toBe("started");
    if (first.status !== "started") {
      throw new Error("Expected the first recurring run to start.");
    }
    await waitForRunCompletion(repository, first.crawlRunId);

    const second = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now: secondRunAt,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
      expandInventory: async ({ repository: expansionRepository }) => {
        const before = await expansionRepository.listSourceInventory(["greenhouse"]);
        await expansionRepository.upsertSourceInventory([
          createInventoryRecord({
            token: "newco",
            companyHint: "New Co",
            inventoryRank: 60_000,
            health: "unknown",
          }),
        ]);
        const inventory = await expansionRepository.listSourceInventory(["greenhouse"]);

        return createExpansionResult({
          inventory,
          beforeCount: before.length,
          afterExpansionCount: inventory.length,
          newSourceIds: ["greenhouse:newco"],
        });
      },
    });
    expect(second.status).toBe("started");
    if (second.status !== "started") {
      throw new Error("Expected the second recurring run to start.");
    }
    const secondRun = await waitForRunCompletion(repository, second.crawlRunId);

    expect(seenRuns).toEqual([["oldco"], ["newco"]]);
    expect(secondRun.diagnostics.inventoryScheduling?.selectedSourceIds).toEqual([
      "greenhouse:newco",
    ]);
    expect(secondRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 1,
      jobsLinkedToRun: 1,
      indexedEventsEmitted: 1,
    });
    expect(db.snapshot<Record<string, unknown>>(collectionNames.jobs)).toHaveLength(2);
  });

  it("updates inventory metadata across repeated recurring runs and persists jobs idempotently", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const firstRunAt = new Date("2026-04-15T12:00:00.000Z");
    const secondRunAt = new Date("2026-04-15T12:10:00.000Z");
    const source = createInventoryRecord({
      token: "repeatco",
      companyHint: "Repeat Co",
      lastCrawledAt: "2026-04-15T11:00:00.000Z",
      nextEligibleAt: "2026-04-15T11:59:00.000Z",
      health: "healthy",
    });

    await repository.upsertSourceInventory([source]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Staff Engineer",
          sourceJobId: "repeat-job",
        }),
      ],
    }));

    const first = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now: firstRunAt,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });
    expect(first.status).toBe("started");
    if (first.status !== "started") {
      throw new Error("Expected the first recurring run to start.");
    }
    await waitForRunCompletion(repository, first.crawlRunId);

    const second = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now: secondRunAt,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });
    expect(second.status).toBe("started");
    if (second.status !== "started") {
      throw new Error("Expected the second recurring run to start.");
    }
    await waitForRunCompletion(repository, second.crawlRunId);

    const inventory = await repository.listSourceInventory(["greenhouse"]);
    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]?.crawlRunIds).toHaveLength(2);
    expect(inventory[0]?._id).toBe("greenhouse:repeatco");
    expect(inventory[0]?.health).toBe("healthy");
    expect(Date.parse(String(inventory[0]?.lastCrawledAt))).toBeGreaterThanOrEqual(
      secondRunAt.getTime(),
    );
    expect(Date.parse(String(inventory[0]?.lastSucceededAt))).toBeGreaterThanOrEqual(
      secondRunAt.getTime(),
    );
    expect(Date.parse(String(inventory[0]?.nextEligibleAt))).toBeGreaterThan(secondRunAt.getTime());
  });

  it("persists Canada-oriented jobs from multi-platform recurring inventory selection", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const intervalMs = 600_000;
    const scheduleBaseMs = Date.parse("2026-04-15T00:00:00.000Z");
    const canadaProfileIndex = Array.from({ length: 1000 }, (_, cycle) => cycle).find((cycle) => {
      const profile = selectBackgroundSystemSearchProfiles({
        now: new Date(scheduleBaseMs + cycle * intervalMs),
        intervalMs,
        maxProfiles: 1,
      })[0];
      return profile?.filters.country === "Canada";
    });
    if (typeof canadaProfileIndex !== "number") {
      throw new Error("Expected at least one Canada background ingestion profile.");
    }
    const now = new Date(scheduleBaseMs + canadaProfileIndex * intervalMs);

    await repository.upsertSourceInventory([
      createPlatformInventoryRecord({
        platform: "greenhouse",
        url: "https://boards.greenhouse.io/canadagreenhouse",
        token: "canadagreenhouse",
        companyHint: "Canada Greenhouse",
      }),
      createPlatformInventoryRecord({
        platform: "lever",
        url: "https://jobs.lever.co/canadalever",
        token: "canadalever",
        companyHint: "Canada Lever",
      }),
      createPlatformInventoryRecord({
        platform: "ashby",
        url: "https://jobs.ashbyhq.com/canadaashby",
        token: "canadaashby",
        companyHint: "Canada Ashby",
      }),
      createPlatformInventoryRecord({
        platform: "workday",
        url: "https://canadaworkday.wd1.myworkdayjobs.com/External",
        token: "canadaworkday:external",
        companyHint: "Canada Workday",
      }),
    ]);

    const providers = (["greenhouse", "lever", "ashby", "workday"] as const).map((platform) =>
      createStubProvider(platform, async (_context, sources) => ({
        provider: platform,
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length,
        matchedCount: sources.length,
        warningCount: 0,
        jobs: sources.map((source) =>
          createProviderJob({
            title: "Software Engineer",
            company: source.companyHint ?? platform,
            sourcePlatform: platform,
            sourceJobId: `${platform}-canada-role`,
            country: "Canada",
            locationText: "Toronto, ON, Canada",
          }),
        ),
      })),
    );

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers,
      now,
      maxSources: 4,
      schedulingIntervalMs: intervalMs,
      runTimeoutMs: 2_000,
      refreshInventory: () =>
        repository.listSourceInventory(["greenhouse", "lever", "ashby", "workday"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);
    const storedPlatforms = new Set(storedJobs.map((job) => job.sourcePlatform));

    expect(crawlRun.diagnostics.systemProfile?.filters.country).toBe("Canada");
    expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
      selectedSources: 4,
      inventoryByPlatform: {
        greenhouse: 1,
        lever: 1,
        ashby: 1,
        workday: 1,
      },
      selectedByPlatform: {
        greenhouse: 1,
        lever: 1,
        ashby: 1,
        workday: 1,
      },
      platformSelectionBudgets: {
        greenhouse: 1,
        lever: 1,
        ashby: 1,
        workday: 1,
      },
    });
    expect(storedPlatforms).toEqual(new Set(["greenhouse", "lever", "ashby", "workday"]));
    expect(storedJobs.every((job) => job.country === "Canada")).toBe(true);
  });

  it("persists company-hosted career page jobs from recurring inventory selection", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");

    await repository.upsertSourceInventory([
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://careers.globex.example/jobs",
          companyHint: "Globex",
          pageType: "html_page",
          confidence: "medium",
          discoveryMethod: "manual_config",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "manual_config",
          inventoryRank: 0,
        },
      ),
    ]);

    const fetchImpl = vi.fn(async () =>
      new Response(
        `
          <html>
            <body>
              <script id="__NEXT_DATA__" type="application/json">
                {
                  "props": {
                    "pageProps": {
                      "jobs": [
                        {
                          "id": "pm-berlin",
                          "title": "Product Manager",
                          "url": "/jobs/product-manager-berlin",
                          "location": { "city": "Berlin", "country": "Germany" },
                          "description": "Own product discovery and delivery."
                        },
                        {
                          "id": "se-toronto",
                          "title": "Software Engineer",
                          "url": "/jobs/software-engineer-toronto",
                          "location": { "city": "Toronto", "region": "ON", "country": "Canada" },
                          "description": "Build distributed systems."
                        }
                      ]
                    }
                  }
                }
              </script>
            </body>
          </html>
        `,
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      )) as unknown as typeof fetch;

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [createCompanyPageProvider()],
      now,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["company_page"]),
      fetchImpl,
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);
    const inventory = await repository.listSourceInventory(["company_page"]);

    expect(crawlRun.diagnostics.backgroundPersistence).toMatchObject({
      jobsInserted: 2,
      jobsLinkedToRun: 2,
      indexedEventsEmitted: 2,
    });
    expect(storedJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: "Product Manager",
          sourcePlatform: "company_page",
          sourceUrl: "https://careers.globex.example/jobs/product-manager-berlin",
          country: "Germany",
          rawSourceMetadata: expect.objectContaining({
            companyPageExtraction: "embedded_json",
          }),
        }),
        expect.objectContaining({
          title: "Software Engineer",
          sourcePlatform: "company_page",
          sourceUrl: "https://careers.globex.example/jobs/software-engineer-toronto",
          country: "Canada",
        }),
      ]),
    );
    expect(storedJobs[0]?.sourceProvenance).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourcePlatform: "company_page",
          sourceUrl: expect.stringContaining("careers.globex.example/jobs/"),
        }),
      ]),
    );
    expect(inventory[0]).toMatchObject({
      platform: "company_page",
      health: "healthy",
      lastCrawledAt: expect.any(String),
    });
  });

  it("recovers stale recurring runs and starts a fresh crawl without losing durable control", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const staleStartedAt = "2026-04-15T09:00:00.000Z";
    const staleHeartbeatAt = "2026-04-15T09:15:00.000Z";
    const [systemProfile] = selectBackgroundSystemSearchProfiles({
      now,
      intervalMs: 60_000,
      maxProfiles: 1,
    });
    if (!systemProfile) {
      throw new Error("Expected a recurring ingestion system profile.");
    }
    const search = await repository.createSearch(systemProfile.filters, staleStartedAt, {
      systemProfileId: systemProfile.id,
      systemProfileLabel: systemProfile.label,
    });
    const session = await repository.createSearchSession(search._id, staleStartedAt, {
      status: "running",
    });
    const staleRun = await repository.createCrawlRun(search._id, staleStartedAt, {
      stage: "crawling",
      validationMode: "deferred",
      searchSessionId: session._id,
    });

    await repository.enqueueCrawlRun({
      crawlRunId: staleRun._id,
      searchId: search._id,
      searchSessionId: session._id,
      ownerKey: backgroundIngestionOwnerKey,
      queuedAt: staleStartedAt,
    });
    await repository.markCrawlRunStarted(staleRun._id, {
      startedAt: staleStartedAt,
      ownerKey: backgroundIngestionOwnerKey,
      workerId: "worker:stale",
    });
    await repository.heartbeatCrawlRun(staleRun._id, staleHeartbeatAt);

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "recoveryco",
        companyHint: "Recovery Co",
        lastCrawledAt: "2026-04-15T10:00:00.000Z",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
        health: "healthy",
      }),
    ]);

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Reliability Engineer",
          sourceJobId: "recovery-job",
        }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      staleAfterMs: 30 * 60_000,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected stale-run recovery to start a new crawl.");
    }

    await waitForRunCompletion(repository, triggered.crawlRunId);

    const recoveredRun = await repository.getCrawlRun(staleRun._id);
    const recoveredQueueEntry = await repository.getCrawlQueueEntryByRunId(staleRun._id);
    const newRun = await repository.getCrawlRun(triggered.crawlRunId);

    expect(recoveredRun?.status).toBe("aborted");
    expect(recoveredQueueEntry?.status).toBe("aborted");
    expect(newRun?.status).toBe("completed");
    expect(triggered.crawlRunId).not.toBe(staleRun._id);
  });

  it("recovers stale active background queue entries before selecting the next profile", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const staleStartedAt = "2026-04-15T09:00:00.000Z";
    const staleHeartbeatAt = "2026-04-15T09:10:00.000Z";
    const [systemProfile] = selectBackgroundSystemSearchProfiles({
      now,
      intervalMs: 60_000,
      maxProfiles: 1,
    });
    if (!systemProfile) {
      throw new Error("Expected a recurring ingestion system profile.");
    }

    const staleSearch = await repository.createSearch(
      { title: "stale blocked profile", country: "United States", crawlMode: "balanced" },
      staleStartedAt,
      {
        systemProfileId: "stale-blocking-profile",
        systemProfileLabel: "Stale Blocking Profile",
      },
    );
    const staleSession = await repository.createSearchSession(staleSearch._id, staleStartedAt, {
      status: "running",
    });
    const staleRun = await repository.createCrawlRun(staleSearch._id, staleStartedAt, {
      stage: "discovering",
      validationMode: "deferred",
      searchSessionId: staleSession._id,
    });
    await repository.enqueueCrawlRun({
      crawlRunId: staleRun._id,
      searchId: staleSearch._id,
      searchSessionId: staleSession._id,
      ownerKey: backgroundIngestionOwnerKey,
      queuedAt: staleStartedAt,
    });
    await repository.markCrawlRunStarted(staleRun._id, {
      startedAt: staleStartedAt,
      ownerKey: backgroundIngestionOwnerKey,
      workerId: "worker:globally-stale",
    });
    await repository.heartbeatCrawlRun(staleRun._id, staleHeartbeatAt);

    await repository.upsertSourceInventory([
      createInventoryRecord({
        token: "globalrecoveryco",
        companyHint: "Global Recovery Co",
        nextEligibleAt: "2026-04-15T11:00:00.000Z",
      }),
    ]);

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: systemProfile.filters.title,
          sourceJobId: "global-recovery-job",
        }),
      ],
    }));

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      staleAfterMs: 30 * 60_000,
      maxSources: 1,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected global stale recovery to unblock the cycle.");
    }

    const completedRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const recoveredRun = await repository.getCrawlRun(staleRun._id);
    const recoveredQueueEntry = await repository.getCrawlQueueEntryByRunId(staleRun._id);

    expect(recoveredRun?.status).toBe("aborted");
    expect(recoveredQueueEntry?.status).toBe("aborted");
    expect(completedRun.diagnostics.backgroundHealth).toMatchObject({
      recoveredStaleQueueEntries: 1,
      recoveredStaleCrawlRuns: 1,
      staleActiveQueueEntries: 0,
    });
    expect(triggered.crawlRunId).not.toBe(staleRun._id);
  });

  it("skips actively leased inventory sources and caps unknown company pages behind ATS sources", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:00:00.000Z");
    const greenhouse = createInventoryRecord({
      token: "efficientats",
      companyHint: "Efficient ATS",
      inventoryRank: 0,
      nextEligibleAt: "2026-04-15T11:00:00.000Z",
    });
    const leased = createInventoryRecord({
      token: "leasedats",
      companyHint: "Leased ATS",
      inventoryRank: 1,
      nextEligibleAt: "2026-04-15T11:00:00.000Z",
    });
    const companyPages = Array.from({ length: 6 }, (_, index) =>
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://company-${index}.example/careers`,
          companyHint: `Company ${index}`,
          pageType: "html_page",
          confidence: "medium",
          discoveryMethod: "public_search",
        }),
        {
          now: "2026-04-10T00:00:00.000Z",
          inventoryOrigin: "public_search",
          inventoryRank: 50_000 + index,
        },
      ),
    );

    await repository.upsertSourceInventory([greenhouse, leased, ...companyPages]);
    await repository.claimSourceInventoryLeases([leased._id], {
      ownerKey: "other-worker",
      acquiredAt: now.toISOString(),
      expiresAt: "2026-04-15T12:30:00.000Z",
    });

    const seenSources: string[] = [];
    const providers = (["greenhouse", "company_page"] as const).map((platform) =>
      createStubProvider(platform, async (_context, sources) => {
        seenSources.push(...sources.map((source) => source.id));
        return {
          provider: platform,
          status: "success",
          sourceCount: sources.length,
          fetchedCount: sources.length,
          matchedCount: sources.length,
          warningCount: 0,
          jobs: sources.map((source, index) =>
            createProviderJob({
              title: platform === "greenhouse" ? "Software Engineer" : "Product Manager",
              company: source.companyHint ?? platform,
              sourcePlatform: platform,
              sourceJobId: `${platform}-${index}`,
              sourceUrl: `${source.url.replace(/\/$/, "")}/jobs/${index}`,
              applyUrl: `${source.url.replace(/\/$/, "")}/jobs/${index}/apply`,
              canonicalUrl: `${source.url.replace(/\/$/, "")}/jobs/${index}`,
            }),
          ),
        };
      }),
    );

    const triggered = await triggerRecurringBackgroundIngestion({
      repository,
      providers,
      now,
      maxSources: 4,
      schedulingIntervalMs: 60_000,
      runTimeoutMs: 2_000,
      refreshInventory: () => repository.listSourceInventory(["greenhouse", "company_page"]),
    });

    expect(triggered.status).toBe("started");
    if (triggered.status !== "started") {
      throw new Error("Expected recurring ingestion to start.");
    }

    const crawlRun = await waitForRunCompletion(repository, triggered.crawlRunId);
    const selectedIds = crawlRun.diagnostics.inventoryScheduling?.selectedSourceIds ?? [];

    expect(selectedIds).toContain(greenhouse._id);
    expect(selectedIds).not.toContain(leased._id);
    expect(selectedIds.filter((sourceId) => sourceId.startsWith("company_page:"))).toHaveLength(1);
    expect(crawlRun.diagnostics.inventoryScheduling).toMatchObject({
      skippedByReason: expect.objectContaining({
        active_lease: 1,
        unknown_company_page_cap: expect.any(Number),
      }),
      selectedByPlatform: {
        greenhouse: 1,
        company_page: 1,
      },
    });
    expect(seenSources).toEqual(expect.arrayContaining([greenhouse._id]));
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
        createProviderJob({
          title: "Software Engineer",
          sourceJobId: "se-1",
          normalizedTitle: "",
          titleNormalized: "",
        }),
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
      refreshInventory: () => repository.listSourceInventory(["greenhouse"]),
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
  sourcePlatform?: "greenhouse" | "lever" | "ashby" | "workday";
  country?: string;
  locationText?: string;
  normalizedTitle?: string;
  titleNormalized?: string;
}) {
  const company = overrides.company ?? "OpenAI";
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const country = overrides.country ?? "United States";
  const locationText = overrides.locationText ?? "Remote - United States";
  const sourceUrl = `https://example.com/${sourcePlatform}/jobs/${overrides.sourceJobId}`;

  return {
    title: overrides.title,
    normalizedTitle: overrides.normalizedTitle,
    titleNormalized: overrides.titleNormalized,
    company,
    country,
    locationText,
    resolvedLocation: {
      country,
      isRemote: locationText.toLowerCase().includes("remote"),
      isUnitedStates: country === "United States",
      confidence: "high" as const,
      evidence: [
        {
          source: "remote_hint" as const,
          value: locationText,
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

function createPlatformInventoryRecord(overrides: {
  platform: "greenhouse" | "lever" | "ashby" | "workday";
  url: string;
  token: string;
  companyHint: string;
}) {
  return toSourceInventoryRecord(
    classifySourceCandidate({
      url: overrides.url,
      token: overrides.token,
      companyHint: overrides.companyHint,
      confidence: "high",
      discoveryMethod: "platform_registry",
    }),
    {
      now: "2026-04-10T00:00:00.000Z",
      inventoryOrigin:
        overrides.platform === "greenhouse" ? "greenhouse_registry" : "platform_registry",
      inventoryRank:
        overrides.platform === "greenhouse"
          ? 0
          : overrides.platform === "lever"
            ? 10_000
            : overrides.platform === "ashby"
              ? 20_000
              : 30_000,
    },
  );
}

function createInventoryRecord(overrides: {
  token: string;
  companyHint: string;
  inventoryRank?: number;
  lastCrawledAt?: string;
  nextEligibleAt?: string;
  health?: SourceInventoryRecord["health"];
  status?: SourceInventoryRecord["status"];
  failureCount?: number;
  consecutiveFailures?: number;
  lastFailedAt?: string;
}) {
  const firstSeenAt = "2026-04-10T00:00:00.000Z";
  const record = toSourceInventoryRecord(
    classifySourceCandidate({
      url: `https://boards.greenhouse.io/${overrides.token}`,
      token: overrides.token,
      companyHint: overrides.companyHint,
      confidence: "high",
      discoveryMethod: "platform_registry",
    }),
    {
      now: firstSeenAt,
      inventoryOrigin: "greenhouse_registry",
      inventoryRank: overrides.inventoryRank ?? 0,
    },
  );

  return {
    ...record,
    status: overrides.status ?? "active",
    health: overrides.health ?? "unknown",
    failureCount: overrides.failureCount ?? 0,
    consecutiveFailures: overrides.consecutiveFailures ?? 0,
    lastCrawledAt: overrides.lastCrawledAt,
    nextEligibleAt: overrides.nextEligibleAt ?? firstSeenAt,
    lastFailedAt: overrides.lastFailedAt,
  };
}

function createEmptyDiscovery(): DiscoveryService {
  return {
    async discover() {
      return [];
    },
  };
}

function createExpansionResult(input: {
  inventory: SourceInventoryRecord[];
  beforeCount: number;
  afterExpansionCount: number;
  newSourceIds: string[];
}) {
  return {
    inventory: input.inventory,
    diagnostics: {
      beforeCount: input.beforeCount,
      afterRefreshCount: input.beforeCount,
      afterExpansionCount: input.afterExpansionCount,
      selectedSearches: 1,
      candidateSources: input.newSourceIds.length,
      newSourcesAdded: input.newSourceIds.length,
      selectedSearchTitles: ["test expansion"],
      selectedSearchFilters: [
        {
          title: "test expansion",
          crawlMode: "balanced",
        },
      ],
      selectedSourceIds: input.newSourceIds,
      newSourceIds: input.newSourceIds,
      platformCountsBefore: { greenhouse: input.beforeCount },
      platformCountsAfter: { greenhouse: input.afterExpansionCount },
      searchDiagnostics: [],
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
