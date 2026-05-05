import { afterEach, describe, expect, it, vi } from "vitest";

import { executeCrawlPipeline } from "@/lib/server/crawler/pipeline";
import { collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import {
  createDeterministicFakeProvider,
  createDeterministicFakeSource,
  deterministicTestCompany,
  deterministicTestCompanySlug,
  deterministicTestJobId,
  deterministicTestJobUrl,
} from "@/scripts/support/deterministic-persistence-provider";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";
import type { JobListing } from "@/lib/types";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("deterministic fake-provider persistence gate", () => {
  it("fake provider ingestion persists one job without fetch", async () => {
    const result = await runFakeProviderIngestion();

    expect(result.fetchImpl).not.toHaveBeenCalled();
    expect(result.persistedJobs).toHaveLength(1);
    expect(result.persistedJobs[0]).toMatchObject({
      title: "Software Engineer",
      company: deterministicTestCompany,
      sourcePlatform: "greenhouse",
      sourceCompanySlug: deterministicTestCompanySlug,
      sourceJobId: deterministicTestJobId,
      sourceUrl: deterministicTestJobUrl,
      applyUrl: deterministicTestJobUrl,
      locationText: "Seattle, WA",
    });
  });

  it("persisted fake-provider job has searchIndex", async () => {
    const { persistedJobs } = await runFakeProviderIngestion();

    expect(persistedJobs[0]?.searchIndex).toEqual(
      expect.objectContaining({
        titleNormalized: "software engineer",
        titleSearchKeys: expect.arrayContaining(["term:software engineer"]),
      }),
    );
  });

  it("persisted fake-provider job has canonicalJobKey", async () => {
    const { persistedJobs } = await runFakeProviderIngestion();

    expect(persistedJobs[0]?.canonicalJobKey).toBe(
      "platform:greenhouse:deterministic test company:deterministic test job 001",
    );
  });

  it("persisted fake-provider job has sourceLookupKeys", async () => {
    const { persistedJobs } = await runFakeProviderIngestion();

    expect(persistedJobs[0]?.sourceLookupKeys).toEqual([
      "greenhouse:deterministic test company:deterministic test job 001",
    ]);
  });

  it("persisted fake-provider job is linked to crawlRunJobEvents", async () => {
    const { crawlRunId, db, persistedJobs } = await runFakeProviderIngestion();
    const events = db.snapshot<Record<string, unknown>>(collectionNames.crawlRunJobEvents);

    expect(events).toEqual([
      expect.objectContaining({
        crawlRunId,
        jobId: persistedJobs[0]?._id,
        sequence: 1,
      }),
    ]);
  });

  it("persisted fake-provider job is emitted to indexedJobEvents", async () => {
    const { crawlRunId, db, persistedJobs } = await runFakeProviderIngestion();
    const events = db.snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents);

    expect(events).toEqual([
      expect.objectContaining({
        crawlRunId,
        jobId: persistedJobs[0]?._id,
        sequence: 1,
      }),
    ]);
  });

  it("fake-provider crawlSourceResult final status is success or partial, not running", async () => {
    const { sourceResults } = await runFakeProviderIngestion();

    expect(sourceResults).toHaveLength(1);
    expect(sourceResults[0]?.provider).toBe("greenhouse");
    expect(sourceResults[0]?.status).toMatch(/^(success|partial)$/);
    expect(sourceResults[0]?.status).not.toBe("running");
    expect(sourceResults[0]).toMatchObject({
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      savedCount: 1,
    });
  });
});

async function runFakeProviderIngestion() {
  vi.spyOn(console, "info").mockImplementation(() => undefined);
  vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const db = new MongoLikeNullDb();
  const repository = new JobCrawlerRepository(db);
  const now = new Date("2026-05-01T12:00:00.000Z");
  const nowIso = now.toISOString();
  const search = await repository.createSearch(
    {
      title: "Software Engineer",
      crawlMode: "balanced",
    },
    nowIso,
  );
  const searchSession = await repository.createSearchSession(search._id, nowIso, {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, nowIso, {
    searchSessionId: searchSession._id,
    stage: "queued",
    validationMode: "deferred",
  });
  await repository.updateSearchLatestSession(search._id, searchSession._id, "running", nowIso);
  await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", nowIso);
  await repository.updateSearchSession(searchSession._id, {
    latestCrawlRunId: crawlRun._id,
    status: "running",
    updatedAt: nowIso,
  });

  const discovery: DiscoveryService = {
    async discover() {
      return [createDeterministicFakeSource()];
    },
  };
  const fetchImpl = vi.fn(async () => {
    throw new Error("The deterministic fake provider must not call fetch.");
  }) as unknown as typeof fetch & ReturnType<typeof vi.fn>;

  await executeCrawlPipeline({
    search,
    searchSession,
    crawlRun,
    repository,
    discovery,
    providers: [createDeterministicFakeProvider()],
    fetchImpl,
    now,
    linkValidationMode: "deferred",
    providerTimeoutMs: 5_000,
    sourceTimeoutMs: 5_000,
    progressUpdateIntervalMs: 1,
  });

  const [persistedJobs, sourceResults] = await Promise.all([
    repository.getJobsByCrawlRun(crawlRun._id),
    repository.getCrawlSourceResults(crawlRun._id),
  ]);

  return {
    db,
    repository,
    crawlRunId: crawlRun._id,
    fetchImpl,
    persistedJobs: persistedJobs as JobListing[],
    sourceResults,
  };
}
