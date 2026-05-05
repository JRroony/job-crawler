import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ensureDatabaseIndexes,
  collectionNames,
  resetDatabaseIndexesForTests,
} from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { toSourceInventoryRecord } from "@/lib/server/discovery/inventory";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { JobListing } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

afterEach(() => {
  resetDatabaseIndexesForTests();
  vi.restoreAllMocks();
});

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createPersistableJob(
  overrides: Partial<PersistableTestJob> = {},
): PersistableTestJob {
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug =
    "sourceCompanySlug" in overrides ? overrides.sourceCompanySlug : "acme";
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-03-29T00:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? "2026-03-29T00:00:00.000Z";
  const canonicalUrl =
    "canonicalUrl" in overrides ? overrides.canonicalUrl : "https://example.com/jobs/1";
  const resolvedUrl =
    "resolvedUrl" in overrides
      ? overrides.resolvedUrl
      : "https://example.com/jobs/1/apply";
  const applyUrl = overrides.applyUrl ?? "https://example.com/jobs/1/apply";
  const normalizedSourceJobId = sourceJobId.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const canonicalJobKey =
    overrides.canonicalJobKey ??
    (sourceCompanySlug && normalizedSourceJobId
      ? `platform:${sourcePlatform}:${sourceCompanySlug}:${normalizedSourceJobId}`
      : canonicalUrl
        ? `canonical_url:${canonicalUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`
        : resolvedUrl
          ? `resolved_url:${resolvedUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`
          : `apply_url:${applyUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`);

  return {
    canonicalJobKey,
    title: "Software Engineer",
    company: "Acme",
    normalizedCompany: "acme",
    normalizedTitle: "software engineer",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationRaw: "San Francisco, California, United States",
    normalizedLocation: "san francisco california united states",
    locationText: "San Francisco, California, United States",
    remoteType: "onsite",
    seniority: "mid",
    experienceLevel: "mid",
    sourcePlatform,
    sourceCompanySlug,
    sourceJobId,
    sourceUrl: "https://example.com/jobs/1",
    applyUrl,
    resolvedUrl,
    canonicalUrl,
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt,
    crawledAt,
    sponsorshipHint: "unknown",
    linkStatus: "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? crawledAt,
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform,
        sourceJobId,
        sourceUrl: "https://example.com/jobs/1",
    applyUrl,
    resolvedUrl,
    canonicalUrl,
        discoveredAt,
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [`${sourcePlatform}:${sourceJobId}`],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
    ...overrides,
  };
}

describe("JobCrawlerRepository", () => {
  it("creates searches, crawl runs, persists jobs, and reads them back", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );

    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    const [savedJob] = await repository.persistJobs(crawlRun._id, [
      createPersistableJob(),
    ]);

    await repository.saveLinkValidation({
      _id: "validation-1",
      jobId: savedJob._id,
      applyUrl: savedJob.applyUrl,
      resolvedUrl: savedJob.resolvedUrl,
      canonicalUrl: savedJob.canonicalUrl,
      status: "valid",
      method: "GET",
      checkedAt: "2026-03-29T00:00:00.000Z",
    });

    const jobs = await repository.getJobsByCrawlRun(crawlRun._id);
    const validation = await repository.getFreshValidation(
      savedJob.applyUrl,
      "2026-03-28T00:00:00.000Z",
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0]._id).toBe(savedJob._id);
    expect(crawlRun.validationMode).toBe("deferred");
    expect(crawlRun.providerSummary).toEqual([]);
    expect(crawlRun.discoveredSourcesCount).toBe(0);
    expect(crawlRun.crawledSourcesCount).toBe(0);
    expect(validation?.jobId).toBe(savedJob._id);
  });

  it("records a delivery cursor for newly saved jobs so active crawls can poll deltas", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    const [savedJob] = await repository.persistJobs(crawlRun._id, [
      createPersistableJob(),
    ]);

    const firstDelta = await repository.getJobsByCrawlRunAfterSequence(crawlRun._id, 0);

    expect(await repository.getCrawlRunDeliveryCursor(crawlRun._id)).toBe(1);
    expect(firstDelta.cursor).toBe(1);
    expect(firstDelta.jobs.map((job) => job._id)).toEqual([savedJob._id]);
  });

  it("records an indexed-job cursor for newly persisted jobs so active sessions can poll index deltas", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    const [savedJob, secondJob] = await repository.persistJobs(crawlRun._id, [
      createPersistableJob(),
      createPersistableJob({
        sourceJobId: "role-2",
        sourceUrl: "https://example.com/jobs/2",
        applyUrl: "https://example.com/jobs/2/apply",
        resolvedUrl: "https://example.com/jobs/2/apply",
        canonicalUrl: "https://example.com/jobs/2",
        dedupeFingerprint: "fingerprint-2",
        contentFingerprint: "fingerprint-2",
        contentHash: "content-hash:role-2",
      }),
    ]);

    const firstDelta = await repository.getIndexedJobsAfterSequence(0);
    const secondDelta = await repository.getIndexedJobsAfterSequence(1);

    expect(await repository.getIndexedJobDeliveryCursor()).toBe(2);
    expect(firstDelta.cursor).toBe(2);
    expect(firstDelta.jobs.map((job) => job._id)).toEqual([savedJob._id, secondJob._id]);
    expect(secondDelta.cursor).toBe(2);
    expect(secondDelta.jobs.map((job) => job._id)).toEqual([secondJob._id]);
  });

  it("reads search-session jobs by page and sequence without loading earlier events", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const searchSession = await repository.createSearchSession(
      search._id,
      "2026-03-29T00:00:00.000Z",
      {
        status: "running",
      },
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
      {
        searchSessionId: searchSession._id,
      },
    );
    const saved = await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        sourceJobId: "session-role-1",
        sourceUrl: "https://example.com/jobs/session-role-1",
        applyUrl: "https://example.com/jobs/session-role-1/apply",
        resolvedUrl: "https://example.com/jobs/session-role-1/apply",
        canonicalUrl: "https://example.com/jobs/session-role-1",
        dedupeFingerprint: "session-fingerprint-1",
        contentFingerprint: "session-fingerprint-1",
        contentHash: "content-hash:session-role-1",
      }),
      createPersistableJob({
        sourceJobId: "session-role-2",
        sourceUrl: "https://example.com/jobs/session-role-2",
        applyUrl: "https://example.com/jobs/session-role-2/apply",
        resolvedUrl: "https://example.com/jobs/session-role-2/apply",
        canonicalUrl: "https://example.com/jobs/session-role-2",
        dedupeFingerprint: "session-fingerprint-2",
        contentFingerprint: "session-fingerprint-2",
        contentHash: "content-hash:session-role-2",
      }),
      createPersistableJob({
        sourceJobId: "session-role-3",
        sourceUrl: "https://example.com/jobs/session-role-3",
        applyUrl: "https://example.com/jobs/session-role-3/apply",
        resolvedUrl: "https://example.com/jobs/session-role-3/apply",
        canonicalUrl: "https://example.com/jobs/session-role-3",
        dedupeFingerprint: "session-fingerprint-3",
        contentFingerprint: "session-fingerprint-3",
        contentHash: "content-hash:session-role-3",
      }),
    ], {
      searchSessionId: searchSession._id,
    });

    const firstPage = await repository.getSearchSessionJobPage(searchSession._id, 0, 2);
    const secondPage = await repository.getSearchSessionJobPage(
      searchSession._id,
      firstPage.cursor,
      2,
    );
    const delta = await repository.getJobsBySearchSessionAfterSequence(
      searchSession._id,
      2,
    );

    expect(await repository.getSearchSessionJobCount(searchSession._id)).toBe(3);
    expect(firstPage).toMatchObject({
      cursor: 2,
      totalCount: 3,
    });
    expect(firstPage.jobs.map((job) => job._id)).toEqual([
      saved[0]?._id,
      saved[1]?._id,
    ]);
    expect(secondPage).toMatchObject({
      cursor: 3,
      totalCount: 3,
    });
    expect(secondPage.jobs.map((job) => job._id)).toEqual([saved[2]?._id]);
    expect(delta.cursor).toBe(3);
    expect(delta.jobs.map((job) => job._id)).toEqual([saved[2]?._id]);
  });

  it("reports persistence stats for inserted, updated, run-linked, and indexed jobs", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );
    const secondRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
    );
    const thirdRun = await repository.createCrawlRun(
      search._id,
      "2026-03-31T00:00:00.000Z",
    );
    const baseJob = createPersistableJob({
      linkStatus: "unknown",
      lastValidatedAt: undefined,
    });

    const first = await repository.persistJobsWithStats(firstRun._id, [baseJob]);
    const second = await repository.persistJobsWithStats(secondRun._id, [baseJob]);
    const changed = await repository.persistJobsWithStats(thirdRun._id, [
      createPersistableJob({
        ...baseJob,
        linkStatus: "valid",
        lastValidatedAt: "2026-03-31T00:00:00.000Z",
        crawledAt: "2026-03-31T00:00:00.000Z",
        lastSeenAt: "2026-03-31T00:00:00.000Z",
      }),
    ]);

    expect(first).toMatchObject({
      insertedCount: 1,
      updatedCount: 0,
      linkedToRunCount: 1,
      indexedEventCount: 1,
    });
    expect(second).toMatchObject({
      insertedCount: 0,
      updatedCount: 1,
      linkedToRunCount: 1,
      indexedEventCount: 0,
    });
    expect(changed).toMatchObject({
      insertedCount: 0,
      updatedCount: 1,
      linkedToRunCount: 1,
      indexedEventCount: 1,
    });
    expect(first.jobs[0]?._id).toBe(second.jobs[0]?._id);
    expect(second.jobs[0]?._id).toBe(changed.jobs[0]?._id);
    expect(await repository.getIndexedJobDeliveryCursor()).toBe(2);
  });

  it("allocates indexed job event sequences safely across concurrent persistence batches", async () => {
    const db = new FakeDb();
    db.collection(collectionNames.counters).indexes = [
      {
        key: { _id: 1 },
        name: "_id_",
        unique: true,
      },
    ];
    await ensureDatabaseIndexes(db);
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        repository.persistJobsWithStats(crawlRun._id, [
          createPersistableJob({
            sourceJobId: `concurrent-indexed-${index}`,
            sourceUrl: `https://example.com/jobs/concurrent-indexed-${index}`,
            applyUrl: `https://example.com/jobs/concurrent-indexed-${index}/apply`,
            canonicalUrl: `https://example.com/jobs/concurrent-indexed-${index}`,
            resolvedUrl: `https://example.com/jobs/concurrent-indexed-${index}/apply`,
            contentHash: `content-hash:concurrent-indexed-${index}`,
          }),
        ]),
      ),
    );

    const events = db.snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents);
    const sequences = events.map((event) => Number(event.sequence));

    expect(events).toHaveLength(8);
    expect(new Set(sequences).size).toBe(8);
    expect([...sequences].sort((left, right) => left - right)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(db.collection(collectionNames.counters).indexes).toEqual([
      {
        key: { _id: 1 },
        name: "_id_",
        unique: true,
      },
    ]);
  });

  it("allocates strictly unique indexed event sequences for concurrent multi-job batches", async () => {
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await Promise.all(
      Array.from({ length: 4 }, (_, batchIndex) =>
        repository.persistJobsWithStats(
          crawlRun._id,
          Array.from({ length: 3 }, (_, jobIndex) => {
            const id = `concurrent-batch-${batchIndex}-${jobIndex}`;
            return createPersistableJob({
              sourceJobId: id,
              sourceUrl: `https://example.com/jobs/${id}`,
              applyUrl: `https://example.com/jobs/${id}/apply`,
              canonicalUrl: `https://example.com/jobs/${id}`,
              resolvedUrl: `https://example.com/jobs/${id}/apply`,
              contentHash: `content-hash:${id}`,
            });
          }),
        ),
      ),
    );

    const sequences = db
      .snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents)
      .map((event) => Number(event.sequence))
      .sort((left, right) => left - right);

    expect(sequences).toHaveLength(12);
    expect(sequences).toEqual(Array.from({ length: 12 }, (_, index) => index + 1));
  });

  it("allocates unique crawl-run event sequences per crawl run", async () => {
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await Promise.all(
      Array.from({ length: 6 }, (_, index) =>
        repository.persistJobsWithStats(crawlRun._id, [
          createPersistableJob({
            sourceJobId: `concurrent-run-event-${index}`,
            sourceUrl: `https://example.com/jobs/concurrent-run-event-${index}`,
            applyUrl: `https://example.com/jobs/concurrent-run-event-${index}/apply`,
            canonicalUrl: `https://example.com/jobs/concurrent-run-event-${index}`,
            resolvedUrl: `https://example.com/jobs/concurrent-run-event-${index}/apply`,
            contentHash: `content-hash:concurrent-run-event-${index}`,
          }),
        ]),
      ),
    );

    const sequences = db
      .snapshot<Record<string, unknown>>(collectionNames.crawlRunJobEvents)
      .filter((event) => event.crawlRunId === crawlRun._id)
      .map((event) => Number(event.sequence))
      .sort((left, right) => left - right);

    expect(sequences).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("repairs stale indexed event counters before allocating sequences", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await db.collection(collectionNames.indexedJobEvents).insertOne({
      _id: "stale-indexed-event",
      jobId: "already-indexed",
      crawlRunId: "other-run",
      sequence: 1,
      createdAt: "2026-03-29T00:00:00.000Z",
    });
    await db.collection(collectionNames.counters).insertOne({
      _id: "indexedJobEvents",
      sequence: 0,
      updatedAt: "2026-03-29T00:00:00.000Z",
    });

    const result = await repository.persistJobsWithStats(crawlRun._id, [
      createPersistableJob({
        sourceJobId: "retry-indexed-sequence",
        sourceUrl: "https://example.com/jobs/retry-indexed-sequence",
        applyUrl: "https://example.com/jobs/retry-indexed-sequence/apply",
        canonicalUrl: "https://example.com/jobs/retry-indexed-sequence",
        resolvedUrl: "https://example.com/jobs/retry-indexed-sequence/apply",
        contentHash: "content-hash:retry-indexed-sequence",
      }),
    ]);

    const indexedEvents = db.snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents);

    expect(result).toMatchObject({
      insertedCount: 1,
      linkedToRunCount: 1,
      indexedEventCount: 1,
    });
    expect(indexedEvents.map((event) => Number(event.sequence)).sort()).toEqual([1, 2]);
    expect(warnSpy).toHaveBeenCalledWith(
      "[db:event-sequence-counter-repaired]",
      expect.objectContaining({
        eventCollection: "indexedJobEvents",
        previousSequence: 0,
        repairedSequence: 1,
      }),
    );
    expect(warnSpy).not.toHaveBeenCalledWith(
      "[db:event-sequence-duplicate-retry]",
      expect.anything(),
    );
    warnSpy.mockRestore();
  });

  it("creates durable search sessions and tracks session-scoped job deltas independently from crawl runs", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const searchSession = await repository.createSearchSession(
      search._id,
      "2026-03-29T00:00:00.000Z",
      { status: "running" },
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
      { searchSessionId: searchSession._id },
    );

    await repository.updateSearchLatestSession(
      search._id,
      searchSession._id,
      "running",
      "2026-03-29T00:00:00.000Z",
    );
    await repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: "2026-03-29T00:00:00.000Z",
    });

    const [firstSavedJob] = await repository.persistJobs(
      crawlRun._id,
      [createPersistableJob()],
      { searchSessionId: searchSession._id },
    );

    const storedSession = await repository.getSearchSession(searchSession._id);
    const firstDelta = await repository.getJobsBySearchSessionAfterSequence(searchSession._id, 0);

    expect(storedSession?.latestCrawlRunId).toBe(crawlRun._id);
    expect(storedSession?.lastEventSequence).toBe(1);
    expect(await repository.getSearchSessionDeliveryCursor(searchSession._id)).toBe(1);
    expect(firstDelta.cursor).toBe(1);
    expect(firstDelta.jobs.map((job) => job._id)).toEqual([firstSavedJob._id]);

    const secondRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
      { searchSessionId: searchSession._id },
    );
    await repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: secondRun._id,
      status: "running",
      updatedAt: "2026-03-30T00:00:00.000Z",
    });

    await repository.persistJobs(
      secondRun._id,
      [
        createPersistableJob({
          sourceJobId: "role-2",
          sourceUrl: "https://example.com/jobs/2",
          applyUrl: "https://example.com/jobs/2/apply",
          resolvedUrl: "https://example.com/jobs/2/apply",
          canonicalUrl: "https://example.com/jobs/2",
          sourceLookupKeys: ["greenhouse:role-2"],
          sourceProvenance: [
            {
              sourcePlatform: "greenhouse",
              sourceJobId: "role-2",
              sourceUrl: "https://example.com/jobs/2",
              applyUrl: "https://example.com/jobs/2/apply",
              resolvedUrl: "https://example.com/jobs/2/apply",
              canonicalUrl: "https://example.com/jobs/2",
              discoveredAt: "2026-03-30T00:00:00.000Z",
              rawSourceMetadata: {},
            },
          ],
          dedupeFingerprint: "fingerprint-2",
          contentFingerprint: "fingerprint-2",
          discoveredAt: "2026-03-30T00:00:00.000Z",
          crawledAt: "2026-03-30T00:00:00.000Z",
        }),
      ],
      { searchSessionId: searchSession._id },
    );

    const secondDelta = await repository.getJobsBySearchSessionAfterSequence(searchSession._id, 1);

    expect(secondDelta.cursor).toBe(2);
    expect(secondDelta.jobs).toHaveLength(1);
    expect(secondDelta.jobs[0]?.sourceJobId).toBe("role-2");
  });

  it("does not advance the session cursor when a later crawl run only rediscovers the same job", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const searchSession = await repository.createSearchSession(
      search._id,
      "2026-03-29T00:00:00.000Z",
      { status: "running" },
    );
    const firstRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
      { searchSessionId: searchSession._id },
    );
    const secondRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
      { searchSessionId: searchSession._id },
    );

    await repository.persistJobs(firstRun._id, [createPersistableJob()], {
      searchSessionId: searchSession._id,
    });
    await repository.persistJobs(secondRun._id, [createPersistableJob()], {
      searchSessionId: searchSession._id,
    });

    const delta = await repository.getJobsBySearchSessionAfterSequence(searchSession._id, 1);

    expect(await repository.getSearchSessionDeliveryCursor(searchSession._id)).toBe(1);
    expect(delta.cursor).toBe(1);
    expect(delta.jobs).toEqual([]);
  });

  it("does not advance the indexed cursor when a later crawl run only rediscovers the same job", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );
    const secondRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
    );

    await repository.persistJobs(firstRun._id, [createPersistableJob()]);
    await repository.persistJobs(secondRun._id, [createPersistableJob()]);

    const delta = await repository.getIndexedJobsAfterSequence(1);

    expect(await repository.getIndexedJobDeliveryCursor()).toBe(1);
    expect(delta.cursor).toBe(1);
    expect(delta.jobs).toEqual([]);
  });

  it("can seed an existing indexed job into a search session before supplemental crawl events arrive", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Product Manager",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const indexedRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );
    const [indexedJob] = await repository.persistJobs(indexedRun._id, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "indexed-product-manager",
        sourceUrl: "https://example.com/jobs/indexed-product-manager",
        applyUrl: "https://example.com/jobs/indexed-product-manager/apply",
        resolvedUrl: "https://example.com/jobs/indexed-product-manager/apply",
        canonicalUrl: "https://example.com/jobs/indexed-product-manager",
        sourceLookupKeys: ["greenhouse:indexed-product-manager"],
        dedupeFingerprint: "fingerprint-indexed-product-manager",
        contentFingerprint: "fingerprint-indexed-product-manager",
      }),
    ]);
    const searchSession = await repository.createSearchSession(
      search._id,
      "2026-03-30T00:00:00.000Z",
      { status: "running" },
    );
    const supplementalRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
      { searchSessionId: searchSession._id },
    );

    await repository.appendExistingJobsToSearchSession(
      searchSession._id,
      supplementalRun._id,
      [indexedJob._id],
    );

    const seededDelta = await repository.getJobsBySearchSessionAfterSequence(searchSession._id, 0);

    expect(seededDelta.cursor).toBe(1);
    expect(seededDelta.jobs.map((job) => job._id)).toEqual([indexedJob._id]);
    expect(await repository.getSearchSessionDeliveryCursor(searchSession._id)).toBe(1);
  });

  it("persists crawl run cancellation requests and heartbeats", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await repository.heartbeatCrawlRun(crawlRun._id, "2026-03-29T00:01:00.000Z");
    const canceled = await repository.requestCrawlRunCancellation(crawlRun._id, {
      reason: "Stopped by the user.",
      requestedAt: "2026-03-29T00:02:00.000Z",
    });
    const controlState = await repository.getCrawlRunControlState(crawlRun._id);

    expect(canceled?.cancelRequestedAt).toBe("2026-03-29T00:02:00.000Z");
    expect(canceled?.cancelReason).toBe("Stopped by the user.");
    expect(controlState?.lastHeartbeatAt).toBe("2026-03-29T00:01:00.000Z");
    expect(controlState?.cancelRequestedAt).toBe("2026-03-29T00:02:00.000Z");
    expect(controlState?.cancelReason).toBe("Stopped by the user.");
  });

  it("creates durable crawl control records alongside crawl runs", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );

    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
      {
        searchSessionId: "session-1",
      },
    );

    const controlState = await repository.getCrawlRunControlState(crawlRun._id);

    expect(controlState).toMatchObject({
      _id: crawlRun._id,
      crawlRunId: crawlRun._id,
      searchId: search._id,
      status: "running",
      cancelRequestedAt: undefined,
      cancelReason: undefined,
      lastHeartbeatAt: "2026-03-29T00:00:00.000Z",
      finishedAt: undefined,
    });
  });

  it("tracks durable crawl queue state and owner lookup independently of process-local memory", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Data Analyst",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
      {
        searchSessionId: "session-queue",
      },
    );

    await repository.enqueueCrawlRun({
      crawlRunId: crawlRun._id,
      searchId: search._id,
      searchSessionId: "session-queue",
      ownerKey: "client-queue",
      queuedAt: "2026-03-29T00:00:01.000Z",
    });
    await repository.markCrawlRunStarted(crawlRun._id, {
      startedAt: "2026-03-29T00:00:02.000Z",
      workerId: "worker:test",
      ownerKey: "client-queue",
    });

    const bySearch = await repository.getActiveCrawlQueueEntryForSearch(search._id);
    const byOwner = await repository.getActiveCrawlQueueEntryForOwner("client-queue");

    expect(bySearch?.crawlRunId).toBe(crawlRun._id);
    expect(bySearch?.status).toBe("running");
    expect(bySearch?.startedAt).toBe("2026-03-29T00:00:02.000Z");
    expect(bySearch?.workerId).toBe("worker:test");
    expect(byOwner?.crawlRunId).toBe(crawlRun._id);
    expect(await repository.hasActiveCrawlQueueEntryForSearch(search._id)).toBe(true);

    await repository.finalizeCrawlQueueEntry(crawlRun._id, {
      status: "aborted",
      finishedAt: "2026-03-29T00:00:03.000Z",
    });

    expect(await repository.getActiveCrawlQueueEntryForSearch(search._id)).toBeNull();
  });

  it("merges duplicate jobs across crawl runs without updating the immutable _id field", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );
    const secondRun = await repository.createCrawlRun(
      search._id,
      "2026-03-30T00:00:00.000Z",
    );

    await repository.persistJobs(firstRun._id, [
      createPersistableJob({
        resolvedUrl: undefined,
        linkStatus: "unknown",
      }),
    ]);

    const [mergedJob] = await repository.persistJobs(secondRun._id, [
      createPersistableJob({
        postedAt: "2026-03-21T00:00:00.000Z",
        postingDate: "2026-03-21T00:00:00.000Z",
        discoveredAt: "2026-03-30T00:00:00.000Z",
        crawledAt: "2026-03-30T00:00:00.000Z",
        lastValidatedAt: "2026-03-30T00:00:00.000Z",
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            resolvedUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-30T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]._id).toBe(mergedJob._id);
    expect(storedJobs[0].crawlRunIds).toEqual([firstRun._id, secondRun._id]);
    expect(storedJobs[0].linkStatus).toBe("valid");
    expect(storedJobs[0].resolvedUrl).toBe("https://example.com/jobs/1/apply");
    expect(storedJobs[0].postedAt).toBe("2026-03-21T00:00:00.000Z");
  });

  it("persists repeated observations idempotently around canonicalJobKey", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");
    const secondRun = await repository.createCrawlRun(search._id, "2026-03-30T00:00:00.000Z");

    await repository.persistJobs(firstRun._id, [createPersistableJob()]);
    const [savedAgain] = await repository.persistJobs(secondRun._id, [
      createPersistableJob({
        discoveredAt: "2026-03-30T00:00:00.000Z",
        crawledAt: "2026-03-30T00:00:00.000Z",
        lastSeenAt: "2026-03-30T00:00:00.000Z",
        indexedAt: "2026-03-30T00:00:00.000Z",
      }),
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(savedAgain.canonicalJobKey).toBe("platform:greenhouse:acme:role 1");
    expect(storedJobs[0]?.crawlRunIds).toEqual([firstRun._id, secondRun._id]);
    expect(storedJobs[0]?.firstSeenAt).toBe("2026-03-29T00:00:00.000Z");
    expect(storedJobs[0]?.lastSeenAt).toBe("2026-03-30T00:00:00.000Z");
    expect(storedJobs[0]?.indexedAt).toBe("2026-03-30T00:00:00.000Z");
    expect(storedJobs[0]?.isActive).toBe(true);
  });

  it("merges richer later observations while preserving lineage and lifecycle fields", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");
    const secondRun = await repository.createCrawlRun(search._id, "2026-03-30T00:00:00.000Z");

    await repository.persistJobs(firstRun._id, [
      createPersistableJob({
        resolvedUrl: undefined,
        linkStatus: "unknown",
        lastValidatedAt: undefined,
        sourceLookupKeys: ["greenhouse:role-1"],
      }),
    ]);

    const [merged] = await repository.persistJobs(secondRun._id, [
      createPersistableJob({
        discoveredAt: "2026-03-30T00:00:00.000Z",
        crawledAt: "2026-03-30T00:00:00.000Z",
        descriptionSnippet: "Build retrieval infrastructure.",
        sourceLookupKeys: ["greenhouse:role-1", "greenhouse:acme:role-1"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            resolvedUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-30T00:00:00.000Z",
            rawSourceMetadata: { recoveredFrom: "detail-url" },
          },
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1-alt",
            sourceUrl: "https://boards.greenhouse.io/acme",
            applyUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-30T00:00:00.000Z",
            rawSourceMetadata: { recoveredSource: true },
          },
        ],
      }),
    ]);

    expect(merged.descriptionSnippet).toBe("Build retrieval infrastructure.");
    expect(merged.sourceLookupKeys).toEqual(["greenhouse:role-1", "greenhouse:acme:role-1"]);
    expect(merged.sourceProvenance).toHaveLength(2);
    expect(merged.crawlRunIds).toEqual([firstRun._id, secondRun._id]);
    expect(merged.firstSeenAt).toBe("2026-03-29T00:00:00.000Z");
    expect(merged.lastSeenAt).toBe("2026-03-30T00:00:00.000Z");
    expect(merged.indexedAt).toBe("2026-03-30T00:00:00.000Z");
  });

  it("uses a conservative fallback canonical identity when URL and scoped source ids are unavailable", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");

    const [savedJob] = await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        sourceCompanySlug: undefined,
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        applyUrl: "https://example.com/apply/opaque",
        sourceUrl: "https://example.com/source/opaque",
        sourceLookupKeys: [],
      }),
    ]);

    expect(savedJob.canonicalJobKey).toBe("apply_url:https example com apply opaque");
  });

  it("tracks lifecycle closures and reactivations on the same logical job", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");
    const secondRun = await repository.createCrawlRun(search._id, "2026-03-30T00:00:00.000Z");
    const thirdRun = await repository.createCrawlRun(search._id, "2026-03-31T00:00:00.000Z");

    await repository.persistJobs(firstRun._id, [createPersistableJob()]);
    const [closed] = await repository.persistJobs(secondRun._id, [
      createPersistableJob({
        crawledAt: "2026-03-30T00:00:00.000Z",
        discoveredAt: "2026-03-30T00:00:00.000Z",
        isActive: false,
        closedAt: "2026-03-30T00:00:00.000Z",
      }),
    ]);
    const [reopened] = await repository.persistJobs(thirdRun._id, [
      createPersistableJob({
        crawledAt: "2026-03-31T00:00:00.000Z",
        discoveredAt: "2026-03-31T00:00:00.000Z",
        isActive: true,
      }),
    ]);

    expect(closed.isActive).toBe(false);
    expect(closed.closedAt).toBe("2026-03-30T00:00:00.000Z");
    expect(reopened.isActive).toBe(true);
    expect(reopened.closedAt).toBeUndefined();
    expect(reopened.lastSeenAt).toBe("2026-03-31T00:00:00.000Z");
  });

  it("returns a unique saved job list when duplicate candidates collapse during the same persist batch", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    const savedJobs = await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        resolvedUrl: undefined,
        linkStatus: "unknown",
        lastValidatedAt: undefined,
      }),
      createPersistableJob({
        sourcePlatform: "lever",
        sourceJobId: "role-2",
        sourceLookupKeys: ["lever:role-2"],
        resolvedUrl: undefined,
        discoveredAt: "2026-03-29T00:01:00.000Z",
        crawledAt: "2026-03-29T00:01:00.000Z",
        sourceProvenance: [
          {
            sourcePlatform: "lever",
            sourceJobId: "role-2",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-29T00:01:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(savedJobs).toHaveLength(1);
    expect(savedJobs[0].sourceProvenance).toHaveLength(2);
    expect(storedJobs).toHaveLength(1);
  });

  it("preserves same-title same-location jobs when their canonical identities differ", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        sourceJobId: "role-a",
        sourceUrl: "https://example.com/jobs/a",
        applyUrl: "https://example.com/jobs/a/apply",
        resolvedUrl: undefined,
        canonicalUrl: undefined,
        sourceLookupKeys: ["greenhouse:role-a"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-a",
            sourceUrl: "https://example.com/jobs/a",
            applyUrl: "https://example.com/jobs/a/apply",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        linkStatus: "unknown",
        lastValidatedAt: undefined,
      }),
      createPersistableJob({
        sourceJobId: "role-b",
        sourceUrl: "https://example.com/jobs/b",
        applyUrl: "https://example.com/jobs/b/apply",
        resolvedUrl: undefined,
        canonicalUrl: undefined,
        sourceLookupKeys: ["greenhouse:role-b"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-b",
            sourceUrl: "https://example.com/jobs/b",
            applyUrl: "https://example.com/jobs/b/apply",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        postedAt: "2026-03-21T00:00:00.000Z",
        postingDate: "2026-03-21T00:00:00.000Z",
        linkStatus: "unknown",
        lastValidatedAt: undefined,
      }),
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    expect(storedJobs).toHaveLength(2);
  });

  it("preserves same company and title when locations differ", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
      },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        sourceJobId: "role-sf",
        sourceUrl: "https://example.com/jobs/sf",
        applyUrl: "https://example.com/jobs/sf/apply",
        resolvedUrl: undefined,
        canonicalUrl: "https://example.com/jobs/sf",
        sourceLookupKeys: ["greenhouse:role-sf"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-sf",
            sourceUrl: "https://example.com/jobs/sf",
            applyUrl: "https://example.com/jobs/sf/apply",
            canonicalUrl: "https://example.com/jobs/sf",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        dedupeFingerprint: "fingerprint-sf",
        contentFingerprint: "fingerprint-sf",
      }),
      createPersistableJob({
        state: "New York",
        city: "New York",
        locationRaw: "New York, New York, United States",
        normalizedLocation: "new york new york united states",
        locationText: "New York, New York, United States",
        sourceJobId: "role-nyc",
        sourceUrl: "https://example.com/jobs/nyc",
        applyUrl: "https://example.com/jobs/nyc/apply",
        resolvedUrl: undefined,
        canonicalUrl: "https://example.com/jobs/nyc",
        sourceLookupKeys: ["greenhouse:role-nyc"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-nyc",
            sourceUrl: "https://example.com/jobs/nyc",
            applyUrl: "https://example.com/jobs/nyc/apply",
            canonicalUrl: "https://example.com/jobs/nyc",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        postedAt: "2026-03-21T00:00:00.000Z",
        postingDate: "2026-03-21T00:00:00.000Z",
        dedupeFingerprint: "fingerprint-nyc",
        contentFingerprint: "fingerprint-nyc",
      }),
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    expect(storedJobs).toHaveLength(2);
  });

  it("normalizes legacy saved search filters when they are read back", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.searches).insertOne({
      _id: "search-legacy",
      filters: {
        title: "Software Engineer",
        experienceLevel: "senior",
      },
      createdAt: "2026-03-29T00:00:00.000Z",
      updatedAt: "2026-03-29T00:00:00.000Z",
    });

    const search = await repository.getSearch("search-legacy");
    const searches = await repository.listRecentSearches();

    expect(search?.filters).toEqual({
      title: "Software Engineer",
      experienceLevels: ["senior"],
    });
    expect(searches[0]?.filters).toEqual({
      title: "Software Engineer",
      experienceLevels: ["senior"],
    });
  });

  it("normalizes legacy null location filters when searches are read back", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.searches).insertOne({
      _id: "search-null-location",
      filters: {
        title: "Software Engineer",
        country: "United States",
        state: null,
        city: null,
        experienceClassification: null,
      },
      createdAt: "2026-03-29T00:00:00.000Z",
      updatedAt: "2026-03-29T00:00:00.000Z",
    });

    const search = await repository.getSearch("search-null-location");
    const searches = await repository.listRecentSearches();

    expect(search?.filters).toEqual({
      title: "Software Engineer",
      country: "United States",
    });
    expect(searches[0]?.filters).toEqual({
      title: "Software Engineer",
      country: "United States",
    });
  });

  it("normalizes legacy crawl runs with missing crawler metadata on read", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.crawlRuns).insertOne({
      _id: "run-legacy",
      searchId: "search-1",
      startedAt: "2026-03-29T00:00:00.000Z",
      finishedAt: "2026-03-29T00:10:00.000Z",
      status: "completed",
      totalFetchedJobs: 10,
      totalMatchedJobs: 4,
      dedupedJobs: 3,
      diagnostics: {
        discoveredSources: 2,
        crawledSources: 2,
      },
    });

    const crawlRun = await repository.getCrawlRun("run-legacy");

    expect(crawlRun).toMatchObject({
      _id: "run-legacy",
      discoveredSourcesCount: 2,
      crawledSourcesCount: 2,
      validationMode: "deferred",
      providerSummary: [],
      diagnostics: {
        discoveredSources: 2,
        crawledSources: 2,
        providerFailures: 0,
      },
    });
  });

  it("normalizes legacy crawl diagnostics that store nullable optional fields", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.crawlRuns).insertOne({
      _id: "run-null-diagnostics",
      searchId: "search-1",
      startedAt: "2026-03-29T00:00:00.000Z",
      finishedAt: null,
      status: "partial",
      totalFetchedJobs: 5,
      totalMatchedJobs: 2,
      dedupedJobs: 2,
      errorMessage: null,
      providerSummary: [
        {
          provider: "greenhouse",
          status: "partial",
          sourceCount: 1,
          fetchedCount: 5,
          matchedCount: 2,
          savedCount: 2,
          warningCount: 1,
          errorMessage: null,
        },
      ],
      diagnostics: {
        discoveredSources: 1,
        crawledSources: 1,
        discovery: {
          inventorySources: 0,
          configuredSources: 0,
          curatedSources: 0,
          publicSources: 0,
          publicJobs: 0,
          discoveredBeforeFiltering: 0,
          discoveredAfterFiltering: 0,
          platformCounts: {},
          publicJobPlatformCounts: {},
          zeroCoverageReason: null,
          publicSearch: {
            generatedQueries: 3,
            executedQueries: 1,
            skippedQueries: 2,
            maxQueries: 8,
            maxSources: 40,
            maxResultsPerQuery: 4,
            roleQueryCount: 2,
            locationClauseCount: 6,
            rawResultsHarvested: 1,
            normalizedUrlsHarvested: 1,
            platformMatchedUrls: 1,
            candidateUrlsHarvested: 1,
            detailUrlsHarvested: 0,
            sourceUrlsHarvested: 1,
            recoveredSourcesFromDetailUrls: 0,
            directJobsExtracted: 0,
            sourcesAdded: 1,
            engineRequestCounts: {
              bing_rss: 1,
              duckduckgo_html: null,
            },
            engineResultCounts: {
              bing_rss: 1,
            },
            dropReasonCounts: {
              query_budget: 2,
              stale_query: null,
            },
            sampleGeneratedRoleQueries: null,
            sampleGeneratedQueries: [
              "site:boards.greenhouse.io integration engineer",
            ],
            sampleExecutedRoleQueries: null,
            sampleExecutedQueries: null,
            sampleHarvestedCandidateUrls: null,
            sampleHarvestedDetailUrls: null,
            sampleHarvestedSourceUrls: ["https://boards.greenhouse.io/acme"],
            sampleRecoveredSourceUrls: null,
            coverageNotes: null,
          },
        },
      },
    });

    const crawlRun = await repository.getCrawlRun("run-null-diagnostics");

    expect(crawlRun?._id).toBe("run-null-diagnostics");
    expect(crawlRun?.finishedAt).toBeUndefined();
    expect(crawlRun?.errorMessage).toBeUndefined();
    expect(crawlRun?.providerSummary).toEqual([
      {
        provider: "greenhouse",
        status: "partial",
        sourceCount: 1,
        fetchedCount: 5,
        matchedCount: 2,
        savedCount: 2,
        warningCount: 1,
        errorMessage: undefined,
      },
    ]);
    expect(crawlRun?.diagnostics.discovery?.zeroCoverageReason).toBeUndefined();
    expect(crawlRun?.diagnostics.discovery?.publicSearch).toMatchObject({
      generatedQueries: 3,
      executedQueries: 1,
      dropReasonCounts: {
        query_budget: 2,
      },
      engineRequestCounts: {
        bing_rss: 1,
      },
      sampleGeneratedRoleQueries: [],
      sampleExecutedRoleQueries: [],
      sampleExecutedQueries: [],
      sampleHarvestedCandidateUrls: [],
      sampleHarvestedDetailUrls: [],
      sampleRecoveredSourceUrls: [],
      coverageNotes: [],
    });
  });

  it("normalizes legacy jobs into the richer stored shape on read", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.jobs).insertOne({
      _id: "job-legacy",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Remote, United States",
      sourcePlatform: "greenhouse",
      sourceJobId: "role-legacy",
      sourceUrl: "https://example.com/jobs/legacy",
      applyUrl: "https://example.com/jobs/legacy/apply",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      locationNormalized: "remote united states",
      contentFingerprint: "fingerprint-legacy",
    });

    const job = await repository.getJob("job-legacy");

    expect(job).toMatchObject({
      _id: "job-legacy",
      linkStatus: "unknown",
      crawlRunIds: [],
      sourceLookupKeys: ["greenhouse:role legacy"],
    });
    expect(job?.sourceProvenance).toEqual([
      expect.objectContaining({
        sourcePlatform: "greenhouse",
        sourceJobId: "role-legacy",
        applyUrl: "https://example.com/jobs/legacy/apply",
      }),
    ]);
  });

  it("normalizes legacy null optional job fields and nested provenance on read", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.jobs).insertOne({
      _id: "job-null-legacy",
      title: "Software Engineer",
      company: "Acme",
      country: "United States",
      state: null,
      city: null,
      locationText: "San Francisco, CA",
      experienceLevel: null,
      experienceClassification: null,
      sourcePlatform: "greenhouse",
      sourceJobId: "role-null-legacy",
      sourceUrl: "https://example.com/jobs/null-legacy",
      applyUrl: "https://example.com/jobs/null-legacy/apply",
      resolvedUrl: null,
      canonicalUrl: null,
      postedAt: null,
      discoveredAt: "2026-03-29T00:00:00.000Z",
      lastValidatedAt: null,
      linkStatus: "unknown",
      rawSourceMetadata: null,
      sourceProvenance: [
        {
          sourcePlatform: "greenhouse",
          sourceJobId: "role-null-legacy",
          sourceUrl: "https://example.com/jobs/null-legacy",
          applyUrl: "https://example.com/jobs/null-legacy/apply",
          resolvedUrl: null,
          canonicalUrl: null,
          discoveredAt: "2026-03-29T00:00:00.000Z",
          rawSourceMetadata: null,
        },
      ],
      sourceLookupKeys: ["greenhouse:role null legacy"],
      crawlRunIds: ["run-legacy"],
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      locationNormalized: "san francisco ca united states",
      contentFingerprint: "fingerprint-null-legacy",
    });

    const job = await repository.getJob("job-null-legacy");

    expect(job).toMatchObject({
      _id: "job-null-legacy",
      title: "Software Engineer",
      company: "Acme",
      normalizedCompany: "acme",
      normalizedTitle: "software engineer",
      country: "United States",
      state: undefined,
      city: undefined,
      locationRaw: "San Francisco, CA",
      normalizedLocation: "san francisco ca united states",
      remoteType: "onsite",
      resolvedUrl: undefined,
      canonicalUrl: undefined,
      postingDate: undefined,
      postedAt: undefined,
      crawledAt: "2026-03-29T00:00:00.000Z",
      sponsorshipHint: "unknown",
      lastValidatedAt: undefined,
      experienceLevel: undefined,
      experienceClassification: undefined,
      dedupeFingerprint: "fingerprint-null-legacy",
      rawSourceMetadata: {},
    });
    expect(job?.sourceProvenance).toEqual([
      {
        sourcePlatform: "greenhouse",
        sourceJobId: "role-null-legacy",
        sourceUrl: "https://example.com/jobs/null-legacy",
        applyUrl: "https://example.com/jobs/null-legacy/apply",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
      },
    ]);
  });

  it("backfills structured experience classification fields for legacy stored jobs", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.jobs).insertOne({
      _id: "job-legacy-experience",
      title: "Software Engineer",
      company: "Acme",
      country: "United States",
      locationText: "Remote, United States",
      experienceClassification: {
        inferredLevel: "staff",
        confidence: "high",
        source: "structured_metadata",
        reasons: ["Detected staff markers in structured metadata."],
        isUnspecified: false,
        diagnostics: {
          originalTitle: "Software Engineer",
          normalizedTitle: "software engineer",
          finalSeniority: "staff",
          matchedSignals: [
            {
              ruleId: "title_staff_keyword",
              signalType: "structured_hint",
              source: "structured_metadata",
              level: "staff",
              confidence: "high",
              matchedText: "SMTS",
              rationale: "Detected staff markers in structured metadata: \"SMTS\".",
            },
          ],
          rationale: ["Detected staff markers in structured metadata."],
        },
      },
      sourcePlatform: "greenhouse",
      sourceJobId: "role-legacy-experience",
      sourceUrl: "https://example.com/jobs/legacy-experience",
      applyUrl: "https://example.com/jobs/legacy-experience/apply",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      crawledAt: "2026-03-29T00:00:00.000Z",
      linkStatus: "unknown",
      rawSourceMetadata: {},
      sourceProvenance: [
        {
          sourcePlatform: "greenhouse",
          sourceJobId: "role-legacy-experience",
          sourceUrl: "https://example.com/jobs/legacy-experience",
          applyUrl: "https://example.com/jobs/legacy-experience/apply",
          discoveredAt: "2026-03-29T00:00:00.000Z",
          rawSourceMetadata: {},
        },
      ],
      sourceLookupKeys: ["greenhouse:role legacy experience"],
      crawlRunIds: ["run-legacy"],
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      locationNormalized: "remote united states",
      contentFingerprint: "fingerprint-legacy-experience",
    });

    const job = await repository.getJob("job-legacy-experience");

    expect(job?.experienceClassification).toMatchObject({
      experienceVersion: 2,
      experienceBand: "advanced",
      experienceSource: "structured_metadata",
      experienceConfidence: "high",
      inferredLevel: "staff",
      source: "structured_metadata",
      confidence: "high",
      experienceSignals: [
        expect.objectContaining({
          ruleId: "title_staff_keyword",
          level: "staff",
        }),
      ],
    });
  });

  it("persists query-friendly title search facets for indexed retrieval", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Product Manager" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-03-29T00:00:00.000Z",
    );

    await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        title: "Senior Product Manager",
        normalizedTitle: "senior product manager",
        titleNormalized: "senior product manager",
        sourceJobId: "search-facet-product-manager",
        sourceUrl: "https://example.com/jobs/search-facet-product-manager",
        applyUrl: "https://example.com/jobs/search-facet-product-manager/apply",
        canonicalUrl: "https://example.com/jobs/search-facet-product-manager",
      }),
    ]);

    const [job] = await repository.listJobs();

    expect(job?.searchIndex).toMatchObject({
      titleFamily: "product",
      titleNormalized: "senior product manager",
      titleStrippedNormalized: "product manager",
      titleConceptIds: expect.arrayContaining(["product_manager"]),
      titleSearchTerms: expect.arrayContaining(["product manager"]),
    });
  });

  it("creates the expected MongoDB indexes", async () => {
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);

    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_listing_by_run_and_sort");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_canonical_job_key");
    expect(
      db.collection(collectionNames.linkValidations).indexes.map((index) => index.name),
    ).toContain("linkValidations_applyUrl_checkedAt_desc");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_export_by_platform_and_postedAt");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_source_url");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_lifecycle_activity_lastSeenAt_desc");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_search_active_platform_family_recent");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_search_title_concepts_active_recent");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_search_location_activity_recent");
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_search_experience_activity_recent");
    expect(
      db.collection(collectionNames.crawlRuns).indexes.map((index) => index.name),
    ).toContain("crawlRuns_validationMode_startedAt_desc");
    expect(
      db.collection(collectionNames.crawlControls).indexes.map((index) => index.name),
    ).toContain("crawlControls_crawlRunId");
    expect(
      db.collection(collectionNames.crawlQueue).indexes.map((index) => index.name),
    ).toContain("crawlQueue_searchId_status_updatedAt_desc");
    expect(
      db.collection(collectionNames.crawlRunJobEvents).indexes.map((index) => index.name),
    ).toContain("crawlRunJobEvents_run_sequence");
    expect(
      db.collection(collectionNames.searchSessions).indexes.map((index) => index.name),
    ).toContain("searchSessions_searchId_createdAt_desc");
    expect(
      db.collection(collectionNames.searchSessionJobEvents).indexes.map((index) => index.name),
    ).toContain("searchSessionJobEvents_session_sequence");
    expect(
      db.collection(collectionNames.indexedJobEvents).indexes.map((index) => index.name),
    ).toContain("indexedJobEvents_sequence");
    expect(
      db.collection(collectionNames.counters).indexes.map((index) => index.name),
    ).not.toContain("counters_id");
    expect(
      db.collection(collectionNames.counters).indexes.some(
        (index) => JSON.stringify(index.key) === JSON.stringify({ _id: 1 }),
      ),
    ).toBe(false);
  });

  it("groups duplicate updates into a single bulk write instead of issuing per-job mutations", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const firstRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");
    const secondRun = await repository.createCrawlRun(search._id, "2026-03-30T00:00:00.000Z");

    await repository.persistJobs(firstRun._id, [
      createPersistableJob({
        state: undefined,
        city: undefined,
        locationRaw: "Remote, United States",
        normalizedLocation: "remote united states",
        locationText: "Remote, United States",
        remoteType: "remote",
        sourceLookupKeys: ["greenhouse:role-1"],
        resolvedUrl: undefined,
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        linkStatus: "unknown",
        lastValidatedAt: undefined,
      }),
    ]);

    const jobsCollection = db.collection<JobListing>(collectionNames.jobs);
    jobsCollection.stats.insertOneCalls = 0;
    jobsCollection.stats.updateOneCalls = 0;
    jobsCollection.stats.bulkWriteCalls = 0;

    await repository.persistJobs(secondRun._id, [
      createPersistableJob({
        state: undefined,
        city: undefined,
        locationRaw: "Remote, United States",
        normalizedLocation: "remote united states",
        locationText: "Remote, United States",
        remoteType: "remote",
        resolvedUrl: undefined,
        discoveredAt: "2026-03-30T00:00:00.000Z",
        crawledAt: "2026-03-30T00:00:00.000Z",
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-30T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
      createPersistableJob({
        state: undefined,
        city: undefined,
        locationRaw: "Remote, United States",
        normalizedLocation: "remote united states",
        locationText: "Remote, United States",
        remoteType: "remote",
        discoveredAt: "2026-03-30T00:00:01.000Z",
        crawledAt: "2026-03-30T00:00:01.000Z",
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            resolvedUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-30T00:00:01.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
    ]);

    expect(jobsCollection.stats.bulkWriteCalls).toBe(1);
    expect(jobsCollection.stats.updateOneCalls).toBe(1);
  });

  it("upserts persistent source inventory records without duplicating stable source ids", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const firstSeenAt = "2026-04-14T00:00:00.000Z";
    const refreshedAt = "2026-04-15T00:00:00.000Z";

    const greenhouse = toSourceInventoryRecord(
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/openai",
        token: "openai",
        companyHint: "OpenAI",
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
      {
        now: firstSeenAt,
        inventoryOrigin: "greenhouse_registry",
        inventoryRank: 0,
      },
    );

    await repository.upsertSourceInventory([greenhouse]);
    await repository.upsertSourceInventory([
      {
        ...greenhouse,
        companyHint: "OpenAI Careers",
        lastSeenAt: refreshedAt,
        lastRefreshedAt: refreshedAt,
      },
    ]);

    const inventory = await repository.listSourceInventory();

    expect(inventory).toHaveLength(1);
    expect(inventory[0]).toMatchObject({
      _id: greenhouse._id,
      sourceType: "ats_board",
      sourceKey: "openai",
      status: "active",
      health: "unknown",
      crawlPriority: 0,
      companyHint: "OpenAI Careers",
      firstSeenAt,
      lastSeenAt: refreshedAt,
      lastRefreshedAt: refreshedAt,
    });
    expect(db.snapshot(collectionNames.sourceInventory)).toHaveLength(1);
  });

  it("claims and releases source inventory crawl leases", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = "2026-04-15T00:00:00.000Z";
    const expiresAt = "2026-04-15T00:30:00.000Z";
    const greenhouse = toSourceInventoryRecord(
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/openai",
        token: "openai",
        companyHint: "OpenAI",
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
      {
        now,
        inventoryOrigin: "greenhouse_registry",
        inventoryRank: 0,
      },
    );

    await repository.upsertSourceInventory([greenhouse]);

    const firstClaim = await repository.claimSourceInventoryLeases([greenhouse._id], {
      ownerKey: "owner:one",
      acquiredAt: now,
      expiresAt,
    });
    const blockedClaim = await repository.claimSourceInventoryLeases([greenhouse._id], {
      ownerKey: "owner:two",
      acquiredAt: "2026-04-15T00:05:00.000Z",
      expiresAt: "2026-04-15T00:35:00.000Z",
    });

    expect(firstClaim).toHaveLength(1);
    expect(firstClaim[0]).toMatchObject({
      crawlLeaseOwnerKey: "owner:one",
      crawlLeaseAcquiredAt: now,
      crawlLeaseExpiresAt: expiresAt,
    });
    expect(blockedClaim).toEqual([]);

    await repository.releaseSourceInventoryLeasesForOwner("owner:one");
    const secondClaim = await repository.claimSourceInventoryLeases([greenhouse._id], {
      ownerKey: "owner:two",
      acquiredAt: "2026-04-15T00:05:00.000Z",
      expiresAt: "2026-04-15T00:35:00.000Z",
    });

    expect(secondClaim).toHaveLength(1);
    expect(secondClaim[0]?.crawlLeaseOwnerKey).toBe("owner:two");

    await repository.recordSourceInventoryObservations([
      {
        sourceId: greenhouse._id,
        observedAt: "2026-04-15T00:10:00.000Z",
        succeeded: true,
        errorType: "none",
      },
    ]);
    const [released] = await repository.listSourceInventory(["greenhouse"]);

    expect(released?.crawlLeaseOwnerKey).toBeUndefined();
    expect(released?.crawlLeaseAcquiredAt).toBeUndefined();
    expect(released?.crawlLeaseExpiresAt).toBeUndefined();
  });

  it("records source inventory crawl observations with health and failure tracking", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const observedAt = "2026-04-15T00:00:00.000Z";
    const failedAt = "2026-04-16T00:00:00.000Z";
    const recoveredAt = "2026-04-17T00:00:00.000Z";

    const greenhouse = toSourceInventoryRecord(
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/openai",
        token: "openai",
        companyHint: "OpenAI",
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
      {
        now: observedAt,
        inventoryOrigin: "greenhouse_registry",
        inventoryRank: 0,
      },
    );

    await repository.upsertSourceInventory([greenhouse]);
    await repository.recordSourceInventoryObservations([
      {
        sourceId: greenhouse._id,
        observedAt: failedAt,
        health: "failing",
        succeeded: false,
        errorType: "source_timeout",
        failureReason: "Timed out fetching board payload",
      },
    ]);
    await repository.recordSourceInventoryObservations([
      {
        sourceId: greenhouse._id,
        observedAt: recoveredAt,
        health: "healthy",
        succeeded: true,
        errorType: "none",
      },
    ]);

    const inventory = await repository.listSourceInventory(["greenhouse"]);

    expect(inventory).toHaveLength(1);
    expect(inventory[0]).toMatchObject({
      _id: greenhouse._id,
      health: "healthy",
      failureCount: 1,
      consecutiveFailures: 0,
      lastCrawledAt: recoveredAt,
      lastSucceededAt: recoveredAt,
      lastFailedAt: failedAt,
    });
    expect(inventory[0]?.lastFailureReason).toBeUndefined();
    expect(Date.parse(String(inventory[0]?.nextEligibleAt))).toBeGreaterThan(Date.parse(recoveredAt));
  });

  it("batches multiple inventory observations into a single bulkWrite", async () => {
    const observedAt = "2026-04-15T12:00:00.000Z";
    const laterAt = "2026-04-15T13:00:00.000Z";
    const repository = new JobCrawlerRepository(new FakeDb());

    const greenhouse = toSourceInventoryRecord(
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/openai",
        token: "openai",
        companyHint: "OpenAI",
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
      {
        now: observedAt,
        inventoryOrigin: "greenhouse_registry",
        inventoryRank: 0,
      },
    );

    const lever = toSourceInventoryRecord(
      classifySourceCandidate({
        url: "https://jobs.lever.co/vercel",
        token: "vercel",
        companyHint: "Vercel",
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
      {
        now: observedAt,
        inventoryOrigin: "platform_registry",
        inventoryRank: 1,
      },
    );

    await repository.upsertSourceInventory([greenhouse, lever]);

    // Record both observations in a single call to exercise the batched path
    await repository.recordSourceInventoryObservations([
      {
        sourceId: greenhouse._id,
        observedAt: laterAt,
        health: "healthy",
        succeeded: true,
        errorType: "none",
      },
      {
        sourceId: lever._id,
        observedAt: laterAt,
        health: "healthy",
        succeeded: true,
        errorType: "none",
      },
    ]);

    const inventory = await repository.listSourceInventory(["greenhouse", "lever"]);
    expect(inventory).toHaveLength(2);
    for (const record of inventory) {
      expect(record.health).toBe("healthy");
      expect(record.lastSucceededAt).toBe(laterAt);
      expect(record.nextEligibleAt).toBeTruthy();
    }
  });

  it("resolves existing jobs with consolidated $or query for diverse dedupe keys", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const search = await repository.createSearch(
      { title: "Software Engineer" },
      "2026-03-29T00:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(search._id, "2026-03-29T00:00:00.000Z");

    // Persist a batch to establish the existing job baseline
    const firstJob = createPersistableJob({
      sourceJobId: "role-1",
      canonicalUrl: "https://boards.greenhouse.io/acme/jobs/1",
      applyUrl: "https://boards.greenhouse.io/acme/jobs/1",
      sourceUrl: "https://boards.greenhouse.io/acme/jobs/1",
    });
    await repository.persistJobs(crawlRun._id, [firstJob]);

    // Second persist with a job that has a different canonicalJobKey but matching applyUrl
    const secondJob = createPersistableJob({
      sourceJobId: "role-2",
      canonicalUrl: "https://boards.greenhouse.io/acme/jobs/2",
      applyUrl: firstJob.applyUrl, // same applyUrl as first job → dedupe match
      sourceUrl: "https://boards.greenhouse.io/acme/jobs/2",
    });
    const stats = await repository.persistJobsWithStats(crawlRun._id, [secondJob]);

    expect(stats.insertedCount).toBe(0);
    expect(stats.updatedCount).toBe(1);
    expect(stats.jobs).toHaveLength(1);
    expect(stats.jobs[0]?.sourceJobId).toBe("role-1"); // preserved original identity
  });

  it("batches inventory observations with zero observations as a no-op", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const inventory = await repository.recordSourceInventoryObservations([]);
    expect(inventory).toEqual([]);
  });
});
