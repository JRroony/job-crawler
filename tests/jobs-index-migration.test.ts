import { afterEach, describe, expect, it } from "vitest";

import { collectionNames } from "@/lib/server/db/collections";
import { migrateLegacyJobsForCanonicalKey } from "@/lib/server/db/job-migration";
import {
  ensureDatabaseIndexes,
  resetDatabaseIndexesForTests,
} from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { JobListing } from "@/lib/types";

import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createPersistableJob(
  overrides: Partial<PersistableTestJob> = {},
): PersistableTestJob {
  const title = overrides.title ?? "Software Engineer";
  const company = overrides.company ?? "Acme";
  const companyNormalized = overrides.companyNormalized ?? company.toLowerCase();
  const titleNormalized = overrides.titleNormalized ?? title.toLowerCase();
  const locationText = overrides.locationText ?? "Remote - United States";
  const canonicalUrl = overrides.canonicalUrl ?? "https://example.com/jobs/legacy-role";
  const applyUrl = overrides.applyUrl ?? `${canonicalUrl}/apply`;
  const sourceUrl = overrides.sourceUrl ?? canonicalUrl;
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug = overrides.sourceCompanySlug ?? companyNormalized;
  const sourceJobId = overrides.sourceJobId ?? "legacy-role";
  const discoveredAt = overrides.discoveredAt ?? "2026-04-15T12:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? discoveredAt;

  return {
    canonicalJobKey:
      overrides.canonicalJobKey ??
      `platform:${sourcePlatform}:${sourceCompanySlug}:${sourceJobId.toLowerCase()}`,
    title,
    company,
    normalizedCompany: companyNormalized,
    normalizedTitle: titleNormalized,
    country: overrides.country ?? "United States",
    state: overrides.state,
    city: overrides.city,
    locationRaw: overrides.locationRaw ?? locationText,
    normalizedLocation:
      overrides.normalizedLocation ?? locationText.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim(),
    locationText,
    resolvedLocation: overrides.resolvedLocation,
    remoteType: overrides.remoteType ?? "remote",
    employmentType: overrides.employmentType,
    seniority: overrides.seniority,
    experienceLevel: overrides.experienceLevel,
    experienceClassification: overrides.experienceClassification,
    sourcePlatform,
    sourceCompanySlug,
    sourceJobId,
    sourceUrl,
    applyUrl,
    resolvedUrl: overrides.resolvedUrl ?? applyUrl,
    canonicalUrl,
    postingDate: overrides.postingDate ?? "2026-04-14T00:00:00.000Z",
    postedAt: overrides.postedAt ?? overrides.postingDate ?? "2026-04-14T00:00:00.000Z",
    discoveredAt,
    crawledAt,
    descriptionSnippet: overrides.descriptionSnippet,
    salaryInfo: overrides.salaryInfo,
    sponsorshipHint: overrides.sponsorshipHint ?? "unknown",
    linkStatus: overrides.linkStatus ?? "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? crawledAt,
    rawSourceMetadata: overrides.rawSourceMetadata ?? {},
    sourceProvenance: overrides.sourceProvenance ?? [
      {
        sourcePlatform,
        sourceJobId,
        sourceUrl,
        applyUrl,
        resolvedUrl: overrides.resolvedUrl ?? applyUrl,
        canonicalUrl,
        discoveredAt,
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [
      `${sourcePlatform}:${sourceCompanySlug}:${sourceJobId.toLowerCase()}`,
    ],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: overrides.dedupeFingerprint ?? `dedupe:${sourceJobId}`,
    companyNormalized,
    titleNormalized,
    locationNormalized:
      overrides.locationNormalized ?? locationText.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim(),
    contentFingerprint: overrides.contentFingerprint ?? `content:${sourceJobId}`,
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
  };
}

function createLegacyJobDocument(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    _id: overrides._id ?? "legacy-job",
    title: overrides.title ?? "Software Engineer",
    company: overrides.company ?? "Acme",
    country: overrides.country ?? "United States",
    locationText: overrides.locationText ?? "Remote - United States",
    sourcePlatform: overrides.sourcePlatform ?? "greenhouse",
    sourceCompanySlug: overrides.sourceCompanySlug ?? "acme",
    sourceJobId: overrides.sourceJobId ?? "Legacy Role",
    sourceUrl: overrides.sourceUrl ?? "https://example.com/jobs/legacy-role",
    applyUrl: overrides.applyUrl ?? "https://example.com/jobs/legacy-role/apply",
    resolvedUrl: overrides.resolvedUrl,
    canonicalUrl: overrides.canonicalUrl,
    discoveredAt: overrides.discoveredAt ?? "2026-03-29T00:00:00.000Z",
    crawledAt: overrides.crawledAt,
    companyNormalized: overrides.companyNormalized ?? "acme",
    titleNormalized: overrides.titleNormalized ?? "software engineer",
    locationNormalized: overrides.locationNormalized ?? "remote united states",
    contentFingerprint: overrides.contentFingerprint ?? "legacy-fingerprint",
    canonicalJobKey: overrides.canonicalJobKey,
    firstSeenAt: overrides.firstSeenAt,
    lastSeenAt: overrides.lastSeenAt,
    indexedAt: overrides.indexedAt,
    contentHash: overrides.contentHash,
    isActive: overrides.isActive,
    closedAt: overrides.closedAt,
    sourceLookupKeys: overrides.sourceLookupKeys ?? ["greenhouse:acme:legacy role"],
    crawlRunIds: overrides.crawlRunIds ?? ["run-legacy-a"],
    sourceProvenance: overrides.sourceProvenance ?? [
      {
        sourcePlatform: overrides.sourcePlatform ?? "greenhouse",
        sourceJobId: overrides.sourceJobId ?? "Legacy Role",
        sourceUrl: overrides.sourceUrl ?? "https://example.com/jobs/legacy-role",
        applyUrl: overrides.applyUrl ?? "https://example.com/jobs/legacy-role/apply",
        resolvedUrl: overrides.resolvedUrl,
        canonicalUrl: overrides.canonicalUrl,
        discoveredAt: overrides.discoveredAt ?? "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: overrides.rawSourceMetadata ?? {},
      },
    ],
    linkStatus: overrides.linkStatus ?? "unknown",
    lastValidatedAt: overrides.lastValidatedAt,
    rawSourceMetadata: overrides.rawSourceMetadata ?? {},
  };
}

describe("jobs canonicalJobKey migration", () => {
  afterEach(() => {
    resetDatabaseIndexesForTests();
  });

  it("backfills legacy canonical fields, merges duplicate legacy rows, and allows the unique index rollout", async () => {
    const db = new MongoLikeNullDb();

    await db.collection(collectionNames.jobs).insertOne(
      createLegacyJobDocument({
        _id: "legacy-a",
        canonicalJobKey: null,
        sourceLookupKeys: ["greenhouse:legacy role"],
        crawlRunIds: ["run-1"],
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "Legacy Role",
            sourceUrl: "https://example.com/jobs/legacy-role",
            applyUrl: "https://example.com/jobs/legacy-role/apply",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: { observedFromLegacyA: true },
          },
        ],
      }),
    );
    await db.collection(collectionNames.jobs).insertOne(
      createLegacyJobDocument({
        _id: "legacy-b",
        canonicalJobKey: undefined,
        sourceLookupKeys: ["greenhouse:acme:legacy role"],
        crawlRunIds: ["run-2"],
        resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=1",
        canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
        crawledAt: "2026-03-31T00:00:00.000Z",
        firstSeenAt: "2026-03-30T00:00:00.000Z",
        lastSeenAt: "2026-03-31T00:00:00.000Z",
        indexedAt: "2026-03-31T00:00:00.000Z",
        isActive: false,
        closedAt: "2026-03-31T00:00:00.000Z",
        lastValidatedAt: "2026-03-31T00:00:00.000Z",
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "Legacy Role",
            sourceUrl: "https://example.com/jobs/legacy-role",
            applyUrl: "https://example.com/jobs/legacy-role/apply",
            resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=1",
            canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
            discoveredAt: "2026-03-30T00:00:00.000Z",
            rawSourceMetadata: { recoveredFromLegacyB: true },
          },
        ],
      }),
    );

    await expect(
      db.collection(collectionNames.jobs).createIndexes([
        {
          key: { canonicalJobKey: 1 },
          name: "jobs_canonical_job_key",
          unique: true,
        },
      ]),
    ).rejects.toThrow(/E11000 duplicate key error/);

    await expect(ensureDatabaseIndexes(db)).resolves.not.toThrow();

    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);
    expect(storedJobs).toHaveLength(1);
    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_canonical_job_key");

    const repository = new JobCrawlerRepository(db);
    const [job] = await repository.listJobs();

    expect(job).toMatchObject({
      _id: "legacy-b",
      canonicalJobKey: "platform:greenhouse:acme:legacy role",
      firstSeenAt: "2026-03-29T00:00:00.000Z",
      lastSeenAt: "2026-03-31T00:00:00.000Z",
      indexedAt: "2026-03-31T00:00:00.000Z",
      isActive: true,
      closedAt: undefined,
      canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
      resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=1",
    });
    expect(job?.contentHash).toEqual(expect.any(String));
    expect(job?.crawlRunIds).toEqual(expect.arrayContaining(["run-1", "run-2"]));
    expect(job?.sourceLookupKeys).toEqual(
      expect.arrayContaining(["greenhouse:legacy role", "greenhouse:acme:legacy role"]),
    );
    expect(job?.sourceProvenance).toEqual([
      expect.objectContaining({
        discoveredAt: "2026-03-29T00:00:00.000Z",
        canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
        resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=1",
        rawSourceMetadata: {
          observedFromLegacyA: true,
          recoveredFromLegacyB: true,
        },
      }),
    ]);
  });

  it("is idempotent when the migration runs multiple times", async () => {
    const db = new MongoLikeNullDb();
    await db.collection(collectionNames.jobs).insertOne(
      createLegacyJobDocument({
        _id: "legacy-idempotent",
        canonicalJobKey: null,
        crawlRunIds: ["run-idempotent"],
      }),
    );

    const first = await migrateLegacyJobsForCanonicalKey(db);
    const snapshotAfterFirstRun = db.snapshot<Record<string, unknown>>(collectionNames.jobs);
    const second = await migrateLegacyJobsForCanonicalKey(db);
    const snapshotAfterSecondRun = db.snapshot<Record<string, unknown>>(collectionNames.jobs);

    expect(first.canonicalBackfillCount).toBe(1);
    expect(first.rewrittenCount).toBe(1);
    expect(second.canonicalBackfillCount).toBe(0);
    expect(second.lifecycleBackfillCount).toBe(0);
    expect(second.rewrittenCount).toBe(0);
    expect(second.deletedCount).toBe(0);
    expect(second.duplicateGroupCount).toBe(0);
    expect(snapshotAfterSecondRun).toEqual(snapshotAfterFirstRun);
  });

  it("keeps persistJobs idempotent after the migration has repaired legacy rows", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.jobs).insertOne(
      createLegacyJobDocument({
        _id: "legacy-upsert",
        canonicalJobKey: null,
        crawledAt: "2026-03-29T00:00:00.000Z",
      }),
    );

    await ensureDatabaseIndexes(db);

    const search = await repository.createSearch(
      { title: "Software Engineer", country: "United States" },
      "2026-04-15T12:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(search._id, "2026-04-16T00:00:00.000Z");

    const [updatedJob] = await repository.persistJobs(crawlRun._id, [
      createPersistableJob({
        sourceJobId: "Legacy Role",
        sourceUrl: "https://example.com/jobs/legacy-role",
        applyUrl: "https://example.com/jobs/legacy-role/apply",
        canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
        resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=2",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        crawledAt: "2026-04-16T00:00:00.000Z",
        lastSeenAt: "2026-04-16T00:00:00.000Z",
        indexedAt: "2026-04-16T00:00:00.000Z",
      }),
    ]);

    const storedJobs = db.snapshot<Record<string, unknown>>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(updatedJob).toMatchObject({
      _id: "legacy-upsert",
      canonicalJobKey: "platform:greenhouse:acme:legacy role",
      lastSeenAt: "2026-04-16T00:00:00.000Z",
      indexedAt: "2026-04-16T00:00:00.000Z",
      canonicalUrl: "https://example.com/jobs/legacy-role?canonical=1",
      resolvedUrl: "https://example.com/jobs/legacy-role/apply?resolved=2",
    });
    expect(updatedJob.crawlRunIds).toEqual(expect.arrayContaining([crawlRun._id]));
  });
});
