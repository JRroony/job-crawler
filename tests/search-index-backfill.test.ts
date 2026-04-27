import { describe, expect, it } from "vitest";

import { collectionNames } from "@/lib/server/db/collections";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { getIndexedJobsForSearch } from "@/lib/server/search/indexed-jobs";
import { buildSearchIndexBackfillRepair } from "@/lib/server/search/search-index-backfill";
import { FakeDb } from "@/tests/helpers/fake-db";

describe("search index backfill repair", () => {
  it("generates United States location keys for a legacy Seattle, WA job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-seattle",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Seattle, WA",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      state: "Washington",
      stateCode: "WA",
      city: "Seattle",
      isUnitedStates: true,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "country_code:us",
        "region:united states:washington",
        "region_code:united states:wa",
        "city:united states:washington:seattle",
      ]),
    );
  });

  it("generates United States remote keys for a legacy Remote US job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-remote-us",
      title: "Software Engineer",
      company: "Acme",
      locationRaw: "Remote US",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "country_code:us",
        "remote_country:united states",
      ]),
    );
  });

  it("does not invent a city for a state-only United States location", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-florida",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Florida",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      state: "Florida",
      stateCode: "FL",
      isUnitedStates: true,
    });
    expect(repair.update.resolvedLocation?.city).toBeUndefined();
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "region:united states:florida",
        "region_code:united states:fl",
      ]),
    );
    expect(
      repair.update.searchIndex.locationSearchKeys.some((key) =>
        key.includes(":florida:florida"),
      ),
    ).toBe(false);
  });

  it("generates AI/ML role keys for a legacy Machine Learning Engineer job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-ml",
      title: "Machine Learning Engineer",
      company: "Acme",
      locationText: "Remote United States",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.searchIndex).toMatchObject({
      titleNormalized: "machine learning engineer",
      titleStrippedNormalized: "machine learning engineer",
      titleFamily: "ai_ml_science",
      titleRoleGroup: "engineering",
      titleConceptIds: expect.arrayContaining(["machine_learning_engineer"]),
      titleSearchKeys: expect.arrayContaining([
        "family:ai_ml_science",
        "family_role:ai_ml_science:engineering",
        "concept:machine_learning_engineer",
        "term:machine learning engineer",
        "term:ml engineer",
        "term:ai engineer",
        "term:applied ml engineer",
        "term:computer vision engineer",
        "term:nlp engineer",
      ]),
    });
  });

  it("does not generate United States keys for a non-US legacy job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-canada",
      title: "Machine Learning Engineer",
      company: "Acme",
      locationText: "Toronto, Canada",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "Canada",
      isUnitedStates: false,
    });
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country:united states",
    );
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country_code:us",
    );
    expect(
      repair.update.searchIndex.locationSearchKeys.some((key) =>
        key.startsWith("remote_country:united states"),
      ),
    ).toBe(false);
  });

  it("keeps global remote text from becoming a guessed country", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-worldwide",
      title: "Linux Kernel Engineer",
      company: "Acme",
      locationText: "Home based - Worldwide",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      isRemote: true,
      isUnitedStates: false,
    });
    expect(repair.update.resolvedLocation?.country).toBeUndefined();
    expect(repair.update.searchIndex.locationSearchKeys).toContain("remote_global");
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country:home based worldwide",
    );
  });

  it("canonicalizes clear two-letter non-US country codes in derived location data", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-france",
      title: "Marketing Analyst",
      company: "Acme",
      locationText: "FR",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "France",
      isUnitedStates: false,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toContain("country:france");
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain("country:fr");
  });

  it("backfills searchable title and location keys across multiple role families", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const legacyJobs = [
      createLegacyJob({
        _id: "legacy-applied-scientist",
        title: "Applied Scientist",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createLegacyJob({
        _id: "legacy-machine-learning-engineer",
        title: "Machine Learning Engineer",
        locationText: "San Francisco, CA",
        state: "California",
        city: "San Francisco",
      }),
      createLegacyJob({
        _id: "legacy-data-analyst",
        title: "Data Analyst",
        country: "Canada",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
      }),
      createLegacyJob({
        _id: "legacy-product-manager",
        title: "Product Manager",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
    ];

    for (const job of legacyJobs) {
      await db.collection(collectionNames.jobs).insertOne(job);
      const repair = buildSearchIndexBackfillRepair(job);
      await db.collection(collectionNames.jobs).updateOne(
        { _id: job._id },
        { $set: repair.update },
      );
    }

    await expectIndexedSourceJobIds(repository, {
      title: "applied scientist",
      country: "United States",
    }, ["legacy-applied-scientist"]);
    await expectIndexedSourceJobIds(repository, {
      title: "machine learning engineer",
      country: "United States",
    }, ["legacy-applied-scientist", "legacy-machine-learning-engineer"]);
    await expectIndexedSourceJobIds(repository, {
      title: "data analyst",
      country: "Canada",
    }, ["legacy-data-analyst"]);
    await expectIndexedSourceJobIds(repository, {
      title: "product manager",
      country: "United States",
    }, ["legacy-product-manager"]);
  });
});

function createLegacyJob(input: {
  _id: string;
  title: string;
  locationText: string;
  country?: string;
  state?: string;
  city?: string;
}) {
  const sourceJobId = input._id;
  const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;

  return {
    _id: input._id,
    title: input.title,
    company: "Acme",
    country: input.country ?? "United States",
    state: input.state,
    city: input.city,
    locationText: input.locationText,
    sourcePlatform: "greenhouse" as const,
    sourceCompanySlug: "acme",
    sourceJobId,
    sourceUrl: canonicalUrl,
    applyUrl: `${canonicalUrl}/apply`,
    canonicalUrl,
    discoveredAt: "2026-04-01T00:00:00.000Z",
    crawledAt: "2026-04-01T00:00:00.000Z",
    firstSeenAt: "2026-04-01T00:00:00.000Z",
    lastSeenAt: "2026-04-01T00:00:00.000Z",
    indexedAt: "2026-04-01T00:00:00.000Z",
    isActive: true,
    linkStatus: "unknown" as const,
    rawSourceMetadata: {},
    sourceLookupKeys: [`greenhouse:${sourceJobId}`],
    sourceProvenance: [],
    crawlRunIds: ["legacy-backfill-run"],
    canonicalJobKey: `platform:greenhouse:acme:${sourceJobId}`,
    companyNormalized: "acme",
    normalizedCompany: "acme",
    dedupeFingerprint: `dedupe:${sourceJobId}`,
    contentFingerprint: `content:${sourceJobId}`,
    contentHash: `content-hash:${sourceJobId}`,
  };
}

async function expectIndexedSourceJobIds(
  repository: JobCrawlerRepository,
  filters: { title: string; country: string },
  expectedIds: string[],
) {
  const result = await getIndexedJobsForSearch(repository, filters);
  const ids = result.matches.map(({ job }) => job.sourceJobId);

  expect(ids).toEqual(expect.arrayContaining(expectedIds));
}
