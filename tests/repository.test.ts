import { describe, expect, it } from "vitest";

import { ensureDatabaseIndexes, collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { JobListing } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

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
      {
        title: "Software Engineer",
        company: "Acme",
        country: "United States",
        state: "California",
        city: "San Francisco",
        locationText: "San Francisco, California, United States",
        experienceLevel: "mid",
        sourcePlatform: "greenhouse",
        sourceJobId: "role-1",
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        resolvedUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1",
        postedAt: "2026-03-20T00:00:00.000Z",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        linkStatus: "valid",
        lastValidatedAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
        sourceProvenance: [
          {
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            resolvedUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
        sourceLookupKeys: ["greenhouse:role-1"],
        companyNormalized: "acme",
        titleNormalized: "software engineer",
        locationNormalized: "san francisco california united states",
        contentFingerprint: "fingerprint-1",
      },
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
    expect(validation?.jobId).toBe(savedJob._id);
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
      {
        title: "Software Engineer",
        company: "Acme",
        country: "United States",
        state: "California",
        city: "San Francisco",
        locationText: "San Francisco, California, United States",
        experienceLevel: "mid",
        sourcePlatform: "greenhouse",
        sourceJobId: "role-1",
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1",
        postedAt: "2026-03-20T00:00:00.000Z",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        linkStatus: "unknown",
        lastValidatedAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
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
        sourceLookupKeys: ["greenhouse:role-1"],
        companyNormalized: "acme",
        titleNormalized: "software engineer",
        locationNormalized: "san francisco california united states",
        contentFingerprint: "fingerprint-1",
      },
    ]);

    const [mergedJob] = await repository.persistJobs(secondRun._id, [
      {
        title: "Software Engineer",
        company: "Acme",
        country: "United States",
        state: "California",
        city: "San Francisco",
        locationText: "San Francisco, California, United States",
        experienceLevel: "mid",
        sourcePlatform: "greenhouse",
        sourceJobId: "role-1",
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        resolvedUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1",
        postedAt: "2026-03-21T00:00:00.000Z",
        discoveredAt: "2026-03-30T00:00:00.000Z",
        linkStatus: "valid",
        lastValidatedAt: "2026-03-30T00:00:00.000Z",
        rawSourceMetadata: {},
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
        sourceLookupKeys: ["greenhouse:role-1"],
        companyNormalized: "acme",
        titleNormalized: "software engineer",
        locationNormalized: "san francisco california united states",
        contentFingerprint: "fingerprint-1",
      },
    ]);

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]._id).toBe(mergedJob._id);
    expect(storedJobs[0].crawlRunIds).toEqual([firstRun._id, secondRun._id]);
    expect(storedJobs[0].linkStatus).toBe("valid");
    expect(storedJobs[0].resolvedUrl).toBe("https://example.com/jobs/1/apply");
    expect(storedJobs[0].postedAt).toBe("2026-03-21T00:00:00.000Z");
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

  it("creates the expected MongoDB indexes", async () => {
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);

    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_listing_by_run_and_sort");
    expect(
      db.collection(collectionNames.linkValidations).indexes.map((index) => index.name),
    ).toContain("linkValidations_applyUrl_checkedAt_desc");
  });
});
