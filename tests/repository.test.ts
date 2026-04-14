import { describe, expect, it } from "vitest";

import { ensureDatabaseIndexes, collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { JobListing } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createPersistableJob(
  overrides: Partial<PersistableTestJob> = {},
): PersistableTestJob {
  return {
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
    sourcePlatform: "greenhouse",
    sourceCompanySlug: "acme",
    sourceJobId: "role-1",
    sourceUrl: "https://example.com/jobs/1",
    applyUrl: "https://example.com/jobs/1/apply",
    resolvedUrl: "https://example.com/jobs/1/apply",
    canonicalUrl: "https://example.com/jobs/1",
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt: "2026-03-29T00:00:00.000Z",
    crawledAt: "2026-03-29T00:00:00.000Z",
    sponsorshipHint: "unknown",
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
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
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

  it("creates the expected MongoDB indexes", async () => {
    const db = new FakeDb();
    await ensureDatabaseIndexes(db);

    expect(
      db.collection(collectionNames.jobs).indexes.map((index) => index.name),
    ).toContain("jobs_listing_by_run_and_sort");
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
      db.collection(collectionNames.crawlRuns).indexes.map((index) => index.name),
    ).toContain("crawlRuns_validationMode_startedAt_desc");
    expect(
      db.collection(collectionNames.crawlRunJobEvents).indexes.map((index) => index.name),
    ).toContain("crawlRunJobEvents_run_sequence");
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
});
