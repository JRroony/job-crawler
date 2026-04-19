import { describe, expect, it, vi } from "vitest";

import {
  getSearchDetails,
  getSearchJobDeltas,
  runSearchFromFilters,
  runSearchIngestionFromFilters,
  startSearchFromFilters,
} from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { getIndexedJobsForSearch } from "@/lib/server/search/indexed-jobs";
import type { JobListing } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createPersistableJob(
  overrides: Partial<PersistableTestJob> = {},
): PersistableTestJob {
  const title = overrides.title ?? "Software Engineer";
  const company = overrides.company ?? "Acme";
  const companyNormalized = overrides.companyNormalized ?? company.toLowerCase();
  const titleNormalized = overrides.titleNormalized ?? title.toLowerCase();
  const locationText = overrides.locationText ?? "Seattle, WA";
  const locationNormalized =
    overrides.locationNormalized ??
    locationText
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  const canonicalUrl = overrides.canonicalUrl ?? "https://example.com/jobs/role-1";
  const applyUrl = overrides.applyUrl ?? `${canonicalUrl}/apply`;
  const sourceUrl = overrides.sourceUrl ?? canonicalUrl;
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug = overrides.sourceCompanySlug ?? companyNormalized;
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-04-10T12:00:00.000Z";
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
    normalizedLocation: locationNormalized,
    locationText,
    resolvedLocation: overrides.resolvedLocation,
    remoteType: overrides.remoteType ?? "unknown",
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
    postingDate: overrides.postingDate ?? "2026-04-09T00:00:00.000Z",
    postedAt: overrides.postedAt ?? overrides.postingDate ?? "2026-04-09T00:00:00.000Z",
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
      `${sourcePlatform}:${sourceJobId.toLowerCase()}`,
    ],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: overrides.dedupeFingerprint ?? `dedupe:${sourceJobId}`,
    companyNormalized,
    titleNormalized,
    locationNormalized,
    contentFingerprint: overrides.contentFingerprint ?? `content:${sourceJobId}`,
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
  };
}

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

function createDiscovery(): DiscoveryService {
  return {
    async discover() {
      return [
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/acme",
          token: "acme",
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
      ];
    },
  };
}

async function seedIndexedJobs(
  repository: JobCrawlerRepository,
  jobs: readonly PersistableTestJob[],
) {
  const search = await repository.createSearch(
    {
      title: "Seed Search",
    },
    "2026-04-10T12:00:00.000Z",
  );
  const crawlRun = await repository.createCrawlRun(
    search._id,
    "2026-04-10T12:00:00.000Z",
  );

  await repository.persistJobs(crawlRun._id, [...jobs]);

  return { search, crawlRun };
}

async function createActiveSearchState(
  repository: JobCrawlerRepository,
  filters: {
    title: string;
    country?: string;
    platforms?: Array<"greenhouse" | "lever" | "ashby" | "workday">;
  },
  now = "2026-04-15T12:00:00.000Z",
) {
  const search = await repository.createSearch(filters, now);
  const searchSession = await repository.createSearchSession(search._id, now, {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, now, {
    searchSessionId: searchSession._id,
    stage: "discovering",
  });

  await Promise.all([
    repository.updateSearchLatestSession(search._id, searchSession._id, "running", now),
    repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now),
    repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: now,
    }),
  ]);

  return { search, searchSession, crawlRun };
}

describe("jobs-first indexed search", () => {
  it("treats runSearchFromFilters as an indexed-first search entry point instead of waiting for ingestion completion", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "indexed-primary",
        canonicalUrl: "https://example.com/jobs/indexed-primary",
        applyUrl: "https://example.com/jobs/indexed-primary/apply",
        sourceUrl: "https://example.com/jobs/indexed-primary",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T11:55:00.000Z",
        crawledAt: "2026-04-15T11:55:00.000Z",
        indexedAt: "2026-04-15T11:55:00.000Z",
      }),
    ]);

    let providerFinished = false;
    const provider = createStubProvider("greenhouse", async () => {
      await new Promise((resolve) => setTimeout(resolve, 150));
      providerFinished = true;

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          {
            title: "Software Engineer II",
            company: "Acme",
            country: "United States",
            locationText: "Remote - United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "supplemental-role",
            sourceUrl: "https://example.com/jobs/supplemental-role",
            applyUrl: "https://example.com/jobs/supplemental-role/apply",
            canonicalUrl: "https://example.com/jobs/supplemental-role",
            discoveredAt: "2026-04-15T12:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const initial = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "run-search-default",
      },
    );

    expect(providerFinished).toBe(false);
    expect(initial.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(initial.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
      source: "jobs_collection",
    });
    expect(initial.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      triggerReason: "insufficient_indexed_coverage",
    });

    await new Promise((resolve) => setTimeout(resolve, 200));

    const final = await getSearchDetails(initial.search._id, {
      repository,
      now: new Date("2026-04-15T12:00:01.000Z"),
    });

    expect(providerFinished).toBe(true);
    expect(final.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Software Engineer II",
    ]);
  });

  it("keeps the full crawl pipeline available through the explicit ingestion entry point", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 0,
      jobs: [
        {
          title: "Software Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote - United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "dedupe-a",
          sourceUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          canonicalUrl: "https://example.com/jobs/shared-role",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote - United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "dedupe-b",
          sourceUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          canonicalUrl: "https://example.com/jobs/shared-role",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
      ],
    }));

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
      },
    );

    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]?.title).toBe("Software Engineer");
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      fetchedCount: 2,
      matchedCount: 2,
      savedCount: 1,
    });
    expect(result.crawlRun.status).toBe("completed");
  });

  it("returns indexed jobs immediately before the supplemental crawl finishes", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "indexed-seattle",
        canonicalUrl: "https://example.com/jobs/indexed-seattle",
        applyUrl: "https://example.com/jobs/indexed-seattle/apply",
        sourceUrl: "https://example.com/jobs/indexed-seattle",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T11:58:00.000Z",
        crawledAt: "2026-04-15T11:58:00.000Z",
        indexedAt: "2026-04-15T11:58:00.000Z",
      }),
    ]);

    let providerResolved = false;
    const provider = createStubProvider("greenhouse", async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      providerResolved = true;

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "jobs-first-indexed",
      },
    );

    expect(providerResolved).toBe(false);
    expect(started.result.jobs).toHaveLength(1);
    expect(started.result.jobs[0]?.title).toBe("Software Engineer");
    expect(started.result.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
      source: "jobs_collection",
    });
    expect(started.result.delivery?.cursor).toBe(1);
    expect(started.result.crawlRun.status).toBe("running");
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      triggerReason: "insufficient_indexed_coverage",
    });

    const initialDelta = await getSearchJobDeltas(started.result.search._id, 1, {
      repository,
      now: new Date("2026-04-15T12:00:00.000Z"),
    });

    expect(initialDelta.jobs).toEqual([]);
    expect(initialDelta.delivery.cursor).toBe(1);

    await new Promise((resolve) => setTimeout(resolve, 250));

    const finalResult = await getSearchDetails(started.result.search._id, {
      repository,
      now: new Date("2026-04-15T12:00:01.000Z"),
    });

    expect(providerResolved).toBe(true);
    expect(finalResult.jobs).toHaveLength(1);
    expect(finalResult.jobs[0]?.title).toBe("Software Engineer");
  });

  it("skips supplemental crawling when indexed coverage is already sufficient for the session", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "pm-1",
        canonicalUrl: "https://example.com/jobs/pm-1",
        applyUrl: "https://example.com/jobs/pm-1/apply",
        sourceUrl: "https://example.com/jobs/pm-1",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        sourceJobId: "pm-2",
        canonicalUrl: "https://example.com/jobs/pm-2",
        applyUrl: "https://example.com/jobs/pm-2/apply",
        sourceUrl: "https://example.com/jobs/pm-2",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      createPersistableJob({
        title: "Principal Product Manager",
        sourceJobId: "pm-3",
        canonicalUrl: "https://example.com/jobs/pm-3",
        applyUrl: "https://example.com/jobs/pm-3/apply",
        sourceUrl: "https://example.com/jobs/pm-3",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
      }),
      createPersistableJob({
        title: "Group Product Manager",
        sourceJobId: "pm-4",
        canonicalUrl: "https://example.com/jobs/pm-4",
        applyUrl: "https://example.com/jobs/pm-4/apply",
        sourceUrl: "https://example.com/jobs/pm-4",
        locationText: "Remote - California",
      }),
      createPersistableJob({
        title: "Lead Product Manager",
        sourceJobId: "pm-5",
        canonicalUrl: "https://example.com/jobs/pm-5",
        applyUrl: "https://example.com/jobs/pm-5/apply",
        sourceUrl: "https://example.com/jobs/pm-5",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
    ]);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    const started = await startSearchFromFilters(
      {
        title: "Product Manager",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:03:00.000Z"),
      },
    );

    expect(started.queued).toBe(false);
    expect(crawlSources).not.toHaveBeenCalled();
    expect(started.result.crawlRun.status).toBe("completed");
    expect(started.result.delivery?.cursor).toBe(5);
    expect(started.result.jobs).toHaveLength(5);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 5,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 5,
      supplementalQueued: false,
      supplementalRunning: false,
      triggerReason: "indexed_coverage_sufficient",
    });

    const delta = await getSearchJobDeltas(
      started.result.search._id,
      started.result.delivery?.cursor ?? 0,
      { repository },
    );

    expect(delta.jobs).toEqual([]);
    expect(delta.delivery.cursor).toBe(5);
  });

  it("does not fall back to a full listJobs scan when priming indexed search results", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Remote - United States",
        sourceJobId: "no-full-scan-match",
        canonicalUrl: "https://example.com/jobs/no-full-scan-match",
        applyUrl: "https://example.com/jobs/no-full-scan-match/apply",
        sourceUrl: "https://example.com/jobs/no-full-scan-match",
      }),
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "no-full-scan-noise",
        canonicalUrl: "https://example.com/jobs/no-full-scan-noise",
        applyUrl: "https://example.com/jobs/no-full-scan-noise/apply",
        sourceUrl: "https://example.com/jobs/no-full-scan-noise",
      }),
    ]);

    const listJobsSpy = vi.spyOn(repository, "listJobs");
    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:04:00.000Z"),
      },
    );

    expect(listJobsSpy).not.toHaveBeenCalled();
    expect(started.result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
  });

  it("returns matching background-indexed jobs in delta responses for an active search session", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:01:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Senior Software Engineer",
        sourceJobId: "background-match",
        canonicalUrl: "https://example.com/jobs/background-match",
        applyUrl: "https://example.com/jobs/background-match/apply",
        sourceUrl: "https://example.com/jobs/background-match",
        locationText: "Remote - United States",
      }),
    ]);

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:01:00.000Z"),
    });

    expect(delta.jobs.map((job) => job.sourceJobId)).toEqual(["background-match"]);
    expect(delta.delivery.cursor).toBe(initial.delivery?.cursor ?? 0);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("does not leak non-matching background-indexed jobs into an active session delta", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:02:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "background-non-match",
        canonicalUrl: "https://example.com/jobs/background-non-match",
        applyUrl: "https://example.com/jobs/background-non-match/apply",
        sourceUrl: "https://example.com/jobs/background-non-match",
        locationText: "Remote - United States",
      }),
    ]);

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:02:00.000Z"),
    });

    expect(delta.jobs).toEqual([]);
    expect(delta.delivery.cursor).toBe(initial.delivery?.cursor ?? 0);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("dedupes overlapping session and indexed deltas when background and active-session writes converge on the same job", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:03:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "shared-active-background",
        canonicalUrl: "https://example.com/jobs/shared-active-background",
        applyUrl: "https://example.com/jobs/shared-active-background/apply",
        sourceUrl: "https://example.com/jobs/shared-active-background",
        locationText: "Remote - United States",
      }),
    ]);
    await repository.persistJobs(
      active.crawlRun._id,
      [
        createPersistableJob({
          title: "Software Engineer",
          sourceJobId: "shared-active-background",
          canonicalUrl: "https://example.com/jobs/shared-active-background",
          applyUrl: "https://example.com/jobs/shared-active-background/apply",
          sourceUrl: "https://example.com/jobs/shared-active-background",
          locationText: "Remote - United States",
        }),
      ],
      { searchSessionId: active.searchSession._id },
    );

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:03:00.000Z"),
    });

    expect(delta.jobs).toHaveLength(1);
    expect(delta.jobs[0]?.sourceJobId).toBe("shared-active-background");
    expect(delta.delivery.cursor).toBe(1);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("reuses persisted results for repeated identical searches instead of re-crawling sparse completed searches", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    let providerCalls = 0;
    const provider = createStubProvider("greenhouse", async () => {
      providerCalls += 1;

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 2,
        matchedCount: 2,
        warningCount: 0,
        jobs: [
          {
            title: "Business Analyst",
            company: "Acme",
            country: "United States",
            locationText: "Chicago, IL",
            state: "Illinois",
            city: "Chicago",
            sourcePlatform: "greenhouse",
            sourceJobId: "business-analyst-repeat-1",
            sourceUrl: "https://example.com/jobs/business-analyst-repeat-1",
            applyUrl: "https://example.com/jobs/business-analyst-repeat-1/apply",
            canonicalUrl: "https://example.com/jobs/business-analyst-repeat-1",
            discoveredAt: "2026-04-15T12:10:00.000Z",
            rawSourceMetadata: {},
          },
          {
            title: "Business Systems Analyst",
            company: "Acme",
            country: "United States",
            locationText: "Remote - United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "business-analyst-repeat-2",
            sourceUrl: "https://example.com/jobs/business-analyst-repeat-2",
            applyUrl: "https://example.com/jobs/business-analyst-repeat-2/apply",
            canonicalUrl: "https://example.com/jobs/business-analyst-repeat-2",
            discoveredAt: "2026-04-15T12:10:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const runtime = {
      repository,
      providers: [provider],
      discovery: createDiscovery(),
      fetchImpl: vi.fn() as unknown as typeof fetch,
    };

    const firstSearch = await startSearchFromFilters(
      {
        title: "Business Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        ...runtime,
        now: new Date("2026-04-15T12:10:00.000Z"),
        requestOwnerKey: "business-analyst-repeat-initial",
      },
    );

    expect(firstSearch.queued).toBe(true);

    const completedFirstSearch = await getSearchDetails(firstSearch.result.search._id, {
      repository,
      now: new Date("2026-04-15T12:10:01.000Z"),
    });

    expect(providerCalls).toBe(1);
    expect(completedFirstSearch.jobs.map((job) => job.title)).toEqual([
      "Business Analyst",
      "Business Systems Analyst",
    ]);

    const repeatedSearch = await startSearchFromFilters(
      {
        title: "Business Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        ...runtime,
        now: new Date("2026-04-15T12:11:00.000Z"),
        requestOwnerKey: "business-analyst-repeat-followup",
      },
    );

    expect(repeatedSearch.result.search._id).toBe(firstSearch.result.search._id);
    expect(repeatedSearch.queued).toBe(false);
    expect(providerCalls).toBe(1);
    expect(repeatedSearch.result.jobs.map((job) => job.title)).toEqual([
      "Business Analyst",
      "Business Systems Analyst",
    ]);
    expect(repeatedSearch.result.diagnostics.session).toMatchObject({
      supplementalQueued: false,
      supplementalRunning: false,
      triggerReason: "reused_completed_coverage",
    });
  });

  it("triggers bounded supplemental freshness recovery when sparse indexed coverage is stale", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Data Analyst",
        sourceJobId: "stale-data-analyst",
        canonicalUrl: "https://example.com/jobs/stale-data-analyst",
        applyUrl: "https://example.com/jobs/stale-data-analyst/apply",
        sourceUrl: "https://example.com/jobs/stale-data-analyst",
        locationText: "Remote - United States",
        postingDate: "2026-03-01T00:00:00.000Z",
        discoveredAt: "2026-03-01T12:00:00.000Z",
        crawledAt: "2026-03-01T12:00:00.000Z",
        indexedAt: "2026-03-01T12:00:00.000Z",
      }),
    ]);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        {
          title: "Senior Data Analyst",
          company: "Acme",
          country: "United States",
          locationText: "Chicago, IL",
          state: "Illinois",
          city: "Chicago",
          sourcePlatform: "greenhouse" as const,
          sourceJobId: "fresh-data-analyst",
          sourceUrl: "https://example.com/jobs/fresh-data-analyst",
          applyUrl: "https://example.com/jobs/fresh-data-analyst/apply",
          canonicalUrl: "https://example.com/jobs/fresh-data-analyst",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
      ],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    const started = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "stale-data-analyst-recovery",
      },
    );

    expect(started.queued).toBe(true);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      triggerReason: "freshness_recovery",
    });
    expect(started.result.diagnostics.session?.latestIndexedJobAgeMs).toBeGreaterThan(0);

    await new Promise((resolve) => setTimeout(resolve, 100));

    const final = await getSearchDetails(started.result.search._id, {
      repository,
      now: new Date("2026-04-15T12:00:01.000Z"),
    });

    expect(final.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining(["Data Analyst", "Senior Data Analyst"]),
    );
    expect(final.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 1,
      totalVisibleResultsCount: 2,
      supplementalQueued: true,
      supplementalRunning: false,
      triggerReason: "freshness_recovery",
    });
  });

  it("serves representative United States searches from the index first without supplemental crawl when coverage is already strong", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const scenarios = [
      {
        title: "Software Engineer",
        jobs: [
          createPersistableJob({
            title: "Software Engineer",
            sourceJobId: "scenario-se-1",
            canonicalUrl: "https://example.com/jobs/scenario-se-1",
            applyUrl: "https://example.com/jobs/scenario-se-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Backend Engineer",
            sourceJobId: "scenario-se-2",
            canonicalUrl: "https://example.com/jobs/scenario-se-2",
            applyUrl: "https://example.com/jobs/scenario-se-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-2",
            locationText: "Seattle, WA",
            state: "Washington",
            city: "Seattle",
          }),
          createPersistableJob({
            title: "Full Stack Engineer",
            sourceJobId: "scenario-se-3",
            canonicalUrl: "https://example.com/jobs/scenario-se-3",
            applyUrl: "https://example.com/jobs/scenario-se-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-3",
            locationText: "Austin, TX",
            state: "Texas",
            city: "Austin",
          }),
        ],
      },
      {
        title: "Data Analyst",
        jobs: [
          createPersistableJob({
            title: "Data Analyst",
            sourceJobId: "scenario-da-1",
            canonicalUrl: "https://example.com/jobs/scenario-da-1",
            applyUrl: "https://example.com/jobs/scenario-da-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Senior Data Analyst",
            sourceJobId: "scenario-da-2",
            canonicalUrl: "https://example.com/jobs/scenario-da-2",
            applyUrl: "https://example.com/jobs/scenario-da-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-2",
            locationText: "New York, NY",
            state: "New York",
            city: "New York",
          }),
          createPersistableJob({
            title: "Product Data Analyst",
            sourceJobId: "scenario-da-3",
            canonicalUrl: "https://example.com/jobs/scenario-da-3",
            applyUrl: "https://example.com/jobs/scenario-da-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-3",
            locationText: "Chicago, IL",
            state: "Illinois",
            city: "Chicago",
          }),
        ],
      },
      {
        title: "Business Analyst",
        jobs: [
          createPersistableJob({
            title: "Business Analyst",
            sourceJobId: "scenario-ba-1",
            canonicalUrl: "https://example.com/jobs/scenario-ba-1",
            applyUrl: "https://example.com/jobs/scenario-ba-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-1",
            locationText: "Chicago, IL",
            state: "Illinois",
            city: "Chicago",
          }),
          createPersistableJob({
            title: "Senior Business Analyst",
            sourceJobId: "scenario-ba-2",
            canonicalUrl: "https://example.com/jobs/scenario-ba-2",
            applyUrl: "https://example.com/jobs/scenario-ba-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-2",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Business Systems Analyst",
            sourceJobId: "scenario-ba-3",
            canonicalUrl: "https://example.com/jobs/scenario-ba-3",
            applyUrl: "https://example.com/jobs/scenario-ba-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-3",
            locationText: "Boston, MA",
            state: "Massachusetts",
            city: "Boston",
          }),
        ],
      },
      {
        title: "Product Manager",
        jobs: [
          createPersistableJob({
            title: "Product Manager",
            sourceJobId: "scenario-pm-1",
            canonicalUrl: "https://example.com/jobs/scenario-pm-1",
            applyUrl: "https://example.com/jobs/scenario-pm-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Senior Product Manager",
            sourceJobId: "scenario-pm-2",
            canonicalUrl: "https://example.com/jobs/scenario-pm-2",
            applyUrl: "https://example.com/jobs/scenario-pm-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-2",
            locationText: "Seattle, WA",
            state: "Washington",
            city: "Seattle",
          }),
          createPersistableJob({
            title: "Technical Product Manager",
            sourceJobId: "scenario-pm-3",
            canonicalUrl: "https://example.com/jobs/scenario-pm-3",
            applyUrl: "https://example.com/jobs/scenario-pm-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-3",
            locationText: "Austin, TX",
            state: "Texas",
            city: "Austin",
          }),
        ],
      },
    ] as const;

    for (const scenario of scenarios) {
      await seedIndexedJobs(repository, scenario.jobs);
    }

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    for (const scenario of scenarios) {
      const started = await startSearchFromFilters(
        {
          title: scenario.title,
          country: "United States",
          crawlMode: "fast",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:20:00.000Z"),
          requestOwnerKey: `representative-${scenario.title}`,
        },
      );

      expect(started.queued).toBe(false);
      expect(started.result.jobs.length).toBeGreaterThanOrEqual(3);
      expect(
        started.result.jobs.every(
          (job) => job.resolvedLocation?.isUnitedStates ?? job.country === "United States",
        ),
      ).toBe(true);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: started.result.jobs.length,
        supplementalResultsCount: 0,
        totalVisibleResultsCount: started.result.jobs.length,
        supplementalQueued: false,
        supplementalRunning: false,
        triggerReason: "indexed_coverage_sufficient",
      });
    }

    expect(crawlSources).not.toHaveBeenCalled();
  });

  it("serves requested role-family and country scenarios from indexed jobs using resolved location evidence", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const withoutTopLevelCountry = (job: PersistableTestJob): PersistableTestJob => {
      const { country: _country, ...rest } = job;
      return rest;
    };
    const scenarioJobs = [
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "scenario-expanded-se-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-1",
        locationText: "Remote - United States",
        resolvedLocation: {
          country: "United States",
          isRemote: true,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - United States" }],
        },
      }),
      createPersistableJob({
        title: "Backend Developer",
        sourceJobId: "scenario-expanded-se-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-2",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        resolvedLocation: {
          country: "United States",
          state: "Washington",
          stateCode: "WA",
          city: "Seattle",
          isRemote: false,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Seattle, WA" }],
        },
      }),
      createPersistableJob({
        title: "Full Stack Engineer",
        sourceJobId: "scenario-expanded-se-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-3",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
        resolvedLocation: {
          country: "United States",
          state: "Texas",
          stateCode: "TX",
          city: "Austin",
          isRemote: false,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Austin, TX" }],
        },
      }),
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "scenario-expanded-pm-us-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-1",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        sourceJobId: "scenario-expanded-pm-us-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-2",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      createPersistableJob({
        title: "Technical Product Manager",
        sourceJobId: "scenario-expanded-pm-us-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-3",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
      }),
      createPersistableJob({
        title: "AI Engineer",
        sourceJobId: "scenario-expanded-ai-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-1",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Machine Learning Engineer",
        sourceJobId: "scenario-expanded-ai-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-2",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Applied Scientist",
        sourceJobId: "scenario-expanded-ai-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-3",
        locationText: "Cambridge, MA",
        state: "Massachusetts",
        city: "Cambridge",
      }),
      createPersistableJob({
        title: "Research Scientist",
        sourceJobId: "scenario-expanded-ai-4",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-4",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-4/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-4",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      createPersistableJob({
        title: "Data Scientist",
        sourceJobId: "scenario-expanded-ai-5",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-5",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-5/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-5",
        locationText: "Remote - United States",
      }),
      withoutTopLevelCountry(createPersistableJob({
        title: "Solutions Architect",
        sourceJobId: "scenario-expanded-canada-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-1",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
        resolvedLocation: {
          country: "Canada",
          state: "Ontario",
          stateCode: "ON",
          city: "Toronto",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Toronto, ON" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Cloud Architect",
        sourceJobId: "scenario-expanded-canada-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-2",
        locationText: "Vancouver, BC",
        state: "British Columbia",
        city: "Vancouver",
        resolvedLocation: {
          country: "Canada",
          state: "British Columbia",
          stateCode: "BC",
          city: "Vancouver",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Vancouver, BC" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Solutions Engineer",
        sourceJobId: "scenario-expanded-canada-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-3",
        locationText: "Remote - Canada",
        resolvedLocation: {
          country: "Canada",
          isRemote: true,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - Canada" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Product Manager",
        sourceJobId: "scenario-expanded-germany-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-1",
        locationText: "Berlin, Germany",
        state: "Berlin",
        city: "Berlin",
        resolvedLocation: {
          country: "Germany",
          state: "Berlin",
          city: "Berlin",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Berlin, Germany" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Technical Product Manager",
        sourceJobId: "scenario-expanded-germany-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-2",
        locationText: "Munich, Germany",
        state: "Bavaria",
        city: "Munich",
        resolvedLocation: {
          country: "Germany",
          state: "Bavaria",
          stateCode: "BY",
          city: "Munich",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Munich, Germany" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Product Owner",
        sourceJobId: "scenario-expanded-germany-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-3",
        locationText: "Remote - Germany",
        resolvedLocation: {
          country: "Germany",
          isRemote: true,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - Germany" }],
        },
      })),
    ];

    await seedIndexedJobs(repository, scenarioJobs);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);
    const scenarios = [
      {
        title: "software engineer",
        country: "United States",
        expectedTitles: ["Software Engineer", "Backend Developer", "Full Stack Engineer"],
      },
      {
        title: "product manager",
        country: "United States",
        expectedTitles: ["Product Manager", "Senior Product Manager", "Technical Product Manager"],
      },
      {
        title: "applied scientist",
        country: "United States",
        expectedTitles: ["Applied Scientist", "Research Scientist", "Data Scientist"],
      },
      {
        title: "research scientist",
        country: "United States",
        expectedTitles: ["Research Scientist", "Applied Scientist", "Data Scientist"],
      },
      {
        title: "ai engineer",
        country: "United States",
        expectedTitles: ["AI Engineer", "Machine Learning Engineer", "Applied Scientist"],
      },
      {
        title: "solution architect",
        country: "Canada",
        expectedTitles: ["Solutions Architect", "Cloud Architect", "Solutions Engineer"],
      },
      {
        title: "product manager",
        country: "Germany",
        expectedTitles: ["Product Manager", "Technical Product Manager", "Product Owner"],
      },
    ];

    for (const scenario of scenarios) {
      const started = await startSearchFromFilters(
        {
          title: scenario.title,
          country: scenario.country,
          crawlMode: "fast",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:30:00.000Z"),
          requestOwnerKey: `expanded-${scenario.title}-${scenario.country}`,
        },
      );

      expect(started.queued).toBe(false);
      expect(started.result.jobs.map((job) => job.title)).toEqual(
        expect.arrayContaining(scenario.expectedTitles),
      );
      expect(started.result.jobs.length).toBeGreaterThanOrEqual(scenario.expectedTitles.length);
      expect(started.result.jobs.every((job) => job.rawSourceMetadata.indexedSearch)).toBe(true);
      expect(
        started.result.jobs.every(
          (job) => job.resolvedLocation?.country === scenario.country || job.country === scenario.country,
        ),
      ).toBe(true);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: started.result.jobs.length,
        supplementalResultsCount: 0,
        totalVisibleResultsCount: started.result.jobs.length,
        supplementalQueued: false,
        supplementalRunning: false,
        triggerReason: "indexed_coverage_sufficient",
      });
      expect(started.result.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
        candidateQuery: expect.objectContaining({
          strategy: "coarse_prefilter",
          usedLocationPrefilter: true,
        }),
        titleMatch: expect.objectContaining({
          explanation: expect.any(String),
        }),
        locationMatch: expect.objectContaining({
          matches: true,
          explanation: expect.any(String),
        }),
      });
    }

    expect(crawlSources).not.toHaveBeenCalled();
  });

  it("keeps semantic title, US location, experience filters, and ranking on indexed jobs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Remote - United States",
        sourceJobId: "software-exact",
        canonicalUrl: "https://example.com/jobs/software-exact",
        applyUrl: "https://example.com/jobs/software-exact/apply",
        sourceUrl: "https://example.com/jobs/software-exact",
        experienceLevel: "mid",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T12:00:00.000Z",
        crawledAt: "2026-04-14T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Backend Developer",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
        sourceJobId: "software-semantic",
        canonicalUrl: "https://example.com/jobs/software-semantic",
        applyUrl: "https://example.com/jobs/software-semantic/apply",
        sourceUrl: "https://example.com/jobs/software-semantic",
        experienceLevel: "mid",
        postingDate: "2026-04-12T00:00:00.000Z",
        discoveredAt: "2026-04-12T12:00:00.000Z",
        crawledAt: "2026-04-12T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Senior Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "software-senior",
        canonicalUrl: "https://example.com/jobs/software-senior",
        applyUrl: "https://example.com/jobs/software-senior/apply",
        sourceUrl: "https://example.com/jobs/software-senior",
        experienceLevel: "senior",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T12:00:00.000Z",
        crawledAt: "2026-04-15T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Toronto, ON, Canada",
        country: "Canada",
        sourceJobId: "software-canada",
        canonicalUrl: "https://example.com/jobs/software-canada",
        applyUrl: "https://example.com/jobs/software-canada/apply",
        sourceUrl: "https://example.com/jobs/software-canada",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T12:00:00.000Z",
        crawledAt: "2026-04-13T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Data Analyst",
        locationText: "Remote - US",
        sourceJobId: "data-analyst-exact",
        canonicalUrl: "https://example.com/jobs/data-analyst-exact",
        applyUrl: "https://example.com/jobs/data-analyst-exact/apply",
        sourceUrl: "https://example.com/jobs/data-analyst-exact",
        experienceLevel: "mid",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T13:00:00.000Z",
        crawledAt: "2026-04-14T13:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Intelligence Analyst",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
        sourceJobId: "data-analyst-semantic",
        canonicalUrl: "https://example.com/jobs/data-analyst-semantic",
        applyUrl: "https://example.com/jobs/data-analyst-semantic/apply",
        sourceUrl: "https://example.com/jobs/data-analyst-semantic",
        experienceLevel: "mid",
        postingDate: "2026-04-11T00:00:00.000Z",
        discoveredAt: "2026-04-11T13:00:00.000Z",
        crawledAt: "2026-04-11T13:00:00.000Z",
      }),
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "product-manager-noise",
        canonicalUrl: "https://example.com/jobs/product-manager-noise",
        applyUrl: "https://example.com/jobs/product-manager-noise/apply",
        sourceUrl: "https://example.com/jobs/product-manager-noise",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T15:00:00.000Z",
        crawledAt: "2026-04-14T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Analyst",
        locationText: "Chicago, IL",
        state: "Illinois",
        city: "Chicago",
        sourceJobId: "business-analyst-exact",
        canonicalUrl: "https://example.com/jobs/business-analyst-exact",
        applyUrl: "https://example.com/jobs/business-analyst-exact/apply",
        sourceUrl: "https://example.com/jobs/business-analyst-exact",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T15:00:00.000Z",
        crawledAt: "2026-04-13T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Systems Analyst",
        locationText: "Remote - United States",
        sourceJobId: "business-analyst-semantic",
        canonicalUrl: "https://example.com/jobs/business-analyst-semantic",
        applyUrl: "https://example.com/jobs/business-analyst-semantic/apply",
        sourceUrl: "https://example.com/jobs/business-analyst-semantic",
        postingDate: "2026-04-12T00:00:00.000Z",
        discoveredAt: "2026-04-12T15:00:00.000Z",
        crawledAt: "2026-04-12T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        locationText: "Remote - California",
        sourceJobId: "product-manager-semantic",
        canonicalUrl: "https://example.com/jobs/product-manager-semantic",
        applyUrl: "https://example.com/jobs/product-manager-semantic/apply",
        sourceUrl: "https://example.com/jobs/product-manager-semantic",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T16:00:00.000Z",
        crawledAt: "2026-04-13T16:00:00.000Z",
      }),
    ]);

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const softwareSearch = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        experienceLevels: ["mid"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:05:00.000Z"),
      },
    );

    expect(softwareSearch.result.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Backend Developer",
    ]);
    expect(softwareSearch.result.jobs.every((job) => job.rawSourceMetadata.indexedSearch)).toBe(true);
    expect(softwareSearch.result.jobs[0]?.rawSourceMetadata.crawlRanking).toMatchObject({
      relevanceTier: "exact",
    });
    expect(softwareSearch.result.jobs[1]?.rawSourceMetadata.crawlRanking).toMatchObject({
      relevanceTier: expect.not.stringMatching(/^exact$/),
    });

    const dataSearch = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:06:00.000Z"),
      },
    );

    expect(dataSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Data Analyst",
        "Business Intelligence Analyst",
      ]),
    );
    expect(dataSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Product Manager" }),
      ]),
    );
    expect(dataSearch.result.jobs[0]?.locationText).toBe("Remote - US");
    expect(dataSearch.result.jobs[1]?.locationText).toBe("New York, NY");

    const businessSearch = await startSearchFromFilters(
      {
        title: "Business Analyst",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:07:00.000Z"),
      },
    );

    expect(businessSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Business Analyst",
        "Business Systems Analyst",
      ]),
    );
    expect(businessSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Product Manager" }),
      ]),
    );

    const productSearch = await startSearchFromFilters(
      {
        title: "Product Manager",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:08:00.000Z"),
      },
    );

    expect(productSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Product Manager",
        "Senior Product Manager",
      ]),
    );
    expect(productSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Business Analyst" }),
        expect.objectContaining({ title: "Data Analyst" }),
      ]),
    );
  }, 10_000);

  it("uses a selective coarse indexed candidate set before final precision filtering", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const seededJobs = [
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "candidate-product-exact",
        canonicalUrl: "https://example.com/jobs/candidate-product-exact",
        applyUrl: "https://example.com/jobs/candidate-product-exact/apply",
        sourceUrl: "https://example.com/jobs/candidate-product-exact",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        locationText: "Los Angeles, CA",
        state: "California",
        city: "Los Angeles",
        sourceJobId: "candidate-product-senior",
        canonicalUrl: "https://example.com/jobs/candidate-product-senior",
        applyUrl: "https://example.com/jobs/candidate-product-senior/apply",
        sourceUrl: "https://example.com/jobs/candidate-product-senior",
      }),
      createPersistableJob({
        title: "Technical Program Manager",
        locationText: "Remote - United States",
        sourceJobId: "candidate-program-nearby",
        canonicalUrl: "https://example.com/jobs/candidate-program-nearby",
        applyUrl: "https://example.com/jobs/candidate-program-nearby/apply",
        sourceUrl: "https://example.com/jobs/candidate-program-nearby",
      }),
      createPersistableJob({
        title: "Business Analyst",
        locationText: "Chicago, IL",
        state: "Illinois",
        city: "Chicago",
        sourceJobId: "candidate-business-noise",
        canonicalUrl: "https://example.com/jobs/candidate-business-noise",
        applyUrl: "https://example.com/jobs/candidate-business-noise/apply",
        sourceUrl: "https://example.com/jobs/candidate-business-noise",
      }),
      createPersistableJob({
        title: "Recruiter",
        locationText: "Remote - United States",
        sourceJobId: "candidate-recruiter-noise",
        canonicalUrl: "https://example.com/jobs/candidate-recruiter-noise",
        applyUrl: "https://example.com/jobs/candidate-recruiter-noise/apply",
        sourceUrl: "https://example.com/jobs/candidate-recruiter-noise",
      }),
    ];

    await seedIndexedJobs(repository, seededJobs);

    const indexedSearch = await getIndexedJobsForSearch(repository, {
      title: "Product Manager",
      country: "United States",
    });

    expect(indexedSearch.candidateCount).toBeLessThan(seededJobs.length);
    expect(indexedSearch.candidateQuery).toMatchObject({
      strategy: "coarse_prefilter",
      titleFamily: "product",
      usedLocationPrefilter: true,
    });
    expect(indexedSearch.matches.map(({ job }) => job.title)).toEqual([
      "Product Manager",
      "Senior Product Manager",
    ]);
    expect(indexedSearch.matches.every(({ job }) => job.rawSourceMetadata.indexedSearch)).toBe(true);
    expect(indexedSearch.matches[0]?.job.rawSourceMetadata.indexedSearch).toMatchObject({
      candidateCount: indexedSearch.candidateCount,
      candidateQuery: expect.objectContaining({
        strategy: "coarse_prefilter",
      }),
    });
  });
});
