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
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-04-10T12:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? discoveredAt;

  return {
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
    sourcePlatform: overrides.sourcePlatform ?? "greenhouse",
    sourceCompanySlug: overrides.sourceCompanySlug ?? companyNormalized,
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
        sourcePlatform: overrides.sourcePlatform ?? "greenhouse",
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
      `${overrides.sourcePlatform ?? "greenhouse"}:${sourceJobId.toLowerCase()}`,
    ],
    dedupeFingerprint: overrides.dedupeFingerprint ?? `dedupe:${sourceJobId}`,
    companyNormalized,
    titleNormalized,
    locationNormalized,
    contentFingerprint: overrides.contentFingerprint ?? `content:${sourceJobId}`,
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
  jobs: PersistableTestJob[],
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

  await repository.persistJobs(crawlRun._id, jobs);

  return { search, crawlRun };
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
        postingDate: "2026-04-10T00:00:00.000Z",
        discoveredAt: "2026-04-10T12:00:00.000Z",
        crawledAt: "2026-04-10T12:00:00.000Z",
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

    const delta = await getSearchJobDeltas(
      started.result.search._id,
      started.result.delivery?.cursor ?? 0,
      { repository },
    );

    expect(delta.jobs).toEqual([]);
    expect(delta.delivery.cursor).toBe(5);
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
});
