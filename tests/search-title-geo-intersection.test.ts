import { describe, expect, it, vi } from "vitest";

import { startSearchFromFilters } from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { getIndexedJobsForSearch } from "@/lib/server/search/indexed-jobs";
import { buildIndexedJobCandidateQuery } from "@/lib/server/search/job-search-index";
import type { JobListing } from "@/lib/types";
import { FakeDb } from "@/tests/helpers/fake-db";

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createJob(overrides: Partial<PersistableTestJob> = {}): PersistableTestJob {
  const title = overrides.title ?? "Software Engineer";
  const company = overrides.company ?? "Acme";
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceJobId = overrides.sourceJobId ?? `${title}-${overrides.locationText ?? "remote-us"}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
  const canonicalUrl = overrides.canonicalUrl ?? `https://example.com/jobs/${sourceJobId}`;
  const locationText = overrides.locationText ?? "Remote - United States";
  const locationNormalized = locationText.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const discoveredAt = overrides.discoveredAt ?? "2026-04-20T12:00:00.000Z";

  return {
    canonicalJobKey: overrides.canonicalJobKey ?? `platform:${sourcePlatform}:acme:${sourceJobId}`,
    title,
    company,
    normalizedCompany: overrides.normalizedCompany ?? company.toLowerCase(),
    normalizedTitle: overrides.normalizedTitle ?? title.toLowerCase(),
    country: overrides.country,
    state: overrides.state,
    city: overrides.city,
    locationRaw: overrides.locationRaw ?? locationText,
    normalizedLocation: overrides.normalizedLocation ?? locationNormalized,
    locationText,
    resolvedLocation: overrides.resolvedLocation,
    remoteType: overrides.remoteType ?? "unknown",
    employmentType: overrides.employmentType,
    seniority: overrides.seniority,
    experienceLevel: overrides.experienceLevel,
    experienceClassification: overrides.experienceClassification,
    sourcePlatform,
    sourceCompanySlug: overrides.sourceCompanySlug ?? "acme",
    sourceJobId,
    sourceUrl: overrides.sourceUrl ?? canonicalUrl,
    applyUrl: overrides.applyUrl ?? `${canonicalUrl}/apply`,
    resolvedUrl: overrides.resolvedUrl ?? `${canonicalUrl}/apply`,
    canonicalUrl,
    postingDate: overrides.postingDate ?? "2026-04-20T00:00:00.000Z",
    postedAt: overrides.postedAt ?? "2026-04-20T00:00:00.000Z",
    discoveredAt,
    crawledAt: overrides.crawledAt ?? discoveredAt,
    descriptionSnippet: overrides.descriptionSnippet,
    salaryInfo: overrides.salaryInfo,
    sponsorshipHint: overrides.sponsorshipHint ?? "unknown",
    linkStatus: overrides.linkStatus ?? "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? discoveredAt,
    rawSourceMetadata: overrides.rawSourceMetadata ?? {},
    sourceProvenance: overrides.sourceProvenance ?? [],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [`${sourcePlatform}:${sourceJobId}`],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? discoveredAt,
    indexedAt: overrides.indexedAt ?? discoveredAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: overrides.dedupeFingerprint ?? `dedupe:${sourceJobId}`,
    companyNormalized: overrides.companyNormalized ?? company.toLowerCase(),
    titleNormalized: overrides.titleNormalized ?? title.toLowerCase(),
    locationNormalized: overrides.locationNormalized ?? locationNormalized,
    contentFingerprint: overrides.contentFingerprint ?? `content:${sourceJobId}`,
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
  };
}

async function seed(repository: JobCrawlerRepository, jobs: PersistableTestJob[]) {
  const search = await repository.createSearch({ title: "seed" }, "2026-04-20T12:00:00.000Z");
  const crawlRun = await repository.createCrawlRun(search._id, "2026-04-20T12:00:00.000Z");
  await repository.persistJobs(crawlRun._id, jobs);
}

function sourceId(title: string, suffix: string) {
  return `${title.toLowerCase().replace(/[^a-z0-9]+/g, "-")}-${suffix}`;
}

function locationJob(title: string, suffix: string, country: string, locationText: string, extra: Partial<PersistableTestJob> = {}) {
  const sourceJobId = sourceId(title, suffix);
  const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
  return createJob({
    title,
    country,
    locationText,
    sourceJobId,
    canonicalUrl,
    applyUrl: `${canonicalUrl}/apply`,
    sourceUrl: canonicalUrl,
    ...extra,
  });
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

async function expectSearchIds(
  repository: JobCrawlerRepository,
  filters: { title: string; country?: string; state?: string; city?: string },
  expected: string[],
  excluded: string[],
) {
  const result = await getIndexedJobsForSearch(repository, filters);
  const ids = result.matches.map(({ job }) => job.sourceJobId);

  expect(ids).toEqual(expect.arrayContaining(expected));
  for (const id of excluded) {
    expect(ids).not.toContain(id);
  }
  expect(result.matches.every(({ evaluation }) => evaluation.titleMatch.matches)).toBe(true);
  expect(result.matches.every(({ evaluation }) => evaluation.locationMatch?.matches !== false)).toBe(true);
  expect(result.candidateQuery).toMatchObject({
    hasTitleConstraint: true,
    hasLocationConstraint: true,
    locationConstraintAppliedToEveryTitleChannel: true,
    usesGeoOnlyChannelForVisibleResults: false,
    usesTitleOnlyChannelForLocationSearch: false,
  });
}

describe("indexed title and geo intent intersection", () => {
  it("keeps title families intersected with United States location intent", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const scenarios = [
      ["software engineer", "Backend Developer", "Product Manager"],
      ["machine learning engineer", "AI Engineer", "Product Manager"],
      ["applied scientist", "Research Scientist", "Product Manager"],
      ["data analyst", "Business Intelligence Analyst", "Software Engineer"],
      ["product manager", "Product Owner", "Software Engineer"],
      ["business analyst", "Data Analyst", "Software Engineer"],
      ["data engineer", "Analytics Engineer", "Software Engineer"],
    ] as const;
    const jobs = scenarios.flatMap(([queryTitle, relatedTitle, unrelatedTitle]) => [
      locationJob(queryTitle, "us-exact", "United States", "Seattle, WA", {
        state: "Washington",
        city: "Seattle",
      }),
      locationJob(relatedTitle, "us-related", "United States", "Remote - United States", {
        sourceJobId: sourceId(queryTitle, "us-related"),
      }),
      locationJob(queryTitle, "canada-exact", "Canada", "Toronto, Canada", {
        city: "Toronto",
      }),
      locationJob(queryTitle, "japan-exact", "Japan", "Tokyo, Japan", {
        city: "Tokyo",
      }),
      locationJob(unrelatedTitle, "us-unrelated", "United States", "Austin, TX", {
        sourceJobId: sourceId(queryTitle, "us-unrelated"),
        state: "Texas",
        city: "Austin",
      }),
    ]);
    await seed(repository, jobs);

    for (const [queryTitle] of scenarios) {
      await expectSearchIds(
        repository,
        { title: queryTitle, country: "United States" },
        [
          sourceId(queryTitle, "us-exact"),
          sourceId(queryTitle, "us-related"),
        ],
        [
          sourceId(queryTitle, "canada-exact"),
          sourceId(queryTitle, "japan-exact"),
          sourceId(queryTitle, "us-unrelated"),
        ],
      );
    }
  });

  it("keeps same-title searches isolated by country across role families", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const titles = ["software engineer", "applied scientist", "data analyst", "product manager"];
    await seed(
      repository,
      titles.flatMap((title) => [
        locationJob(title, "us", "United States", "Seattle, WA", { state: "Washington", city: "Seattle" }),
        locationJob(title, "canada", "Canada", "Toronto, Canada", { city: "Toronto" }),
        locationJob(title, "germany", "Germany", "Berlin, Germany", { city: "Berlin" }),
        locationJob(title, "japan", "Japan", "Tokyo, Japan", { city: "Tokyo" }),
      ]),
    );

    for (const title of titles) {
      await expectSearchIds(repository, { title, country: "United States" }, [sourceId(title, "us")], [sourceId(title, "canada"), sourceId(title, "germany"), sourceId(title, "japan")]);
      await expectSearchIds(repository, { title, country: "Canada" }, [sourceId(title, "canada")], [sourceId(title, "us"), sourceId(title, "germany"), sourceId(title, "japan")]);
      await expectSearchIds(repository, { title, country: "Germany" }, [sourceId(title, "germany")], [sourceId(title, "us"), sourceId(title, "canada"), sourceId(title, "japan")]);
      await expectSearchIds(repository, { title, country: "Japan" }, [sourceId(title, "japan")], [sourceId(title, "us"), sourceId(title, "canada"), sourceId(title, "germany")]);
    }
  });

  it("keeps city-level searches pinned to the requested city", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seed(repository, [
      locationJob("Data Analyst", "toronto", "Canada", "Toronto, Canada", { city: "Toronto" }),
      locationJob("Data Analyst", "vancouver", "Canada", "Vancouver, Canada", { city: "Vancouver" }),
      locationJob("Data Analyst", "new-york", "United States", "New York, NY", { state: "New York", city: "New York" }),
    ]);

    await expectSearchIds(
      repository,
      { title: "data analyst", city: "Toronto" },
      [sourceId("Data Analyst", "toronto")],
      [sourceId("Data Analyst", "vancouver"), sourceId("Data Analyst", "new-york")],
    );
  });

  it("keeps remote-country searches from leaking other remote countries", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seed(repository, [
      locationJob("ML Engineer", "remote-us", "United States", "Remote US", { remoteType: "remote" }),
      locationJob("ML Engineer", "remote-canada", "Canada", "Remote Canada", { remoteType: "remote" }),
      createJob({
        title: "ML Engineer",
        country: undefined,
        locationText: "Remote Europe",
        remoteType: "remote",
        sourceJobId: sourceId("ML Engineer", "remote-europe"),
        canonicalUrl: "https://example.com/jobs/ml-engineer-remote-europe",
      }),
      locationJob("ML Engineer", "san-francisco", "United States", "San Francisco, CA", {
        state: "California",
        city: "San Francisco",
      }),
    ]);

    await expectSearchIds(
      repository,
      { title: "ml engineer", city: "Remote", country: "United States" },
      [sourceId("ML Engineer", "remote-us")],
      [
        sourceId("ML Engineer", "remote-canada"),
        sourceId("ML Engineer", "remote-europe"),
        sourceId("ML Engineer", "san-francisco"),
      ],
    );
  });

  it("exposes a title-intent AND geo-intent query shape for title plus location searches", () => {
    const searches = [
      { title: "software engineer", country: "United States" },
      { title: "machine learning engineer", country: "Canada" },
      { title: "applied scientist", country: "Germany" },
      { title: "data analyst", city: "Toronto" },
      { title: "product manager", city: "Remote", country: "Canada" },
    ];

    for (const filters of searches) {
      const query = buildIndexedJobCandidateQuery(filters);
      expect(query.diagnostics).toMatchObject({
        hasTitleFilter: true,
        hasLocationFilter: true,
        hasTitleConstraint: true,
        hasLocationConstraint: true,
        locationConstraintAppliedToEveryTitleChannel: true,
        usesGeoOnlyChannelForVisibleResults: false,
        usesTitleOnlyChannelForLocationSearch: false,
      });
      expect(query.diagnostics.queryShape).toContain("base AND locationConstraint AND (");
      expect(query.channels.map((channel) => channel.name)).not.toContain("geoChannel");
      expect(query.channels.map((channel) => channel.name)).not.toContain("legacyLocationFallbackChannel");
      expect(query.channels.every((channel) => channel.diagnostics.requiresLocation === true)).toBe(true);
      const legacyTitleChannel = query.channels.find((channel) => channel.name === "legacyTitleFallbackChannel");
      expect(JSON.stringify(legacyTitleChannel?.filter ?? {})).toContain("normalizedLocation");
      expect(JSON.stringify(legacyTitleChannel?.filter ?? {})).toMatch(/normalizedTitle|titleNormalized|"title"/);
    }
  });

  it("returns indexed DB results without waiting for hanging request-time providers", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seed(repository, [
      locationJob("Product Manager", "us", "United States", "Remote - United States", {
        remoteType: "remote",
      }),
    ]);
    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(
      () => new Promise<never>(() => undefined),
    );
    const provider: CrawlProvider = {
      provider: "greenhouse",
      supportsSource(source: DiscoveredSource): source is DiscoveredSource {
        return source.platform === "greenhouse";
      },
      crawlSources,
    };
    const started = await Promise.race([
      startSearchFromFilters(
        { title: "product manager", country: "United States" },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-20T12:30:00.000Z"),
          initialVisibleWaitMs: 0,
        },
      ),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 250)),
    ]);

    expect(started).not.toBe("timeout");
    if (started !== "timeout") {
      expect(started.queued).toBe(false);
      expect(started.result.jobs.map((job) => job.sourceJobId)).toEqual([
        sourceId("Product Manager", "us"),
      ]);
      expect(started.result.diagnostics.session?.supplementalQueued).toBe(false);
    }
    expect(crawlSources).not.toHaveBeenCalled();
  });
});
