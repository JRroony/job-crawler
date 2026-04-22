import { describe, expect, it, vi } from "vitest";

import { collectionNames } from "@/lib/server/db/indexes";
import { seedToPersistableJob } from "@/lib/server/crawler/pipeline";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { toSourceInventoryRecord } from "@/lib/server/discovery/inventory";
import { createDiscoveryService } from "@/lib/server/discovery/service";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import { normalizeGreenhouseJob } from "@/lib/server/providers/greenhouse";
import { runSearchIngestionFromFilters, runSearchFromFilters } from "@/lib/server/search/service";
import type { CrawlProvider } from "@/lib/server/providers/types";
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
    normalizedLocation: locationNormalized,
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

function createEmptyDiscovery(): DiscoveryService {
  return {
    async discover() {
      return [];
    },
  };
}

describe("Mongo-backed ingestion and indexed search regressions", () => {
  it("persists Greenhouse seeds with valid titles even when normalized title aliases arrive empty", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");
    const search = await repository.createSearch({ title: "Backend Engineer" }, now.toISOString());
    const searchSession = await repository.createSearchSession(search._id, now.toISOString());
    const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
      searchSessionId: searchSession._id,
    });
    const greenhouseSeed = normalizeGreenhouseJob({
      companyToken: "openai",
      companyName: "OpenAI",
      discoveredAt: now.toISOString(),
      job: {
        id: "backend-role",
        title: "Backend Engineer",
        absolute_url: "https://boards.greenhouse.io/openai/jobs/backend-role",
        first_published: "2026-04-14T00:00:00.000Z",
        company_name: "OpenAI",
        location: { name: "Remote, United States" },
      },
    });

    const persistable = seedToPersistableJob(
      {
        ...greenhouseSeed,
        normalizedTitle: "",
        titleNormalized: "",
      },
      now,
    );
    const [savedJob] = await repository.persistJobs(crawlRun._id, [persistable], {
      searchSessionId: searchSession._id,
    });

    expect(savedJob).toMatchObject({
      title: "Backend Engineer",
      normalizedTitle: "backend engineer",
      titleNormalized: "backend engineer",
      sourcePlatform: "greenhouse",
    });
    expect(db.snapshot<JobListing>(collectionNames.jobs)).toHaveLength(1);
  });

  it("reads sourceInventory records with Mongo-style null optional fields", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const observedAt = "2026-04-15T00:00:00.000Z";

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
          now: observedAt,
          inventoryOrigin: "greenhouse_registry",
          inventoryRank: 0,
        },
      ),
    ]);

    const storedSnapshot = db.snapshot<Record<string, unknown>>(collectionNames.sourceInventory);
    expect(storedSnapshot).toHaveLength(1);
    expect(storedSnapshot[0]).toMatchObject({
      jobId: null,
      hostedUrl: null,
      pageType: null,
      sitePath: null,
      careerSitePath: null,
    });

    const inventory = await repository.listSourceInventory(["greenhouse"]);

    expect(inventory).toHaveLength(1);
    expect(inventory[0]).toMatchObject({
      _id: "greenhouse:openai",
      token: "openai",
      jobId: undefined,
      hostedUrl: undefined,
      pageType: undefined,
      sitePath: undefined,
      careerSitePath: undefined,
    });
  });

  it("persists jobs from the sourceInventory -> provider crawl path with Mongo-style null storage", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");
    let sawInventorySource = false;

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
      sawInventorySource = sources.some((source) => source.token === "openai");

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sawInventorySource ? 1 : 0,
        matchedCount: sawInventorySource ? 1 : 0,
        warningCount: 0,
        jobs: sawInventorySource
          ? [
              {
                title: "Software Engineer",
                normalizedTitle: "",
                titleNormalized: "",
                company: "OpenAI",
                country: "United States",
                locationText: "Remote - United States",
                sourcePlatform: "greenhouse",
                sourceJobId: "inventory-role",
                sourceUrl: "https://boards.greenhouse.io/openai/jobs/inventory-role",
                applyUrl: "https://boards.greenhouse.io/openai/jobs/inventory-role/apply",
                canonicalUrl: "https://boards.greenhouse.io/openai/jobs/inventory-role",
                discoveredAt: now.toISOString(),
                rawSourceMetadata: {
                  source: "inventory-regression",
                },
              },
            ]
          : [],
      };
    });

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
        crawlMode: "fast",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscoveryService({
          repository,
          env: {
            greenhouseBoardTokens: [],
            leverSiteTokens: [],
            ashbyBoardTokens: [],
            companyPageSources: [],
            PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
            PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
            PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 20,
            PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 12,
            PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 2,
            GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 8,
          },
        }),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    const indexedEvents = db.snapshot<Record<string, unknown>>(collectionNames.indexedJobEvents);
    const crawlRunEvents = db.snapshot<Record<string, unknown>>(collectionNames.crawlRunJobEvents);
    const sessionEvents = db.snapshot<Record<string, unknown>>(
      collectionNames.searchSessionJobEvents,
    );

    expect(sawInventorySource).toBe(true);
    expect(storedJobs).toHaveLength(1);
    expect(indexedEvents).toHaveLength(1);
    expect(crawlRunEvents).toHaveLength(1);
    expect(sessionEvents).toHaveLength(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.diagnostics.performance.persistenceBatchCount).toBeGreaterThanOrEqual(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "OpenAI",
      country: "United States",
      sourcePlatform: "greenhouse",
      sourceJobId: "inventory-role",
    });
  });

  it("returns persisted indexed jobs from Mongo-style storage before supplemental crawl finishes", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const now = new Date("2026-04-15T12:30:00.000Z");

    const seedSearch = await repository.createSearch(
      {
        title: "Seed Search",
      },
      "2026-04-15T12:00:00.000Z",
    );
    const seedRun = await repository.createCrawlRun(
      seedSearch._id,
      "2026-04-15T12:00:00.000Z",
    );
    await repository.persistJobs(seedRun._id, [
      createPersistableJob({
        title: "Backend Engineer",
        titleNormalized: "backend engineer",
        locationText: "Seattle, WA",
        locationRaw: "Seattle, WA",
        normalizedLocation: "seattle wa",
        city: "Seattle",
        state: "Washington",
        country: "United States",
        sourceJobId: "backend-indexed-role",
        sourceUrl: "https://example.com/jobs/backend-indexed-role",
        applyUrl: "https://example.com/jobs/backend-indexed-role/apply",
        canonicalUrl: "https://example.com/jobs/backend-indexed-role",
      }),
    ]);

    let providerFinished = false;
    const provider = createStubProvider("greenhouse", async () => {
      await new Promise((resolve) => setTimeout(resolve, 150));
      providerFinished = true;

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

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createEmptyDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
        requestOwnerKey: "mongo-indexed-regression",
      },
    );

    expect(providerFinished).toBe(false);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Backend Engineer",
      company: "Acme",
      country: "United States",
    });
    expect(result.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
      source: "jobs_collection",
    });
  });

  it("persists SmartRecruiters jobs from inventory-backed discovery into Mongo-style storage", async () => {
    const db = new MongoLikeNullDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-15T12:00:00.000Z");
    let sawInventorySource = false;

    await repository.upsertSourceInventory([
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://careers.smartrecruiters.com/Acme",
          companyHint: "Acme",
          confidence: "high",
          discoveryMethod: "future_search",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "public_search",
          inventoryRank: 0,
        },
      ),
    ]);

    const provider = createStubProvider("smartrecruiters", async (_context, sources) => {
      sawInventorySource = sources.some((source) => source.platform === "smartrecruiters");

      return {
        provider: "smartrecruiters",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sawInventorySource ? 1 : 0,
        matchedCount: sawInventorySource ? 1 : 0,
        warningCount: 0,
        jobs: sawInventorySource
          ? [
              {
                title: "Senior Product Analyst",
                company: "Acme",
                country: "United States",
                locationText: "Austin, TX",
                sourcePlatform: "smartrecruiters",
                sourceJobId: "744000067444685",
                sourceUrl:
                  "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
                applyUrl:
                  "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
                canonicalUrl:
                  "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
                discoveredAt: now.toISOString(),
                rawSourceMetadata: {
                  source: "smartrecruiters-regression",
                },
              },
            ]
          : [],
      };
    });

    const result = await runSearchIngestionFromFilters(
      {
        title: "Product Analyst",
        country: "United States",
        platforms: ["smartrecruiters"],
        crawlMode: "fast",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscoveryService({
          repository,
          env: {
            greenhouseBoardTokens: [],
            leverSiteTokens: [],
            ashbyBoardTokens: [],
            companyPageSources: [],
            PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
            PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
            PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 20,
            PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 12,
            PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 2,
            GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 8,
          },
        }),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(sawInventorySource).toBe(true);
    expect(storedJobs).toHaveLength(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Product Analyst",
      company: "Acme",
      sourcePlatform: "smartrecruiters",
      sourceJobId: "744000067444685",
    });
  });
});
