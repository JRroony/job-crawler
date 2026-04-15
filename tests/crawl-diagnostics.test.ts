import { describe, expect, it, vi } from "vitest";

import { getSearchDetails, runSearchFromFilters } from "@/lib/server/crawler/service";
import { collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";

import { FakeDb } from "@/tests/helpers/fake-db";

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

describe("crawl diagnostics", () => {
  it("loads search details from legacy crawl runs whose diagnostics and source results contain null optional fields", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.searches).insertOne({
      _id: "search-legacy-details",
      filters: {
        title: "Integration Engineer",
        country: "United States",
      },
      latestCrawlRunId: "run-legacy-details",
      createdAt: "2026-03-29T12:00:00.000Z",
      updatedAt: "2026-03-29T12:10:00.000Z",
      lastStatus: "completed",
    });
    await db.collection(collectionNames.crawlRuns).insertOne({
      _id: "run-legacy-details",
      searchId: "search-legacy-details",
      startedAt: "2026-03-29T12:00:00.000Z",
      finishedAt: "2026-03-29T12:10:00.000Z",
      status: "completed",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      diagnostics: {
        discoveredSources: 0,
        crawledSources: 0,
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
          publicSearch: null,
        },
      },
      providerSummary: [
        {
          provider: "greenhouse",
          status: "failed",
          sourceCount: 1,
          fetchedCount: 0,
          matchedCount: 0,
          savedCount: 0,
          warningCount: 1,
          errorMessage: null,
        },
      ],
      errorMessage: null,
    });
    await db.collection(collectionNames.crawlSourceResults).insertOne({
      _id: "source-result-legacy",
      crawlRunId: "run-legacy-details",
      searchId: "search-legacy-details",
      provider: "greenhouse",
      status: "failed",
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      savedCount: 0,
      warningCount: 1,
      errorMessage: null,
      startedAt: "2026-03-29T12:00:00.000Z",
      finishedAt: "2026-03-29T12:10:00.000Z",
    });

    const result = await getSearchDetails("search-legacy-details", {
      repository,
      now: new Date("2026-03-29T12:11:00.000Z"),
      fetchImpl: vi.fn() as unknown as typeof fetch,
    });

    expect(result.search._id).toBe("search-legacy-details");
    expect(result.crawlRun.diagnostics.discovery?.zeroCoverageReason).toBeUndefined();
    expect(result.crawlRun.providerSummary[0]?.errorMessage).toBeUndefined();
    expect(result.sourceResults[0]?.errorMessage).toBeUndefined();
    expect(result.jobs).toEqual([]);
  });

  it("accepts nullable optional filters and strips legacy experienceClassification on incoming search requests", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/role-1",
            applyUrl: "https://example.com/jobs/role-1/apply",
            canonicalUrl: "https://example.com/jobs/role-1",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/openai",
            token: "openai",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        state: null,
        city: null,
        experienceClassification: null,
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(result.search.filters).toEqual({
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    expect(result.jobs).toHaveLength(1);
  });

  it("tracks discovery, filter exclusions, dedupe, and deferred validation separately", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 5,
        matchedCount: 5,
        warningCount: 0,
        jobs: [
          {
            title: "Data Scientist",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "title-miss",
            sourceUrl: "https://example.com/jobs/title-miss",
            applyUrl: "https://example.com/jobs/title-miss/apply",
            canonicalUrl: "https://example.com/jobs/title-miss",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Software Engineer",
            company: "Acme",
            country: "Canada",
            locationText: "Toronto, Canada",
            sourcePlatform: "greenhouse",
            sourceJobId: "location-miss",
            sourceUrl: "https://example.com/jobs/location-miss",
            applyUrl: "https://example.com/jobs/location-miss/apply",
            canonicalUrl: "https://example.com/jobs/location-miss",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Software Engineer",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            experienceLevel: "senior",
            sourcePlatform: "greenhouse",
            sourceJobId: "experience-miss",
            sourceUrl: "https://example.com/jobs/experience-miss",
            applyUrl: "https://example.com/jobs/experience-miss/apply",
            canonicalUrl: "https://example.com/jobs/experience-miss",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Software Engineering Intern",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            experienceLevel: "intern",
            sourcePlatform: "greenhouse",
            sourceJobId: "matched-a",
            sourceUrl: "https://example.com/jobs/matched-a",
            applyUrl: "https://example.com/jobs/matched-a/apply",
            canonicalUrl: "https://example.com/jobs/shared-role",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Software Engineering Intern",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            experienceLevel: "intern",
            sourcePlatform: "greenhouse",
            sourceJobId: "matched-b",
            sourceUrl: "https://example.com/jobs/matched-b",
            applyUrl: "https://example.com/jobs/matched-b/apply",
            canonicalUrl: "https://example.com/jobs/shared-role",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/openai",
            token: "openai",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        experienceLevel: "intern",
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(result.jobs).toHaveLength(1);
    expect(result.crawlRun.totalFetchedJobs).toBe(5);
    expect(result.crawlRun.totalMatchedJobs).toBe(2);
    expect(result.crawlRun.dedupedJobs).toBe(1);
    expect(result.diagnostics).toMatchObject({
      discoveredSources: 1,
      crawledSources: 1,
      providerFailures: 0,
      excludedByTitle: 1,
      excludedByLocation: 1,
      excludedByExperience: 1,
      dedupedOut: 1,
      validationDeferred: 1,
    });
    expect(result.diagnostics.dropReasonCounts).toMatchObject({
      "filter:title_below_threshold": 1,
      "filter:location_not_in_requested_country": 1,
      "filter:experience_level_mismatch": 1,
      "dedupe:canonical_url": 1,
    });
    expect(result.diagnostics.filterDecisionTraces).toHaveLength(5);
    expect(result.diagnostics.filterDecisionTraces).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceJobId: "title-miss",
          filterStage: "title",
          outcome: "dropped",
          dropReason: "filter:title_below_threshold",
          titleDiagnostics: expect.objectContaining({
            passed: false,
          }),
        }),
        expect.objectContaining({
          sourceJobId: "location-miss",
          filterStage: "location",
          outcome: "dropped",
          dropReason: "filter:location_not_in_requested_country",
          locationDiagnostics: expect.objectContaining({
            passed: false,
            isUnitedStates: false,
          }),
        }),
        expect.objectContaining({
          sourceJobId: "experience-miss",
          filterStage: "experience",
          outcome: "dropped",
          dropReason: "filter:experience_level_mismatch",
          experienceDiagnostics: expect.objectContaining({
            passed: false,
          }),
        }),
      ]),
    );
    expect(result.diagnostics.dedupeDecisionTraces).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceJobId: "matched-a",
          outcome: "deduped",
          dropReason: "dedupe:canonical_url",
          decisionReason: expect.stringContaining("dedupe:canonical_url"),
          originalIdentifiers: expect.objectContaining({
            sourceJobId: "matched-a",
          }),
          normalizedIdentity: expect.objectContaining({
            company: "acme",
          }),
        }),
      ]),
    );
    expect(result.crawlRun.diagnostics).toMatchObject(result.diagnostics);
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      sourceCount: 1,
      fetchedCount: 5,
      matchedCount: 2,
      savedCount: 1,
      warningCount: 0,
    });
  });

  it("surfaces structured discovery funnel diagnostics when the discovery service provides them", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "role-1",
            sourceUrl: "https://example.com/jobs/role-1",
            applyUrl: "https://example.com/jobs/role-1/apply",
            canonicalUrl: "https://example.com/jobs/role-1",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverWithDiagnostics() {
        return {
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/openai",
              token: "openai",
              confidence: "high",
              discoveryMethod: "configured_env",
            }),
          ],
          diagnostics: {
            inventorySources: 0,
            configuredSources: 20,
            curatedSources: 0,
            publicSources: 5,
            publicJobs: 2,
            discoveredBeforeFiltering: 25,
            discoveredAfterFiltering: 1,
            platformCounts: {
              greenhouse: 1,
            },
            publicJobPlatformCounts: {
              greenhouse: 2,
            },
            publicSearch: {
              generatedQueries: 96,
              executedQueries: 24,
              skippedQueries: 72,
              maxQueries: 24,
              maxSources: 120,
              maxResultsPerQuery: 8,
              roleQueryCount: 6,
              locationClauseCount: 12,
              rawResultsHarvested: 80,
              normalizedUrlsHarvested: 64,
              platformMatchedUrls: 18,
              candidateUrlsHarvested: 18,
              detailUrlsHarvested: 12,
              sourceUrlsHarvested: 6,
              recoveredSourcesFromDetailUrls: 12,
              directJobsExtracted: 2,
              sourcesAdded: 5,
              engineRequestCounts: {
                bing_rss: 24,
              },
              engineResultCounts: {
                bing_rss: 64,
              },
              dropReasonCounts: {
                query_budget: 72,
              },
              sampleGeneratedRoleQueries: [
                "software engineer",
                "software developer",
              ],
              sampleGeneratedQueries: ["site:boards.greenhouse.io software engineer"],
              sampleExecutedRoleQueries: ["software engineer"],
              sampleExecutedQueries: ["site:boards.greenhouse.io software engineer"],
              sampleHarvestedCandidateUrls: ["https://boards.greenhouse.io/openai/jobs/role-1"],
              sampleHarvestedDetailUrls: ["https://boards.greenhouse.io/openai/jobs/role-1"],
              sampleHarvestedSourceUrls: ["https://boards.greenhouse.io/openai"],
              sampleRecoveredSourceUrls: ["https://boards.greenhouse.io/openai"],
              coverageNotes: ["Query budgeting skipped part of the generated search plan."],
            },
          },
        };
      },
    };

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(result.diagnostics.discovery).toMatchObject({
      configuredSources: 20,
      publicSources: 5,
      publicJobs: 2,
      discoveredBeforeFiltering: 25,
      discoveredAfterFiltering: 1,
      publicSearch: {
        generatedQueries: 96,
        executedQueries: 24,
        sourcesAdded: 5,
        directJobsExtracted: 2,
      },
    });
  });
});
