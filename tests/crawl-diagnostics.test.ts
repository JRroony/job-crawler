import { describe, expect, it, vi } from "vitest";

import { runSearchFromFilters } from "@/lib/server/crawler/service";
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
            configuredSources: 20,
            curatedSources: 0,
            publicSources: 5,
            discoveredBeforeFiltering: 25,
            discoveredAfterFiltering: 1,
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
              sampleGeneratedQueries: ["site:boards.greenhouse.io software engineer"],
              sampleExecutedQueries: ["site:boards.greenhouse.io software engineer"],
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
      discoveredBeforeFiltering: 25,
      discoveredAfterFiltering: 1,
      publicSearch: {
        generatedQueries: 96,
        executedQueries: 24,
        sourcesAdded: 5,
      },
    });
  });
});
