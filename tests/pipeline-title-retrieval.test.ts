import { describe, expect, it, vi } from "vitest";

import { runSearchIngestionFromFilters } from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
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

describe("pipeline title retrieval", () => {
  it("keeps semantically related unknown-title matches instead of requiring exact titles", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T12:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 3,
      matchedCount: 3,
      warningCount: 0,
      jobs: [
        {
          title: "Integration Developer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "integration-developer",
          sourceUrl: "https://example.com/jobs/integration-developer",
          applyUrl: "https://example.com/jobs/integration-developer/apply",
          canonicalUrl: "https://example.com/jobs/integration-developer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Senior Integration Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "integration-engineer",
          sourceUrl: "https://example.com/jobs/integration-engineer",
          applyUrl: "https://example.com/jobs/integration-engineer/apply",
          canonicalUrl: "https://example.com/jobs/integration-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Solutions Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "solutions-engineer",
          sourceUrl: "https://example.com/jobs/solutions-engineer",
          applyUrl: "https://example.com/jobs/solutions-engineer/apply",
          canonicalUrl: "https://example.com/jobs/solutions-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: "Integration Engineer",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Senior Integration Engineer",
      "Integration Developer",
    ]);
    expect(result.jobs[0]?.rawSourceMetadata).toMatchObject({
      crawlTitleMatch: {
        tier: "canonical_variant",
      },
    });
    expect(result.jobs[1]?.rawSourceMetadata).toMatchObject({
      crawlTitleMatch: {
        tier: "same_family_related",
      },
    });
    expect(result.diagnostics.excludedByTitle).toBe(1);
  });

  it("keeps adjacent concept matches in the pipeline while rejecting cross-family manager roles", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T12:30:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 3,
      matchedCount: 3,
      warningCount: 0,
      jobs: [
        {
          title: "Technical Product Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "technical-product-manager",
          sourceUrl: "https://example.com/jobs/technical-product-manager",
          applyUrl: "https://example.com/jobs/technical-product-manager/apply",
          canonicalUrl: "https://example.com/jobs/technical-product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Growth Product Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "growth-product-manager",
          sourceUrl: "https://example.com/jobs/growth-product-manager",
          applyUrl: "https://example.com/jobs/growth-product-manager/apply",
          canonicalUrl: "https://example.com/jobs/growth-product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Technical Program Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "technical-program-manager",
          sourceUrl: "https://example.com/jobs/technical-program-manager",
          applyUrl: "https://example.com/jobs/technical-program-manager/apply",
          canonicalUrl: "https://example.com/jobs/technical-program-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: "Product Manager",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Growth Product Manager",
      "Technical Product Manager",
    ]);
    expect(result.diagnostics.excludedByTitle).toBe(1);
  });

  it("combines title precision and resolved US filtering so only relevant US data-engineering jobs survive", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T13:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 5,
      matchedCount: 5,
      warningCount: 0,
      jobs: [
        {
          title: "Analytics Engineer",
          company: "Acme",
          locationText: "Austin, TX",
          sourcePlatform: "greenhouse",
          sourceJobId: "analytics-engineer",
          sourceUrl: "https://example.com/jobs/analytics-engineer",
          applyUrl: "https://example.com/jobs/analytics-engineer/apply",
          canonicalUrl: "https://example.com/jobs/analytics-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Data Platform Engineer",
          company: "Acme",
          locationText: "Remote - California",
          sourcePlatform: "greenhouse",
          sourceJobId: "data-platform-engineer",
          sourceUrl: "https://example.com/jobs/data-platform-engineer",
          applyUrl: "https://example.com/jobs/data-platform-engineer/apply",
          canonicalUrl: "https://example.com/jobs/data-platform-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          company: "Acme",
          locationText: "Bellevue, WA",
          sourcePlatform: "greenhouse",
          sourceJobId: "software-engineer",
          sourceUrl: "https://example.com/jobs/software-engineer",
          applyUrl: "https://example.com/jobs/software-engineer/apply",
          canonicalUrl: "https://example.com/jobs/software-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Recruiter",
          company: "Acme",
          locationText: "Seattle, Washington",
          sourcePlatform: "greenhouse",
          sourceJobId: "recruiter",
          sourceUrl: "https://example.com/jobs/recruiter",
          applyUrl: "https://example.com/jobs/recruiter/apply",
          canonicalUrl: "https://example.com/jobs/recruiter",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Data Engineer",
          company: "Acme",
          locationText: "Toronto, Ontario",
          sourcePlatform: "greenhouse",
          sourceJobId: "data-engineer-canada",
          sourceUrl: "https://example.com/jobs/data-engineer-canada",
          applyUrl: "https://example.com/jobs/data-engineer-canada/apply",
          canonicalUrl: "https://example.com/jobs/data-engineer-canada",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: "Data Engineer",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Analytics Engineer",
      "Data Platform Engineer",
    ]);
    expect(result.jobs.map((job) => job.locationText)).toEqual([
      "Austin, TX",
      "Remote - California",
    ]);
    expect(result.diagnostics.excludedByTitle).toBe(2);
    expect(result.diagnostics.excludedByLocation).toBe(1);
  });

  it("keeps arbitrary multi-token engineering searches specific while still preserving nearby concept recall", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T13:15:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 4,
      matchedCount: 4,
      warningCount: 0,
      jobs: [
        {
          title: "Cloud Platform Engineer",
          company: "Acme",
          locationText: "Seattle, WA",
          sourcePlatform: "greenhouse",
          sourceJobId: "cloud-platform-engineer",
          sourceUrl: "https://example.com/jobs/cloud-platform-engineer",
          applyUrl: "https://example.com/jobs/cloud-platform-engineer/apply",
          canonicalUrl: "https://example.com/jobs/cloud-platform-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Platform Engineer",
          company: "Acme",
          locationText: "Remote - California",
          sourcePlatform: "greenhouse",
          sourceJobId: "platform-engineer",
          sourceUrl: "https://example.com/jobs/platform-engineer",
          applyUrl: "https://example.com/jobs/platform-engineer/apply",
          canonicalUrl: "https://example.com/jobs/platform-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          company: "Acme",
          locationText: "Austin, TX",
          sourcePlatform: "greenhouse",
          sourceJobId: "software-engineer",
          sourceUrl: "https://example.com/jobs/software-engineer",
          applyUrl: "https://example.com/jobs/software-engineer/apply",
          canonicalUrl: "https://example.com/jobs/software-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Cloud Platform Engineer",
          company: "Acme",
          locationText: "Toronto, Ontario",
          sourcePlatform: "greenhouse",
          sourceJobId: "cloud-platform-engineer-canada",
          sourceUrl: "https://example.com/jobs/cloud-platform-engineer-canada",
          applyUrl: "https://example.com/jobs/cloud-platform-engineer-canada/apply",
          canonicalUrl: "https://example.com/jobs/cloud-platform-engineer-canada",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: "Cloud Platform Engineer",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Cloud Platform Engineer",
      "Platform Engineer",
    ]);
    expect(result.jobs.map((job) => job.locationText)).toEqual([
      "Seattle, WA",
      "Remote - California",
    ]);
    expect(result.diagnostics.excludedByTitle).toBe(1);
    expect(result.diagnostics.excludedByLocation).toBe(1);
  });

  it("persists direct jobs harvested during discovery even when provider board crawling returns no matches", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T13:30:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverWithDiagnostics() {
        return {
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/gitlab",
              token: "gitlab",
              confidence: "high",
              discoveryMethod: "future_search",
            }),
          ],
          jobs: [
            {
              title: "Data Engineer",
              company: "GitLab",
              country: "United States",
              state: "Texas",
              city: "Austin",
              locationText: "Austin, TX",
              sourcePlatform: "greenhouse",
              sourceJobId: "8455464002",
              sourceUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
              applyUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
              canonicalUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {
                harvestedFrom: "public_search_detail",
              },
            },
          ],
          diagnostics: {
            inventorySources: 0,
            configuredSources: 0,
            curatedSources: 0,
            publicSources: 1,
            publicJobs: 1,
            discoveredBeforeFiltering: 1,
            discoveredAfterFiltering: 1,
            platformCounts: {
              greenhouse: 1,
            },
            publicJobPlatformCounts: {
              greenhouse: 1,
            },
            publicSearch: {
              generatedQueries: 24,
              executedQueries: 8,
              skippedQueries: 16,
              maxQueries: 24,
              maxSources: 120,
              maxResultsPerQuery: 4,
              roleQueryCount: 6,
              locationClauseCount: 12,
              rawResultsHarvested: 8,
              normalizedUrlsHarvested: 6,
              platformMatchedUrls: 3,
              candidateUrlsHarvested: 3,
              detailUrlsHarvested: 1,
              sourceUrlsHarvested: 2,
              recoveredSourcesFromDetailUrls: 1,
              directJobsExtracted: 1,
              sourcesAdded: 1,
              engineRequestCounts: {
                bing_rss: 8,
              },
              engineResultCounts: {
                bing_rss: 6,
              },
              dropReasonCounts: {},
              sampleGeneratedRoleQueries: ["data engineer"],
              sampleGeneratedQueries: ["site:boards.greenhouse.io data engineer"],
              sampleExecutedRoleQueries: ["data engineer"],
              sampleExecutedQueries: ["site:boards.greenhouse.io data engineer"],
              sampleHarvestedCandidateUrls: [
                "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
              ],
              sampleHarvestedDetailUrls: [
                "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
              ],
              sampleHarvestedSourceUrls: ["https://boards.greenhouse.io/gitlab"],
              sampleRecoveredSourceUrls: ["https://boards.greenhouse.io/gitlab"],
              coverageNotes: [],
            },
          },
        };
      },
    };

    const result = await runSearchIngestionFromFilters(
      {
        title: "Data Engineer",
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

    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Data Engineer",
      company: "GitLab",
      city: "Austin",
      state: "Texas",
    });
    expect(result.diagnostics).toMatchObject({
      directJobsHarvested: 1,
      providersEnqueued: 1,
      jobsBeforeDedupe: 1,
      jobsAfterDedupe: 1,
      discovery: {
        publicJobs: 1,
        publicSearch: {
          directJobsExtracted: 1,
          recoveredSourcesFromDetailUrls: 1,
        },
      },
    });
  });

  it("counts title and location exclusions once after the provider refactor", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T14:00:00.000Z");

    const discovery: DiscoveryService = {
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

    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "software-engineer-us",
              title: "Software Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/software-engineer-us",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "software-engineer-canada",
              title: "Software Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/software-engineer-canada",
              company_name: "Acme",
              location: { name: "Toronto, Ontario" },
            },
            {
              id: "software-engineer-in-test",
              title: "Software Engineer in Test",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/software-engineer-in-test",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
          ],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [createGreenhouseProvider()],
        discovery,
        fetchImpl,
        now,
      },
    );

    expect(result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(result.diagnostics).toMatchObject({
      excludedByTitle: 1,
      excludedByLocation: 1,
    });
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      fetchedCount: 3,
      matchedCount: 1,
    });
  });

  it("broadens software-engineer recall to application and sibling software roles without letting unrelated families through", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T14:30:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 6,
      matchedCount: 6,
      warningCount: 0,
      jobs: [
        {
          title: "Backend Engineer",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "backend-engineer",
          sourceUrl: "https://example.com/jobs/backend-engineer",
          applyUrl: "https://example.com/jobs/backend-engineer/apply",
          canonicalUrl: "https://example.com/jobs/backend-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Application Developer",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "application-developer",
          sourceUrl: "https://example.com/jobs/application-developer",
          applyUrl: "https://example.com/jobs/application-developer/apply",
          canonicalUrl: "https://example.com/jobs/application-developer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Software Developer",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "software-developer",
          sourceUrl: "https://example.com/jobs/software-developer",
          applyUrl: "https://example.com/jobs/software-developer/apply",
          canonicalUrl: "https://example.com/jobs/software-developer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Frontend Engineer",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "frontend-engineer",
          sourceUrl: "https://example.com/jobs/frontend-engineer",
          applyUrl: "https://example.com/jobs/frontend-engineer/apply",
          canonicalUrl: "https://example.com/jobs/frontend-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Sales Engineer",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "sales-engineer",
          sourceUrl: "https://example.com/jobs/sales-engineer",
          applyUrl: "https://example.com/jobs/sales-engineer/apply",
          canonicalUrl: "https://example.com/jobs/sales-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Data Analyst",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "data-analyst",
          sourceUrl: "https://example.com/jobs/data-analyst",
          applyUrl: "https://example.com/jobs/data-analyst/apply",
          canonicalUrl: "https://example.com/jobs/data-analyst",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
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

    expect(result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Software Developer",
        "Backend Engineer",
        "Frontend Engineer",
        "Application Developer",
      ]),
    );
    expect(result.diagnostics.excludedByTitle).toBe(2);
  });

  it("persists visible baseline results before supplemental discovery finishes", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:00:00.000Z");

    const provider = createStubProvider("greenhouse", async (_context, sources) => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: sources.length,
      fetchedCount: sources.length,
      matchedCount: sources.length,
      warningCount: 0,
      jobs: sources.map((source, index) => ({
        title: `Software Engineer ${index + 1}`,
        company: "Acme",
        locationText: "Remote, United States",
        sourcePlatform: "greenhouse",
        sourceJobId: `${source.id}:${index + 1}`,
        sourceUrl: `https://example.com/jobs/${index + 1}`,
        applyUrl: `https://example.com/jobs/${index + 1}/apply`,
        canonicalUrl: `https://example.com/jobs/${index + 1}`,
        discoveredAt: now.toISOString(),
        rawSourceMetadata: {},
      })),
    }));

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverBaseline() {
        return {
          label: "baseline",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/acme",
              token: "acme",
              confidence: "high",
              discoveryMethod: "configured_env",
            }),
          ],
          jobs: [],
        };
      },
      async discoverSupplemental() {
        await new Promise((resolve) => setTimeout(resolve, 50));
        return {
          label: "public_search",
          sources: [],
          jobs: [],
        };
      },
    };

    const runPromise = runSearchIngestionFromFilters(
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

    await new Promise((resolve) => setTimeout(resolve, 20));
    const [search] = await repository.listRecentSearches(1);
    const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId as string);
    const midJobs = await repository.getJobsByCrawlRun(crawlRun?._id as string);

    expect(midJobs).toHaveLength(1);

    await runPromise;
  });

  it("persists supplemental Greenhouse results without waiting for a slower baseline Greenhouse batch", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:05:00.000Z");

    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      const source = sources[0];
      if (source?.token === "acme") {
        await new Promise((resolve) => setTimeout(resolve, 80));
      }

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: sources.length,
        matchedCount: sources.length,
        warningCount: 0,
        jobs: sources.map((currentSource) => ({
          title:
            currentSource.token === "datadog"
              ? "Supplemental Software Engineer"
              : "Baseline Software Engineer",
          company: currentSource.companyHint ?? "Acme",
          locationText: "Seattle, WA",
          sourcePlatform: "greenhouse" as const,
          sourceJobId: `${currentSource.token}-job`,
          sourceUrl: `https://example.com/jobs/${currentSource.token}-job`,
          applyUrl: `https://example.com/jobs/${currentSource.token}-job/apply`,
          canonicalUrl: `https://example.com/jobs/${currentSource.token}-job`,
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        })),
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverBaseline() {
        return {
          label: "baseline",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/acme",
              token: "acme",
              confidence: "high",
              discoveryMethod: "configured_env",
            }),
          ],
          jobs: [],
        };
      },
      async discoverSupplemental() {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return {
          label: "public_search",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/datadog",
              token: "datadog",
              confidence: "medium",
              discoveryMethod: "future_search",
            }),
          ],
          jobs: [],
        };
      },
    };

    const runPromise = runSearchIngestionFromFilters(
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
        progressUpdateIntervalMs: 5,
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 35));
    const [search] = await repository.listRecentSearches(1);
    const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId as string);
    const midJobs = await repository.getJobsByCrawlRun(crawlRun?._id as string);

    expect(midJobs.map((job) => job.title)).toContain("Supplemental Software Engineer");

    const result = await runPromise;

    expect(result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Baseline Software Engineer",
        "Supplemental Software Engineer",
      ]),
    );
  });

  it("delays supplemental recall in fast mode until the first visible batch has been saved", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:07:00.000Z");
    const startedAtMs = Date.now();
    let supplementalStartedAtMs: number | undefined;

    const provider = createStubProvider("greenhouse", async (context, sources) => {
      const source = sources[0];

      if (source?.token === "acme") {
        await new Promise((resolve) => setTimeout(resolve, 35));
        await context.onBatch?.({
          provider: "greenhouse",
          sourceCount: 1,
          fetchedCount: 1,
          jobs: [
            {
              title: "Baseline Software Engineer",
              company: "Acme",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "baseline-fast-1",
              sourceUrl: "https://example.com/jobs/baseline-fast-1",
              applyUrl: "https://example.com/jobs/baseline-fast-1/apply",
              canonicalUrl: "https://example.com/jobs/baseline-fast-1",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
          ],
        });
        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: 1,
          fetchedCount: 1,
          matchedCount: 1,
          warningCount: 0,
          jobs: [],
        };
      }

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

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverBaseline() {
        return {
          label: "baseline",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/acme",
              token: "acme",
              confidence: "high",
              discoveryMethod: "configured_env",
            }),
          ],
          jobs: [],
        };
      },
      async discoverSupplemental() {
        supplementalStartedAtMs = Date.now() - startedAtMs;
        return {
          label: "public_search",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/datadog",
              token: "datadog",
              confidence: "medium",
              discoveryMethod: "future_search",
            }),
          ],
          jobs: [],
        };
      },
    };

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
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
        progressUpdateIntervalMs: 5,
      },
    );

    expect(result.jobs.map((job) => job.title)).toContain("Baseline Software Engineer");
    expect(result.diagnostics.performance.timeToFirstVisibleResultMs).toBeGreaterThanOrEqual(30);
    expect(supplementalStartedAtMs).toBeGreaterThanOrEqual(
      result.diagnostics.performance.timeToFirstVisibleResultMs ?? 0,
    );
  });

  it("starts supplemental recall earlier in balanced mode than in fast mode", async () => {
    const now = new Date("2026-04-10T15:08:00.000Z");
    const buildRun = async (crawlMode: "fast" | "balanced") => {
      const repository = new JobCrawlerRepository(new FakeDb());
      const startedAtMs = Date.now();
      let supplementalStartedAtMs: number | undefined;

      const provider = createStubProvider("greenhouse", async (context, sources) => {
        if (sources[0]?.token === "acme") {
          await new Promise((resolve) => setTimeout(resolve, 35));
          await context.onBatch?.({
            provider: "greenhouse",
            sourceCount: 1,
            fetchedCount: 1,
            jobs: [
              {
                title: `${crawlMode} baseline job`,
                company: "Acme",
                locationText: "Remote, United States",
                sourcePlatform: "greenhouse",
                sourceJobId: `${crawlMode}-baseline-job`,
                sourceUrl: `https://example.com/jobs/${crawlMode}-baseline-job`,
                applyUrl: `https://example.com/jobs/${crawlMode}-baseline-job/apply`,
                canonicalUrl: `https://example.com/jobs/${crawlMode}-baseline-job`,
                discoveredAt: now.toISOString(),
                rawSourceMetadata: {},
              },
            ],
          });
        }

        return {
          provider: "greenhouse",
          status: "success",
          sourceCount: sources.length,
          fetchedCount: 1,
          matchedCount: 1,
          warningCount: 0,
          jobs: [],
        };
      });

      const discovery: DiscoveryService = {
        async discover() {
          return [];
        },
        async discoverBaseline() {
          return {
            label: "baseline",
            sources: [
              classifySourceCandidate({
                url: "https://boards.greenhouse.io/acme",
                token: "acme",
                confidence: "high",
                discoveryMethod: "configured_env",
              }),
            ],
            jobs: [],
          };
        },
        async discoverSupplemental() {
          supplementalStartedAtMs = Date.now() - startedAtMs;
          return {
            label: "public_search",
            sources: [],
            jobs: [],
          };
        },
      };

      const result = await runSearchIngestionFromFilters(
        {
          title: "Software Engineer",
          country: "United States",
          platforms: ["greenhouse"],
          crawlMode,
        },
        {
          repository,
          providers: [provider],
          discovery,
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now,
          progressUpdateIntervalMs: 5,
        },
      );

      return {
        result,
        supplementalStartedAtMs,
      };
    };

    const fastRun = await buildRun("fast");
    const balancedRun = await buildRun("balanced");

    expect(fastRun.supplementalStartedAtMs).toBeGreaterThanOrEqual(
      fastRun.result.diagnostics.performance.timeToFirstVisibleResultMs ?? 0,
    );
    expect(balancedRun.supplementalStartedAtMs).toBeLessThan(
      balancedRun.result.diagnostics.performance.timeToFirstVisibleResultMs ?? Number.POSITIVE_INFINITY,
    );
  });

  it("keeps already-saved jobs when supplemental discovery fails later in the run", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:09:00.000Z");

    const provider = createStubProvider("greenhouse", async (context) => {
      await context.onBatch?.({
        provider: "greenhouse",
        sourceCount: 1,
        fetchedCount: 1,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "saved-before-failure",
            sourceUrl: "https://example.com/jobs/saved-before-failure",
            applyUrl: "https://example.com/jobs/saved-before-failure/apply",
            canonicalUrl: "https://example.com/jobs/saved-before-failure",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      });

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [],
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [];
      },
      async discoverBaseline() {
        return {
          label: "baseline",
          sources: [
            classifySourceCandidate({
              url: "https://boards.greenhouse.io/acme",
              token: "acme",
              confidence: "high",
              discoveryMethod: "configured_env",
            }),
          ],
          jobs: [],
        };
      },
      async discoverSupplemental() {
        await new Promise((resolve) => setTimeout(resolve, 80));
        throw new Error("Supplemental discovery failed after the baseline batch.");
      },
    };

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
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
        progressUpdateIntervalMs: 5,
      },
    );

    expect(result.crawlRun.status).toBe("partial");
    expect(result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(result.search.lastStatus).toBe("partial");

    const [search] = await repository.listRecentSearches(1);
    const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId as string);
    const savedJobs = await repository.getJobsByCrawlRun(crawlRun?._id as string);

    expect(crawlRun?.status).toBe("partial");
    expect(savedJobs.map((job) => job.title)).toEqual(["Software Engineer"]);
  });

  it("persists fast-provider results while a slower provider is still running", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:10:00.000Z");

    const fastProvider = createStubProvider("greenhouse", async (context, sources) => {
      await context.onBatch?.({
        provider: "greenhouse",
        sourceCount: sources.length,
        fetchedCount: 1,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "fast-1",
            sourceUrl: "https://example.com/jobs/fast-1",
            applyUrl: "https://example.com/jobs/fast-1/apply",
            canonicalUrl: "https://example.com/jobs/fast-1",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      });

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [],
      };
    });

    const slowProvider = createStubProvider("lever", async () => {
      await new Promise((resolve) => setTimeout(resolve, 80));
      return {
        provider: "lever",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "slow-1",
            sourceUrl: "https://example.com/jobs/slow-1",
            applyUrl: "https://example.com/jobs/slow-1/apply",
            canonicalUrl: "https://example.com/jobs/slow-1",
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
            url: "https://boards.greenhouse.io/acme",
            token: "acme",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
          classifySourceCandidate({
            url: "https://jobs.lever.co/acme",
            token: "acme",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const runPromise = runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse", "lever"],
      },
      {
        repository,
        providers: [fastProvider, slowProvider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
        providerTimeoutMs: 500,
        progressUpdateIntervalMs: 5,
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 25));
    const [search] = await repository.listRecentSearches(1);
    const crawlRun = await repository.getCrawlRun(search.latestCrawlRunId as string);
    const midJobs = await repository.getJobsByCrawlRun(crawlRun?._id as string);

    expect(crawlRun?.status).toBe("running");
    expect(midJobs.map((job) => job.title)).toEqual(["Software Engineer"]);

    const result = await runPromise;

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Backend Engineer",
    ]);
    expect(result.diagnostics.performance.timeToFirstVisibleResultMs).toBeLessThan(250);
  });

  it("fails slow providers fast without blocking useful results from other providers", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:20:00.000Z");

    const fastProvider = createStubProvider("greenhouse", async () => ({
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
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "fast-timeout-1",
          sourceUrl: "https://example.com/jobs/fast-timeout-1",
          applyUrl: "https://example.com/jobs/fast-timeout-1/apply",
          canonicalUrl: "https://example.com/jobs/fast-timeout-1",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const slowProvider = createStubProvider("lever", async () => {
      await new Promise((resolve) => setTimeout(resolve, 150));
      return {
        provider: "lever",
        status: "success",
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/acme",
            token: "acme",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
          classifySourceCandidate({
            url: "https://jobs.lever.co/acme",
            token: "acme",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const startedAt = Date.now();
    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse", "lever"],
      },
      {
        repository,
        providers: [fastProvider, slowProvider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
        providerTimeoutMs: 40,
        progressUpdateIntervalMs: 5,
      },
    );
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeLessThan(500);
    expect(result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(result.crawlRun.status).toBe("partial");
    expect(result.diagnostics.providerFailures).toBe(1);
    expect(
      result.sourceResults.find((sourceResult) => sourceResult.provider === "lever"),
    ).toMatchObject({
      status: "failed",
      errorMessage: expect.stringContaining("crawl budget"),
    });
    expect(result.diagnostics.performance.providerTimingsMs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          provider: "lever",
          timedOut: true,
        }),
      ]),
    );
  });

  it.each([
    {
      query: "Software Engineer",
      expected: [
        "Software Development Engineer",
        "Backend Engineer",
        "Full Stack Engineer",
        "Frontend Engineer",
        "Java Developer",
        "Platform Engineer",
        "Application Engineer",
        "Developer",
        "Member of Technical Staff",
      ],
      excluded: ["Sales Engineer", "Recruiter"],
    },
    {
      query: "Data Analyst",
      expected: [
        "Business Intelligence Analyst",
        "Reporting Analyst",
        "Insights Analyst",
        "Business Analyst",
      ],
      excluded: ["Data Engineer", "Sales Analyst"],
    },
    {
      query: "Business Analyst",
      expected: [
        "Business Systems Analyst",
        "Systems Analyst",
        "Process Analyst",
        "Operations Analyst",
      ],
      excluded: ["Data Engineer", "Product Manager"],
    },
    {
      query: "Product Manager",
      expected: [
        "Technical Product Manager",
        "Growth Product Manager",
        "Associate Product Manager",
        "Product Owner",
      ],
      excluded: ["Technical Program Manager", "Engineering Manager"],
    },
  ])(
    "retrieves related titles for %s without over-including weak matches",
    async ({ query, expected, excluded }) => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:30:00.000Z");

    const jobs = [
      ...expected,
      ...excluded,
    ].map((title, index) => ({
      title,
      company: "Acme",
      country: "United States",
      locationText: "Remote, United States",
      sourcePlatform: "greenhouse" as const,
      sourceJobId: `${query}-${index}`,
      sourceUrl: `https://example.com/jobs/${query}-${index}`,
      applyUrl: `https://example.com/jobs/${query}-${index}/apply`,
      canonicalUrl: `https://example.com/jobs/${query}-${index}`,
      discoveredAt: now.toISOString(),
      rawSourceMetadata: {},
    }));

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: jobs.length,
      matchedCount: jobs.length,
      warningCount: 0,
      jobs,
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: query,
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

    expect(result.jobs.map((job) => job.title)).toEqual(expect.arrayContaining(expected));
    expect(result.jobs.map((job) => job.title)).not.toEqual(expect.arrayContaining(excluded));
    expect(result.jobs[0]?.rawSourceMetadata).toMatchObject({
      crawlTitleMatch: expect.objectContaining({
        originalQueryTitle: query,
        normalizedQueryTitle: expect.any(String),
        queryAliasesUsed: expect.any(Array),
        explanation: expect.any(String),
      }),
    });
    },
    10_000,
  );

  it.each([
    {
      title: "Software Engineer",
      expectedTitles: [
        "Software Development Engineer",
        "Backend Engineer",
        "Full Stack Engineer",
        "Frontend Engineer",
        "Application Engineer",
        "Developer",
      ],
      excludedTitles: ["Sales Engineer", "Recruiter"],
      expectedCounts: { fetchedCount: 8, matchedCount: 6, savedCount: 6 },
    },
    {
      title: "Data Analyst",
      expectedTitles: [
        "Business Intelligence Analyst",
        "Reporting Analyst",
        "Business Analyst",
      ],
      excludedTitles: ["Data Engineer", "Sales Analyst"],
      expectedCounts: { fetchedCount: 5, matchedCount: 3, savedCount: 3 },
    },
    {
      title: "Business Analyst",
      expectedTitles: [
        "Business Systems Analyst",
        "Systems Analyst",
        "Operations Analyst",
      ],
      excludedTitles: ["Data Engineer", "Product Manager"],
      expectedCounts: { fetchedCount: 5, matchedCount: 3, savedCount: 3 },
    },
    {
      title: "Product Manager",
      expectedTitles: [
        "Technical Product Manager",
        "Growth Product Manager",
        "Associate Product Manager",
      ],
      excludedTitles: ["Technical Program Manager", "Engineering Manager"],
      expectedCounts: { fetchedCount: 5, matchedCount: 3, savedCount: 3 },
    },
  ])("reports Greenhouse recall counts for $title + United States", async ({
    title,
    expectedTitles,
    excludedTitles,
    expectedCounts,
  }) => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T15:45:00.000Z");

    const jobs = [...expectedTitles, ...excludedTitles].map((jobTitle, index) => ({
      title: jobTitle,
      company: "Acme",
      country: "United States",
      locationText:
        title === "Software Engineer" && jobTitle === "Backend Engineer"
          ? "Bellevue, WA"
          : title === "Software Engineer" && jobTitle === "Full Stack Engineer"
            ? "Seattle, WA"
            : title === "Software Engineer" && jobTitle === "Frontend Engineer"
              ? "Austin, TX"
              : title === "Software Engineer" && jobTitle === "Application Engineer"
                ? "San Jose, CA"
                : "Remote, United States",
      sourcePlatform: "greenhouse" as const,
      sourceJobId: `${title}-${index}`,
      sourceUrl: `https://example.com/jobs/${title}-${index}`,
      applyUrl: `https://example.com/jobs/${title}-${index}/apply`,
      canonicalUrl: `https://example.com/jobs/${title}-${index}`,
      discoveredAt: now.toISOString(),
      rawSourceMetadata: {},
    }));

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: jobs.length,
      matchedCount: jobs.length,
      warningCount: 0,
      jobs,
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title,
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

  expect(result.jobs.map((job) => job.title)).toEqual(expect.arrayContaining(expectedTitles));
  expect(result.jobs.map((job) => job.title)).not.toEqual(expect.arrayContaining(excludedTitles));
  expect(result.sourceResults[0]).toMatchObject(expectedCounts);
  });

  it.each([
    {
      title: "Software Engineer",
      expectedTitles: [
        "Software Development Engineer",
        "Backend Engineer",
        "Full Stack Engineer",
        "Frontend Engineer",
        "Application Developer",
        "Platform Engineer",
        "Java Developer",
      ],
      excludedTitles: ["Data Analyst", "Business Analyst", "Product Manager"],
    },
    {
      title: "Software Development Engineer",
      expectedTitles: [
        "Software Development Engineer",
        "Backend Engineer",
        "Full Stack Engineer",
        "Java Developer",
      ],
      excludedTitles: ["Data Analyst", "Product Manager"],
    },
    {
      title: "Backend Engineer",
      expectedTitles: ["Backend Engineer"],
      excludedTitles: ["Frontend Engineer", "Data Analyst", "Product Manager"],
    },
    {
      title: "Full Stack Engineer",
      expectedTitles: ["Full Stack Engineer"],
      excludedTitles: ["Data Analyst", "Product Manager"],
    },
    {
      title: "Frontend Engineer",
      expectedTitles: ["Frontend Engineer"],
      excludedTitles: ["Backend Engineer", "Data Analyst", "Product Manager"],
    },
    {
      title: "Application Developer",
      expectedTitles: ["Application Developer"],
      excludedTitles: ["Data Analyst", "Product Manager"],
    },
    {
      title: "Platform Engineer",
      expectedTitles: ["Platform Engineer"],
      excludedTitles: ["Data Analyst", "Business Analyst", "Product Manager"],
    },
    {
      title: "Java Developer",
      expectedTitles: ["Java Developer"],
      excludedTitles: ["Data Analyst", "Product Manager"],
    },
    {
      title: "Data Analyst",
      expectedTitles: ["Data Analyst", "Business Intelligence Analyst", "Business Analyst"],
      excludedTitles: ["Backend Engineer", "Product Manager"],
    },
    {
      title: "Business Analyst",
      expectedTitles: ["Business Analyst"],
      excludedTitles: ["Data Analyst", "Backend Engineer", "Product Manager"],
    },
    {
      title: "Product Manager",
      expectedTitles: ["Product Manager", "Technical Product Manager"],
      excludedTitles: ["Business Analyst", "Backend Engineer", "Program Manager"],
    },
  ])("keeps Greenhouse recall in the provider while the pipeline preserves precision for $title", async ({
    title,
    expectedTitles,
    excludedTitles,
  }) => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-11T12:00:00.000Z");
    const discovery: DiscoveryService = {
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
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "software-development-engineer",
              title: "Software Development Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/software-development-engineer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "backend-engineer",
              title: "Backend Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/backend-engineer",
              company_name: "Acme",
              location: { name: "Bellevue, WA" },
            },
            {
              id: "full-stack-engineer",
              title: "Full Stack Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/full-stack-engineer",
              company_name: "Acme",
              location: { name: "Seattle, WA" },
            },
            {
              id: "frontend-engineer",
              title: "Frontend Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/frontend-engineer",
              company_name: "Acme",
              location: { name: "Austin, TX" },
            },
            {
              id: "application-developer",
              title: "Application Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/application-developer",
              company_name: "Acme",
              location: { name: "San Jose, CA" },
            },
            {
              id: "platform-engineer",
              title: "Platform Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/platform-engineer",
              company_name: "Acme",
              location: { name: "Remote - California" },
            },
            {
              id: "java-developer",
              title: "Java Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/java-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "data-analyst",
              title: "Data Analyst",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/data-analyst",
              company_name: "Acme",
              location: { name: "New York, NY" },
            },
            {
              id: "bi-analyst",
              title: "Business Intelligence Analyst",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/bi-analyst",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "business-analyst",
              title: "Business Analyst",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/business-analyst",
              company_name: "Acme",
              location: { name: "Chicago, IL" },
            },
            {
              id: "product-manager",
              title: "Product Manager",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/product-manager",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "technical-product-manager",
              title: "Technical Product Manager",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/technical-product-manager",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "program-manager",
              title: "Program Manager",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/program-manager",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
          ],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await runSearchIngestionFromFilters(
      {
        title,
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [createGreenhouseProvider()],
        discovery,
        fetchImpl,
        now,
      },
    );

    const returnedTitles = result.jobs.map((job) => job.title);
    expect(returnedTitles).toEqual(expect.arrayContaining(expectedTitles));
    expect(returnedTitles).not.toEqual(expect.arrayContaining(excludedTitles));
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      fetchedCount: 13,
    });
  });

  it("keeps non-senior product manager roles when experience filtering is active", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T16:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 0,
      jobs: [
        {
          title: "Product Manager",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "product-manager",
          sourceUrl: "https://example.com/jobs/product-manager",
          applyUrl: "https://example.com/jobs/product-manager/apply",
          canonicalUrl: "https://example.com/jobs/product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Senior Product Manager",
          company: "Acme",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "senior-product-manager",
          sourceUrl: "https://example.com/jobs/senior-product-manager",
          applyUrl: "https://example.com/jobs/senior-product-manager/apply",
          canonicalUrl: "https://example.com/jobs/senior-product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));

    const discovery: DiscoveryService = {
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

    const result = await runSearchIngestionFromFilters(
      {
        title: "Product Manager",
        country: "United States",
        experienceLevel: "mid",
        includeUnspecifiedExperience: true,
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

    expect(result.jobs.map((job) => job.title)).toEqual(["Product Manager"]);
    expect(result.diagnostics.excludedByExperience).toBe(1);
    expect(result.diagnostics.filterDecisionTraces).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceJobId: "product-manager",
          outcome: "passed",
          experienceDiagnostics: expect.objectContaining({
            passed: true,
          }),
        }),
        expect.objectContaining({
          sourceJobId: "senior-product-manager",
          outcome: "dropped",
          dropReason: "filter:experience_level_mismatch",
        }),
      ]),
    );
  });
});
