import { describe, expect, it, vi } from "vitest";

import { runSearchFromFilters } from "@/lib/server/crawler/service";
import { collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { createDiscoveryService, refreshSourceInventory } from "@/lib/server/discovery/service";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import { discoverSources } from "@/lib/server/discovery/service";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import { normalizeGreenhouseJob } from "@/lib/server/providers/greenhouse";
import type { CrawlProvider } from "@/lib/server/providers/types";
import type { JobListing } from "@/lib/types";

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

function expectNoDirectJobFetches(fetchImpl: unknown) {
  const calls = (
    fetchImpl as {
      mock: {
        calls: Array<[unknown, ...unknown[]]>;
      };
    }
  ).mock.calls;

  expect(
    calls.some(([input]) => {
      const url = String(input);
      return !url.includes("duckduckgo.com") && !url.includes("bing.com");
    }),
  ).toBe(false);
}

describe("crawl orchestration", () => {
  it("routes only matching discovered sources into each provider", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");
    const seenSourcePlatforms: string[] = [];

    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      seenSourcePlatforms.push(...sources.map((source) => source.platform));

      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 0,
        matchedCount: 0,
        jobs: [],
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
          classifySourceCandidate({
            url: "https://jobs.lever.co/figma",
            token: "figma",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    await runSearchFromFilters(
      {
        title: "Software Engineer",
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(seenSourcePlatforms).toEqual(["greenhouse"]);
  });

  it("runs only the selected provider families when platforms are specified", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");
    const greenhouseCrawl = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      fetchedCount: 0,
      matchedCount: 0,
      jobs: [],
    }));
    const leverCrawl = vi.fn(async () => ({
      provider: "lever" as const,
      status: "success" as const,
      fetchedCount: 0,
      matchedCount: 0,
      jobs: [],
    }));

    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/openai",
            token: "openai",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
          classifySourceCandidate({
            url: "https://jobs.lever.co/figma",
            token: "figma",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    await runSearchFromFilters(
      {
        title: "Software Engineer",
        platforms: ["lever"],
      },
      {
        repository,
        providers: [
          createStubProvider("greenhouse", greenhouseCrawl),
          createStubProvider("lever", leverCrawl),
        ],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(greenhouseCrawl).not.toHaveBeenCalled();
    expect(leverCrawl).toHaveBeenCalledTimes(1);
  });

  it("routes registry-backed Greenhouse sources into the Greenhouse provider", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-08T12:00:00.000Z");
    const seenTokens: string[] = [];

    const greenhouseProvider = createStubProvider("greenhouse", async (_context, sources) => {
      seenTokens.push(
        ...sources
          .map((source) => source.token)
          .filter((token): token is string => Boolean(token)),
      );

      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 0,
        matchedCount: 0,
        jobs: [],
      };
    });

    const discovery: DiscoveryService = {
      async discover(input) {
        return discoverSources({
          ...input,
          env: {
            greenhouseBoardTokens: ["openai", "benchling", "datadog"],
            leverSiteTokens: [],
            ashbyBoardTokens: [],
            companyPageSources: [],
            PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
            PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
            PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 120,
            PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 72,
            PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 4,
            GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 24,
          },
        });
      },
    };

    await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [greenhouseProvider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(seenTokens.length).toBeGreaterThan(0);
    expect(seenTokens).toContain("openai");
    expect(seenTokens).toContain("benchling");
  });

  it("normalizes query-like role input before discovery runs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");
    let seenFilters: Record<string, unknown> | undefined;

    const discovery: DiscoveryService = {
      async discover(input) {
        seenFilters = input.filters;
        return [];
      },
    };

    await runSearchFromFilters(
      {
        title: "Greenhouse software engineer jobs in the US",
      },
      {
        repository,
        providers: [
          createStubProvider("greenhouse", async () => ({
            provider: "greenhouse",
            status: "success",
            fetchedCount: 0,
            matchedCount: 0,
            jobs: [],
          })),
        ],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(seenFilters).toMatchObject({
      title: "software engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
  });

  it("uses persistent inventory-backed Greenhouse sources before slower supplemental discovery runs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-14T12:00:00.000Z");
    let supplementalFetchStarted = false;
    let inventoryRoutedBeforeSupplemental = false;

    await refreshSourceInventory({
      repository,
      now,
      env: {
        greenhouseBoardTokens: ["openai"],
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
    });

    const discovery = createDiscoveryService({
      repository,
      env: {
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [
          {
            type: "html_page",
            company: "Datadog",
            url: "https://careers.datadog.com/jobs",
          },
        ],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
        PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 20,
        PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 12,
        PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 2,
        GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 8,
      },
    });

    const provider = createStubProvider("greenhouse", async (_context, sources) => {
      if (sources.some((source) => source.token === "openai") && !supplementalFetchStarted) {
        inventoryRoutedBeforeSupplemental = true;
      }
      const datadogSource = sources.find((source) => source.token === "datadog");
      if (datadogSource) {
        await new Promise((resolve) => setTimeout(resolve, 60));
      }

      const emittedSources = sources.filter(
        (source) => source.token === "openai" || source.token === "datadog",
      );

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: sources.length,
        fetchedCount: emittedSources.length,
        matchedCount: emittedSources.length,
        jobs: emittedSources.map((currentSource) => ({
          title: currentSource.token === "openai" ? "Inventory Software Engineer" : "Supplemental Software Engineer",
          company: currentSource.companyHint ?? "Acme",
          locationText: "Remote, United States",
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

    const fetchImpl = vi.fn(async () => {
      supplementalFetchStarted = true;
      await new Promise((resolve) => setTimeout(resolve, 40));
      return new Response(
        '<html><body><a href="https://boards.greenhouse.io/datadog/jobs/123">Datadog</a></body></html>',
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      );
    }) as unknown as typeof fetch;

    const runPromise = runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl,
        now,
        progressUpdateIntervalMs: 5,
      },
    );

    const result = await runPromise;

    expect(inventoryRoutedBeforeSupplemental).toBe(true);
    expect(result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Inventory Software Engineer",
        "Supplemental Software Engineer",
      ]),
    );
  }, 10000);

  it("returns jobs in the crawl response when registry-backed Greenhouse sources are available", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-08T12:00:00.000Z");
    const discovery: DiscoveryService = {
      async discover(input) {
        return discoverSources({
          ...input,
          env: {
            greenhouseBoardTokens: ["openai", "benchling", "datadog"],
            leverSiteTokens: [],
            ashbyBoardTokens: [],
            companyPageSources: [],
            PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
            PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
            PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 120,
            PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 72,
            PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 4,
            GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 24,
          },
        });
      },
    };
    const fetchImpl = vi.fn(async (request: RequestInfo | URL) => {
      const url = String(request);

      if (url.includes("/openai/")) {
        return new Response(
          JSON.stringify({
            jobs: [
              {
                id: "software-engineer-1",
                title: "Software Engineer",
                absolute_url: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
                first_published: "2026-04-01T00:00:00.000Z",
                company_name: "OpenAI",
                location: {
                  name: "San Francisco, CA",
                },
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
      }

      return new Response(
        JSON.stringify({
          jobs: [],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
        crawlMode: "fast",
        experienceMatchMode: "balanced",
      },
      {
        repository,
        providers: [createGreenhouseProvider()],
        discovery,
        fetchImpl,
        now,
      },
    );

    expect(result.diagnostics.discoveredSources).toBeGreaterThan(0);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "OpenAI",
      country: "United States",
      sourcePlatform: "greenhouse",
    });
  });

  it("marks an all-provider failure as failed instead of a completed empty crawl", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const failingProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "failed",
        fetchedCount: 0,
        matchedCount: 0,
        jobs: [],
        errorMessage: "Greenhouse returned 404 for openai.",
      };
    });

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
      },
      {
        repository,
        providers: [failingProvider],
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(result.jobs).toHaveLength(0);
    expect(result.crawlRun.status).toBe("failed");
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      status: "failed",
      fetchedCount: 0,
      matchedCount: 0,
      errorMessage: "Greenhouse returned 404 for openai.",
    });
    expect(result.crawlRun).not.toHaveProperty("warnings");
    expect(result.sourceResults[0]).not.toHaveProperty("warnings");
  });

  it("keeps a legitimate no-match crawl distinct from provider failure", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Data Scientist",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "data-scientist",
            sourceUrl: "https://example.com/data-scientist",
            applyUrl: "https://example.com/data-scientist/apply",
            canonicalUrl: "https://example.com/data-scientist",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    expect(result.jobs).toHaveLength(0);
    expect(result.crawlRun.status).toBe("completed");
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      status: "success",
      fetchedCount: 1,
      matchedCount: 0,
    });
  });

  it("keeps software engineering internship titles through crawl filtering and persistence", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("lever", async () => {
      return {
        provider: "lever",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Software Engineering Intern",
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "software-engineering-intern",
            sourceUrl: "https://example.com/software-engineering-intern",
            applyUrl: "https://example.com/software-engineering-intern/apply",
            canonicalUrl: "https://example.com/software-engineering-intern",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevel: "intern",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(result.sourceResults[0]?.matchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]?.title).toBe("Software Engineering Intern");
    expect(result.jobs[0]?.experienceLevel).toBe("intern");
    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]?.title).toBe("Software Engineering Intern");
    expect(storedJobs[0]?.experienceLevel).toBe("intern");
  });

  it(
    "matches experience filters from inferred titles when normalized experience is missing",
    async () => {
    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const cases = [
      {
        level: "intern" as const,
        title: "Software Engineer Intern",
      },
      {
        level: "new_grad" as const,
        title: "New Graduate Software Engineer",
      },
      {
        level: "junior" as const,
        title: "Junior Software Engineer",
      },
      {
        level: "mid" as const,
        title: "Software Engineer II",
      },
      {
        level: "senior" as const,
        title: "Senior Software Engineer",
      },
      {
        level: "staff" as const,
        title: "Staff Software Engineer",
      },
    ];

    for (const currentCase of cases) {
      const db = new FakeDb();
      const repository = new JobCrawlerRepository(db);
      const now = new Date("2026-03-29T12:00:00.000Z");

      const successProvider = createStubProvider("greenhouse", async () => {
        return {
          provider: "greenhouse",
          status: "success",
          fetchedCount: cases.length,
          matchedCount: cases.length,
          jobs: cases.map((entry, index) => ({
            title: entry.title,
            company: "Acme",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse" as const,
            sourceJobId: `level-role-${index}`,
            sourceUrl: `https://example.com/level-role-${index}`,
            applyUrl: `https://example.com/level-role-${index}/apply`,
            canonicalUrl: `https://example.com/level-role-${index}`,
            postedAt: `2026-03-${20 + index}T00:00:00.000Z`,
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          })),
        };
      });

      const result = await runSearchFromFilters(
        {
          title: "Software Engineer",
          experienceLevel: currentCase.level,
        },
        {
          repository,
          providers: [successProvider],
          fetchImpl,
          now,
        },
      );

      const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
      const storedRuns = db.snapshot<Record<string, unknown>>(collectionNames.crawlRuns);
      const storedSourceResults = db.snapshot<Record<string, unknown>>(collectionNames.crawlSourceResults);

      expect(result.sourceResults[0]?.matchedCount).toBe(1);
      expect(result.jobs.map((job) => job.title)).toEqual([currentCase.title]);
      expect(result.jobs[0]?.experienceLevel).toBe(currentCase.level);
      expect(storedJobs[0]?.experienceLevel).toBe(currentCase.level);
      expect(result.crawlRun).not.toHaveProperty("warnings");
      expect(result.sourceResults[0]).not.toHaveProperty("warnings");
      expect(storedRuns[0]).not.toHaveProperty("warnings");
      expect(storedSourceResults[0]).not.toHaveProperty("warnings");
    }
    },
    10_000,
  );

  it("matches and persists experience filters from provider metadata when the title is generic", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 2,
        matchedCount: 2,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme Mid",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "software-engineer-mid",
            sourceUrl: "https://example.com/software-engineer-mid",
            applyUrl: "https://example.com/software-engineer-mid/apply",
            canonicalUrl: "https://example.com/software-engineer-mid",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {
              description:
                "We are looking for engineers with 2-5 years of experience building product features.",
            },
          },
          {
            title: "Software Engineer",
            company: "Acme Intern",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "software-engineer-intern",
            sourceUrl: "https://example.com/software-engineer-intern",
            applyUrl: "https://example.com/software-engineer-intern/apply",
            canonicalUrl: "https://example.com/software-engineer-intern",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {
              description:
                "This is part of our 2026 software engineering internship program for students.",
            },
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const midResult = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevel: "mid",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(midResult.jobs).toHaveLength(1);
    expect(midResult.jobs[0]?.sourceJobId).toBe("software-engineer-mid");
    expect(midResult.jobs[0]?.experienceLevel).toBe("mid");

    const internResult = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevel: "intern",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(internResult.jobs).toHaveLength(1);
    expect(internResult.jobs[0]?.sourceJobId).toBe("software-engineer-intern");
    expect(internResult.jobs[0]?.experienceLevel).toBe("intern");

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    expect(storedJobs.some((job) => job.sourceJobId === "software-engineer-mid" && job.experienceLevel === "mid")).toBe(true);
    expect(storedJobs.some((job) => job.sourceJobId === "software-engineer-intern" && job.experienceLevel === "intern")).toBe(true);
  });

  it("matches any selected experience level during crawl orchestration", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("lever", async () => {
      return {
        provider: "lever",
        status: "success",
        fetchedCount: 3,
        matchedCount: 3,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme Senior",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "software-engineer-senior",
            sourceUrl: "https://example.com/software-engineer-senior/detail",
            applyUrl: "https://example.com/software-engineer-senior/apply",
            canonicalUrl: "https://example.com/software-engineer-senior/detail",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {
              description:
                "Minimum qualifications: 5+ years of experience building APIs.",
            },
          },
          {
            title: "Software Engineering Intern",
            company: "Acme Intern",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "software-engineering-intern",
            sourceUrl: "https://example.com/software-engineering-intern/detail",
            applyUrl: "https://example.com/software-engineering-intern/apply",
            canonicalUrl: "https://example.com/software-engineering-intern/detail",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Software Engineer",
            company: "Acme Mid",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "software-engineer-mid",
            sourceUrl: "https://example.com/software-engineer-mid/detail",
            applyUrl: "https://example.com/software-engineer-mid/apply",
            canonicalUrl: "https://example.com/software-engineer-mid/detail",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {
              description:
                "We are looking for candidates with 2-5 years of experience building product features.",
            },
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string | URL, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevels: ["intern", "senior"],
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(result.sourceResults[0]?.matchedCount).toBe(2);
    expect(result.jobs.map((job) => job.sourceJobId)).toEqual(
      expect.arrayContaining([
        "software-engineering-intern",
        "software-engineer-senior",
      ]),
    );
    expect(result.jobs.map((job) => job.experienceLevel)).toEqual(
      expect.arrayContaining(["intern", "senior"]),
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    expect(storedJobs).toHaveLength(2);
    expect(storedJobs.some((job) => job.sourceJobId === "software-engineering-intern")).toBe(true);
    expect(storedJobs.some((job) => job.sourceJobId === "software-engineer-senior")).toBe(true);
  });

  it("does not deep-fetch source pages to infer experience during filtering", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("lever", async () => {
      return {
        provider: "lever",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Software Engineer",
            company: "Acme Senior",
            country: "United States",
            locationText: "Remote, United States",
            sourcePlatform: "lever",
            sourceJobId: "software-engineer-senior",
            sourceUrl: "https://example.com/software-engineer-senior/detail",
            applyUrl: "https://example.com/software-engineer-senior/apply",
            canonicalUrl: "https://example.com/software-engineer-senior/detail",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string | URL, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevel: "senior",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(result.jobs).toHaveLength(0);
    expectNoDirectJobFetches(fetchImpl);
  });

  it("keeps generic Greenhouse internships while excluding disclaimer-only and non-US matches", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 3,
        matchedCount: 3,
        jobs: [
            normalizeGreenhouseJob({
              companyToken: "stripe",
              discoveredAt: now.toISOString(),
              job: {
                id: "generic-intern-us",
                title: "Software Engineer",
                absolute_url: "https://boards.greenhouse.io/stripe/jobs/generic-intern-us",
                first_published: "2026-03-20T00:00:00.000Z",
                location: { name: "Seattle" },
                metadata: [
                  {
                    name: "Program",
                    value: "Student Program",
                  },
                ],
                content:
                  "&lt;p&gt;Our internship program gives software engineering students meaningful projects.&lt;/p&gt;",
              },
            }),
            normalizeGreenhouseJob({
              companyToken: "stripe",
              discoveredAt: now.toISOString(),
              job: {
                id: "generic-regular-us",
                title: "Software Engineer",
                absolute_url: "https://boards.greenhouse.io/stripe/jobs/generic-regular-us",
                first_published: "2026-03-21T00:00:00.000Z",
                location: { name: "San Francisco, CA" },
                content:
                  "&lt;p&gt;Note: if you are an intern, new grad, or staff applicant, please do not apply using this link.&lt;/p&gt;",
              },
            }),
            normalizeGreenhouseJob({
              companyToken: "stripe",
              discoveredAt: now.toISOString(),
              job: {
                id: "generic-intern-canada",
                title: "Software Engineer",
                absolute_url: "https://boards.greenhouse.io/stripe/jobs/generic-intern-canada",
                first_published: "2026-03-22T00:00:00.000Z",
                location: { name: "Toronto" },
                metadata: [
                  {
                    name: "Program",
                    value: "Student Program",
                  },
                ],
                content:
                  "&lt;p&gt;Our internship program gives software engineering students meaningful projects.&lt;/p&gt;",
              },
            }),
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
        experienceLevel: "intern",
        country: "United States",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(result.sourceResults[0]?.matchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]?.sourceJobId).toBe("generic-intern-us");
    expect(result.jobs[0]?.experienceLevel).toBe("intern");
    expect(result.jobs[0]?.locationText).toBe("Seattle");
  });

  it("filters unrelated roles and sorts matched jobs by title relevance before recency", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 6,
        matchedCount: 6,
        jobs: [
            {
              title: "Backend Engineer",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "backend-role",
              sourceUrl: "https://example.com/backend-role",
              applyUrl: "https://example.com/backend-role/apply",
              canonicalUrl: "https://example.com/backend-role",
              postedAt: "2026-03-28T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
            {
              title: "SWE",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "swe-role",
              sourceUrl: "https://example.com/swe-role",
              applyUrl: "https://example.com/swe-role/apply",
              canonicalUrl: "https://example.com/swe-role",
              postedAt: "2026-03-27T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
            {
              title: "Software Developer",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "developer-role",
              sourceUrl: "https://example.com/developer-role",
              applyUrl: "https://example.com/developer-role/apply",
              canonicalUrl: "https://example.com/developer-role",
              postedAt: "2026-03-26T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
            {
              title: "Senior Software Engineer",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "senior-role",
              sourceUrl: "https://example.com/senior-role",
              applyUrl: "https://example.com/senior-role/apply",
              canonicalUrl: "https://example.com/senior-role",
              postedAt: "2026-03-25T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
            {
              title: "Software Engineer",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "exact-role",
              sourceUrl: "https://example.com/exact-role",
              applyUrl: "https://example.com/exact-role/apply",
              canonicalUrl: "https://example.com/exact-role",
              postedAt: "2026-03-24T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
            {
              title: "Data Engineer",
              company: "Acme",
              country: "United States",
              locationText: "Remote, United States",
              sourcePlatform: "greenhouse",
              sourceJobId: "data-role",
              sourceUrl: "https://example.com/data-role",
              applyUrl: "https://example.com/data-role/apply",
              canonicalUrl: "https://example.com/data-role",
              postedAt: "2026-03-29T00:00:00.000Z",
              discoveredAt: now.toISOString(),
              rawSourceMetadata: {},
            },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Software Engineer",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(result.sourceResults[0]?.matchedCount).toBe(5);
    expect(result.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Senior Software Engineer",
      "Software Developer",
      "SWE",
      "Backend Engineer",
    ]);
  });

  it("matches a United States country filter against US aliases during crawl filtering", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            country: "US",
            locationText: "Remote US",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-us-1",
            sourceUrl: "https://example.com/job-us-1",
            applyUrl: "https://example.com/job-us-1/apply",
            canonicalUrl: "https://example.com/job-us-1",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
        country: "United States",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(result.jobs).toHaveLength(1);
    expect(result.sourceResults[0]?.matchedCount).toBe(1);
    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]?.country).toBe("US");
    expect(storedJobs[0]?.locationText).toBe("Remote US");
  });

  it("matches a United States country-only filter against inferred US city and state locations during crawl filtering", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 3,
        matchedCount: 3,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            locationText: "Seattle",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-us-city-only",
            sourceUrl: "https://example.com/job-us-city-only",
            applyUrl: "https://example.com/job-us-city-only/apply",
            canonicalUrl: "https://example.com/job-us-city-only",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Backend Engineer",
            company: "Acme",
            locationText: "San Francisco, CA",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-us-city-state",
            sourceUrl: "https://example.com/job-us-city-state",
            applyUrl: "https://example.com/job-us-city-state/apply",
            canonicalUrl: "https://example.com/job-us-city-state",
            postedAt: "2026-03-21T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Backend Engineer",
            company: "Acme",
            locationText: "Toronto",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-non-us-city",
            sourceUrl: "https://example.com/job-non-us-city",
            applyUrl: "https://example.com/job-non-us-city/apply",
            canonicalUrl: "https://example.com/job-non-us-city",
            postedAt: "2026-03-22T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
        country: "United States",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(result.jobs).toHaveLength(2);
    expect(result.sourceResults[0]?.matchedCount).toBe(2);
    expect(storedJobs).toHaveLength(2);
    expect(storedJobs.map((job) => job.locationText).sort()).toEqual([
      "San Francisco, CA",
      "Seattle",
    ]);
    expect(storedJobs[0]?.rawSourceMetadata).toMatchObject({
      crawlLocationMatch: expect.objectContaining({
        originalQueryLocation: "United States",
        normalizedQueryLocation: "united states",
        queryExpandedTermsUsed: expect.arrayContaining(["united states", "usa", "us"]),
        rawJobLocation: expect.any(String),
        normalizedJobLocation: expect.any(String),
        explanation: expect.any(String),
      }),
    });
    expectNoDirectJobFetches(fetchImpl);
  });

  it("passes explicit city and state filters into providers while keeping validation deferred", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");
    const providerFilters: Array<Record<string, unknown>> = [];

    const successProvider = createStubProvider("greenhouse", async (context) => {
      providerFilters.push(context.filters);

      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 3,
        matchedCount: 3,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme SF",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-sf",
            sourceUrl: "https://example.com/job-sf",
            applyUrl: "https://example.com/job-sf/apply",
            canonicalUrl: "https://example.com/job-sf",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Backend Engineer",
            company: "Acme Austin",
            city: "Austin",
            state: "Texas",
            country: "United States",
            locationText: "Austin, Texas, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-austin",
            sourceUrl: "https://example.com/job-austin",
            applyUrl: "https://example.com/job-austin/apply",
            canonicalUrl: "https://example.com/job-austin",
            postedAt: "2026-03-21T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Product Designer",
            company: "Acme Designer",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-designer",
            sourceUrl: "https://example.com/job-designer",
            applyUrl: "https://example.com/job-designer/apply",
            canonicalUrl: "https://example.com/job-designer",
            postedAt: "2026-03-22T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
        country: "US",
        state: "CA",
        city: "San Francisco",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    expect(providerFilters).toEqual([
      {
        title: "Backend Engineer",
        country: "US",
        state: "CA",
        city: "San Francisco",
      },
    ]);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]?.sourceJobId).toBe("job-sf");
    expect(result.sourceResults[0]?.matchedCount).toBe(1);
    expectNoDirectJobFetches(fetchImpl);
  });

  it("persists normalized crawl results into the jobs collection without inline validation by default", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-1",
            sourceUrl: "https://example.com/job-1",
            applyUrl: "https://example.com/job-1/apply",
            canonicalUrl: "https://example.com/job-1",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {
              boardToken: "acme",
            },
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return {
          status: 200,
          url: "https://example.com/job-1/apply",
        } as Response;
      }

      return {
        status: 200,
        url: "https://example.com/job-1/apply",
        text: async () => "<html><body>Apply here</body></html>",
      } as Response;
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(result.jobs).toHaveLength(1);
    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]).toMatchObject({
      _id: result.jobs[0]._id,
      title: "Backend Engineer",
      company: "Acme",
      sourcePlatform: "greenhouse",
      sourceJobId: "job-1",
      sourceUrl: "https://example.com/job-1",
      applyUrl: "https://example.com/job-1/apply",
      canonicalUrl: "https://example.com/job-1",
      city: "San Francisco",
      state: "California",
      country: "United States",
      locationText: "San Francisco, California, United States",
      postedAt: "2026-03-20T00:00:00.000Z",
      discoveredAt: now.toISOString(),
      linkStatus: "unknown",
      companyNormalized: "acme",
      titleNormalized: "backend engineer",
      contentFingerprint: result.jobs[0].contentFingerprint,
      sourceLookupKeys: ["greenhouse:acme:job 1"],
      crawlRunIds: [result.crawlRun._id],
    });
    expect(storedJobs[0].sourceProvenance).toMatchObject([
      {
        sourcePlatform: "greenhouse",
        sourceJobId: "job-1",
        sourceUrl: "https://example.com/job-1",
        applyUrl: "https://example.com/job-1/apply",
        canonicalUrl: "https://example.com/job-1",
        discoveredAt: now.toISOString(),
        rawSourceMetadata: expect.objectContaining({
          boardToken: "acme",
        }),
      },
    ]);
    expect(storedJobs[0]?.resolvedUrl).toBeUndefined();
    expect(storedJobs[0]?.lastValidatedAt).toBeUndefined();
    expectNoDirectJobFetches(fetchImpl);
  });

  it("supports explicit full inline validation when requested", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-1",
            sourceUrl: "https://example.com/job-1",
            applyUrl: "https://example.com/job-1/apply",
            canonicalUrl: "https://example.com/job-1",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return {
          status: 200,
          url: "https://example.com/job-1/apply",
        } as Response;
      }

      return {
        status: 200,
        url: "https://example.com/job-1/apply",
        text: async () => "<html><body>Apply here</body></html>",
      } as Response;
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
        linkValidationMode: "full_inline",
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]).toMatchObject({
      _id: result.jobs[0]._id,
      resolvedUrl: "https://example.com/job-1/apply",
      canonicalUrl: "https://example.com/job-1/apply",
      linkStatus: "valid",
      lastValidatedAt: now.toISOString(),
    });
    expect(fetchImpl).toHaveBeenCalled();
  });

  it("validates only the newest jobs when inline_top_n is enabled", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");
    const headCalls: string[] = [];

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 2,
        matchedCount: 2,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme New",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-new",
            sourceUrl: "https://example.com/job-new",
            applyUrl: "https://example.com/job-new/apply",
            canonicalUrl: "https://example.com/job-new",
            postedAt: "2026-03-21T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
          {
            title: "Backend Engineer",
            company: "Acme Legacy",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-old",
            sourceUrl: "https://example.com/job-old",
            applyUrl: "https://example.com/job-old/apply",
            canonicalUrl: "https://example.com/job-old",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const fetchImpl = vi.fn(async (input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        headCalls.push(input);
        return {
          status: 200,
          url: input,
        } as Response;
      }

      return {
        status: 200,
        url: input,
        text: async () => "<html><body>Apply here</body></html>",
      } as Response;
    }) as unknown as typeof fetch;

    await runSearchFromFilters(
      {
        title: "Backend Engineer",
      },
      {
        repository,
        providers: [successProvider],
        fetchImpl,
        now,
        linkValidationMode: "inline_top_n",
        inlineValidationTopN: 1,
      },
    );

    const storedJobs = db.snapshot<JobListing>(collectionNames.jobs);
    const newestJob = storedJobs.find((job) => job.sourceJobId === "job-new");
    const olderJob = storedJobs.find((job) => job.sourceJobId === "job-old");

    expect(headCalls).toEqual(["https://example.com/job-new/apply"]);
    expect(newestJob?.linkStatus).toBe("valid");
    expect(newestJob?.lastValidatedAt).toBe(now.toISOString());
    expect(olderJob?.linkStatus).toBe("unknown");
    expect(olderJob?.lastValidatedAt).toBeUndefined();
  });

  it("continues when one provider fails", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-03-29T12:00:00.000Z");

    const successProvider = createStubProvider("greenhouse", async () => {
      return {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        jobs: [
          {
            title: "Backend Engineer",
            company: "Acme",
            city: "San Francisco",
            state: "California",
            country: "United States",
            locationText: "San Francisco, California, United States",
            sourcePlatform: "greenhouse",
            sourceJobId: "job-1",
            sourceUrl: "https://example.com/job-1",
            applyUrl: "https://example.com/job-1/apply",
            canonicalUrl: "https://example.com/job-1",
            postedAt: "2026-03-20T00:00:00.000Z",
            discoveredAt: now.toISOString(),
            rawSourceMetadata: {},
          },
        ],
      };
    });

    const failingProvider = createStubProvider("lever", async () => {
      throw new Error("Lever is unavailable");
    });

    const fetchImpl = vi.fn(async (_input: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Apply here</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await runSearchFromFilters(
      {
        title: "Backend Engineer",
      },
      {
        repository,
        providers: [successProvider, failingProvider],
        fetchImpl,
        now,
      },
    );

    expect(result.jobs).toHaveLength(1);
    expect(result.crawlRun.status).toBe("partial");
    expect(
      result.sourceResults.find((sourceResult) => sourceResult.provider === "lever")?.status,
    ).toBe("failed");
    expect(result.crawlRun).not.toHaveProperty("warnings");
    expect(result.sourceResults[0]).not.toHaveProperty("warnings");
  });
});
