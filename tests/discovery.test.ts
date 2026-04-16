import { describe, expect, it, vi } from "vitest";

import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { getDefaultGreenhouseRegistryEntries } from "@/lib/server/discovery/greenhouse-registry";
import { toSourceInventoryRecord } from "@/lib/server/discovery/inventory";
import {
  buildPublicSearchQueryPlan,
  discoverSourcesFromPublicSearch,
  discoverSourcesFromPublicSearchDetailed,
  selectQueriesForExecution,
} from "@/lib/server/discovery/public-search";
import {
  discoverBaselineSourcesDetailed,
  discoverConfiguredSources,
  discoverSupplementalSourcesDetailed,
  discoverSources,
  discoverSourcesDetailed,
  refreshSourceInventory,
  resolvePublicSearchExecutionOptions,
} from "@/lib/server/discovery/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { buildUsDiscoveryLocationTokens } from "@/lib/server/locations/us";
import { capSourcesWithPlatformDiversity } from "@/lib/server/crawler/source-capper";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { FakeDb } from "@/tests/helpers/fake-db";

const discoveryEnvDefaults = {
  greenhouseBoardTokens: ["openai"],
  leverSiteTokens: ["figma"],
  ashbyBoardTokens: ["notion"],
  companyPageSources: [],
  PUBLIC_SEARCH_DISCOVERY_ENABLED: true,
  PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 8,
  PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 120,
  PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 96,
  PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: 4,
  GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 32,
};

describe("source discovery", () => {
  it("classifies known ATS URLs and generic career pages", () => {
    const greenhouse = classifySourceCandidate({
      url: "https://boards-api.greenhouse.io/v1/boards/openai/jobs?content=true",
      discoveryMethod: "future_search",
    });
    const greenhouseJobBoard = classifySourceCandidate({
      url: "https://job-boards.greenhouse.io/acme/jobs/123",
      discoveryMethod: "future_search",
    });
    const lever = classifySourceCandidate({
      url: "https://api.lever.co/v0/postings/figma?mode=json",
      discoveryMethod: "future_search",
    });
    const ashby = classifySourceCandidate({
      url: "https://jobs.ashbyhq.com/notion",
      discoveryMethod: "future_search",
    });
    const smartRecruiters = classifySourceCandidate({
      url: "https://careers.smartrecruiters.com/Acme",
      discoveryMethod: "future_search",
    });
    const workday = classifySourceCandidate({
      url: "https://acme.wd1.myworkdayjobs.com/Careers",
      discoveryMethod: "future_search",
    });
    const companyPage = classifySourceCandidate({
      url: "https://careers.acme.com/jobs",
      discoveryMethod: "future_search",
    });

    expect(greenhouse).toMatchObject({
      platform: "greenhouse",
      token: "openai",
      boardUrl: "https://boards.greenhouse.io/openai",
    });
    expect(greenhouseJobBoard).toMatchObject({
      platform: "greenhouse",
      token: "acme",
      boardUrl: "https://boards.greenhouse.io/acme",
    });
    expect(lever).toMatchObject({
      platform: "lever",
      token: "figma",
      hostedUrl: "https://jobs.lever.co/figma",
    });
    expect(ashby).toMatchObject({
      platform: "ashby",
      token: "notion",
      boardUrl: "https://jobs.ashbyhq.com/notion",
    });
    expect(smartRecruiters).toMatchObject({
      platform: "smartrecruiters",
      token: "Acme",
      boardUrl: "https://careers.smartrecruiters.com/Acme",
    });
    expect(workday).toMatchObject({
      platform: "workday",
      url: "https://acme.wd1.myworkdayjobs.com/Careers",
    });
    expect(companyPage).toMatchObject({
      platform: "company_page",
      pageType: "html_page",
      companyHint: "Acme",
    });
  });

  it("classifies embedded Greenhouse board URLs into the real board token", () => {
    const embeddedBoard = classifySourceCandidate({
      url: "https://boards.greenhouse.io/embed/job_board?for=Benchling",
      discoveryMethod: "future_search",
    });
    const embeddedBoardScript = classifySourceCandidate({
      url: "https://job-boards.greenhouse.io/embed/job_board?for=discord",
      discoveryMethod: "future_search",
    });

    expect(embeddedBoard).toMatchObject({
      platform: "greenhouse",
      token: "benchling",
      boardUrl: "https://boards.greenhouse.io/benchling",
    });
    expect(embeddedBoardScript).toMatchObject({
      platform: "greenhouse",
      token: "discord",
      boardUrl: "https://boards.greenhouse.io/discord",
    });
  });

  it("classifies www-prefixed Greenhouse board URLs into the real board token", () => {
    const prefixedBoard = classifySourceCandidate({
      url: "https://www.boards.greenhouse.io/voltrondata/jobs/4351155006",
      discoveryMethod: "future_search",
    });

    expect(prefixedBoard).toMatchObject({
      platform: "greenhouse",
      token: "voltrondata",
      boardUrl: "https://boards.greenhouse.io/voltrondata",
    });
  });

  it("classifies hosted Greenhouse detail URLs into board-level sources with stable identifiers", () => {
    const hostedDetail = classifySourceCandidate({
      url: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002?gh_jid=8455464002&utm_source=linkedin",
      discoveryMethod: "future_search",
    });

    expect(hostedDetail).toMatchObject({
      platform: "greenhouse",
      token: "gitlab",
      jobId: "8455464002",
      url: "https://boards.greenhouse.io/gitlab",
      boardUrl: "https://boards.greenhouse.io/gitlab",
      apiUrl: "https://boards-api.greenhouse.io/v1/boards/gitlab/jobs?content=true",
    });
  });

  it("recovers Lever, Ashby, and Workday sources from detail URLs instead of treating them as opaque pages", () => {
    const leverDetail = classifySourceCandidate({
      url: "https://jobs.lever.co/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc?lever-source=LinkedIn",
      discoveryMethod: "future_search",
    });
    const ashbyDetail = classifySourceCandidate({
      url: "https://jobs.ashbyhq.com/notion/497fcc20-d3fd-42f0-9b24-123456789abc",
      discoveryMethod: "future_search",
    });
    const smartRecruitersDetail = classifySourceCandidate({
      url: "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
      discoveryMethod: "future_search",
    });
    const workdayDetail = classifySourceCandidate({
      url: "https://acme.wd1.myworkdayjobs.com/en-US/Careers/job/Seattle-WA/Data-Engineer_R12345",
      discoveryMethod: "future_search",
    });

    expect(leverDetail).toMatchObject({
      platform: "lever",
      token: "figma",
      jobId: "4d6f3f0b-1cdd-4d2e-a0a7-123456789abc",
      url: "https://jobs.lever.co/figma",
      hostedUrl: "https://jobs.lever.co/figma",
      apiUrl: "https://api.lever.co/v0/postings/figma?mode=json",
    });
    expect(ashbyDetail).toMatchObject({
      platform: "ashby",
      token: "notion",
      jobId: "497fcc20-d3fd-42f0-9b24-123456789abc",
      url: "https://jobs.ashbyhq.com/notion",
      boardUrl: "https://jobs.ashbyhq.com/notion",
    });
    expect(smartRecruitersDetail).toMatchObject({
      platform: "smartrecruiters",
      token: "Acme",
      jobId: "744000067444685",
      url: "https://careers.smartrecruiters.com/Acme",
      boardUrl: "https://careers.smartrecruiters.com/Acme",
      jobUrl: "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
    });
    expect(workdayDetail).toMatchObject({
      platform: "workday",
      token: "acme:careers",
      jobId: "Seattle-WA/Data-Engineer_R12345",
      careerSitePath: "Careers",
      sitePath: "en-US/Careers",
      url: "https://acme.wd1.myworkdayjobs.com/en-US/Careers",
    });
  });

  it("ships a materially larger default Greenhouse registry than the tiny initial seed set", () => {
    const entries = getDefaultGreenhouseRegistryEntries();

    expect(entries.length).toBeGreaterThanOrEqual(20);
    expect(entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ token: "figma", companyHint: "Figma" }),
        expect.objectContaining({ token: "gitlab", companyHint: "GitLab" }),
        expect.objectContaining({ token: "omadahealth", companyHint: "Omada Health" }),
        expect.objectContaining({ token: "public", companyHint: "Public" }),
        expect.objectContaining({ token: "chalkinc", companyHint: "Chalk" }),
        expect.objectContaining({ token: "doordashusa", companyHint: "DoorDash" }),
        expect.objectContaining({ token: "graphcore", companyHint: "Graphcore" }),
        expect.objectContaining({ token: "alarmcom", companyHint: "Alarm.com" }),
        expect.objectContaining({
          token: "bottomlinetechnologies",
          companyHint: "Bottomline",
        }),
      ]),
    );
  });

  it("keeps company-page expansion out of the baseline stage so supplemental recall can do it later", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(
        `
          <html>
            <body>
              <a href="https://boards.greenhouse.io/datadog/jobs/789">Datadog</a>
            </body>
          </html>
        `,
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      )) as unknown as typeof fetch;

    const baseline = await discoverBaselineSourcesDetailed({
      filters: {
        title: "Software Engineer",
      },
      now: new Date("2026-04-12T00:00:00.000Z"),
      fetchImpl,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
        companyPageSources: [
          {
            type: "html_page",
            company: "Datadog",
            url: "https://careers.datadog.com/jobs",
          },
        ],
      },
    });

    expect(fetchImpl).not.toHaveBeenCalled();
    expect(baseline.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "company_page",
          url: "https://careers.datadog.com/jobs",
        }),
      ]),
    );

    const supplemental = await discoverSupplementalSourcesDetailed(
      {
        filters: {
          title: "Software Engineer",
        },
        now: new Date("2026-04-12T00:00:00.000Z"),
        fetchImpl,
        env: {
          ...discoveryEnvDefaults,
          greenhouseBoardTokens: [],
          leverSiteTokens: [],
          ashbyBoardTokens: [],
          PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
          companyPageSources: [
            {
              type: "html_page",
              company: "Datadog",
              url: "https://careers.datadog.com/jobs",
            },
          ],
        },
      },
      {
        baselineSources: baseline.sources,
      },
    );

    expect(fetchImpl).toHaveBeenCalledWith(
      "https://careers.datadog.com/jobs",
      expect.anything(),
    );
    expect(supplemental.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "greenhouse",
          token: "datadog",
        }),
      ]),
    );
  });

  it("discovers configured env and manual sources into typed discovered sources", () => {
    const sources = discoverConfiguredSources({
      filters: {
        title: "Software Engineer",
      },
      now: new Date("2026-04-07T00:00:00.000Z"),
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai"],
        leverSiteTokens: ["figma"],
        ashbyBoardTokens: ["notion"],
        companyPageSources: [
          {
            type: "json_feed",
            company: "Acme",
            url: "https://careers.acme.com/feed.json",
          },
        ],
      },
    });

    expect(sources).toHaveLength(4);
    expect(sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "greenhouse",
          token: "openai",
          discoveryMethod: "platform_registry",
        }),
        expect.objectContaining({
          platform: "lever",
          token: "figma",
          discoveryMethod: "configured_env",
        }),
        expect.objectContaining({
          platform: "ashby",
          token: "notion",
          discoveryMethod: "configured_env",
        }),
        expect.objectContaining({
          platform: "company_page",
          companyHint: "Acme",
          pageType: "json_feed",
          discoveryMethod: "manual_config",
        }),
      ]),
    );
  });

  it("limits discovered sources to the selected implemented platforms", () => {
    const sources = discoverConfiguredSources({
      filters: {
        title: "Software Engineer",
        platforms: ["lever", "workday"],
      },
      now: new Date("2026-04-07T00:00:00.000Z"),
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai"],
        leverSiteTokens: ["figma"],
        ashbyBoardTokens: ["notion"],
        companyPageSources: [
          {
            type: "json_feed",
            company: "Acme",
            url: "https://careers.acme.com/feed.json",
          },
        ],
      },
    });

    expect(sources).toHaveLength(1);
    expect(sources).toEqual([
      expect.objectContaining({
        platform: "lever",
        token: "figma",
      }),
    ]);
  });

  it("discovers additional public greenhouse sources from search results", async () => {
    const html = `
      <html>
        <body>
          <a href="https://boards.greenhouse.io/acme/jobs/123">Acme</a>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fboards.greenhouse.io%2Fembed%2Fjob_board%3Ffor%3DBenchling">Benchling</a>
          <a href="https://jobs.lever.co/ignored/abc">Ignored Lever</a>
        </body>
      </html>
    `;

    const fetchImpl = (async () =>
      new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      })) as unknown as typeof fetch;

    const sources = await discoverSourcesFromPublicSearch(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
      },
    );

    expect(sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "greenhouse",
          token: "acme",
          discoveryMethod: "future_search",
        }),
        expect.objectContaining({
          platform: "greenhouse",
          token: "benchling",
          discoveryMethod: "future_search",
        }),
      ]),
    );
    expect(sources.every((source) => source.platform === "greenhouse")).toBe(true);
  });

  it("harvests direct Greenhouse detail jobs from SERP hits while still recovering the parent board", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://job-boards.greenhouse.io/gitlab/jobs/8455464002?gh_jid=8455464002</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://html.duckduckgo.com/html/")) {
        return new Response("<html><body></body></html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      if (url === "https://boards-api.greenhouse.io/v1/boards/gitlab/jobs/8455464002?content=true") {
        return new Response(
          JSON.stringify({
            id: "8455464002",
            title: "Senior Data Engineer",
            absolute_url: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
            company_name: "GitLab",
            location: { name: "Austin, TX" },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }

      throw new Error(`Unexpected fetch: ${url}`);
    }) as unknown as typeof fetch;

    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "Data Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 1,
      },
    );

    expect(result.sources).toEqual([
      expect.objectContaining({
        platform: "greenhouse",
        token: "gitlab",
        jobId: "8455464002",
      }),
    ]);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        title: "Senior Data Engineer",
        company: "GitLab",
        sourcePlatform: "greenhouse",
        locationText: "Austin, TX",
      }),
    ]);
    expect(result.diagnostics).toMatchObject({
      candidateUrlsHarvested: 1,
      detailUrlsHarvested: 1,
      sourceUrlsHarvested: 0,
      recoveredSourcesFromDetailUrls: 1,
      directJobsExtracted: 1,
      sourcesAdded: 1,
    });
  });

  it("harvests direct Lever detail jobs from SERP hits while recovering the site token", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://jobs.lever.co/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://html.duckduckgo.com/html/")) {
        return new Response("<html><body></body></html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      if (url === "https://api.lever.co/v0/postings/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc?mode=json") {
        return new Response(
          JSON.stringify({
            id: "4d6f3f0b-1cdd-4d2e-a0a7-123456789abc",
            text: "Senior Product Manager",
            hostedUrl: "https://jobs.lever.co/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc",
            applyUrl: "https://jobs.lever.co/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc/apply",
            categories: {
              location: "Remote - California",
            },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }

      throw new Error(`Unexpected fetch: ${url}`);
    }) as unknown as typeof fetch;

    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "Product Manager",
        country: "United States",
        platforms: ["lever"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 1,
      },
    );

    expect(result.sources).toEqual([
      expect.objectContaining({
        platform: "lever",
        token: "figma",
        jobId: "4d6f3f0b-1cdd-4d2e-a0a7-123456789abc",
      }),
    ]);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        title: "Senior Product Manager",
        sourcePlatform: "lever",
        locationText: "Remote - California",
      }),
    ]);
    expect(result.diagnostics).toMatchObject({
      detailUrlsHarvested: 1,
      recoveredSourcesFromDetailUrls: 1,
      directJobsExtracted: 1,
      sampleHarvestedDetailUrls: [
        "https://jobs.lever.co/figma/4d6f3f0b-1cdd-4d2e-a0a7-123456789abc",
      ],
    });
  });

  it("harvests direct Ashby detail jobs from SERP hits while recovering the board token", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://jobs.ashbyhq.com/notion/497fcc20-d3fd-42f0-9b24-123456789abc</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://html.duckduckgo.com/html/")) {
        return new Response("<html><body></body></html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      if (url === "https://jobs.ashbyhq.com/notion/497fcc20-d3fd-42f0-9b24-123456789abc") {
        return new Response(
          `
            <html>
              <body>
                <script>
                  window.__appData = {
                    "jobBoard": {
                      "jobPostings": [
                        {
                          "id": "497fcc20-d3fd-42f0-9b24-123456789abc",
                          "title": "QA Engineer",
                          "locationName": "Remote, United States",
                          "jobUrl": "https://jobs.ashbyhq.com/notion/497fcc20-d3fd-42f0-9b24-123456789abc"
                        }
                      ]
                    }
                  };
                </script>
              </body>
            </html>
          `,
          {
            status: 200,
            headers: {
              "content-type": "text/html",
            },
          },
        );
      }

      throw new Error(`Unexpected fetch: ${url}`);
    }) as unknown as typeof fetch;

    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "QA Engineer",
        country: "United States",
        platforms: ["ashby"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 1,
      },
    );

    expect(result.sources).toEqual([
      expect.objectContaining({
        platform: "ashby",
        token: "notion",
        jobId: "497fcc20-d3fd-42f0-9b24-123456789abc",
      }),
    ]);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        title: "QA Engineer",
        sourcePlatform: "ashby",
        locationText: "Remote, United States",
      }),
    ]);
    expect(result.diagnostics).toMatchObject({
      detailUrlsHarvested: 1,
      recoveredSourcesFromDetailUrls: 1,
      directJobsExtracted: 1,
      sampleRecoveredSourceUrls: ["https://jobs.ashbyhq.com/notion"],
    });
  });

  it("harvests direct Workday detail jobs from SERP hits while recovering the tenant and career site", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://acme.wd1.myworkdayjobs.com/en-US/Careers/job/Seattle-WA/Data-Engineer_R12345</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://html.duckduckgo.com/html/")) {
        return new Response("<html><body></body></html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      if (
        url ===
        "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/Careers/jobs/Seattle-WA/Data-Engineer_R12345"
      ) {
        return new Response(
          JSON.stringify({
            jobPostingInfo: {
              title: "Principal Data Engineer",
              locationText: "Bellevue, WA",
              externalPath: "job/Seattle-WA/Data-Engineer_R12345",
              postedOn: "2026-03-10T00:00:00.000Z",
            },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }

      throw new Error(`Unexpected fetch: ${url}`);
    }) as unknown as typeof fetch;

    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "Data Engineer",
        country: "United States",
        platforms: ["workday"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 1,
      },
    );

    expect(result.sources).toEqual([
      expect.objectContaining({
        platform: "workday",
        token: "acme:careers",
        careerSitePath: "Careers",
        jobId: "Seattle-WA/Data-Engineer_R12345",
      }),
    ]);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        title: "Principal Data Engineer",
        sourcePlatform: "workday",
        locationText: "Bellevue, WA",
      }),
    ]);
    expect(result.diagnostics).toMatchObject({
      detailUrlsHarvested: 1,
      recoveredSourcesFromDetailUrls: 1,
      directJobsExtracted: 1,
      sampleRecoveredSourceUrls: ["https://acme.wd1.myworkdayjobs.com/en-US/Careers"],
    });
  });

  it("harvests direct SmartRecruiters detail jobs from SERP hits while recovering the company board", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(
          `
            <rss>
              <channel>
                <item>
                  <link>https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst</link>
                </item>
              </channel>
            </rss>
          `,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      if (url.startsWith("https://html.duckduckgo.com/html/")) {
        return new Response("<html><body></body></html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      if (url === "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst") {
        return new Response(
          `
            <html>
              <head>
                <script type="application/ld+json">
                  {
                    "@context": "https://schema.org",
                    "@type": "JobPosting",
                    "title": "Senior Product Analyst",
                    "description": "Analyze product signals across US markets.",
                    "datePosted": "2026-03-18T00:00:00.000Z",
                    "employmentType": "Full-time",
                    "url": "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
                    "hiringOrganization": {
                      "@type": "Organization",
                      "name": "Acme"
                    },
                    "jobLocation": {
                      "@type": "Place",
                      "address": {
                        "@type": "PostalAddress",
                        "addressLocality": "Austin",
                        "addressRegion": "TX",
                        "addressCountry": "US"
                      }
                    }
                  }
                </script>
              </head>
              <body></body>
            </html>
          `,
          {
            status: 200,
            headers: {
              "content-type": "text/html",
            },
          },
        );
      }

      throw new Error(`Unexpected fetch: ${url}`);
    }) as unknown as typeof fetch;

    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "Product Analyst",
        country: "United States",
        platforms: ["smartrecruiters"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 1,
      },
    );

    expect(result.sources).toEqual([
      expect.objectContaining({
        platform: "smartrecruiters",
        token: "Acme",
        jobId: "744000067444685",
        boardUrl: "https://careers.smartrecruiters.com/Acme",
      }),
    ]);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        title: "Senior Product Analyst",
        company: "Acme",
        sourcePlatform: "smartrecruiters",
        locationText: "Austin, TX, US",
      }),
    ]);
    expect(result.diagnostics).toMatchObject({
      detailUrlsHarvested: 1,
      recoveredSourcesFromDetailUrls: 1,
      directJobsExtracted: 1,
      sampleRecoveredSourceUrls: ["https://careers.smartrecruiters.com/Acme"],
    });
  });

  it("expands configured company career pages into Greenhouse board sources even when Greenhouse is the selected crawl platform", async () => {
    const fetchImpl = vi.fn(async () => {
      return new Response(
        `
          <html>
            <body>
              <a href="https://job-boards.greenhouse.io/gitlab/jobs/8455464002?gh_jid=8455464002&utm_source=linkedin">
                Senior Data Analyst
              </a>
            </body>
          </html>
        `,
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      );
    }) as unknown as typeof fetch;

    const sources = await discoverSources({
      filters: {
        title: "Data Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      now: new Date("2026-04-07T00:00:00.000Z"),
      fetchImpl,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [
          {
            type: "html_page",
            company: "GitLab",
            url: "https://about.gitlab.com/jobs/",
          },
        ],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
      },
    });

    expect(fetchImpl).toHaveBeenCalledWith(
      "https://about.gitlab.com/jobs/",
      expect.anything(),
    );
    expect(sources).toEqual([
      expect.objectContaining({
        platform: "greenhouse",
        token: "gitlab",
        jobId: "8455464002",
        boardUrl: "https://boards.greenhouse.io/gitlab",
      }),
    ]);
  });

  it("expands broad US Greenhouse discovery into ranked remote and metro clauses", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    expect(plan.maxSources).toBe(120);
    expect(plan.maxQueries).toBe(96);
    expect(plan.roleQueries.slice(0, 10)).toEqual([
      "software engineer",
      "software developer",
      "software development engineer",
      "backend engineer",
      "frontend engineer",
      "full stack engineer",
      "backend developer",
      "frontend developer",
      "full stack developer",
      "application developer",
    ]);
    expect(plan.platformPlans).toHaveLength(1);
    expect(plan.platformPlans[0]).toMatchObject({
      platform: "greenhouse",
      locationIntent: {
        kind: "broad_us",
      },
    });
    expect(plan.platformPlans[0].locationClauses).toEqual(
      expect.arrayContaining([
        "",
        "united states",
        "usa",
        "us",
        "remote united states",
        "remote usa",
        "remote us",
        "united states remote",
        "remote california",
        "remote texas",
        "california",
        "ca usa",
        "texas",
        "tx usa",
        "new york state",
        "ny usa",
        "washington state",
        "seattle wa",
        "seattle washington",
        "bellevue wa",
        "bellevue washington",
      ]),
    );
    expect(plan.queries.length).toBeGreaterThan(300);
    expect(plan.queries.slice(0, 6).map((query) => query.query)).toEqual([
      "site:boards.greenhouse.io software engineer",
      "site:boards.greenhouse.io/embed/job_board software engineer",
      "site:job-boards.greenhouse.io software engineer",
      "site:job-boards.greenhouse.io/embed/job_board software engineer",
      "site:boards.greenhouse.io software engineer united states",
      "site:boards.greenhouse.io/embed/job_board software engineer united states",
    ]);
    expect(plan.queries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          hostKind: "embed",
          query: "site:boards.greenhouse.io/embed/job_board software engineer",
        }),
        expect.objectContaining({
          hostKind: "embed",
          query: "site:job-boards.greenhouse.io/embed/job_board software engineer",
        }),
      ]),
    );
    expect(new Set(plan.queries.map((query) => query.query)).size).toBe(plan.queries.length);
    expect(plan.queries.some((query) => query.query.includes("\""))).toBe(false);
    expect(plan.platformPlans[0]?.locationClauses).toHaveLength(32);
  });

  it("lets broad US public-search execution reach long-tail title variants instead of exhausting the budget on the first role only", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
        maxQueries: 24,
      },
    );

    const selected = selectQueriesForExecution(plan);

    expect(selected).toHaveLength(24);
    expect(selected.slice(0, 6).map((query) => query.roleQuery)).toEqual([
      "software engineer",
      "software developer",
      "software development engineer",
      "backend engineer",
      "frontend engineer",
      "full stack engineer",
    ]);
    expect(selected.some((query) => query.roleQuery === "software developer")).toBe(true);
    expect(selected.some((query) => query.roleQuery === "backend engineer")).toBe(true);
    expect(selected.some((query) => query.roleQuery === "application developer")).toBe(true);
    expect(selected.some((query) => query.roleQuery === "java developer")).toBe(true);
    expect(selected.some((query) => query.roleQuery === "mobile engineer")).toBe(true);
    expect(selected.some((query) => query.hostKind === "embed")).toBe(false);
  });

  it("brings Greenhouse embed-board queries into the plan after the higher-yield ATS detail hosts", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
        maxQueries: 56,
      },
    );

    const selected = selectQueriesForExecution(plan);
    const firstEmbedIndex = selected.findIndex((query) => query.hostKind === "embed");

    expect(selected).toHaveLength(56);
    expect(firstEmbedIndex).toBeGreaterThanOrEqual(40);
    expect(selected.slice(firstEmbedIndex).some((query) => query.hostKind === "embed")).toBe(true);
  });

  it("broadens data analyst discovery into close title variants instead of a single narrow query", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Data Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    expect(plan.roleQueries).toEqual(
      expect.arrayContaining([
        "data analyst",
        "analytics analyst",
        "business intelligence analyst",
        "reporting analyst",
        "insights analyst",
        "product analyst",
        "decision scientist",
        "business analyst",
        "operations analyst",
        "business operations analyst",
      ]),
    );
  });

  it("keeps data engineer discovery inside the data-engineering family", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Data Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    expect(plan.roleQueries).toEqual(
      expect.arrayContaining([
        "data engineer",
        "analytics engineer",
        "data platform engineer",
        "etl engineer",
        "data warehouse engineer",
        "data pipeline engineer",
      ]),
    );
    expect(plan.roleQueries).not.toContain("software engineer");
  });

  it("scopes US state-only Greenhouse discovery to the direct state clause and same-state metros", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        state: "California",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    const greenhousePlan = plan.platformPlans[0];
    const locationClauses = greenhousePlan?.locationClauses.filter(Boolean) ?? [];

    expect(greenhousePlan).toMatchObject({
      platform: "greenhouse",
      locationIntent: expect.objectContaining({
        kind: "us_state",
        stateName: "California",
        stateCode: "CA",
      }),
    });
    expect(locationClauses[0]).toBe("california");
    expect(locationClauses).toEqual(
      expect.arrayContaining([
        "ca usa",
        "remote california",
        "california remote",
        "san francisco ca",
        "san francisco california",
        "san jose ca",
        "mountain view ca",
      ]),
    );
    expect(locationClauses).not.toContain("remote us");
    expect(locationClauses).not.toContain("seattle wa");
    expect(locationClauses.length).toBeLessThanOrEqual(23);
  });

  it("scopes explicit Greenhouse city searches to the explicit city clause plus the broad fallback", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        city: "Seattle",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    expect(plan.platformPlans[0]).toMatchObject({
      platform: "greenhouse",
      locationIntent: expect.objectContaining({
        kind: "us_city",
        city: "Seattle",
        stateCode: "WA",
      }),
      locationClauses: expect.arrayContaining([
        "",
        "seattle",
        "seattle wa",
        "seattle washington",
        "washington state",
        "wa usa",
        "united states",
      ]),
    });
    expect(plan.queries.map((query) => query.query)).toEqual(
      expect.arrayContaining([
        "site:boards.greenhouse.io software engineer seattle wa",
        "site:job-boards.greenhouse.io software engineer seattle wa",
        "site:boards.greenhouse.io software developer seattle wa",
        "site:job-boards.greenhouse.io software developer seattle wa",
      ]),
    );
  });

  it("prefers Bing RSS first, skips DuckDuckGo when Bing already returns enough Greenhouse matches, and keeps queries broad", async () => {
    const requests: string[] = [];
    const bingRss = `<?xml version="1.0" encoding="utf-8" ?>
      <rss version="2.0">
        <channel>
          <item>
            <link>https://boards.greenhouse.io/figma/jobs/5616603004?gh_jid=5616603004</link>
          </item>
          <item>
            <link>https://www.boards.greenhouse.io/voltrondata/jobs/4351155006</link>
          </item>
          <item>
            <link>https://boards.greenhouse.io/chalkinc/jobs/4031707005</link>
          </item>
          <item>
            <link>https://boards.greenhouse.io/datadog/jobs/1234567</link>
          </item>
        </channel>
      </rss>`;
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);

      if (url.startsWith("https://www.bing.com/search")) {
        return new Response(bingRss, {
          status: 200,
          headers: {
            "content-type": "application/rss+xml",
          },
        });
      }

      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await discoverSourcesFromPublicSearch(
      {
        title: "Senior Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
      },
    );

    const requestedQueries = requests.map((requestUrl) =>
      new URL(requestUrl).searchParams.get("q"),
    );

    expect(sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "greenhouse",
          token: "figma",
          discoveryMethod: "future_search",
        }),
        expect.objectContaining({
          platform: "greenhouse",
          token: "voltrondata",
          discoveryMethod: "future_search",
        }),
      ]),
    );
    expect(requestedQueries).toEqual(
      expect.arrayContaining([
        "site:boards.greenhouse.io software engineer",
        "site:job-boards.greenhouse.io software engineer",
      ]),
    );
    expect(requests.some((requestUrl) => requestUrl.startsWith("https://html.duckduckgo.com/"))).toBe(false);
    expect(requestedQueries.some((query) => query?.includes("United States"))).toBe(false);
    expect(requestedQueries.some((query) => query?.includes("\""))).toBe(false);
  });

  it("respects the configured query budget, returns more than 24 sources when allowed, and reports funnel diagnostics", async () => {
    let queryIndex = 0;
    const requestedQueries: string[] = [];
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const requestUrl = String(input);
      const query = new URL(requestUrl).searchParams.get("q") ?? "";
      if (requestUrl.startsWith("https://html.duckduckgo.com/")) {
        requestedQueries.push(query);
      }

      if (requestUrl.startsWith("https://www.bing.com/search")) {
        return new Response(
          `<?xml version="1.0" encoding="utf-8" ?><rss version="2.0"><channel></channel></rss>`,
          {
            status: 200,
            headers: {
              "content-type": "application/rss+xml",
            },
          },
        );
      }

      const html = `
        <html>
          <body>
            ${Array.from({ length: 4 }, (_, offset) => {
              const token = `acme${queryIndex}${offset}`;
              return `<a href="https://boards.greenhouse.io/${token}/jobs/${queryIndex}${offset}">Job</a>`;
            }).join("")}
          </body>
        </html>
      `;
      queryIndex += 1;

      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        maxResultsPerQuery: 4,
        maxQueries: 8,
        maxSources: 40,
      },
    );
    const result = await discoverSourcesFromPublicSearchDetailed(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        fetchImpl,
        maxResultsPerQuery: 4,
        maxQueries: 8,
        maxSources: 40,
      },
    );

    expect(result.sources).toHaveLength(32);
    expect(result.diagnostics).toMatchObject({
      generatedQueries: plan.queries.length,
      executedQueries: 8,
      skippedQueries: plan.queries.length - 8,
      maxQueries: 8,
      maxSources: 40,
      maxResultsPerQuery: 4,
      rawResultsHarvested: 32,
      platformMatchedUrls: 32,
      sourcesAdded: 32,
    });
    expect(result.diagnostics.dropReasonCounts.query_budget).toBe(plan.queries.length - 8);
    expect(requestedQueries).toEqual([
      "site:boards.greenhouse.io software engineer",
      "site:boards.greenhouse.io software developer",
      "site:boards.greenhouse.io software development engineer",
      "site:boards.greenhouse.io backend engineer",
      "site:job-boards.greenhouse.io software engineer",
      "site:job-boards.greenhouse.io software developer",
      "site:job-boards.greenhouse.io software development engineer",
      "site:job-boards.greenhouse.io backend engineer",
    ]);
  });

  it("builds United States discovery tokens from country aliases, state aliases, metros, and remote variants", () => {
    const plan = buildUsDiscoveryLocationTokens({
      country: "US",
    });

    expect(plan.intent).toEqual({
      kind: "broad_us",
    });
    expect(plan.tokens.map((token) => token.value)).toEqual(
      expect.arrayContaining([
        "united states",
        "usa",
        "us",
        "remote united states",
        "remote usa",
        "remote us",
        "remote california",
        "california",
        "ca usa",
        "texas",
        "tx usa",
        "seattle wa",
        "seattle washington",
      ]),
    );
  });

  it("applies broad US discovery clauses to Lever, Ashby, and Workday too so board discovery is not Greenhouse-only", () => {
    const plan = buildPublicSearchQueryPlan(
      {
        title: "Software Engineer",
        country: "United States",
      },
      {
        maxResultsPerQuery: 4,
      },
    );

    expect(
      plan.platformPlans.find((platformPlan) => platformPlan.platform === "lever"),
    ).toEqual(
      expect.objectContaining({
        locationIntent: expect.objectContaining({
          kind: "broad_us",
        }),
        locationClauses: expect.arrayContaining([
          "",
          "united states",
          "usa",
          "remote us",
          "remote california",
          "california",
        ]),
      }),
    );
    expect(
      plan.platformPlans.find((platformPlan) => platformPlan.platform === "ashby"),
    ).toEqual(
      expect.objectContaining({
        locationIntent: expect.objectContaining({
          kind: "broad_us",
        }),
        locationClauses: expect.arrayContaining([
          "",
          "united states",
          "usa",
          "remote us",
          "remote texas",
          "texas",
        ]),
      }),
    );
    expect(
      plan.platformPlans.find((platformPlan) => platformPlan.platform === "workday"),
    ).toEqual(
      expect.objectContaining({
        locationIntent: expect.objectContaining({
          kind: "broad_us",
        }),
        locationClauses: expect.arrayContaining([
          "",
          "united states",
          "usa",
          "remote us",
          "remote california",
          "california",
        ]),
      }),
    );
  });

  it("returns non-zero Greenhouse registry sources even when public search is disabled", async () => {
    const sources = await discoverSources({
      filters: {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      now: new Date("2026-04-08T00:00:00.000Z"),
      fetchImpl: (async () =>
        new Response("", {
          status: 500,
        })) as unknown as typeof fetch,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai", "benchling", "datadog"],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
      },
    });

    expect(sources.length).toBeGreaterThan(0);
    expect(sources.every((source) => source.platform === "greenhouse")).toBe(true);
    expect(sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "greenhouse",
          token: "openai",
          discoveryMethod: "platform_registry",
        }),
        expect.objectContaining({
          platform: "greenhouse",
          token: "benchling",
          discoveryMethod: "platform_registry",
        }),
      ]),
    );
  });

  it("keeps Greenhouse fast mode active even when registry coverage already exists", async () => {
    const fastFetchImpl = vi.fn(async () => {
      return new Response("<html></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;
    const balancedFetchImpl = vi.fn(async () => {
      return new Response(
        `
          <html>
            <body>
              <a href="https://job-boards.greenhouse.io/datadog/jobs/789">Datadog</a>
            </body>
          </html>
        `,
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      );
    }) as unknown as typeof fetch;

    const fastResult = await discoverSourcesDetailed({
      filters: {
        title: "Software Engineer",
        platforms: ["greenhouse"],
        crawlMode: "fast",
      },
      now: new Date("2026-04-12T00:00:00.000Z"),
      fetchImpl: fastFetchImpl,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai", "benchling"],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
      },
    });

    const balancedResult = await discoverSourcesDetailed({
      filters: {
        title: "Software Engineer",
        platforms: ["greenhouse"],
        crawlMode: "balanced",
      },
      now: new Date("2026-04-12T00:00:00.000Z"),
      fetchImpl: balancedFetchImpl,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai", "benchling"],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
      },
    });

    expect(fastFetchImpl).toHaveBeenCalled();
    expect(fastResult.diagnostics.publicSearchSkippedReason).toBeUndefined();
    expect(fastResult.diagnostics.publicSearch).toMatchObject({
      executedQueries: expect.any(Number),
      maxQueries: 12,
      roleQueryCount: 20,
    });
    expect(fastResult.sources.map((source) => source.token)).toEqual(
      expect.arrayContaining(["openai", "benchling"]),
    );
    expect(balancedFetchImpl).toHaveBeenCalled();
    expect(balancedResult.diagnostics.publicSearch).toMatchObject({
      executedQueries: expect.any(Number),
      maxQueries: 24,
      roleQueryCount: 20,
    });
  });

  it("merges registry-backed Greenhouse sources with public search additions without duplicates", async () => {
    const html = `
      <html>
        <body>
          <a href="https://boards.greenhouse.io/openai/jobs/123">OpenAI</a>
          <a href="https://boards.greenhouse.io/benchling/jobs/456">Benchling</a>
          <a href="https://boards.greenhouse.io/datadog/jobs/789">Datadog</a>
        </body>
      </html>
    `;
    const fetchImpl = (async () =>
      new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      })) as unknown as typeof fetch;

    const sources = await discoverSources({
      filters: {
        title: "Software Engineer",
        platforms: ["greenhouse"],
      },
      now: new Date("2026-04-08T00:00:00.000Z"),
      fetchImpl,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai", "benchling"],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: true,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
      },
    });

    expect(sources.filter((source) => source.platform === "greenhouse")).toHaveLength(3);
    expect(sources.map((source) => source.token)).toEqual(
      expect.arrayContaining(["openai", "benchling", "datadog"]),
    );
  });

  it("does not add legacy greenhouse catalog sources on top of the registry path", () => {
    const greenhouseOnly = discoverCatalogSources(["greenhouse"]);

    expect(greenhouseOnly).toEqual([]);
  });

  it("returns zero sources only when the registry is empty and public search also returns nothing", async () => {
    const sources = await discoverSources({
      filters: {
        title: "Software Engineer",
        platforms: ["greenhouse"],
      },
      now: new Date("2026-04-08T00:00:00.000Z"),
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 4,
      },
    });

    expect(sources).toEqual([]);
  });
});

describe("discovery budget capping per mode", () => {
  const makeEnv = (overrides: Partial<typeof discoveryEnvDefaults> = {}) => ({
    ...discoveryEnvDefaults,
    ...overrides,
  });

  it("caps fast mode to 12 queries and 30 sources", () => {
    const options = resolvePublicSearchExecutionOptions(
      "fast",
      makeEnv(),
      undefined,
    );

    expect(options.maxQueries).toBe(12);
    expect(options.maxSources).toBe(30);
    expect(options.maxLocationClauses).toBe(4);
    expect(options.maxDirectJobs).toBe(8);
    expect(options.maxRoleQueries).toBe(8);
  });

  it("caps fast mode with greenhouse-only platform to 12 queries and 6 direct jobs", () => {
    const options = resolvePublicSearchExecutionOptions(
      "fast",
      makeEnv(),
      ["greenhouse"],
    );

    expect(options.maxQueries).toBe(12);
    expect(options.maxSources).toBe(30);
    expect(options.maxLocationClauses).toBe(4);
    expect(options.maxDirectJobs).toBe(6);
    expect(options.maxRoleQueries).toBe(8);
  });

  it("caps balanced mode to 24 queries and 50 sources", () => {
    const options = resolvePublicSearchExecutionOptions(
      "balanced",
      makeEnv(),
      undefined,
    );

    expect(options.maxQueries).toBe(24);
    expect(options.maxSources).toBe(50);
    expect(options.maxLocationClauses).toBe(8);
    expect(options.maxDirectJobs).toBe(12);
    expect(options.maxRoleQueries).toBe(12);
  });

  it("keeps deep mode at full defaults (96 queries, 120 sources)", () => {
    const options = resolvePublicSearchExecutionOptions(
      "deep",
      makeEnv(),
      undefined,
    );

    expect(options.maxQueries).toBe(96);
    expect(options.maxSources).toBe(120);
    expect(options.maxLocationClauses).toBe(32);
    expect(options.maxDirectJobs).toBe(24);
    expect(options.maxRoleQueries).toBe(18);
  });

  it("uses env overrides when env values are lower than mode caps", () => {
    const options = resolvePublicSearchExecutionOptions(
      "fast",
      makeEnv({
        PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: 8,
        PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: 20,
        GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: 2,
      }),
      undefined,
    );

    expect(options.maxQueries).toBe(8);
    expect(options.maxSources).toBe(20);
    expect(options.maxLocationClauses).toBe(2);
  });
});

describe("source capping with platform diversity", () => {
  const makeSource = (platform: string, id: string): DiscoveredSource =>
    ({
      id,
      platform,
      url: `https://example.com/${id}`,
      confidence: "high",
      discoveryMethod: "future_search",
    }) as DiscoveredSource;

  it("returns all sources when under the cap", () => {
    const sources = [
      makeSource("greenhouse", "gh-1"),
      makeSource("lever", "lev-1"),
    ];

    const result = capSourcesWithPlatformDiversity(sources, 10);

    expect(result).toEqual(sources);
    expect(result.length).toBe(2);
  });

  it("ensures at least 1 source per platform before filling remaining slots", () => {
    const sources = [
      makeSource("greenhouse", "gh-1"),
      makeSource("greenhouse", "gh-2"),
      makeSource("greenhouse", "gh-3"),
      makeSource("lever", "lev-1"),
      makeSource("lever", "lev-2"),
      makeSource("ashby", "ash-1"),
    ];

    const result = capSourcesWithPlatformDiversity(sources, 4);

    // Should have at least 1 from each of 3 platforms = 3, then 1 more from round-robin
    expect(result.length).toBe(4);
    const platforms = result.map((s) => s.platform);
    expect(platforms).toContain("greenhouse");
    expect(platforms).toContain("lever");
    expect(platforms).toContain("ashby");
  });

  it("distributes sources round-robin style across platforms", () => {
    const sources = [
      makeSource("greenhouse", "gh-1"),
      makeSource("greenhouse", "gh-2"),
      makeSource("greenhouse", "gh-3"),
      makeSource("greenhouse", "gh-4"),
      makeSource("lever", "lev-1"),
      makeSource("lever", "lev-2"),
      makeSource("lever", "lev-3"),
      makeSource("ashby", "ash-1"),
    ];

    const result = capSourcesWithPlatformDiversity(sources, 6);

    expect(result.length).toBe(6);
    const platforms = result.map((s) => s.platform);
    // Phase 1: 1 from each platform (3 total)
    // Phase 2: 3 more slots filled round-robin: gh-2, lev-2, ashby has no more, so gh-3
    expect(platforms.filter((p) => p === "greenhouse").length).toBeGreaterThanOrEqual(2);
    expect(platforms.filter((p) => p === "lever").length).toBeGreaterThanOrEqual(1);
    expect(platforms.filter((p) => p === "ashby").length).toBeGreaterThanOrEqual(1);
  });

  it("respects hard cap even when platforms have many sources", () => {
    const sources = Array.from({ length: 50 }, (_, i) =>
      makeSource(i % 3 === 0 ? "greenhouse" : i % 3 === 1 ? "lever" : "ashby", `src-${i}`),
    );

    const result = capSourcesWithPlatformDiversity(sources, 10);

    expect(result.length).toBe(10);
    const platforms = new Set(result.map((s) => s.platform));
    expect(platforms.size).toBe(3); // All 3 platforms represented
  });

  it("handles single-platform sources gracefully", () => {
    const sources = Array.from({ length: 20 }, (_, i) =>
      makeSource("greenhouse", `gh-${i}`),
    );

    const result = capSourcesWithPlatformDiversity(sources, 5);

    expect(result.length).toBe(5);
    expect(result.every((s) => s.platform === "greenhouse")).toBe(true);
  });

  it("returns empty array for empty input", () => {
    const result = capSourcesWithPlatformDiversity([], 10);
    expect(result).toEqual([]);
  });

  it("refreshes a persistent source inventory with Greenhouse-priority coverage without dropping Lever or Ashby", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());

    const inventory = await refreshSourceInventory({
      repository,
      now: new Date("2026-04-14T00:00:00.000Z"),
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai", "datadog"],
        leverSiteTokens: ["figma", "plaid"],
        ashbyBoardTokens: ["notion", "ramp"],
      },
    });

    expect(inventory.length).toBeGreaterThanOrEqual(20);
    expect(inventory[0]).toMatchObject({
      platform: "greenhouse",
      inventoryOrigin: "greenhouse_registry",
    });
    expect(inventory.some((record) => record.platform === "lever" && record.token === "figma")).toBe(
      true,
    );
    expect(inventory.some((record) => record.platform === "ashby" && record.token === "notion")).toBe(
      true,
    );
  });

  it("refreshes existing inventory records and grows inventory from company-page SmartRecruiters expansion", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await repository.upsertSourceInventory([
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: "https://careers.acme.com/jobs",
          companyHint: "Acme",
          pageType: "html_page",
          confidence: "medium",
          discoveryMethod: "manual_config",
        }),
        {
          now: "2026-04-10T00:00:00.000Z",
          inventoryOrigin: "manual_config",
          inventoryRank: 0,
        },
      ),
    ]);

    const inventory = await refreshSourceInventory({
      repository,
      now: new Date("2026-04-15T00:00:00.000Z"),
      fetchImpl: vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url !== "https://careers.acme.com/jobs") {
          throw new Error(`Unexpected fetch: ${url}`);
        }

        return new Response(
          `
            <html>
              <body>
                <a href="https://careers.smartrecruiters.com/Acme">Careers</a>
              </body>
            </html>
          `,
          {
            status: 200,
            headers: {
              "content-type": "text/html",
            },
          },
        );
      }) as unknown as typeof fetch,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        companyPageSources: [],
      },
    });

    expect(inventory.some((record) => record.platform === "company_page")).toBe(true);
    expect(inventory.some((record) => record.platform === "smartrecruiters")).toBe(true);
    expect(
      inventory.find((record) => record.platform === "company_page" && record.url === "https://careers.acme.com/jobs"),
    ).toMatchObject({
      lastRefreshedAt: "2026-04-15T00:00:00.000Z",
    });
    expect(
      inventory.find((record) => record.platform === "smartrecruiters" && record.token === "Acme"),
    ).toMatchObject({
      inventoryOrigin: "public_search",
      lastSeenAt: "2026-04-15T00:00:00.000Z",
    });
  });

  it("uses persistent source inventory in the baseline stage before falling back to configured seeds", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await refreshSourceInventory({
      repository,
      now: new Date("2026-04-14T00:00:00.000Z"),
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: ["openai"],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
      },
    });

    const baseline = await discoverBaselineSourcesDetailed({
      filters: {
        title: "Software Engineer",
        platforms: ["greenhouse"],
      },
      now: new Date("2026-04-14T00:00:00.000Z"),
      repository,
      env: {
        ...discoveryEnvDefaults,
        greenhouseBoardTokens: [],
        leverSiteTokens: [],
        ashbyBoardTokens: [],
        PUBLIC_SEARCH_DISCOVERY_ENABLED: false,
      },
    });

    expect(baseline.diagnostics).toMatchObject({
      inventorySources: expect.any(Number),
      configuredSources: 0,
    });
    expect(baseline.sources.some((source) => source.discoveryMethod === "source_inventory")).toBe(
      true,
    );
    expect(baseline.sources.some((source) => source.platform === "greenhouse")).toBe(true);
    expect(
      baseline.sources.some(
        (source) =>
          source.platform === "greenhouse" && source.discoveryMethod !== "source_inventory",
      ),
    ).toBe(false);
  });
});
