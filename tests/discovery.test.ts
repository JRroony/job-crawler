import { describe, expect, it, vi } from "vitest";

import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { getDefaultGreenhouseRegistryEntries } from "@/lib/server/discovery/greenhouse-registry";
import { discoverSourcesFromPublicSearch } from "@/lib/server/discovery/public-search";
import { discoverConfiguredSources, discoverSources } from "@/lib/server/discovery/service";

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

  it("ships a materially larger default Greenhouse registry than the tiny initial seed set", () => {
    const entries = getDefaultGreenhouseRegistryEntries();

    expect(entries.length).toBeGreaterThanOrEqual(20);
    expect(entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ token: "figma", companyHint: "Figma" }),
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

  it("discovers configured env and manual sources into typed discovered sources", () => {
    const sources = discoverConfiguredSources({
      filters: {
        title: "Software Engineer",
      },
      now: new Date("2026-04-07T00:00:00.000Z"),
      env: {
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
        PUBLIC_SEARCH_DISCOVERY_ENABLED: true,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 8,
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
        PUBLIC_SEARCH_DISCOVERY_ENABLED: true,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: 8,
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

  it("falls back from challenged DuckDuckGo HTML to Bing RSS and keeps greenhouse discovery queries broad", async () => {
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
        </channel>
      </rss>`;
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);

      if (url.startsWith("https://html.duckduckgo.com/")) {
        return new Response(
          `
            <html>
              <body>
                <form id="challenge-form"></form>
                <div>Unfortunately, bots use DuckDuckGo too.</div>
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
    expect(requestedQueries.some((query) => query?.includes("United States"))).toBe(false);
    expect(requestedQueries.some((query) => query?.includes("\""))).toBe(false);
    expect(requestedQueries.some((query) => query?.includes("Senior"))).toBe(false);
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
