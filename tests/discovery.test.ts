import { describe, expect, it } from "vitest";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { discoverSourcesFromPublicSearch } from "@/lib/server/discovery/public-search";
import { discoverConfiguredSources } from "@/lib/server/discovery/service";

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
          discoveryMethod: "configured_env",
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
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fjob-boards.greenhouse.io%2Fwidgetco%2Fjobs%2F456">WidgetCo</a>
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
          token: "widgetco",
          discoveryMethod: "future_search",
        }),
      ]),
    );
    expect(sources.every((source) => source.platform === "greenhouse")).toBe(true);
  });
});
