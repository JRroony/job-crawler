import { describe, expect, it, vi } from "vitest";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { createAshbyProvider } from "@/lib/server/providers/ashby";
import { createCompanyPageProvider } from "@/lib/server/providers/company-page";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import type { CrawlProvider, ProviderExecutionContext } from "@/lib/server/providers/types";
import type { CompanyPageSourceConfig } from "@/lib/types";

function greenhouseSource(token: string) {
  return classifySourceCandidate({
    url: `https://boards.greenhouse.io/${token}`,
    token,
    confidence: "high",
    discoveryMethod: "configured_env",
  });
}

function ashbySource(token: string) {
  return classifySourceCandidate({
    url: `https://jobs.ashbyhq.com/${token}`,
    token,
    confidence: "high",
    discoveryMethod: "configured_env",
  });
}

function companyPageSource(source: CompanyPageSourceConfig) {
  return classifySourceCandidate({
    url: source.url,
    companyHint: source.company,
    pageType: source.type,
    confidence: source.type === "json_feed" ? "high" : "medium",
    discoveryMethod: "manual_config",
  });
}

async function crawlProvider(
  provider: CrawlProvider,
  input: ProviderExecutionContext & { sources: DiscoveredSource[] },
) {
  return provider.crawlSources(
    {
      fetchImpl: input.fetchImpl,
      now: input.now,
      filters: input.filters,
    },
    input.sources.filter((source) => provider.supportsSource(source)),
  );
}

describe("provider crawl status and live parsing", () => {
  it("crawls a Greenhouse boards-api response into normalized jobs", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "backend-role",
              title: "Software Engineer, Backend",
              absolute_url: "https://boards.greenhouse.io/openai/jobs/backend-role",
              first_published: "2026-03-10T00:00:00.000Z",
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
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [greenhouseSource("openai")],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer, Backend",
      company: "OpenAI",
      country: "United States",
      state: "California",
      city: "San Francisco",
      sourcePlatform: "greenhouse",
      sourceUrl: "https://boards.greenhouse.io/openai/jobs/backend-role",
    });
  });

  it("ignores non-string Greenhouse metadata values instead of failing the provider crawl", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "metadata-role",
              title: "Software Engineer",
              absolute_url: "https://boards.greenhouse.io/openai/jobs/metadata-role",
              first_published: "2026-03-10T00:00:00.000Z",
              company_name: "OpenAI",
              location: {
                name: "San Francisco, CA",
              },
              metadata: [
                {
                  name: "Location",
                  value: ["San Francisco, CA"],
                },
                {
                  name: "Program",
                  value: {
                    label: "Early Career",
                  },
                },
              ],
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

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [greenhouseSource("openai")],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "OpenAI",
      country: "United States",
      state: "California",
      city: "San Francisco",
      sourcePlatform: "greenhouse",
    });
  });

  it("marks a Greenhouse crawl as failed when every configured board fetch fails", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      throw new Error("Network unavailable");
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [
        greenhouseSource("openai"),
        greenhouseSource("stripe"),
        greenhouseSource("coinbase"),
      ],
    });

    expect(result.status).toBe("failed");
    expect(result.jobs).toHaveLength(0);
    expect(result.fetchedCount).toBe(0);
    expect(result.errorMessage).toContain("Network unavailable");
  });

  it("marks a Greenhouse crawl as partial when some boards fail but one still returns jobs", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (!url.includes("/stripe/")) {
        throw new Error("Board unavailable");
      }

      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "intern-role",
              title: "Software Engineer, Intern",
              absolute_url: "https://example.com/intern-role",
              first_published: "2026-03-10T00:00:00.000Z",
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
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [
        greenhouseSource("openai"),
        greenhouseSource("stripe"),
        greenhouseSource("coinbase"),
      ],
    });

    expect(result.status).toBe("partial");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer, Intern",
      country: "United States",
      state: "California",
      city: "San Francisco",
      experienceLevel: "intern",
    });
    expect(result.errorMessage).toContain("Board unavailable");
  });

  it("parses current Ashby app data pages into normalized jobs", async () => {
    const provider = createAshbyProvider();
    const html = `
      <!DOCTYPE html>
      <html lang="en">
        <body>
          <script>
            window.__appData = {
              "jobBoard": {
                "jobPostings": [
                  {
                    "id": "role-1",
                    "title": "Software Engineer, Fullstack, Early Career",
                    "locationName": "San Francisco, California",
                    "employmentType": "FullTime",
                    "publishedDate": "2026-03-10"
                  }
                ]
              }
            };
          </script>
        </body>
      </html>
    `;

    const fetchImpl = vi.fn(async () => {
      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [ashbySource("notion")],
    });

    expect(result.status).toBe("success");
    expect(result.jobs.length).toBeGreaterThan(0);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer, Fullstack, Early Career",
      country: "United States",
      state: "California",
      city: "San Francisco",
      experienceLevel: "new_grad",
      sourcePlatform: "ashby",
      sourceUrl: "https://jobs.ashbyhq.com/notion/role-1",
    });
  });

  it("parses configured json_ld_page sources from embedded public HTML when JSON-LD is absent", async () => {
    const provider = createCompanyPageProvider();
    const source = {
      type: "json_ld_page",
      company: "Acme",
      url: "https://careers.acme.com/jobs",
    } satisfies CompanyPageSourceConfig;
    const html = `
      <!DOCTYPE html>
      <html lang="en">
        <body>
          <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "jobs": [
                    {
                      "id": "role-1",
                      "title": "Senior Platform Engineer",
                      "url": "/jobs/senior-platform-engineer",
                      "location": "Remote, United States",
                      "employmentType": "Full-time",
                      "description": "Minimum qualifications: 5+ years of experience building backend systems."
                    }
                  ]
                }
              }
            }
          </script>
        </body>
      </html>
    `;

    const fetchImpl = vi.fn(async () => {
      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Platform Engineer",
        country: "United States",
      },
      sources: [companyPageSource(source)],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Platform Engineer",
      country: "United States",
      locationText: "Remote, United States",
      experienceLevel: "senior",
      sourcePlatform: "company_page",
      sourceUrl: "https://careers.acme.com/jobs/senior-platform-engineer",
    });
  });

  it("parses configured html_page sources from anchor listings and reports partial failures", async () => {
    const provider = createCompanyPageProvider();
    const sources = [
      {
        type: "html_page",
        company: "Acme",
        url: "https://careers.acme.com/open-roles",
      },
      {
        type: "html_page",
        company: "BrokenCo",
        url: "https://careers.broken.example/jobs",
      },
    ] satisfies CompanyPageSourceConfig[];
    const html = `
      <!DOCTYPE html>
      <html lang="en">
        <body>
          <main>
            <ul>
              <li>
                <a href="/open-roles/senior-security-engineer">
                  <span>Senior Security Engineer</span>
                  <span>Remote, United States</span>
                </a>
              </li>
            </ul>
          </main>
        </body>
      </html>
    `;

    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.includes("broken.example")) {
        return new Response("Unavailable", {
          status: 503,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Security Engineer",
        country: "United States",
      },
      sources: sources.map(companyPageSource),
    });

    expect(result.status).toBe("partial");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Security Engineer",
      locationText: "Remote, United States",
      experienceLevel: "senior",
      sourceUrl: "https://careers.acme.com/open-roles/senior-security-engineer",
    });
    expect(result.errorMessage).toContain("BrokenCo returned 503");
  });

  it("prefilters provider jobs using execution filters before returning seeds", async () => {
    const provider = createCompanyPageProvider();
    const source = {
      type: "json_ld_page",
      company: "Acme",
      url: "https://careers.acme.com/jobs",
    } satisfies CompanyPageSourceConfig;
    const html = `
      <!DOCTYPE html>
      <html lang="en">
        <body>
          <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "jobs": [
                    {
                      "id": "backend-engineer-sf",
                      "title": "Backend Engineer",
                      "url": "/jobs/backend-engineer-sf",
                      "location": "San Francisco, CA"
                    },
                    {
                      "id": "backend-engineer-toronto",
                      "title": "Backend Engineer",
                      "url": "/jobs/backend-engineer-toronto",
                      "location": "Toronto"
                    }
                  ]
                }
              }
            }
          </script>
        </body>
      </html>
    `;

    const fetchImpl = vi.fn(async () => {
      return new Response(html, {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Backend Engineer",
        country: "United States",
      },
      sources: [companyPageSource(source)],
    });

    expect(result.fetchedCount).toBe(2);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Backend Engineer",
      locationText: "San Francisco, CA",
      sourceUrl: "https://careers.acme.com/jobs/backend-engineer-sf",
    });
  });
});
