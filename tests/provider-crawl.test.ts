import { describe, expect, it, vi } from "vitest";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { createAshbyProvider } from "@/lib/server/providers/ashby";
import { createCompanyPageProvider } from "@/lib/server/providers/company-page";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import { createLeverProvider } from "@/lib/server/providers/lever";
import { createSmartRecruitersProvider } from "@/lib/server/providers/smartrecruiters";
import { createWorkdayProvider } from "@/lib/server/providers/workday";
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

function leverSource(token: string, jobId?: string) {
  return classifySourceCandidate({
    url: jobId ? `https://jobs.lever.co/${token}/${jobId}` : `https://jobs.lever.co/${token}`,
    token,
    confidence: "high",
    discoveryMethod: "configured_env",
  });
}

function workdaySource(url: string) {
  return classifySourceCandidate({
    url,
    confidence: "high",
    discoveryMethod: "future_search",
  });
}

function smartRecruitersSource(url: string) {
  return classifySourceCandidate({
    url,
    confidence: "high",
    discoveryMethod: "future_search",
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
    expect(result.diagnostics).toMatchObject({
      provider: "greenhouse",
      discoveryCount: 1,
      fetchCount: 1,
      parseSuccessCount: 1,
      parseFailureCount: 0,
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

  it("drops malformed Greenhouse job seeds without failing the provider batch", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "empty-title-role",
              title: "",
              absolute_url: "https://boards.greenhouse.io/openai/jobs/empty-title-role",
              company_name: "OpenAI",
              location: {
                name: "San Francisco, CA",
              },
            },
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

    expect(result.status).toBe("partial");
    expect(result.fetchedCount).toBe(2);
    expect(result.matchedCount).toBe(1);
    expect(result.warningCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer, Backend",
      sourceJobId: "backend-role",
    });
    expect(result.diagnostics).toMatchObject({
      parsedSeedCount: 2,
      validSeedCount: 1,
      invalidSeedCount: 1,
      dropReasonCounts: {
        seed_invalid_empty_title: 1,
      },
      sampleInvalidSeeds: [
        expect.objectContaining({
          provider: "greenhouse",
          sourceJobId: "empty-title-role",
          company: "OpenAI",
          rawTitle: "",
          applyUrl: "https://boards.greenhouse.io/openai/jobs/empty-title-role",
          reason: "seed_invalid_empty_title",
        }),
      ],
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
    expect(result.diagnostics).toMatchObject({
      provider: "ashby",
      discoveryCount: 1,
      fetchCount: 1,
      parseSuccessCount: 1,
      parseFailureCount: 0,
    });
  });

  it("drops malformed Ashby jobs without failing valid jobs in the same batch", async () => {
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
                    "id": "missing-title",
                    "title": "",
                    "locationName": "Remote, United States"
                  },
                  {
                    "id": "role-1",
                    "title": "Data Analyst",
                    "locationName": "Remote, United States"
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
        title: "Data Analyst",
        country: "United States",
      },
      sources: [ashbySource("notion")],
    });

    expect(result.status).toBe("partial");
    expect(result.jobs.map((job) => job.title)).toEqual(["Data Analyst"]);
    expect(result.diagnostics).toMatchObject({
      parsedSeedCount: 2,
      validSeedCount: 1,
      invalidSeedCount: 1,
      dropReasonCounts: {
        seed_invalid_empty_title: 1,
      },
    });
  });

  it("crawls Lever postings into normalized jobs and preserves hosted URLs", async () => {
    const provider = createLeverProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify([
          {
            id: "role-1",
            text: "Senior Backend Engineer",
            hostedUrl: "https://jobs.lever.co/figma/role-1",
            applyUrl: "https://jobs.lever.co/figma/role-1/apply",
            createdAt: 1773100800000,
            workplaceType: "Hybrid",
            categories: {
              location: "Seattle, WA",
              commitment: "Full-time",
              department: "Engineering",
            },
          },
        ]),
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
        title: "Backend Engineer",
        country: "United States",
      },
      sources: [leverSource("figma")],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Backend Engineer",
      country: "United States",
      state: "Washington",
      city: "Seattle",
      sourcePlatform: "lever",
      sourceUrl: "https://jobs.lever.co/figma/role-1",
      applyUrl: "https://jobs.lever.co/figma/role-1/apply",
    });
    expect(result.diagnostics).toMatchObject({
      provider: "lever",
      discoveryCount: 1,
      fetchCount: 1,
      parseSuccessCount: 1,
      parseFailureCount: 0,
    });
  });

  it("drops malformed Lever postings without failing the provider batch", async () => {
    const provider = createLeverProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify([
          {
            id: "empty-title",
            text: "   ",
            hostedUrl: "https://jobs.lever.co/figma/empty-title",
            applyUrl: "https://jobs.lever.co/figma/empty-title/apply",
            categories: {
              location: "Remote, United States",
            },
          },
          {
            id: "role-1",
            text: "Backend Engineer",
            hostedUrl: "https://jobs.lever.co/figma/role-1",
            applyUrl: "https://jobs.lever.co/figma/role-1/apply",
            categories: {
              location: "Remote, United States",
            },
          },
        ]),
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
        title: "Backend Engineer",
        country: "United States",
      },
      sources: [leverSource("figma")],
    });

    expect(result.status).toBe("partial");
    expect(result.jobs.map((job) => job.sourceJobId)).toEqual(["role-1"]);
    expect(result.diagnostics).toMatchObject({
      parsedSeedCount: 2,
      validSeedCount: 1,
      invalidSeedCount: 1,
      dropReasonCounts: {
        seed_invalid_empty_title: 1,
      },
    });
  });

  it("uses detail fallback for a Lever detail source when the board list is empty", async () => {
    const provider = createLeverProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url === "https://api.lever.co/v0/postings/figma?mode=json") {
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        });
      }

      if (url === "https://api.lever.co/v0/postings/figma/role-1?mode=json") {
        return new Response(
          JSON.stringify({
            id: "role-1",
            text: "Product Analyst",
            hostedUrl: "https://jobs.lever.co/figma/role-1",
            createdAt: 1773100800000,
            categories: {
              location: "Remote US",
              commitment: "Full-time",
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

      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Product Analyst",
        country: "United States",
      },
      sources: [leverSource("figma", "role-1")],
    });

    expect(result.status).toBe("success");
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Product Analyst",
      locationText: "Remote US",
      sourceUrl: "https://jobs.lever.co/figma/role-1",
    });
    expect(result.diagnostics?.dropReasonCounts).toEqual(
      expect.objectContaining({}),
    );
  });

  it("crawls a Workday source through its JSON endpoint into normalized jobs", async () => {
    const provider = createWorkdayProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url === "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/Careers/jobs") {
        return new Response(
          JSON.stringify({
            jobPostings: [
              {
                title: "Senior Data Engineer",
                externalPath: "job/Seattle-WA/Senior-Data-Engineer_R12345",
                locationsText: "Bellevue, WA",
                postedOn: "2026-03-10T00:00:00.000Z",
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

      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Data Engineer",
        country: "United States",
      },
      sources: [workdaySource("https://acme.wd1.myworkdayjobs.com/en-US/Careers")],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Data Engineer",
      country: "United States",
      state: "Washington",
      city: "Bellevue",
      sourcePlatform: "workday",
      sourceUrl:
        "https://acme.wd1.myworkdayjobs.com/en-US/Careers/job/Seattle-WA/Senior-Data-Engineer_R12345",
    });
    expect(result.diagnostics).toMatchObject({
      provider: "workday",
      discoveryCount: 1,
      fetchCount: 1,
      parseSuccessCount: 1,
      parseFailureCount: 0,
    });
  });

  it("drops incomplete Workday rows without failing valid rows in the same batch", async () => {
    const provider = createWorkdayProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url === "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/Careers/jobs") {
        return new Response(
          JSON.stringify({
            jobPostings: [
              {
                externalPath: "job/Austin-TX/Missing-Title_R000",
                locationsText: "Austin, TX",
              },
              {
                title: "Business Analyst",
                externalPath: "job/Austin-TX/Business-Analyst_R123",
                locationsText: "Austin, TX",
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

      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Business Analyst",
        country: "United States",
      },
      sources: [workdaySource("https://acme.wd1.myworkdayjobs.com/en-US/Careers")],
    });

    expect(result.status).toBe("partial");
    expect(result.jobs.map((job) => job.title)).toEqual(["Business Analyst"]);
    expect(result.diagnostics).toMatchObject({
      parsedSeedCount: 2,
      validSeedCount: 1,
      invalidSeedCount: 1,
      dropReasonCounts: {
        seed_invalid_empty_title: 1,
      },
    });
  });

  it("falls back to Workday POST list search when the JSON endpoint rejects GET", async () => {
    const provider = createWorkdayProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);

      if (url === "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/Careers/jobs") {
        if (init?.method === "POST") {
          expect(init.body ? JSON.parse(String(init.body)) : undefined).toMatchObject({
            appliedFacets: {},
            limit: 100,
            offset: 0,
            searchText: "",
          });

          return new Response(
            JSON.stringify({
              jobPostings: [
                {
                  title: "Software Engineer",
                  externalPath: "job/Austin-TX/Software-Engineer_R987",
                  locationsText: "Austin, TX",
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

        return new Response("", { status: 405 });
      }

      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
        country: "United States",
      },
      sources: [workdaySource("https://acme.wd1.myworkdayjobs.com/en-US/Careers")],
    });

    expect(result.status).toBe("success");
    expect(result.diagnostics).toMatchObject({
      fetchCount: 2,
    });
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer",
      country: "United States",
      state: "Texas",
      city: "Austin",
      sourcePlatform: "workday",
    });
  });

  it("crawls a SmartRecruiters board through detail-page extraction into normalized jobs", async () => {
    const provider = createSmartRecruitersProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url === "https://careers.smartrecruiters.com/Acme") {
        return new Response(
          `
            <html>
              <body>
                <a href="https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst">
                  Senior Product Analyst
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
                    "description": "Analyze product signals and support product planning.",
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

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Product Analyst",
        country: "United States",
      },
      sources: [smartRecruitersSource("https://careers.smartrecruiters.com/Acme")],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Senior Product Analyst",
      company: "Acme",
      sourcePlatform: "smartrecruiters",
      sourceUrl: "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
      city: "Austin",
      state: "Texas",
      country: "United States",
    });
    expect(result.diagnostics).toMatchObject({
      provider: "smartrecruiters",
      discoveryCount: 1,
      parseSuccessCount: 1,
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

  it("parses configured company page JSON feeds with nested API records and relative URLs", async () => {
    const provider = createCompanyPageProvider();
    const source = {
      type: "json_feed",
      company: "Globex",
      url: "https://globex.example/careers/api/jobs",
    } satisfies CompanyPageSourceConfig;
    const fetchImpl = vi.fn(async () =>
      new Response(
        JSON.stringify({
          data: {
            postings: [
              {
                requisitionId: "de-berlin",
                postingTitle: "Data Engineer",
                jobPath: "/careers/jobs/de-berlin",
                locations: [
                  {
                    city: "Berlin",
                    country: "Germany",
                  },
                ],
                employmentType: "Full-time",
              },
            ],
          },
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      )) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Data Engineer",
        country: "Germany",
      },
      sources: [companyPageSource(source)],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Data Engineer",
      company: "Globex",
      locationText: "Berlin, Germany",
      sourcePlatform: "company_page",
      sourceUrl: "https://globex.example/careers/jobs/de-berlin",
      sourceJobId: "de-berlin",
      rawSourceMetadata: expect.objectContaining({
        companyPageExtraction: "json_feed",
        companyPageSourceUrl: "https://globex.example/careers/api/jobs",
      }),
    });
  });

  it("parses JSON-LD company career pages", async () => {
    const provider = createCompanyPageProvider();
    const source = {
      type: "json_ld_page",
      company: "Example Labs",
      url: "https://examplelabs.test/careers/software-engineer",
    } satisfies CompanyPageSourceConfig;
    const fetchImpl = vi.fn(async () =>
      new Response(
        `
          <html>
            <head>
              <script type="application/ld+json">
                {
                  "@context": "https://schema.org",
                  "@type": "JobPosting",
                  "title": "Software Engineer",
                  "url": "https://examplelabs.test/careers/software-engineer",
                  "hiringOrganization": { "name": "Example Labs" },
                  "jobLocation": {
                    "@type": "Place",
                    "address": {
                      "addressLocality": "Toronto",
                      "addressRegion": "ON",
                      "addressCountry": "Canada"
                    }
                  }
                }
              </script>
            </head>
          </html>
        `,
        {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        },
      )) as unknown as typeof fetch;

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
        country: "Canada",
      },
      sources: [companyPageSource(source)],
    });

    expect(result.status).toBe("success");
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "Example Labs",
      locationText: "Toronto, ON, Canada",
      sourcePlatform: "company_page",
      rawSourceMetadata: expect.objectContaining({
        companyPageExtraction: "json_ld",
      }),
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

  it("does not flood noisy company pages with false positive anchor jobs", async () => {
    const provider = createCompanyPageProvider();
    const source = {
      type: "html_page",
      company: "Noise Co",
      url: "https://noise.example/careers",
    } satisfies CompanyPageSourceConfig;
    const fetchImpl = vi.fn(async () =>
      new Response(
        `
          <html>
            <body>
              <a href="/careers">View all jobs</a>
              <a href="/careers/benefits">Benefits</a>
              <a href="/blog/engineering-culture">Engineering culture</a>
              <a href="/privacy">Candidate privacy</a>
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

    const result = await crawlProvider(provider, {
      fetchImpl,
      now: new Date("2026-03-30T12:00:00.000Z"),
      filters: {
        title: "Software Engineer",
      },
      sources: [companyPageSource(source)],
    });

    expect(result.status).toBe("success");
    expect(result.fetchedCount).toBe(0);
    expect(result.jobs).toHaveLength(0);
  });

  it("returns provider seeds for the pipeline to filter centrally", async () => {
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
    expect(result.jobs).toHaveLength(2);
    expect(result.jobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: "Backend Engineer",
          locationText: "San Francisco, CA",
          sourceUrl: "https://careers.acme.com/jobs/backend-engineer-sf",
        }),
        expect.objectContaining({
          title: "Backend Engineer",
          locationText: "Toronto",
          sourceUrl: "https://careers.acme.com/jobs/backend-engineer-toronto",
        }),
      ]),
    );
  });
});
