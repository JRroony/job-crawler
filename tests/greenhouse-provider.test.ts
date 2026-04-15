import { describe, expect, it, vi } from "vitest";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";

describe("Greenhouse provider recall", () => {
  it("normalizes the whole Greenhouse board so the pipeline can apply semantic title filtering later", async () => {
    const provider = createGreenhouseProvider();
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
              id: "backend-developer",
              title: "Backend Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/backend-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "frontend-developer",
              title: "Frontend Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/frontend-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "full-stack-developer",
              title: "Full Stack Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/full-stack-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "application-engineer",
              title: "Application Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/application-engineer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "application-developer",
              title: "Application Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/application-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "developer",
              title: "Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "platform-developer",
              title: "Platform Developer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/platform-developer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "qa-engineer",
              title: "QA Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/qa-engineer",
              company_name: "Acme",
              location: { name: "Remote, United States" },
            },
            {
              id: "recruiter",
              title: "Recruiter",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/recruiter",
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

    const result = await provider.crawlSources(
      {
        fetchImpl,
        now: new Date("2026-04-12T12:00:00.000Z"),
        filters: {
          title: "Software Engineer",
          crawlMode: "balanced",
        },
      },
      [
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/acme",
          token: "acme",
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
      ],
    );

    expect(result).toMatchObject({
      provider: "greenhouse",
      status: "success",
      fetchedCount: 10,
      matchedCount: 10,
    });
    expect(result.jobs.map((job) => job.title)).toEqual([
      "Software Development Engineer",
      "Backend Developer",
      "Frontend Developer",
      "Full Stack Developer",
      "Application Engineer",
      "Application Developer",
      "Developer",
      "Platform Developer",
      "QA Engineer",
      "Recruiter",
    ]);
  });

  it("does not drop a large board when raw-title preselection would return zero matches", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: Array.from({ length: 14 }, (_, index) => ({
            id: `product-${index + 1}`,
            title:
              index % 2 === 0
                ? `Associate Product Manager ${index + 1}`
                : `Growth Product Manager ${index + 1}`,
            absolute_url: `https://boards.greenhouse.io/acme/jobs/product-${index + 1}`,
            company_name: "Acme",
            location: { name: "Remote, United States" },
          })),
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await provider.crawlSources(
      {
        fetchImpl,
        now: new Date("2026-04-12T12:00:00.000Z"),
        filters: {
          title: "Software Engineer",
          crawlMode: "balanced",
        },
      },
      [
        classifySourceCandidate({
          url: "https://boards.greenhouse.io/acme",
          token: "acme",
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
      ],
    );

    expect(result).toMatchObject({
      provider: "greenhouse",
      status: "success",
      fetchedCount: 14,
      matchedCount: 14,
    });
    expect(result.jobs).toHaveLength(14);
    expect(result.diagnostics?.dropReasonCounts).not.toHaveProperty("title_preselection_filtered");
    expect(result.jobs[0]?.title).toContain("Product Manager");
  });

  it("still uses detail fallback when the board payload is empty", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/jobs/12345")) {
        return new Response(
          JSON.stringify({
            job: {
              id: "12345",
              title: "Backend Engineer",
              absolute_url: "https://boards.greenhouse.io/acme/jobs/12345",
              company_name: "Acme",
              location: { name: "Remote, United States" },
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

      return new Response(
        JSON.stringify({ jobs: [] }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await provider.crawlSources(
      {
        fetchImpl,
        now: new Date("2026-04-12T12:00:00.000Z"),
        filters: {
          title: "Backend Engineer",
          crawlMode: "balanced",
        },
      },
      [
        classifySourceCandidate({
          url: "https://job-boards.greenhouse.io/acme/jobs/12345",
          token: "acme",
          confidence: "high",
          discoveryMethod: "future_search",
        }),
      ],
    );

    expect(result).toMatchObject({
      provider: "greenhouse",
      status: "success",
      fetchedCount: 1,
      matchedCount: 1,
    });
    expect(result.jobs.map((job) => job.title)).toEqual(["Backend Engineer"]);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });
});
