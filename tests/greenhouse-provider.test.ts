import { describe, expect, it, vi } from "vitest";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";

describe("Greenhouse provider preselection", () => {
  it("keeps realistic sibling software roles while skipping obvious raw-title misses before normalization", async () => {
    const provider = createGreenhouseProvider();
    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
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
      fetchedCount: 5,
      matchedCount: 3,
    });
    expect(result.jobs.map((job) => job.title)).toEqual([
      "Backend Developer",
      "Frontend Developer",
      "Full Stack Developer",
    ]);
  });
});
