import { describe, expect, it } from "vitest";

import { normalizeAshbyCandidate } from "@/lib/server/providers/ashby";
import { normalizeGreenhouseJob } from "@/lib/server/providers/greenhouse";
import { normalizeLeverJob } from "@/lib/server/providers/lever";

describe("provider normalization", () => {
  it("normalizes greenhouse jobs into the common model", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "openai",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 123,
        title: "Senior Software Engineer",
        absolute_url: "https://boards.greenhouse.io/openai/jobs/123",
        first_published: "2026-03-20T00:00:00.000Z",
        location: { name: "San Francisco, California, United States" },
      },
    });

    expect(job.sourcePlatform).toBe("greenhouse");
    expect(job.company).toBe("Openai");
    expect(job.city).toBe("San Francisco");
    expect(job.experienceLevel).toBe("senior");
    expect(job.experienceClassification).toMatchObject({
      explicitLevel: "senior",
      confidence: "high",
      source: "title",
    });
  });

  it("normalizes lever jobs into the common model", () => {
    const job = normalizeLeverJob({
      siteToken: "figma",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: "abc",
        text: "Junior Product Designer",
        country: "US",
        hostedUrl: "https://jobs.lever.co/figma/abc",
        applyUrl: "https://jobs.lever.co/figma/abc/apply",
        createdAt: 1_774_745_600_000,
        categories: {
          location: "New York, New York, United States",
        },
      },
    });

    expect(job.sourcePlatform).toBe("lever");
    expect(job.applyUrl).toContain("/apply");
    expect(job.city).toBe("New York");
    expect(job.country).toBe("US");
    expect(job.experienceLevel).toBe("junior");
    expect(job.experienceClassification).toMatchObject({
      explicitLevel: "junior",
      source: "title",
    });
  });

  it("uses Lever description content to classify generic titles", () => {
    const job = normalizeLeverJob({
      siteToken: "figma",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: "senior-role",
        text: "Software Engineer",
        hostedUrl: "https://jobs.lever.co/figma/senior-role",
        applyUrl: "https://jobs.lever.co/figma/senior-role/apply",
        descriptionPlain:
          "Minimum qualifications: 5+ years of experience building backend systems.",
        categories: {
          location: "Remote, United States",
        },
      },
    });

    expect(job.experienceLevel).toBe("senior");
    expect(job.experienceClassification).toMatchObject({
      inferredLevel: "senior",
      confidence: "medium",
      source: "description",
    });
  });

  it("normalizes ashby candidates into the common model", () => {
    const job = normalizeAshbyCandidate({
      companyToken: "notion",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      candidate: {
        id: "role-1",
        title: "Staff Data Engineer",
        locationName: "Remote, United States",
        jobUrl: "https://jobs.ashbyhq.com/notion/role-1",
        publishedAt: "2026-03-10T00:00:00.000Z",
      },
    });

    expect(job.sourcePlatform).toBe("ashby");
    expect(job.company).toBe("Notion");
    expect(job.locationText).toBe("Remote, United States");
    expect(job.experienceLevel).toBe("staff");
    expect(job.experienceClassification).toMatchObject({
      explicitLevel: "staff",
      source: "title",
    });
  });

  it("uses Ashby description content to classify generic titles", () => {
    const job = normalizeAshbyCandidate({
      companyToken: "notion",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      candidate: {
        id: "role-2",
        title: "Software Engineer",
        locationName: "Remote, United States",
        jobUrl: "https://jobs.ashbyhq.com/notion/role-2",
        descriptionHtml:
          "<p>This entry-level role is designed for recent graduates joining our product engineering team.</p>",
      },
    });

    expect(job.experienceLevel).toBe("new_grad");
    expect(job.experienceClassification).toMatchObject({
      inferredLevel: "new_grad",
      confidence: "medium",
      source: "description",
    });
  });

  it("normalizes internship roles into the intern level", () => {
    const job = normalizeLeverJob({
      siteToken: "figma",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: "intern-role",
        text: "Software Engineering Intern",
        country: "US",
        hostedUrl: "https://jobs.lever.co/figma/intern-role",
        applyUrl: "https://jobs.lever.co/figma/intern-role/apply",
        categories: {
          location: "San Francisco, California, United States",
          commitment: "Internship",
        },
      },
    });

    expect(job.sourcePlatform).toBe("lever");
    expect(job.country).toBe("US");
    expect(job.experienceLevel).toBe("intern");
    expect(job.experienceClassification).toMatchObject({
      explicitLevel: "intern",
      source: "structured_metadata",
    });
  });

  it("uses Greenhouse metadata and content clues to normalize generic software internships", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "stripe",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 456,
        title: "Software Engineer",
        absolute_url: "https://boards.greenhouse.io/stripe/jobs/456",
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
    });

    expect(job.sourcePlatform).toBe("greenhouse");
    expect(job.experienceLevel).toBe("intern");
    expect(job.locationText).toBe("Seattle");
    expect(job.experienceClassification).toMatchObject({
      inferredLevel: "intern",
      source: "structured_metadata",
    });
  });

  it("uses Greenhouse description content to classify generic senior roles", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "stripe",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 654,
        title: "Software Engineer",
        absolute_url: "https://boards.greenhouse.io/stripe/jobs/654",
        first_published: "2026-03-20T00:00:00.000Z",
        location: { name: "Seattle" },
        content:
          "&lt;p&gt;Minimum qualifications: 5+ years of experience building distributed systems.&lt;/p&gt;",
      },
    });

    expect(job.experienceLevel).toBe("senior");
    expect(job.experienceClassification).toMatchObject({
      inferredLevel: "senior",
      source: "description",
    });
  });

  it("uses Greenhouse office data when the top-level location is missing", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "stripe",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 789,
        title: "Software Engineer, Intern",
        absolute_url: "https://boards.greenhouse.io/stripe/jobs/789",
        first_published: "2026-03-20T00:00:00.000Z",
        offices: [
          {
            name: "US",
            location: { name: "Austin, Texas" },
          },
        ],
      },
    });

    expect(job.locationText).toBe("Austin, Texas, US");
    expect(job.city).toBe("Austin");
    expect(job.state).toBe("Texas");
    expect(job.country).toBe("US");
  });

  it("does not classify disclaimer-only Greenhouse content as an internship", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "stripe",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 987,
        title: "Software Engineer",
        absolute_url: "https://boards.greenhouse.io/stripe/jobs/987",
        first_published: "2026-03-20T00:00:00.000Z",
        location: { name: "San Francisco, CA" },
        content:
          "&lt;p&gt;Note: if you are an intern, new grad, or staff applicant, please do not apply using this link.&lt;/p&gt;",
      },
    });

    expect(job.experienceLevel).toBeUndefined();
    expect(job.experienceClassification).toEqual({
      confidence: "none",
      source: "unknown",
      reasons: [],
      isUnspecified: true,
    });
  });
});
