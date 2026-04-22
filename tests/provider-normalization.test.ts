import { describe, expect, it } from "vitest";

import { normalizeAshbyCandidate } from "@/lib/server/providers/ashby";
import { normalizeGreenhouseJob } from "@/lib/server/providers/greenhouse";
import { normalizeLeverJob } from "@/lib/server/providers/lever";
import { normalizeSmartRecruitersJob } from "@/lib/server/providers/smartrecruiters";
import { buildSeed, normalizeProviderJobSeed } from "@/lib/server/providers/shared";
import { normalizeWorkdayJob } from "@/lib/server/providers/workday";

describe("provider normalization", () => {
  it("builds provider seeds with consistent normalized title aliases", () => {
    const job = buildSeed({
      title: "  Senior Software Engineer  ",
      companyToken: "openai",
      company: "OpenAI",
      locationText: "Remote, United States",
      sourcePlatform: "greenhouse",
      sourceJobId: "alias-role",
      sourceUrl: "https://boards.greenhouse.io/openai/jobs/alias-role",
      applyUrl: "https://boards.greenhouse.io/openai/jobs/alias-role/apply",
      rawSourceMetadata: {},
      discoveredAt: "2026-03-29T00:00:00.000Z",
    });

    expect(job.title).toBe("Senior Software Engineer");
    expect(job.normalizedTitle).toBe("senior software engineer");
    expect(job.titleNormalized).toBe("senior software engineer");
  });

  it("repairs empty normalized title aliases from a non-empty provider title", () => {
    const job = normalizeProviderJobSeed({
      title: "  Backend Engineer  ",
      company: "OpenAI",
      normalizedTitle: "",
      titleNormalized: "",
      locationText: "Remote, United States",
      sourcePlatform: "greenhouse",
      sourceJobId: "backend-role",
      sourceUrl: "https://boards.greenhouse.io/openai/jobs/backend-role",
      applyUrl: "https://boards.greenhouse.io/openai/jobs/backend-role/apply",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      rawSourceMetadata: {},
    });

    expect(job.title).toBe("Backend Engineer");
    expect(job.normalizedTitle).toBe("backend engineer");
    expect(job.titleNormalized).toBe("backend engineer");
  });

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
    expect(job.normalizedCompany).toBe("openai");
    expect(job.normalizedTitle).toBe("senior software engineer");
    expect(job.titleNormalized).toBe("senior software engineer");
    expect(job.locationRaw).toBe("San Francisco, California, United States");
    expect(job.remoteType).toBe("onsite");
    expect(job.seniority).toBe("senior");
    expect(job.sourceCompanySlug).toBe("openai");
    expect(job.city).toBe("San Francisco");
    expect(job.experienceLevel).toBe("senior");
    expect(job.experienceClassification).toMatchObject({
      experienceVersion: 2,
      experienceBand: "senior",
      experienceSource: "title",
      experienceConfidence: "high",
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
    expect(job.country).toBe("United States");
    expect(job.employmentType).toBeUndefined();
    expect(job.remoteType).toBe("onsite");
    expect(job.sourceCompanySlug).toBe("figma");
    expect(job.experienceLevel).toBe("junior");
    expect(job.experienceClassification).toMatchObject({
      experienceBand: "entry",
      experienceSource: "title",
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
    expect(job.remoteType).toBe("remote");
    expect(job.descriptionSnippet).toContain("Minimum qualifications");
    expect(job.experienceClassification).toMatchObject({
      experienceBand: "senior",
      experienceSource: "description",
      experienceConfidence: "medium",
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
    expect(job.remoteType).toBe("remote");
    expect(job.sourceCompanySlug).toBe("notion");
    expect(job.experienceLevel).toBe("staff");
    expect(job.experienceClassification).toMatchObject({
      experienceBand: "advanced",
      experienceSource: "title",
      explicitLevel: "staff",
      source: "title",
    });
  });

  it("normalizes SmartRecruiters jobs into the common model", () => {
    const job = normalizeSmartRecruitersJob({
      companyToken: "acme",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      candidate: {
        id: "744000067444685",
        title: "Senior Product Analyst",
        locationText: "Austin, TX",
        jobUrl: "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
        applyUrl: "https://jobs.smartrecruiters.com/Acme/744000067444685-senior-product-analyst",
        postedAt: "2026-03-18T00:00:00.000Z",
        typeOfEmployment: "Full-time",
        description: "Analyze product signals and partner with product leaders.",
        company: "Acme",
      },
    });

    expect(job.sourcePlatform).toBe("smartrecruiters");
    expect(job.company).toBe("Acme");
    expect(job.locationText).toBe("Austin, TX");
    expect(job.country).toBe("United States");
    expect(job.state).toBe("Texas");
    expect(job.city).toBe("Austin");
    expect(job.employmentType).toBe("full_time");
    expect(job.sourceCompanySlug).toBe("acme");
  });

  it("normalizes workday jobs into the common model", () => {
    const job = normalizeWorkdayJob({
      source: {
        url: "https://acme.wd1.myworkdayjobs.com/en-US/Careers",
        token: "acme:careers",
        sitePath: "en-US/Careers",
        careerSitePath: "Careers",
        companyHint: "Acme",
      },
      discoveredAt: "2026-03-29T00:00:00.000Z",
      candidate: {
        jobPostingInfo: {
          ignored: true,
        },
        title: "Principal Data Engineer",
        externalPath: "job/Seattle-WA/Principal-Data-Engineer_R12345",
        locationText: "Seattle, Washington",
        postedOn: "2026-03-20T00:00:00.000Z",
      },
    });

    expect(job.sourcePlatform).toBe("workday");
    expect(job.company).toBe("Acme");
    expect(job.city).toBe("Seattle");
    expect(job.state).toBe("Washington");
    expect(job.country).toBe("United States");
    expect(job.experienceLevel).toBe("principal");
    expect(job.seniority).toBe("principal");
    expect(job.sourceCompanySlug).toBe("careers");
    expect(job.canonicalUrl).toBe(
      "https://acme.wd1.myworkdayjobs.com/en-US/Careers/job/Seattle-WA/Principal-Data-Engineer_R12345",
    );
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
    expect(job.country).toBe("United States");
    expect(job.employmentType).toBe("internship");
    expect(job.seniority).toBe("intern");
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
    expect(job.descriptionSnippet).toContain("5+ years of experience");
    expect(job.experienceClassification).toMatchObject({
      inferredLevel: "senior",
      source: "description",
    });
  });

  it.each([
    {
      title: "Software Engineer III - Platform",
      expected: "senior",
    },
    {
      title: "Principal Software Engineer",
      expected: "principal",
    },
    {
      title: "Lead Backend Engineer",
      expected: "lead",
    },
  ])(
    "applies shared title seniority precedence through Greenhouse normalization for $title",
    ({ title, expected }) => {
      const job = normalizeGreenhouseJob({
        companyToken: "stripe",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        job: {
          id: `${title}-role`,
          title,
          absolute_url: "https://boards.greenhouse.io/stripe/jobs/title-role",
          first_published: "2026-03-20T00:00:00.000Z",
          location: { name: "Seattle" },
        },
      });

      expect(job.experienceLevel).toBe(expected);
      expect(job.experienceClassification).toMatchObject({
        explicitLevel: expected,
        source: "title",
      });
    },
  );

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
    expect(job.country).toBe("United States");
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
    expect(job.experienceClassification).toMatchObject({
      confidence: "none",
      source: "unknown",
      reasons: [],
      isUnspecified: true,
    });
  });

  it("canonicalizes hosted Greenhouse job URLs across boards hosts and strips tracking params", () => {
    const job = normalizeGreenhouseJob({
      companyToken: "gitlab",
      discoveredAt: "2026-03-29T00:00:00.000Z",
      job: {
        id: 8455464002,
        title: "Senior Data Analyst",
        absolute_url:
          "https://boards.greenhouse.io/gitlab/jobs/8455464002?gh_jid=8455464002&utm_source=linkedin",
        first_published: "2026-03-20T00:00:00.000Z",
        location: { name: "Remote, US" },
      },
    });

    expect(job.sourcePlatform).toBe("greenhouse");
    expect(job.sourceUrl).toBe(
      "https://boards.greenhouse.io/gitlab/jobs/8455464002?gh_jid=8455464002&utm_source=linkedin",
    );
    expect(job.canonicalUrl).toBe(
      "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
    );
    expect(job.rawSourceMetadata).toMatchObject({
      greenhouseBoardToken: "gitlab",
      greenhouseJobId: "8455464002",
    });
  });
});
