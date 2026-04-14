import { describe, expect, it } from "vitest";

import { dedupeJobs } from "@/lib/server/crawler/dedupe";
import type { JobListing } from "@/lib/types";

function createCandidate(overrides: Partial<Omit<JobListing, "_id" | "crawlRunIds">> = {}) {
  return {
    title: "Software Engineer",
    company: "Acme",
    normalizedCompany: "acme",
    normalizedTitle: "software engineer",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationRaw: "San Francisco, California, United States",
    normalizedLocation: "san francisco california united states",
    locationText: "San Francisco, California, United States",
    remoteType: "onsite",
    seniority: "mid",
    experienceLevel: "mid",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: "acme",
    sourceJobId: "1",
    sourceUrl: "https://example.com/job",
    applyUrl: "https://example.com/job/apply",
    resolvedUrl: "https://example.com/job/apply",
    canonicalUrl: "https://example.com/job",
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt: "2026-03-29T00:00:00.000Z",
    crawledAt: "2026-03-29T00:00:00.000Z",
    sponsorshipHint: "unknown",
    linkStatus: "valid",
    lastValidatedAt: "2026-03-29T00:00:00.000Z",
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform: "greenhouse",
        sourceJobId: "1",
        sourceUrl: "https://example.com/job",
        applyUrl: "https://example.com/job/apply",
        resolvedUrl: "https://example.com/job/apply",
        canonicalUrl: "https://example.com/job",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: ["greenhouse:1"],
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    ...overrides,
  } satisfies Omit<JobListing, "_id" | "crawlRunIds">;
}

describe("dedupeJobs", () => {
  it("dedupes by canonical URL and preserves provenance", () => {
    const result = dedupeJobs([
      createCandidate(),
      createCandidate({
        sourcePlatform: "lever",
        sourceJobId: "2",
        sourceLookupKeys: ["lever:2"],
        sourceProvenance: [
          {
            sourcePlatform: "lever",
            sourceJobId: "2",
            sourceUrl: "https://example.com/job",
            applyUrl: "https://example.com/job/apply",
            resolvedUrl: "https://example.com/job/apply",
            canonicalUrl: "https://example.com/job",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
    ]);

    expect(result).toHaveLength(1);
    expect(result[0].sourceProvenance).toHaveLength(2);
  });

  it("dedupes the same job discovered through different paths when the canonical posting URL matches", () => {
    const result = dedupeJobs([
      createCandidate({
        sourcePlatform: "company_page",
        sourceJobId: "detail-role-1",
        sourceLookupKeys: ["company_page:detail-role-1"],
        sourceUrl: "https://example.com/careers/software-engineer",
        applyUrl: "https://example.com/careers/software-engineer",
        resolvedUrl: undefined,
      }),
      createCandidate({
        sourcePlatform: "greenhouse",
        sourceJobId: "role-1-recovered",
        sourceLookupKeys: ["greenhouse:role-1-recovered"],
        sourceUrl: "https://boards.greenhouse.io/acme/jobs/role-1",
        applyUrl: "https://boards.greenhouse.io/acme/jobs/role-1/apply",
        canonicalUrl: "https://example.com/job",
      }),
    ]);

    expect(result).toHaveLength(1);
  });

  it("dedupes by apply URL when validation metadata is still unknown", () => {
    const result = dedupeJobs([
      createCandidate({
        resolvedUrl: undefined,
        canonicalUrl: undefined,
        linkStatus: "unknown",
        lastValidatedAt: undefined,
      }),
      createCandidate({
        resolvedUrl: undefined,
        canonicalUrl: undefined,
        linkStatus: "unknown",
        lastValidatedAt: undefined,
        sourcePlatform: "lever",
        sourceJobId: "2",
        sourceLookupKeys: ["lever:2"],
        sourceProvenance: [
          {
            sourcePlatform: "lever",
            sourceJobId: "2",
            sourceUrl: "https://example.com/job",
            applyUrl: "https://example.com/job/apply",
            discoveredAt: "2026-03-29T00:00:00.000Z",
            rawSourceMetadata: {},
          },
        ],
      }),
    ]);

    expect(result).toHaveLength(1);
  });

  it("dedupes by shared platform job identity when the same job reappears with incomplete URL metadata", () => {
    const result = dedupeJobs([
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        applyUrl: "https://example.com/job/apply?ref=one",
      }),
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        applyUrl: "https://example.com/job/apply?ref=two",
        sourceLookupKeys: ["greenhouse:1"],
      }),
    ]);

    expect(result).toHaveLength(1);
  });

  it("preserves distinct jobs at the same company when similar titles have different canonical identities", () => {
    const result = dedupeJobs([
      createCandidate({
        title: "Software Engineer",
        titleNormalized: "software engineer",
        sourceJobId: "role-backend",
        sourceLookupKeys: ["greenhouse:role-backend"],
        sourceUrl: "https://example.com/jobs/backend",
        applyUrl: "https://example.com/jobs/backend/apply",
        resolvedUrl: "https://example.com/jobs/backend/apply",
        canonicalUrl: "https://example.com/jobs/backend",
      }),
      createCandidate({
        title: "Senior Software Engineer",
        titleNormalized: "senior software engineer",
        sourceJobId: "role-senior",
        sourceLookupKeys: ["greenhouse:role-senior"],
        sourceUrl: "https://example.com/jobs/senior",
        applyUrl: "https://example.com/jobs/senior/apply",
        resolvedUrl: "https://example.com/jobs/senior/apply",
        canonicalUrl: "https://example.com/jobs/senior",
      }),
    ]);

    expect(result).toHaveLength(2);
  });

  it("preserves same company and title when locations differ", () => {
    const result = dedupeJobs([
      createCandidate({
        sourceJobId: "role-sf",
        sourceLookupKeys: ["greenhouse:role-sf"],
        sourceUrl: "https://example.com/jobs/sf",
        applyUrl: "https://example.com/jobs/sf/apply",
        resolvedUrl: "https://example.com/jobs/sf/apply",
        canonicalUrl: "https://example.com/jobs/sf",
      }),
      createCandidate({
        sourceJobId: "role-nyc",
        sourceLookupKeys: ["greenhouse:role-nyc"],
        sourceUrl: "https://example.com/jobs/nyc",
        applyUrl: "https://example.com/jobs/nyc/apply",
        resolvedUrl: "https://example.com/jobs/nyc/apply",
        canonicalUrl: "https://example.com/jobs/nyc",
        city: "New York",
        state: "New York",
        locationText: "New York, New York, United States",
        locationNormalized: "new york new york united states",
      }),
    ]);

    expect(result).toHaveLength(2);
  });

  it("keeps Greenhouse jobs from different boards distinct when the scoped identity differs", () => {
    const result = dedupeJobs([
      createCandidate({
        sourceJobId: "8455464002",
        sourceUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
        applyUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
        canonicalUrl: "https://job-boards.greenhouse.io/gitlab/jobs/8455464002",
        resolvedUrl: undefined,
        sourceLookupKeys: ["greenhouse:gitlab:8455464002"],
      }),
      createCandidate({
        sourceJobId: "8455464002",
        sourceUrl: "https://job-boards.greenhouse.io/openai/jobs/8455464002",
        applyUrl: "https://job-boards.greenhouse.io/openai/jobs/8455464002",
        canonicalUrl: "https://job-boards.greenhouse.io/openai/jobs/8455464002",
        resolvedUrl: undefined,
        sourceLookupKeys: ["greenhouse:openai:8455464002"],
      }),
    ]);

    expect(result).toHaveLength(2);
  });
});
