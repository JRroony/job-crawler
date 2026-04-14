import { describe, expect, it } from "vitest";

import {
  isRemoteJob,
  isVisaFriendlyJob,
  matchesPostedDateFilter,
} from "@/components/job-search/helpers";
import type { JobListing } from "@/lib/types";

function createJob(overrides: Partial<JobListing> = {}): JobListing {
  return {
    _id: "job-1",
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
    sourceJobId: "role-1",
    sourceUrl: "https://example.com/jobs/1",
    applyUrl: "https://example.com/jobs/1/apply",
    resolvedUrl: "https://example.com/jobs/1/apply",
    canonicalUrl: "https://example.com/jobs/1",
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt: "2026-03-29T00:00:00.000Z",
    crawledAt: "2026-03-29T00:00:00.000Z",
    descriptionSnippet: "Team is open to visa sponsorship.",
    sponsorshipHint: "unknown",
    linkStatus: "valid",
    lastValidatedAt: "2026-03-29T00:00:00.000Z",
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform: "greenhouse",
        sourceJobId: "role-1",
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        resolvedUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: ["greenhouse:role-1"],
    crawlRunIds: ["run-1"],
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    ...overrides,
  };
}

describe("job-search helpers", () => {
  it("uses normalized remote type before falling back to raw text", () => {
    expect(
      isRemoteJob(
        createJob({
          remoteType: "remote",
          locationRaw: "Austin, Texas, United States",
          locationText: "Austin, Texas, United States",
        }),
      ),
    ).toBe(true);
  });

  it("uses sponsorship hint before raw text heuristics", () => {
    expect(
      isVisaFriendlyJob(
        createJob({
          sponsorshipHint: "not_supported",
          descriptionSnippet: "We can sponsor visas for exceptional candidates.",
        }),
      ),
    ).toBe(false);
  });

  it("uses postingDate when applying posted-date filters", () => {
    const recentJob = createJob({
      postingDate: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      postedAt: undefined,
    });

    expect(matchesPostedDateFilter(recentJob, "7d")).toBe(true);
    expect(matchesPostedDateFilter(recentJob, "24h")).toBe(false);
  });
});
