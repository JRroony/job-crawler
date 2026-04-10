import { describe, expect, it } from "vitest";

import { dedupeJobs } from "@/lib/server/crawler/dedupe";
import type { JobListing } from "@/lib/types";

function createCandidate(overrides: Partial<Omit<JobListing, "_id" | "crawlRunIds">> = {}) {
  return {
    title: "Software Engineer",
    company: "Acme",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationText: "San Francisco, California, United States",
    experienceLevel: "mid",
    sourcePlatform: "greenhouse",
    sourceJobId: "1",
    sourceUrl: "https://example.com/job",
    applyUrl: "https://example.com/job/apply",
    resolvedUrl: "https://example.com/job/apply",
    canonicalUrl: "https://example.com/job",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt: "2026-03-29T00:00:00.000Z",
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

  it("does not merge distinct jobs that only share company, title, and location", () => {
    const result = dedupeJobs([
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        applyUrl: "https://example.com/job/a/apply",
        sourceUrl: "https://example.com/job/a",
        sourceJobId: "role-a",
        sourceLookupKeys: ["greenhouse:role-a"],
      }),
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        applyUrl: "https://example.com/job/b/apply",
        sourceUrl: "https://example.com/job/b",
        sourcePlatform: "greenhouse",
        sourceJobId: "role-b",
        sourceLookupKeys: ["greenhouse:role-b"],
      }),
    ]);

    expect(result).toHaveLength(2);
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

  it("dedupes by shared source lookup keys when the same job reappears with incomplete URL metadata", () => {
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
