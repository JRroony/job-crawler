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

  it("falls back to normalized company title and location", () => {
    const result = dedupeJobs([
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
      }),
      createCandidate({
        canonicalUrl: undefined,
        resolvedUrl: undefined,
        sourcePlatform: "ashby",
        sourceJobId: "3",
        sourceLookupKeys: ["ashby:3"],
      }),
    ]);

    expect(result).toHaveLength(1);
  });
});
