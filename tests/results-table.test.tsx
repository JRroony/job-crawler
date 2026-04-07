import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { ResultsTable } from "@/components/results-table";
import type { JobListing } from "@/lib/types";

function createJob(overrides: Partial<JobListing> = {}): JobListing {
  return {
    _id: "job-1",
    title: "Software Engineer",
    company: "Acme",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationText: "San Francisco, California, United States",
    experienceLevel: "mid",
    sourcePlatform: "greenhouse",
    sourceJobId: "role-1",
    sourceUrl: "https://example.com/jobs/1",
    applyUrl: "https://example.com/jobs/1/apply",
    resolvedUrl: "https://example.com/jobs/1/apply",
    canonicalUrl: "https://example.com/jobs/1/apply",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt: "2026-03-29T00:00:00.000Z",
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
        canonicalUrl: "https://example.com/jobs/1/apply",
        discoveredAt: "2026-03-29T00:00:00.000Z",
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: ["greenhouse:role-1"],
    crawlRunIds: ["run-1"],
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    ...overrides,
  };
}

describe("ResultsTable", () => {
  it("links the visible action to the posting URL instead of the apply URL", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={[
          createJob({
            sourceUrl: "https://example.com/jobs/1",
            applyUrl: "https://example.com/jobs/1/apply",
            resolvedUrl: "https://example.com/jobs/1/apply",
            canonicalUrl: "https://example.com/jobs/1/apply",
          }),
        ]}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(html).toContain('href="https://example.com/jobs/1"');
    expect(html).not.toContain('href="https://example.com/jobs/1/apply"');
    expect(html).toContain("View Post");
  });
});
