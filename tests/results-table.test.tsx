import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { buildStableJobRenderKeys } from "@/components/job-search/helpers";
import { ResultsTable } from "@/components/results-table";
import type { JobListing } from "@/lib/types";
import { formatPostedDate } from "@/lib/utils";

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
    canonicalUrl: "https://example.com/jobs/1/apply",
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
    dedupeFingerprint: "fingerprint-1",
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

  it("shows pending validation state for newly crawled unknown links", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={[
          createJob({
            linkStatus: "unknown",
            lastValidatedAt: undefined,
            resolvedUrl: undefined,
          }),
        ]}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(html).toContain("Pending validation");
    expect(html).toContain("Validate");
  });

  it("renders practical card metadata including source, posting date, and workplace tags", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={[
          createJob({
            remoteType: "hybrid",
            postingDate: "2026-03-20T00:00:00.000Z",
          }),
        ]}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(html).toContain("Source: Greenhouse");
    expect(html).toContain(formatPostedDate("2026-03-20T00:00:00.000Z"));
    expect(html).toContain("Hybrid");
    expect(html).toContain("Selected job stays pinned on larger screens");
  });

  it("builds unique render keys even when malformed jobs reuse the same database id", () => {
    const keys = buildStableJobRenderKeys([
      createJob(),
      createJob({
        sourceJobId: "role-2",
        sourceLookupKeys: ["greenhouse:role-2"],
        applyUrl: "https://example.com/jobs/2/apply",
        sourceUrl: "https://example.com/jobs/2",
        canonicalUrl: "https://example.com/jobs/2",
      }),
    ]);

    expect(keys[0]).not.toBe(keys[1]);
    expect(keys[0]).toBe("canonical:https://example.com/jobs/1/apply");
    expect(keys[1]).toBe("canonical:https://example.com/jobs/2");
  });
});
