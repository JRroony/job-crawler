import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { buildStableJobRenderKeys } from "@/components/job-search/helpers";
import { ResultsTable } from "@/components/results-table";
import type { JobListing } from "@/lib/types";
import { formatPostedDate } from "@/lib/utils";

function createJob(overrides: Partial<JobListing> = {}): JobListing {
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug =
    "sourceCompanySlug" in overrides ? overrides.sourceCompanySlug : "acme";
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-03-29T00:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? "2026-03-29T00:00:00.000Z";
  const canonicalUrl = overrides.canonicalUrl ?? "https://example.com/jobs/1/apply";
  const resolvedUrl = overrides.resolvedUrl ?? "https://example.com/jobs/1/apply";
  const applyUrl = overrides.applyUrl ?? "https://example.com/jobs/1/apply";
  const normalizedSourceJobId = sourceJobId.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const canonicalJobKey =
    overrides.canonicalJobKey ??
    (sourceCompanySlug && normalizedSourceJobId
      ? `platform:${sourcePlatform}:${sourceCompanySlug}:${normalizedSourceJobId}`
      : canonicalUrl
        ? `canonical_url:${canonicalUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`
        : resolvedUrl
          ? `resolved_url:${resolvedUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`
          : `apply_url:${applyUrl.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}`);

  return {
    _id: "job-1",
    canonicalJobKey,
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
    sourcePlatform,
    sourceCompanySlug,
    sourceJobId,
    sourceUrl: "https://example.com/jobs/1",
    applyUrl,
    resolvedUrl,
    canonicalUrl,
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt,
    crawledAt,
    sponsorshipHint: "unknown",
    linkStatus: "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? crawledAt,
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform,
        sourceJobId,
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        resolvedUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1/apply",
        discoveredAt,
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [`${sourcePlatform}:${sourceJobId}`],
    crawlRunIds: ["run-1"],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
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
    expect(keys[0]).toBe("platform:greenhouse:acme:role 1");
    expect(keys[1]).toBe("platform:greenhouse:acme:role 2");
  });
});
