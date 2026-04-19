import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { buildStableJobRenderKeys } from "@/components/job-search/helpers";
import {
  buildResultsCsv,
  buildResultsExportFilename,
  buildResultsExportRows,
  buildResultsPaginationState,
  downloadResultsCsv,
  resolveAdjacentResultsPage,
  resolveJobExportLink,
  resolveVisiblePageSelection,
  ResultsTable,
} from "@/components/results-table";
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
  function createJobs(count: number) {
    return Array.from({ length: count }, (_, index) => {
      const jobNumber = index + 1;

      return createJob({
        _id: `job-${jobNumber}`,
        sourceJobId: `role-${jobNumber}`,
        sourceLookupKeys: [`greenhouse:role-${jobNumber}`],
        title: `Software Engineer ${jobNumber}`,
        sourceUrl: `https://example.com/jobs/${jobNumber}`,
        applyUrl: `https://example.com/jobs/${jobNumber}/apply`,
        resolvedUrl: `https://example.com/jobs/${jobNumber}/apply`,
        canonicalUrl: `https://example.com/jobs/${jobNumber}/apply`,
        contentHash: `content-hash:${jobNumber}`,
        contentFingerprint: `fingerprint-${jobNumber}`,
        dedupeFingerprint: `fingerprint-${jobNumber}`,
      });
    });
  }

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

  it("renders only the current page of jobs", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={createJobs(3)}
        pageSize={2}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(html).toContain("Software Engineer 1");
    expect(html).toContain("Software Engineer 2");
    expect(html).not.toContain("Software Engineer 3");
    expect(html).toContain("Showing 1-2 of 3");
    expect(html).toContain("Page 1 of 2");
  });

  it("resolves previous and next page navigation within the visible result count", () => {
    expect(resolveAdjacentResultsPage(1, "next", 3, 2)).toBe(2);
    expect(resolveAdjacentResultsPage(2, "previous", 3, 2)).toBe(1);
    expect(resolveAdjacentResultsPage(1, "previous", 3, 2)).toBe(1);
    expect(resolveAdjacentResultsPage(2, "next", 3, 2)).toBe(2);
  });

  it("updates page count and clamps the current page when visible jobs change", () => {
    expect(buildResultsPaginationState(2, 51, 25)).toMatchObject({
      currentPage: 2,
      totalPages: 3,
      visibleStart: 26,
      visibleEnd: 50,
    });

    expect(buildResultsPaginationState(2, 20, 25)).toMatchObject({
      currentPage: 1,
      totalPages: 1,
      visibleStart: 1,
      visibleEnd: 20,
    });
  });

  it("keeps selected jobs valid for the current page or resets safely", () => {
    expect(resolveVisiblePageSelection("job-key-1", ["job-key-1", "job-key-2"])).toBe(
      "job-key-1",
    );
    expect(resolveVisiblePageSelection("job-key-9", ["job-key-3", "job-key-4"])).toBe(
      "job-key-3",
    );
    expect(resolveVisiblePageSelection("job-key-9", [])).toBeUndefined();
  });

  it("keeps the empty state visible with disabled pagination metadata", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={[]}
        emptyMessage="Nothing passed the current filters."
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(html).toContain("No jobs match these filters");
    expect(html).toContain("Nothing passed the current filters.");
    expect(html).toContain("Showing 0-0 of 0");
    expect(html).toContain("Page 0 of 0");
  });

  it("exports rows with job title, company name, and the chosen link", () => {
    const csv = buildResultsCsv([
      createJob({
        title: "Software Engineer",
        company: "Acme",
        canonicalUrl: "https://example.com/jobs/1/canonical",
        resolvedUrl: "https://example.com/jobs/1/resolved",
        applyUrl: "https://example.com/jobs/1/apply",
        sourceUrl: "https://example.com/jobs/1",
      }),
    ]);

    expect(csv).toBe(
      [
        "Job Title,Company Name,Link",
        "Software Engineer,Acme,https://example.com/jobs/1/canonical",
      ].join("\r\n"),
    );
  });

  it("uses canonical, resolved, apply, then source URL priority for exported links", () => {
    const canonicalJob = createJob({
      canonicalUrl: "https://example.com/jobs/1/canonical",
      resolvedUrl: "https://example.com/jobs/1/resolved",
      applyUrl: "https://example.com/jobs/1/apply",
      sourceUrl: "https://example.com/jobs/1",
    });
    const resolvedJob = createJob({
      canonicalUrl: undefined,
      resolvedUrl: "https://example.com/jobs/2/resolved",
      applyUrl: "https://example.com/jobs/2/apply",
      sourceUrl: "https://example.com/jobs/2",
    });
    const applyJob = createJob({
      canonicalUrl: undefined,
      resolvedUrl: undefined,
      applyUrl: "https://example.com/jobs/3/apply",
      sourceUrl: "https://example.com/jobs/3",
    });
    const sourceJob = createJob({
      canonicalUrl: undefined,
      resolvedUrl: undefined,
      applyUrl: "   ",
      sourceUrl: "https://example.com/jobs/4",
    });

    expect(resolveJobExportLink(canonicalJob)).toBe("https://example.com/jobs/1/canonical");
    expect(resolveJobExportLink(resolvedJob)).toBe("https://example.com/jobs/2/resolved");
    expect(resolveJobExportLink(applyJob)).toBe("https://example.com/jobs/3/apply");
    expect(resolveJobExportLink(sourceJob)).toBe("https://example.com/jobs/4");
  });

  it("escapes CSV commas, quotes, and line breaks", () => {
    const csv = buildResultsCsv([
      createJob({
        title: 'Senior "Platform", Engineer',
        company: "Acme,\nInc.",
        canonicalUrl: "https://example.com/jobs/1",
      }),
    ]);

    expect(csv).toBe(
      [
        "Job Title,Company Name,Link",
        '"Senior ""Platform"", Engineer","Acme,\nInc.",https://example.com/jobs/1',
      ].join("\r\n"),
    );
  });

  it("handles empty export state safely", () => {
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={[]}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(buildResultsCsv([])).toBeNull();
    expect(downloadResultsCsv([], "software-engineer-us-results.csv")).toBe(false);
    expect(html).toContain("Export CSV");
    expect(html).toContain("disabled");
    expect(html).toContain('aria-label="Export 0 visible results as CSV"');
  });

  it("generates a browser CSV download for non-empty visible results", () => {
    const originalDocumentDescriptor = Object.getOwnPropertyDescriptor(globalThis, "document");
    const originalCreateObjectUrlDescriptor = Object.getOwnPropertyDescriptor(
      URL,
      "createObjectURL",
    );
    const originalRevokeObjectUrlDescriptor = Object.getOwnPropertyDescriptor(
      URL,
      "revokeObjectURL",
    );
    const anchor = {
      href: "",
      download: "",
      style: {
        display: "",
      },
      click: vi.fn(),
      remove: vi.fn(),
    };
    const appendChild = vi.fn();
    const createObjectURL = vi.fn(() => "blob:job-results");
    const revokeObjectURL = vi.fn();

    try {
      Object.defineProperty(globalThis, "document", {
        configurable: true,
        value: {
          createElement: vi.fn(() => anchor),
          body: {
            appendChild,
          },
        },
      });
      Object.defineProperty(URL, "createObjectURL", {
        configurable: true,
        value: createObjectURL,
      });
      Object.defineProperty(URL, "revokeObjectURL", {
        configurable: true,
        value: revokeObjectURL,
      });

      expect(downloadResultsCsv([createJob()], "Software Engineer US Results")).toBe(true);
      expect(createObjectURL).toHaveBeenCalledWith(expect.any(Blob));
      expect(anchor.href).toBe("blob:job-results");
      expect(anchor.download).toBe("software-engineer-us-results.csv");
      expect(anchor.style.display).toBe("none");
      expect(appendChild).toHaveBeenCalledWith(anchor);
      expect(anchor.click).toHaveBeenCalled();
      expect(anchor.remove).toHaveBeenCalled();
      expect(revokeObjectURL).toHaveBeenCalledWith("blob:job-results");
    } finally {
      restoreProperty(globalThis, "document", originalDocumentDescriptor);
      restoreProperty(URL, "createObjectURL", originalCreateObjectUrlDescriptor);
      restoreProperty(URL, "revokeObjectURL", originalRevokeObjectUrlDescriptor);
    }
  });

  it("deduplicates exported rows from the visible result set", () => {
    const duplicate = createJob({
      _id: "job-duplicate",
      title: "Software Engineer",
      company: "Acme",
      canonicalUrl: "https://example.com/jobs/1/apply",
    });

    expect(buildResultsExportRows([createJob(), duplicate])).toHaveLength(1);
  });

  it("builds a search-reflective CSV filename", () => {
    expect(
      buildResultsExportFilename({
        title: "Software Engineer",
        city: "",
        state: "",
        country: "United States",
      }),
    ).toBe("software-engineer-us-results.csv");
  });
});

function restoreProperty<T extends object, K extends PropertyKey>(
  target: T,
  property: K,
  descriptor: PropertyDescriptor | undefined,
) {
  if (descriptor) {
    Object.defineProperty(target, property, descriptor);
    return;
  }

  Reflect.deleteProperty(target, property);
}
