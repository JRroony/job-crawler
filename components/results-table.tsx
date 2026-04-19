"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

import { JobDetailPanel } from "@/components/job-search/job-detail-panel";
import { buildStableJobRenderKeys } from "@/components/job-search/helpers";
import { JobList } from "@/components/job-search/job-list";
import { buildStableJobRenderIdentity } from "@/lib/job-identity";
import type { JobListing, SearchFilters } from "@/lib/types";

type ResultsTableProps = {
  jobs: JobListing[];
  onRevalidate: (jobId: string) => Promise<void>;
  revalidatingIds: string[];
  totalJobs?: number;
  emptyMessage?: string;
  pageSize?: number;
  exportFilename?: string;
};

export const defaultResultsPageSize = 25;
const resultsCsvHeaders = ["Job Title", "Company Name", "Link"] as const;

export type ResultsCsvRow = {
  jobTitle: string;
  companyName: string;
  link: string;
};

export type ResultsPaginationState = {
  currentPage: number;
  totalPages: number;
  pageSize: number;
  totalItems: number;
  startIndex: number;
  endIndex: number;
  visibleStart: number;
  visibleEnd: number;
  hasPreviousPage: boolean;
  hasNextPage: boolean;
};

export function buildResultsPaginationState(
  requestedPage: number,
  totalItems: number,
  requestedPageSize: number = defaultResultsPageSize,
): ResultsPaginationState {
  const pageSize =
    Number.isFinite(requestedPageSize) && requestedPageSize > 0
      ? Math.floor(requestedPageSize)
      : defaultResultsPageSize;
  const normalizedTotalItems = Math.max(0, Math.floor(totalItems));
  const totalPages =
    normalizedTotalItems === 0 ? 0 : Math.ceil(normalizedTotalItems / pageSize);
  const normalizedRequestedPage =
    Number.isFinite(requestedPage) && requestedPage > 0 ? Math.floor(requestedPage) : 1;
  const currentPage =
    totalPages === 0
      ? 0
      : Math.min(Math.max(normalizedRequestedPage, 1), totalPages);
  const startIndex = currentPage === 0 ? 0 : (currentPage - 1) * pageSize;
  const endIndex = currentPage === 0 ? 0 : Math.min(startIndex + pageSize, normalizedTotalItems);

  return {
    currentPage,
    totalPages,
    pageSize,
    totalItems: normalizedTotalItems,
    startIndex,
    endIndex,
    visibleStart: currentPage === 0 ? 0 : startIndex + 1,
    visibleEnd: endIndex,
    hasPreviousPage: currentPage > 1,
    hasNextPage: currentPage > 0 && currentPage < totalPages,
  };
}

export function resolveAdjacentResultsPage(
  currentPage: number,
  direction: "previous" | "next",
  totalItems: number,
  pageSize: number = defaultResultsPageSize,
) {
  const targetPage = direction === "previous" ? currentPage - 1 : currentPage + 1;

  return buildResultsPaginationState(targetPage, totalItems, pageSize).currentPage;
}

export function resolveVisiblePageSelection(
  selectedJobKey: string | undefined,
  visiblePageJobKeys: string[],
) {
  if (selectedJobKey && visiblePageJobKeys.includes(selectedJobKey)) {
    return selectedJobKey;
  }

  return visiblePageJobKeys[0];
}

export function resolveJobExportLink(job: JobListing) {
  return firstNonEmptyString([
    job.canonicalUrl,
    job.resolvedUrl,
    job.applyUrl,
    job.sourceUrl,
  ]);
}

export function buildResultsExportRows(jobs: JobListing[]): ResultsCsvRow[] {
  const seenIdentities = new Set<string>();
  const seenRows = new Set<string>();
  const rows: ResultsCsvRow[] = [];

  for (const job of jobs) {
    const link = resolveJobExportLink(job);
    if (!link) {
      continue;
    }

    const identity = buildStableJobRenderIdentity(job);
    const row: ResultsCsvRow = {
      jobTitle: job.title,
      companyName: job.company,
      link,
    };
    const rowKey = [row.jobTitle, row.companyName, row.link]
      .map(normalizeExportDedupeText)
      .join("|");

    if (seenIdentities.has(identity) || seenRows.has(rowKey)) {
      continue;
    }

    seenIdentities.add(identity);
    seenRows.add(rowKey);
    rows.push(row);
  }

  return rows;
}

export function buildResultsCsv(jobs: JobListing[]) {
  const rows = buildResultsExportRows(jobs);
  if (rows.length === 0) {
    return null;
  }

  return [
    resultsCsvHeaders.join(","),
    ...rows.map((row) =>
      [
        escapeCsvField(row.jobTitle),
        escapeCsvField(row.companyName),
        escapeCsvField(row.link),
      ].join(","),
    ),
  ].join("\r\n");
}

export function buildResultsExportFilename(
  filters: Pick<SearchFilters, "title" | "country" | "state" | "city">,
) {
  const titleSegment = slugifyFilenameSegment(filters.title) || "job-search";
  const locationSegment = buildLocationFilenameSegment(filters);

  return normalizeCsvFilename(
    [titleSegment, locationSegment, "results"].filter(Boolean).join("-"),
  );
}

export function downloadResultsCsv(jobs: JobListing[], filename = "job-results.csv") {
  const csv = buildResultsCsv(jobs);
  if (!csv || typeof document === "undefined" || typeof URL === "undefined") {
    return false;
  }

  if (
    typeof URL.createObjectURL !== "function" ||
    typeof URL.revokeObjectURL !== "function"
  ) {
    return false;
  }

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");

  anchor.href = objectUrl;
  anchor.download = normalizeCsvFilename(filename);
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(objectUrl);

  return true;
}

export function ResultsTable({
  jobs,
  onRevalidate,
  revalidatingIds,
  totalJobs,
  emptyMessage,
  pageSize = defaultResultsPageSize,
  exportFilename = "job-results.csv",
}: ResultsTableProps) {
  const detailPanelRef = useRef<HTMLDivElement | null>(null);
  const jobRenderKeys = useMemo(() => buildStableJobRenderKeys(jobs), [jobs]);
  const exportRowCount = useMemo(() => buildResultsExportRows(jobs).length, [jobs]);
  const [currentPage, setCurrentPage] = useState(1);
  const pagination = useMemo(
    () => buildResultsPaginationState(currentPage, jobs.length, pageSize),
    [currentPage, jobs.length, pageSize],
  );
  const pageJobs = useMemo(
    () => jobs.slice(pagination.startIndex, pagination.endIndex),
    [jobs, pagination.endIndex, pagination.startIndex],
  );
  const pageJobRenderKeys = useMemo(
    () => jobRenderKeys.slice(pagination.startIndex, pagination.endIndex),
    [jobRenderKeys, pagination.endIndex, pagination.startIndex],
  );
  const [selectedJobKey, setSelectedJobKey] = useState<string | undefined>(
    pageJobRenderKeys[0],
  );
  const selectedPageJobKey = resolveVisiblePageSelection(selectedJobKey, pageJobRenderKeys);
  const selectedPageJobIndex = selectedPageJobKey
    ? pageJobRenderKeys.findIndex((jobKey) => jobKey === selectedPageJobKey)
    : -1;
  const selectedJob = selectedPageJobIndex === -1 ? undefined : pageJobs[selectedPageJobIndex];
  const visibleCount = jobs.length;
  const fullCount = totalJobs ?? jobs.length;

  useEffect(() => {
    setCurrentPage((current) => {
      const clampedPage = buildResultsPaginationState(current, jobs.length, pageSize).currentPage;

      return current === clampedPage ? current : clampedPage;
    });
  }, [jobs.length, pageSize]);

  useEffect(() => {
    setSelectedJobKey((current) => {
      const nextSelection = resolveVisiblePageSelection(current, pageJobRenderKeys);

      return current === nextSelection ? current : nextSelection;
    });
  }, [pageJobRenderKeys]);

  useEffect(() => {
    if (!selectedPageJobKey || typeof window === "undefined" || window.innerWidth >= 1024) {
      return;
    }

    detailPanelRef.current?.scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }, [selectedPageJobKey]);

  function goToPreviousPage() {
    setCurrentPage((current) =>
      resolveAdjacentResultsPage(current, "previous", jobs.length, pageSize),
    );
  }

  function goToNextPage() {
    setCurrentPage((current) =>
      resolveAdjacentResultsPage(current, "next", jobs.length, pageSize),
    );
  }

  function exportVisibleResults() {
    downloadResultsCsv(jobs, exportFilename);
  }

  return (
    <section className="grid gap-5 xl:grid-cols-[minmax(0,0.92fr)_minmax(360px,0.78fr)]">
      <div className="rounded-[24px] border border-ink/10 bg-white/94 shadow-[0_18px_50px_rgba(15,23,42,0.06)] backdrop-blur">
        <div className="border-b border-ink/8 px-5 py-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
                Results
              </div>
              <h2 className="mt-1 text-xl font-semibold tracking-tight text-ink">
                {visibleCount === fullCount
                  ? `${visibleCount} job${visibleCount === 1 ? "" : "s"}`
                  : `${visibleCount} of ${fullCount} jobs`}
              </h2>
              <p className="mt-1 text-sm text-slate">
                Compare roles from the list while keeping the selected posting preview visible.
              </p>
              <p className="mt-2 text-sm font-medium text-ink">
                Showing {pagination.visibleStart}-{pagination.visibleEnd} of {visibleCount}
              </p>
            </div>
            <div className="flex flex-col gap-3 sm:items-end">
              <div className="flex flex-wrap items-center gap-2 sm:justify-end">
                <button
                  type="button"
                  onClick={exportVisibleResults}
                  disabled={exportRowCount === 0}
                  className="inline-flex items-center justify-center rounded-full border border-ink/10 bg-white px-3 py-1.5 text-sm font-semibold text-ink transition hover:border-[#0a66c2]/40 hover:text-[#0a66c2] disabled:cursor-not-allowed disabled:border-ink/8 disabled:bg-mist/35 disabled:text-slate/45"
                  aria-label={`Export ${exportRowCount} visible result${exportRowCount === 1 ? "" : "s"} as CSV`}
                >
                  Export CSV
                </button>
                <div className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5 text-sm text-slate">
                  Selected job stays pinned on larger screens
                </div>
              </div>
              <nav
                aria-label="Results pagination"
                className="flex flex-wrap items-center gap-2 text-sm text-slate"
              >
                <button
                  type="button"
                  onClick={goToPreviousPage}
                  disabled={!pagination.hasPreviousPage}
                  className="rounded-full border border-ink/10 bg-white px-3 py-1.5 font-semibold text-ink transition hover:border-[#0a66c2]/40 hover:text-[#0a66c2] disabled:cursor-not-allowed disabled:border-ink/8 disabled:bg-mist/35 disabled:text-slate/45"
                >
                  Previous
                </button>
                <span className="rounded-full border border-ink/8 bg-mist/30 px-3 py-1.5 font-medium text-ink">
                  Page {pagination.currentPage} of {pagination.totalPages}
                </span>
                <button
                  type="button"
                  onClick={goToNextPage}
                  disabled={!pagination.hasNextPage}
                  className="rounded-full border border-ink/10 bg-white px-3 py-1.5 font-semibold text-ink transition hover:border-[#0a66c2]/40 hover:text-[#0a66c2] disabled:cursor-not-allowed disabled:border-ink/8 disabled:bg-mist/35 disabled:text-slate/45"
                >
                  Next
                </button>
              </nav>
            </div>
          </div>
        </div>

        <div className="p-3 sm:p-4">
          <JobList
            jobs={pageJobs}
            jobRenderKeys={pageJobRenderKeys}
            selectedJobKey={selectedPageJobKey}
            onSelect={setSelectedJobKey}
            emptyMessage={emptyMessage}
          />
        </div>
      </div>

      <div ref={detailPanelRef} className="xl:sticky xl:top-5 xl:self-start">
        <JobDetailPanel
          job={selectedJob}
          isRevalidating={Boolean(selectedJob && revalidatingIds.includes(selectedJob._id))}
          onRevalidate={onRevalidate}
        />
      </div>
    </section>
  );
}

function escapeCsvField(value: string) {
  const normalizedValue = value.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!/[",\n]/.test(normalizedValue)) {
    return normalizedValue;
  }

  return `"${normalizedValue.replace(/"/g, '""')}"`;
}

function firstNonEmptyString(values: Array<string | undefined>) {
  return values.map((value) => value?.trim()).find(Boolean);
}

function normalizeExportDedupeText(value: string) {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function buildLocationFilenameSegment(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
) {
  const city = slugifyFilenameSegment(filters.city);
  const state = slugifyFilenameSegment(filters.state);
  const country = isUnitedStatesFilenameSegment(filters.country)
    ? "us"
    : slugifyFilenameSegment(filters.country);

  return [city, state, country].filter(Boolean).join("-");
}

function isUnitedStatesFilenameSegment(value?: string) {
  return /^(us|usa|u\.s\.|u\.s\.a\.|united states|united states of america)$/i.test(
    value?.trim() ?? "",
  );
}

function slugifyFilenameSegment(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function normalizeCsvFilename(filename: string) {
  const withoutExtension = filename
    .trim()
    .replace(/\.csv$/i, "")
    .replace(/[^a-z0-9._-]+/gi, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();

  return `${withoutExtension || "job-results"}.csv`;
}
