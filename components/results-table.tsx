"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

import { JobDetailPanel } from "@/components/job-search/job-detail-panel";
import { JobList } from "@/components/job-search/job-list";
import type { JobListing } from "@/lib/types";

type ResultsTableProps = {
  jobs: JobListing[];
  onRevalidate: (jobId: string) => Promise<void>;
  revalidatingIds: string[];
  totalJobs?: number;
  emptyMessage?: string;
};

export function ResultsTable({
  jobs,
  onRevalidate,
  revalidatingIds,
  totalJobs,
  emptyMessage,
}: ResultsTableProps) {
  const detailPanelRef = useRef<HTMLDivElement | null>(null);
  const [selectedJobId, setSelectedJobId] = useState<string | undefined>(jobs[0]?._id);
  const selectedJob = useMemo(
    () => jobs.find((job) => job._id === selectedJobId) ?? jobs[0],
    [jobs, selectedJobId],
  );
  const visibleCount = jobs.length;
  const fullCount = totalJobs ?? jobs.length;

  useEffect(() => {
    if (!jobs.length) {
      setSelectedJobId(undefined);
      return;
    }

    if (!jobs.some((job) => job._id === selectedJobId)) {
      setSelectedJobId(jobs[0]._id);
    }
  }, [jobs, selectedJobId]);

  useEffect(() => {
    if (!selectedJobId || typeof window === "undefined" || window.innerWidth >= 1024) {
      return;
    }

    detailPanelRef.current?.scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }, [selectedJobId]);

  return (
    <section className="grid gap-4 lg:grid-cols-[minmax(0,0.94fr)_minmax(360px,0.86fr)] xl:grid-cols-[minmax(0,0.98fr)_minmax(380px,0.82fr)]">
      <div className="rounded-[28px] border border-ink/8 bg-white shadow-[0_22px_60px_rgba(15,23,42,0.06)]">
        <div className="border-b border-ink/8 px-5 py-4 sm:px-6">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <div className="text-sm font-medium uppercase tracking-[0.18em] text-slate/65">
                Job results
              </div>
              <h2 className="mt-2 text-2xl font-semibold tracking-tight text-ink">
                {visibleCount === fullCount
                  ? `${visibleCount} job${visibleCount === 1 ? "" : "s"}`
                  : `${visibleCount} of ${fullCount} jobs`}
              </h2>
            </div>
            <p className="max-w-md text-sm leading-6 text-slate">
              Browse the list on the left and keep the selected job details open on the right.
            </p>
          </div>
        </div>

        <div className="p-3 sm:p-4">
          <div className="lg:max-h-[calc(100vh-19rem)] lg:overflow-y-auto lg:pr-1">
            <JobList
              jobs={jobs}
              selectedJobId={selectedJobId}
              onSelect={setSelectedJobId}
              emptyMessage={emptyMessage}
            />
          </div>
        </div>
      </div>

      <div ref={detailPanelRef} className="lg:sticky lg:top-6 lg:self-start">
        <JobDetailPanel
          job={selectedJob}
          isRevalidating={Boolean(selectedJob && revalidatingIds.includes(selectedJob._id))}
          onRevalidate={onRevalidate}
        />
      </div>
    </section>
  );
}
