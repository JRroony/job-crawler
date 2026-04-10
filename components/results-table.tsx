"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

import { JobDetailPanel } from "@/components/job-search/job-detail-panel";
import { buildStableJobRenderKeys } from "@/components/job-search/helpers";
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
  const jobRenderKeys = useMemo(() => buildStableJobRenderKeys(jobs), [jobs]);
  const [selectedJobKey, setSelectedJobKey] = useState<string | undefined>(jobRenderKeys[0]);
  const selectedJobIndex = useMemo(() => {
    if (!selectedJobKey) {
      return 0;
    }

    const matchingIndex = jobRenderKeys.findIndex((jobKey) => jobKey === selectedJobKey);
    return matchingIndex === -1 ? 0 : matchingIndex;
  }, [jobRenderKeys, selectedJobKey]);
  const selectedJob = jobs[selectedJobIndex];
  const visibleCount = jobs.length;
  const fullCount = totalJobs ?? jobs.length;

  useEffect(() => {
    if (!jobs.length) {
      setSelectedJobKey(undefined);
      return;
    }

    if (!selectedJobKey || !jobRenderKeys.includes(selectedJobKey)) {
      setSelectedJobKey(jobRenderKeys[0]);
    }
  }, [jobRenderKeys, jobs, selectedJobKey]);

  useEffect(() => {
    if (!selectedJobKey || typeof window === "undefined" || window.innerWidth >= 1024) {
      return;
    }

    detailPanelRef.current?.scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }, [selectedJobKey]);

  return (
    <section className="grid gap-5 xl:grid-cols-[minmax(0,0.9fr)_minmax(380px,0.82fr)]">
      <div className="rounded-[28px] border border-ink/10 bg-white/92 shadow-[0_18px_50px_rgba(15,23,42,0.06)] backdrop-blur">
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
            </div>
            <div className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5 text-sm text-slate">
              Original posting stays pinned on the right
            </div>
          </div>
        </div>

        <div className="p-3 sm:p-4">
          <div className="xl:max-h-[calc(100vh-15rem)] xl:overflow-y-auto xl:pr-1">
            <JobList
              jobs={jobs}
              selectedJobKey={selectedJobKey}
              onSelect={setSelectedJobKey}
              emptyMessage={emptyMessage}
            />
          </div>
        </div>
      </div>

      <div ref={detailPanelRef} className="xl:sticky xl:top-6 xl:self-start">
        <JobDetailPanel
          job={selectedJob}
          isRevalidating={Boolean(selectedJob && revalidatingIds.includes(selectedJob._id))}
          onRevalidate={onRevalidate}
        />
      </div>
    </section>
  );
}
