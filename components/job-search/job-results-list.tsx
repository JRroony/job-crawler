"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { JobCard } from "@/components/job-search/job-card";

type JobResultsListProps = {
  jobs: JobListing[];
  jobRenderKeys: string[];
  selectedJobKey?: string;
  totalMatchedCount: number;
  returnedCount: number;
  pageSize?: number;
  hasMore?: boolean;
  isLoadingMore?: boolean;
  onSelect: (selectionKey: string) => void;
  onLoadMore: () => void;
};

export function JobResultsList(props: JobResultsListProps) {
  const pageSizeLabel = props.pageSize ? ` / ${props.pageSize}` : "";

  return (
    <section className="space-y-3">
      <div className="rounded-lg border border-slate-200 bg-white px-4 py-3 shadow-sm">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-ink">
              {formatCount(props.totalMatchedCount)} job{props.totalMatchedCount === 1 ? "" : "s"}
            </h2>
            <p className="mt-0.5 text-sm text-slate">
              Showing {formatCount(props.jobs.length)} visible, {formatCount(props.returnedCount)}
              {pageSizeLabel} loaded
            </p>
          </div>
        </div>
      </div>

      <div className="space-y-2">
        {props.jobs.map((job, index) => (
          <JobCard
            key={props.jobRenderKeys[index]}
            job={job}
            selectionKey={props.jobRenderKeys[index] ?? job._id}
            selected={props.jobRenderKeys[index] === props.selectedJobKey}
            onSelect={props.onSelect}
          />
        ))}
      </div>

      {props.hasMore ? (
        <div className="flex justify-center pt-2">
          <button
            type="button"
            onClick={props.onLoadMore}
            disabled={props.isLoadingMore}
            className="inline-flex min-h-10 items-center justify-center rounded-md bg-[#0a66c2] px-5 text-sm font-semibold text-white transition hover:bg-[#004182] disabled:cursor-wait disabled:bg-slate-300"
          >
            {props.isLoadingMore ? "Loading..." : "Load more"}
          </button>
        </div>
      ) : null}
    </section>
  );
}

function formatCount(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}
