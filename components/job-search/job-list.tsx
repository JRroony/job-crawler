"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { JobCard } from "@/components/job-search/job-card";
import { buildStableJobRenderKeys } from "@/components/job-search/helpers";

type JobListProps = {
  jobs: JobListing[];
  jobRenderKeys?: string[];
  selectedJobKey?: string;
  onSelect: (selectionKey: string) => void;
  emptyMessage?: string;
};

export function JobList(props: JobListProps) {
  const renderKeys =
    props.jobRenderKeys?.length === props.jobs.length
      ? props.jobRenderKeys
      : buildStableJobRenderKeys(props.jobs);

  if (props.jobs.length === 0) {
    return (
      <div className="rounded-[24px] border border-dashed border-ink/12 bg-mist/35 px-5 py-8 text-center">
        <h3 className="text-lg font-semibold text-ink">No jobs match these filters</h3>
        <p className="mt-2 text-sm leading-6 text-slate">
          {props.emptyMessage ??
            "Try broadening the filters or running a new search to bring more roles into the list."}
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {props.jobs.map((job, index) => (
        <JobCard
          key={renderKeys[index]}
          job={job}
          selectionKey={renderKeys[index]}
          selected={renderKeys[index] === props.selectedJobKey}
          onSelect={props.onSelect}
        />
      ))}
    </div>
  );
}
