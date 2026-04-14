"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { cn, formatPostedDate } from "@/lib/utils";
import { labelForProviderPlatform } from "@/components/job-crawler/ui-config";
import { getJobTags } from "@/components/job-search/helpers";

type JobCardProps = {
  job: JobListing;
  selected: boolean;
  selectionKey: string;
  onSelect: (selectionKey: string) => void;
};

export function JobCard(props: JobCardProps) {
  const tags = getJobTags(props.job);

  return (
    <button
      type="button"
      onClick={() => props.onSelect(props.selectionKey)}
      aria-pressed={props.selected}
      className={cn(
        "w-full rounded-[20px] border px-4 py-4 text-left transition",
        props.selected
          ? "border-[#0a66c2]/35 bg-[#0a66c2]/[0.05] shadow-[0_12px_28px_rgba(10,102,194,0.12)]"
          : "border-ink/8 bg-white hover:border-ink/18 hover:bg-mist/35 hover:shadow-sm",
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <div className="line-clamp-2 text-base font-semibold leading-6 text-ink">
            {props.job.title}
          </div>
          <div className="mt-1 truncate text-sm font-medium text-slate">{props.job.company}</div>
        </div>

        <div className="shrink-0 rounded-full border border-ink/8 bg-white px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate">
          {labelForProviderPlatform(props.job.sourcePlatform)}
        </div>
      </div>

      <dl className="mt-4 grid gap-2 text-sm text-slate sm:grid-cols-2">
        <div>
          <dt className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate/55">
            Location
          </dt>
          <dd className="mt-1 line-clamp-1">{props.job.locationText}</dd>
        </div>
        <div>
          <dt className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate/55">
            Posted
          </dt>
          <dd className="mt-1">{formatPostedDate(props.job.postingDate ?? props.job.postedAt)}</dd>
        </div>
      </dl>

      <div className="mt-4 flex flex-wrap items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate/70">
        <span className="rounded-full border border-ink/8 bg-mist/35 px-2.5 py-1">
          Source: {labelForProviderPlatform(props.job.sourcePlatform)}
        </span>
        <span className="rounded-full border border-ink/8 bg-mist/35 px-2.5 py-1">
          {props.job.linkStatus === "valid" ? "Checked link" : "Unverified link"}
        </span>
      </div>

      {tags.length > 0 ? (
        <div className="mt-4 flex flex-wrap gap-2">
          {tags.map((tag, index) => (
            <span
              key={`${props.selectionKey}-tag-${index}`}
              className="rounded-full border border-ink/8 bg-[#faf8f2] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-slate"
            >
              {tag}
            </span>
          ))}
        </div>
      ) : null}
    </button>
  );
}
