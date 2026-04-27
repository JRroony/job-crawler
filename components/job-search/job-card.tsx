"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { cn, formatPostedDate, formatRelativeMoment, jobPostingUrl } from "@/lib/utils";
import { labelForProviderPlatform } from "@/components/job-crawler/ui-config";
import {
  getJobTags,
  getSponsorshipLabel,
  getWorkplaceLabel,
} from "@/components/job-search/helpers";

type JobCardProps = {
  job: JobListing;
  selected: boolean;
  selectionKey: string;
  onSelect: (selectionKey: string) => void;
};

export function JobCard(props: JobCardProps) {
  const job = props.job;
  const tags = getJobTags(job);
  const workplaceLabel = getWorkplaceLabel(job);
  const postingUrl = jobPostingUrl(job);
  const dateLabel = job.postingDate ?? job.postedAt
    ? formatPostedDate(job.postingDate ?? job.postedAt)
    : `Last seen ${formatRelativeMoment(job.lastSeenAt ?? job.indexedAt)}`;

  function selectCard() {
    props.onSelect(props.selectionKey);
  }

  return (
    <article
      role="button"
      tabIndex={0}
      aria-pressed={props.selected}
      onClick={selectCard}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          selectCard();
        }
      }}
      className={cn(
        "rounded-lg border bg-white p-4 shadow-sm outline-none transition",
        props.selected
          ? "border-[#0a66c2] shadow-[0_8px_24px_rgba(10,102,194,0.14)] ring-1 ring-[#0a66c2]/20"
          : "border-slate-200 hover:border-[#0a66c2]/35 hover:shadow-md",
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="line-clamp-2 text-base font-semibold leading-6 text-[#0a66c2]">
            {job.title}
          </h3>
          <p className="mt-1 truncate text-sm font-semibold text-ink">{job.company}</p>
          <p className="mt-1 line-clamp-1 text-sm text-slate">
            {job.locationRaw || job.locationText || "Location unavailable"}
          </p>
        </div>

        <a
          href={postingUrl}
          target="_blank"
          rel="noreferrer"
          onClick={(event) => event.stopPropagation()}
          className="shrink-0 rounded-md bg-[#0a66c2] px-3 py-2 text-xs font-semibold text-white transition hover:bg-[#004182]"
        >
          Apply
        </a>
      </div>

      <div className="mt-3 flex flex-wrap gap-2 text-xs">
        {workplaceLabel ? <Badge>{workplaceLabel}</Badge> : null}
        <Badge>{labelForProviderPlatform(job.sourcePlatform)}</Badge>
        <Badge>{dateLabel}</Badge>
        <Badge>{getSponsorshipLabel(job)}</Badge>
      </div>

      {job.descriptionSnippet ? (
        <p className="mt-3 line-clamp-3 text-sm leading-6 text-slate">
          {job.descriptionSnippet}
        </p>
      ) : null}

      {tags.length > 0 ? (
        <div className="mt-3 flex flex-wrap gap-2">
          {tags
            .filter((tag) => tag !== workplaceLabel)
            .slice(0, 3)
            .map((tag, index) => (
              <span
                key={`${props.selectionKey}-tag-${index}`}
                className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate"
              >
                {tag}
              </span>
            ))}
        </div>
      ) : null}
    </article>
  );
}

function Badge(props: { children: React.ReactNode }) {
  return (
    <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 font-medium text-slate">
      {props.children}
    </span>
  );
}
