"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { labelForLinkStatus, formatPostedDate, formatRelativeMoment, jobPostingUrl, cn } from "@/lib/utils";
import { labelForProviderPlatform } from "@/components/job-crawler/ui-config";
import { getJobTags } from "@/components/job-search/helpers";

type JobDetailPanelProps = {
  job?: JobListing;
  isRevalidating: boolean;
  onRevalidate: (jobId: string) => Promise<void>;
};

export function JobDetailPanel(props: JobDetailPanelProps) {
  if (!props.job) {
    return (
      <section className="rounded-[20px] border border-ink/10 bg-white p-6 shadow-sm">
        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
          Details
        </div>
        <h3 className="mt-3 text-2xl font-semibold text-ink">Select a job to inspect it.</h3>
        <p className="mt-2 text-sm leading-6 text-slate">
          The selected posting stays here while you move through the list on the left.
        </p>
      </section>
    );
  }

  const job = props.job;
  const tags = getJobTags(job);
  const postingUrl = jobPostingUrl(job);

  return (
    <section className="rounded-[28px] border border-ink/10 bg-white/94 p-6 shadow-[0_18px_50px_rgba(15,23,42,0.07)] backdrop-blur">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
            Details
          </div>
          <h3 className="mt-2 text-[28px] font-semibold leading-tight text-ink">
            {job.title}
          </h3>
          <div className="mt-1 text-lg text-slate">{job.company}</div>
        </div>

        <span className="rounded-full border border-ink/10 bg-mist/45 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate">
          {labelForProviderPlatform(job.sourcePlatform)}
        </span>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-slate">
        <span>{job.locationText}</span>
        <span className="text-slate/35">•</span>
        <span>{formatPostedDate(job.postedAt)}</span>
      </div>

      {tags.length > 0 ? (
        <div className="mt-4 flex flex-wrap gap-2">
          {tags.map((tag, index) => (
            <span
              key={`${job._id}-tag-${index}`}
              className="rounded-full border border-ink/8 bg-white px-3 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-slate"
            >
              {tag}
            </span>
          ))}
        </div>
      ) : null}

      <div className="mt-6 flex flex-wrap gap-3">
        <a
          href={postingUrl}
          target="_blank"
          rel="noreferrer"
          className="rounded-[14px] bg-[#0a66c2] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[#004182]"
        >
          View Post
        </a>
        <button
          type="button"
          onClick={() => props.onRevalidate(job._id)}
          disabled={props.isRevalidating}
          className="rounded-[14px] border border-ink/10 px-5 py-3 text-sm font-medium text-ink transition hover:border-ink/25 hover:bg-mist/45 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {props.isRevalidating
            ? "Checking..."
            : job.lastValidatedAt
              ? "Revalidate"
              : "Validate"}
        </button>
      </div>

      <div className="mt-6 grid gap-3 sm:grid-cols-2">
        <DetailItem
          label="Validation"
          value={labelForLinkStatus(job.linkStatus)}
          tone={job.linkStatus}
        />
        <DetailItem
          label="Last checked"
          value={
            job.lastValidatedAt
              ? formatRelativeMoment(job.lastValidatedAt)
              : "Pending validation"
          }
        />
        <DetailItem
          label="Posted"
          value={job.postedAt ? formatPostedDate(job.postedAt) : "Date unavailable"}
        />
        <DetailItem
          label="Source"
          value={formatSourceHost(postingUrl)}
        />
      </div>

      <div className="mt-6 rounded-[20px] border border-ink/8 bg-mist/35 px-5 py-5">
        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
          Notes
        </div>
        <p className="mt-3 text-sm leading-7 text-slate">
          This role was pulled from {labelForProviderPlatform(job.sourcePlatform)} and links directly to the original public posting so you can verify the source without leaving the search flow.
          {job.sourceProvenance.length > 1
            ? ` The crawler merged ${job.sourceProvenance.length} matching source records into this listing.`
            : ""}
        </p>
      </div>
    </section>
  );
}

function DetailItem(props: {
  label: string;
  value: string;
  tone?: JobListing["linkStatus"];
}) {
  return (
    <div className="rounded-[22px] border border-ink/8 bg-white px-4 py-4">
      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
        {props.label}
      </div>
      <div
        className={cn(
          "mt-2 text-sm font-medium",
          props.tone === "valid" && "text-pine",
          props.tone === "unknown" && "text-tide",
          props.tone === "stale" && "text-amber-800",
          props.tone === "invalid" && "text-red-700",
          !props.tone && "text-ink",
        )}
      >
        {props.value}
      </div>
    </div>
  );
}

function formatSourceHost(url: string) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}
