"use client";

import React from "react";

import {
  labelForProviderPlatform,
  summarizeExperienceConfidence,
} from "@/components/job-crawler/ui-config";
import type { JobListing } from "@/lib/types";
import {
  cn,
  formatPostedDate,
  formatRelativeMoment,
  jobPostingUrl,
  labelForExperience,
  labelForLinkStatus,
} from "@/lib/utils";

type ResultsTableProps = {
  jobs: JobListing[];
  onRevalidate: (jobId: string) => Promise<void>;
  revalidatingIds: string[];
};

export function ResultsTable({
  jobs,
  onRevalidate,
  revalidatingIds,
}: ResultsTableProps) {
  const linkSignals = summarizeLinkSignals(jobs);

  return (
    <div className="overflow-hidden rounded-[28px] border border-ink/10 bg-white shadow-soft">
      <div className="border-b border-ink/10 bg-[linear-gradient(135deg,rgba(244,239,230,0.9),rgba(255,255,255,0.95))] px-5 py-5 sm:px-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <p className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
              Saved results
            </p>
            <h3 className="mt-3 text-2xl font-semibold text-ink">
              {jobs.length} saved job{jobs.length === 1 ? "" : "s"} ready to review
            </h3>
            <p className="mt-2 text-sm leading-6 text-slate">
              Every row keeps the original post front and center, while validation status tells you
              whether the apply path has been checked yet. On smaller screens the list collapses
              into cards so the same details remain readable.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            {linkSignals.map((signal) => (
              <span
                key={signal.label}
                className={cn(
                  "rounded-full px-3 py-2 text-xs font-semibold uppercase tracking-[0.16em]",
                  signal.tone,
                )}
              >
                {signal.label} {signal.value}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div className="grid gap-4 p-4 lg:hidden">
        {jobs.map((job) => (
          <JobCard
            key={job._id}
            job={job}
            isRevalidating={revalidatingIds.includes(job._id)}
            onRevalidate={onRevalidate}
          />
        ))}
      </div>

      <div className="hidden overflow-x-auto lg:block">
        <table className="min-w-full text-left text-sm">
          <thead className="border-b border-ink/10 bg-sand/60 text-xs uppercase tracking-[0.22em] text-slate">
            <tr>
              <th className="px-5 py-4">Role</th>
              <th className="px-5 py-4">Location</th>
              <th className="px-5 py-4">Experience</th>
              <th className="px-5 py-4">Source</th>
              <th className="px-5 py-4">Posted</th>
              <th className="px-5 py-4">Validation</th>
              <th className="px-5 py-4">Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <DesktopResultRow
                key={job._id}
                job={job}
                isRevalidating={revalidatingIds.includes(job._id)}
                onRevalidate={onRevalidate}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function DesktopResultRow(props: {
  job: JobListing;
  isRevalidating: boolean;
  onRevalidate: ResultsTableProps["onRevalidate"];
}) {
  const { job } = props;
  const experienceSummary = summarizeExperienceConfidence(job.experienceClassification);

  return (
    <tr className="border-b border-ink/10 transition hover:bg-mist/35 last:border-b-0">
      <td className="px-5 py-4 align-top">
        <div className="space-y-2">
          <div>
            <div className="text-base font-semibold text-ink">{job.title}</div>
            <div className="text-slate">{job.company}</div>
          </div>
          <div className="flex flex-wrap gap-2">
            <SourcePlatformBadge platform={job.sourcePlatform} />
            {job.sourceProvenance.length > 1 ? (
              <SubtleBadge>{job.sourceProvenance.length} merged sources</SubtleBadge>
            ) : null}
          </div>
          <div className="font-mono text-xs text-slate/70">
            {job.lastValidatedAt
              ? `Validated ${formatRelativeMoment(job.lastValidatedAt)}`
              : "Pending validation"}
          </div>
        </div>
      </td>

      <td className="px-5 py-4 align-top text-slate">
        <div className="space-y-2">
          <div>{job.locationText}</div>
          <div className="font-mono text-xs text-slate/70">{job.sourceJobId}</div>
        </div>
      </td>

      <td className="px-5 py-4 align-top">
        <ExperienceBlock job={job} summary={experienceSummary} />
      </td>

      <td className="px-5 py-4 align-top">
        <div className="space-y-2">
          <SourcePlatformBadge platform={job.sourcePlatform} />
          <div className="font-mono text-xs text-slate/70">{job.sourceJobId}</div>
        </div>
      </td>

      <td className="px-5 py-4 align-top text-slate">{formatPostedDate(job.postedAt)}</td>

      <td className="px-5 py-4 align-top">
        <ValidationBlock job={job} />
      </td>

      <td className="px-5 py-4 align-top">
        <ResultActions
          job={job}
          isRevalidating={props.isRevalidating}
          onRevalidate={props.onRevalidate}
        />
      </td>
    </tr>
  );
}

function JobCard(props: {
  job: JobListing;
  isRevalidating: boolean;
  onRevalidate: ResultsTableProps["onRevalidate"];
}) {
  const { job } = props;
  const experienceSummary = summarizeExperienceConfidence(job.experienceClassification);

  return (
    <article className="rounded-[24px] border border-ink/10 bg-[rgba(255,255,255,0.88)] p-4 shadow-[0_12px_24px_rgba(15,23,42,0.05)]">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold leading-7 text-ink">{job.title}</div>
          <div className="text-sm leading-6 text-slate">{job.company}</div>
        </div>
        <ValidationPill status={job.linkStatus} />
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <SourcePlatformBadge platform={job.sourcePlatform} />
        <SubtleBadge>{formatPostedDate(job.postedAt)}</SubtleBadge>
        {job.sourceProvenance.length > 1 ? (
          <SubtleBadge>{job.sourceProvenance.length} merged sources</SubtleBadge>
        ) : null}
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2">
        <DetailBlock label="Location" value={job.locationText} />
        <DetailBlock label="Source job ID" value={job.sourceJobId} subdued />
        <DetailBlock
          label="Validation"
          value={
            job.lastValidatedAt
              ? `Checked ${formatRelativeMoment(job.lastValidatedAt)}`
              : "Deferred after crawl"
          }
        />
        <DetailBlock label="Posting" value={jobPostingUrl(job)} subdued />
      </div>

      <div className="mt-4 rounded-[20px] border border-ink/10 bg-sand/45 px-4 py-4">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
          Experience readout
        </div>
        <div className="mt-3">
          <ExperienceBlock job={job} summary={experienceSummary} />
        </div>
      </div>

      <div className="mt-4">
        <ResultActions
          job={job}
          isRevalidating={props.isRevalidating}
          onRevalidate={props.onRevalidate}
        />
      </div>
    </article>
  );
}

function ExperienceBlock(props: {
  job: JobListing;
  summary: ReturnType<typeof summarizeExperienceConfidence>;
}) {
  return (
    <div className="space-y-1">
      <div className="inline-flex flex-wrap gap-2">
        <span className="rounded-full border border-ink/10 bg-white px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-ink">
          {labelForExperience(props.job.experienceLevel)}
        </span>
        {props.job.experienceClassification?.isUnspecified ? (
          <span className="rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-amber-900">
            Unspecified
          </span>
        ) : null}
      </div>
      <div className="text-sm font-medium text-ink">{props.summary.label}</div>
      <div className="text-xs uppercase tracking-[0.16em] text-slate/70">
        {props.summary.detail}
      </div>
    </div>
  );
}

function ValidationBlock(props: { job: JobListing }) {
  return (
    <div className="space-y-2">
      <ValidationPill status={props.job.linkStatus} />
      <div className="text-sm text-slate">
        {props.job.lastValidatedAt
          ? `Checked ${formatRelativeMoment(props.job.lastValidatedAt)}`
          : "Deferred after crawl"}
      </div>
    </div>
  );
}

function ResultActions(props: {
  job: JobListing;
  isRevalidating: boolean;
  onRevalidate: ResultsTableProps["onRevalidate"];
}) {
  return (
    <div className="flex flex-wrap gap-2">
      <a
        href={jobPostingUrl(props.job)}
        target="_blank"
        rel="noreferrer"
        className={cn(
          "rounded-full px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] transition",
          "bg-ink text-white hover:bg-tide",
        )}
      >
        View Post
      </a>
      <button
        type="button"
        onClick={() => props.onRevalidate(props.job._id)}
        disabled={props.isRevalidating}
        className="rounded-full border border-ink/15 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-ink transition hover:border-ink hover:bg-ink hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
      >
        {props.isRevalidating
          ? "Checking..."
          : props.job.lastValidatedAt
            ? "Revalidate"
            : "Validate"}
      </button>
    </div>
  );
}

function DetailBlock(props: {
  label: string;
  value: string;
  subdued?: boolean;
}) {
  return (
    <div className="rounded-[18px] border border-ink/10 bg-white/75 px-3 py-3">
      <div className="font-mono text-[11px] uppercase tracking-[0.16em] text-slate/75">
        {props.label}
      </div>
      <div
        className={cn(
          "mt-2 text-sm leading-6",
          props.subdued ? "font-mono text-slate/70" : "text-ink",
        )}
      >
        {props.value}
      </div>
    </div>
  );
}

function SourcePlatformBadge(props: { platform: JobListing["sourcePlatform"] }) {
  return (
    <span className="inline-flex rounded-full border border-ink/10 bg-white px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-ink">
      {labelForProviderPlatform(props.platform)}
    </span>
  );
}

function SubtleBadge(props: { children: React.ReactNode }) {
  return (
    <span className="inline-flex rounded-full border border-ink/10 bg-sand/50 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-slate/80">
      {props.children}
    </span>
  );
}

function ValidationPill(props: { status: JobListing["linkStatus"] }) {
  return (
    <span
      className={cn(
        "inline-flex rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em]",
        props.status === "valid" && "bg-pine/10 text-pine",
        props.status === "unknown" && "bg-tide/10 text-tide",
        props.status === "stale" && "bg-amber-100 text-amber-800",
        props.status === "invalid" && "bg-red-100 text-red-700",
      )}
    >
      {labelForLinkStatus(props.status)}
    </span>
  );
}

function summarizeLinkSignals(jobs: JobListing[]) {
  const counts = jobs.reduce(
    (summary, job) => {
      summary[job.linkStatus] += 1;
      return summary;
    },
    {
      valid: 0,
      stale: 0,
      invalid: 0,
      unknown: 0,
    },
  );

  return [
    {
      label: "Valid",
      value: counts.valid,
      tone: "bg-pine/10 text-pine",
    },
    {
      label: "Stale",
      value: counts.stale,
      tone: "bg-amber-100 text-amber-800",
    },
    {
      label: "Invalid",
      value: counts.invalid,
      tone: "bg-red-100 text-red-700",
    },
    {
      label: "Unknown",
      value: counts.unknown,
      tone: "bg-tide/10 text-tide",
    },
  ].filter((signal) => signal.value > 0);
}
