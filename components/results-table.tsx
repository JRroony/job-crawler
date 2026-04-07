"use client";

import React from "react";
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
              Normalized review list
            </p>
            <h3 className="mt-3 text-2xl font-semibold text-ink">
              {jobs.length} saved job{jobs.length === 1 ? "" : "s"} ready to review
            </h3>
            <p className="mt-2 text-sm leading-6 text-slate">
              The visible action always points back to the original posting page while link
              validation tracks whether the application path is still healthy.
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

      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm">
          <thead className="border-b border-ink/10 bg-sand/60 text-xs uppercase tracking-[0.22em] text-slate">
            <tr>
              <th className="px-5 py-4">Role</th>
              <th className="px-5 py-4">Location</th>
              <th className="px-5 py-4">Source</th>
              <th className="px-5 py-4">Posted</th>
              <th className="px-5 py-4">Level</th>
              <th className="px-5 py-4">Link</th>
              <th className="px-5 py-4">Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => {
              const isRevalidating = revalidatingIds.includes(job._id);

              return (
                <tr
                  key={job._id}
                  className="border-b border-ink/10 transition hover:bg-mist/35 last:border-b-0"
                >
                  <td className="px-5 py-4 align-top">
                    <div className="space-y-1">
                      <div className="text-base font-semibold text-ink">{job.title}</div>
                      <div className="text-slate">{job.company}</div>
                      <div className="font-mono text-xs text-slate/70">
                        Validated {formatRelativeMoment(job.lastValidatedAt)}
                      </div>
                      {job.sourceProvenance.length > 1 ? (
                        <div className="inline-flex rounded-full border border-ink/10 bg-white px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-slate/80">
                          {job.sourceProvenance.length} records merged
                        </div>
                      ) : null}
                    </div>
                  </td>
                  <td className="px-5 py-4 align-top text-slate">
                    {job.locationText}
                  </td>
                  <td className="px-5 py-4 align-top">
                    <div className="space-y-1">
                      <div className="font-medium capitalize text-ink">
                        {job.sourcePlatform.replace("_", " ")}
                      </div>
                      <div className="font-mono text-xs text-slate/70">
                        {job.sourceJobId}
                      </div>
                    </div>
                  </td>
                  <td className="px-5 py-4 align-top text-slate">
                    {formatPostedDate(job.postedAt)}
                  </td>
                  <td className="px-5 py-4 align-top text-slate">
                    {labelForExperience(job.experienceLevel)}
                  </td>
                  <td className="px-5 py-4 align-top">
                    <span
                      className={cn(
                        "inline-flex rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em]",
                        job.linkStatus === "valid" && "bg-pine/10 text-pine",
                        job.linkStatus === "unknown" && "bg-tide/10 text-tide",
                        job.linkStatus === "stale" && "bg-amber-100 text-amber-800",
                        job.linkStatus === "invalid" && "bg-red-100 text-red-700",
                      )}
                    >
                      {labelForLinkStatus(job.linkStatus)}
                    </span>
                  </td>
                  <td className="px-5 py-4 align-top">
                    <div className="flex flex-wrap gap-2">
                      <a
                        href={jobPostingUrl(job)}
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
                        onClick={() => onRevalidate(job._id)}
                        disabled={isRevalidating}
                        className="rounded-full border border-ink/15 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-ink transition hover:border-ink hover:bg-ink hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {isRevalidating ? "Checking..." : "Revalidate"}
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
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
