"use client";

import React from "react";

import type { JobListing } from "@/lib/types";
import { formatPostedDate, formatRelativeMoment, jobPostingUrl } from "@/lib/utils";
import { labelForProviderPlatform } from "@/components/job-crawler/ui-config";
import {
  getExperienceLabel,
  getSponsorshipLabel,
  getWorkplaceLabel,
  labelForEmploymentType,
} from "@/components/job-search/helpers";

type JobDetailPanelProps = {
  job?: JobListing;
  isRevalidating?: boolean;
  onRevalidate?: (jobId: string) => Promise<void>;
};

export function JobDetailPanel(props: JobDetailPanelProps) {
  if (!props.job) {
    return (
      <section className="rounded-lg border border-slate-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-ink">Select a job to see details</h2>
        <p className="mt-2 text-sm leading-6 text-slate">
          Job description, source, location, and application link will appear here.
        </p>
      </section>
    );
  }

  const job = props.job;
  const postingUrl = jobPostingUrl(job);
  const workplaceLabel = getWorkplaceLabel(job);
  const postedLabel =
    job.postingDate || job.postedAt
      ? formatPostedDate(job.postingDate ?? job.postedAt)
      : "Date unavailable";

  return (
    <section className="rounded-lg border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 p-5">
        <div className="flex flex-col gap-4">
          <div>
            <h2 className="text-2xl font-semibold leading-tight text-ink">{job.title}</h2>
            <p className="mt-2 text-base font-semibold text-slate">{job.company}</p>
            <p className="mt-1 text-sm text-slate">
              {job.locationRaw || job.locationText || "Location unavailable"}
            </p>
          </div>

          <a
            href={postingUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex min-h-11 items-center justify-center rounded-md bg-[#0a66c2] px-5 text-sm font-semibold text-white transition hover:bg-[#004182]"
          >
            Apply
          </a>
        </div>
      </div>

      <div className="space-y-5 p-5">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
          <DetailItem label="Platform" value={labelForProviderPlatform(job.sourcePlatform)} />
          <DetailItem label="Posted" value={postedLabel} />
          <DetailItem
            label="Last seen"
            value={formatRelativeMoment(job.lastSeenAt ?? job.indexedAt)}
          />
          <DetailItem label="Workplace" value={workplaceLabel ?? "Not specified"} />
          <DetailItem label="Employment type" value={labelForEmploymentType(job.employmentType)} />
          <DetailItem label="Experience" value={getExperienceLabel(job) ?? "Not specified"} />
          <DetailItem label="Sponsorship" value={getSponsorshipLabel(job)} />
        </div>

        <section>
          <h3 className="text-base font-semibold text-ink">About the job</h3>
          <p className="mt-3 whitespace-pre-line text-sm leading-7 text-slate">
            {job.descriptionSnippet ||
              "The original posting has the full job description and application details."}
          </p>
        </section>

        <section className="rounded-lg border border-slate-200 bg-slate-50 p-4">
          <h3 className="text-sm font-semibold text-ink">Job metadata</h3>
          <dl className="mt-3 space-y-2 text-sm text-slate">
            <MetadataRow label="Normalized title" value={job.normalizedTitle || job.titleNormalized} />
            <MetadataRow
              label="Normalized location"
              value={job.normalizedLocation || job.locationNormalized}
            />
            <MetadataRow label="Job ID" value={job.sourceJobId} />
          </dl>
        </section>
      </div>
    </section>
  );
}

function DetailItem(props: { label: string; value?: string }) {
  return (
    <div className="rounded-md border border-slate-200 bg-white px-3 py-3">
      <div className="text-xs font-semibold text-slate/70">{props.label}</div>
      <div className="mt-1 text-sm font-medium text-ink">{props.value || "Not specified"}</div>
    </div>
  );
}

function MetadataRow(props: { label: string; value?: string }) {
  if (!props.value) {
    return null;
  }

  return (
    <div className="grid gap-1 sm:grid-cols-[140px_minmax(0,1fr)]">
      <dt className="font-medium text-slate/80">{props.label}</dt>
      <dd className="min-w-0 break-words text-ink">{props.value}</dd>
    </div>
  );
}
