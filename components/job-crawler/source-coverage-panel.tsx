"use client";

import type { CrawlResponse } from "@/lib/types";
import { cn } from "@/lib/utils";
import {
  labelForProviderPlatform,
  labelForProviderStatus,
  sourceStatusTone,
} from "@/components/job-crawler/ui-config";

type SourceCoveragePanelProps = {
  sourceResults: CrawlResponse["sourceResults"];
};

export function SourceCoveragePanel(props: SourceCoveragePanelProps) {
  const orderedSourceResults = [...props.sourceResults].sort(
    (left, right) => statusRank(left.status) - statusRank(right.status),
  );
  const summary = orderedSourceResults.reduce(
    (counts, sourceResult) => {
      counts[sourceResult.status] += 1;
      return counts;
    },
    {
      running: 0,
      success: 0,
      partial: 0,
      failed: 0,
      aborted: 0,
      unsupported: 0,
    },
  );

  return (
    <section className="rounded-[28px] border border-ink/10 bg-white/88 p-5 shadow-soft backdrop-blur sm:p-6">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
            Provider diagnostics
          </p>
          <h2 className="mt-3 text-2xl font-semibold text-ink">
            Source coverage by provider family
          </h2>
        </div>
        <p className="max-w-2xl text-sm leading-6 text-slate">
          Each card shows the provider status first, then the source and job funnel. That makes it
          easier to separate true provider trouble from normal filter pressure.
        </p>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <StatusSummaryPill label="Running" value={summary.running} tone="running" />
        <StatusSummaryPill label="Healthy" value={summary.success} tone="success" />
        <StatusSummaryPill label="Degraded" value={summary.partial} tone="partial" />
        <StatusSummaryPill label="Failed" value={summary.failed} tone="failed" />
        {summary.aborted > 0 ? (
          <StatusSummaryPill label="Stopped" value={summary.aborted} tone="failed" />
        ) : null}
        {summary.unsupported > 0 ? (
          <StatusSummaryPill label="Limited" value={summary.unsupported} tone="unsupported" />
        ) : null}
      </div>

      <div className="mt-5 grid gap-4 xl:grid-cols-2">
        {orderedSourceResults.map((sourceResult) => {
          const tone = sourceStatusTone(sourceResult.status);

          return (
            <article
              key={`${sourceResult.provider}-${sourceResult._id}`}
              className={cn(
                "rounded-[26px] border px-5 py-5 shadow-[0_12px_24px_rgba(15,23,42,0.05)]",
                tone.card,
              )}
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="text-lg font-semibold text-ink">
                    {labelForProviderPlatform(sourceResult.provider)}
                  </div>
                  <div className="mt-1 text-sm leading-6 text-slate">
                    {sourceResult.sourceCount} source{sourceResult.sourceCount === 1 ? "" : "s"} →
                    {" "}
                    {sourceResult.fetchedCount} fetched →
                    {" "}
                    {sourceResult.matchedCount} matched →
                    {" "}
                    {sourceResult.savedCount} saved
                  </div>
                </div>
                <span
                  className={cn(
                    "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                    tone.badge,
                  )}
                >
                  {labelForProviderStatus(sourceResult.status)}
                </span>
              </div>

              <div className="mt-4 grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
                <div className="grid gap-3 sm:grid-cols-5 lg:grid-cols-3 xl:grid-cols-5">
                  <MetricPill label="Sources" value={sourceResult.sourceCount} />
                  <MetricPill label="Fetched" value={sourceResult.fetchedCount} />
                  <MetricPill label="Matched" value={sourceResult.matchedCount} />
                  <MetricPill label="Saved" value={sourceResult.savedCount} />
                  <MetricPill label="Issues" value={sourceResult.warningCount} />
                </div>

                <div className="space-y-3">
                  <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
                    Provider note
                  </div>
                  {sourceResult.errorMessage ? (
                    <div className="rounded-[22px] border border-red-200 bg-red-50 px-4 py-3 text-sm leading-6 text-red-800">
                      {sourceResult.errorMessage}
                    </div>
                  ) : sourceResult.warningCount > 0 ? (
                    <div className="rounded-[22px] border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-6 text-amber-900">
                      {sourceResult.warningCount} warning
                      {sourceResult.warningCount === 1 ? "" : "s"} recorded without a fatal error.
                      Coverage may be incomplete for this provider.
                    </div>
                  ) : (
                    <div className="rounded-[22px] border border-pine/20 bg-pine/5 px-4 py-3 text-sm leading-6 text-slate">
                      No provider-side issues were recorded for this run.
                    </div>
                  )}
                </div>
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function MetricPill(props: { label: string; value: number }) {
  return (
    <div className="rounded-[18px] border border-ink/10 bg-white/75 px-3 py-3">
      <div className="font-mono text-[11px] uppercase tracking-[0.16em] text-slate/75">
        {props.label}
      </div>
      <div className="mt-2 text-lg font-semibold text-ink">{props.value}</div>
    </div>
  );
}

function StatusSummaryPill(props: {
  label: string;
  value: number;
  tone: "running" | "success" | "partial" | "failed" | "unsupported";
}) {
  return (
    <span
      className={cn(
        "rounded-full px-3 py-2 text-xs font-semibold uppercase tracking-[0.16em]",
        props.tone === "running" && "bg-tide/10 text-tide",
        props.tone === "success" && "bg-pine/10 text-pine",
        props.tone === "partial" && "bg-amber-100 text-amber-900",
        props.tone === "failed" && "bg-red-100 text-red-700",
        props.tone === "unsupported" && "bg-tide/10 text-tide",
      )}
    >
      {props.label} {props.value}
    </span>
  );
}

function statusRank(status: SourceCoveragePanelProps["sourceResults"][number]["status"]) {
  if (status === "running") {
    return 0;
  }

  if (status === "failed") {
    return 1;
  }

  if (status === "aborted") {
    return 2;
  }

  if (status === "partial") {
    return 3;
  }

  if (status === "unsupported") {
    return 4;
  }

  return 5;
}
