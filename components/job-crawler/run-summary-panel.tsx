"use client";

import type { CrawlResponse } from "@/lib/types";
import { formatRelativeMoment } from "@/lib/utils";
import { describeValidationMode } from "@/components/job-crawler/ui-config";

type RunSummaryPanelProps = {
  result: CrawlResponse;
};

export function RunSummaryPanel(props: RunSummaryPanelProps) {
  const { result } = props;

  const flowMetrics = [
    {
      label: "Discovered",
      value: result.diagnostics.discoveredSources,
      description: "Configured public sources found for this search.",
    },
    {
      label: "Crawled",
      value: result.diagnostics.crawledSources,
      description: "Sources actually handed to active providers.",
    },
    {
      label: "Fetched",
      value: result.crawlRun.totalFetchedJobs,
      description: "Raw jobs before title, location, and experience filtering.",
    },
    {
      label: "Matched",
      value: result.crawlRun.totalMatchedJobs,
      description: "Jobs that survived the configured filters.",
    },
    {
      label: "Saved",
      value: result.crawlRun.dedupedJobs,
      description: "Final normalized jobs kept after dedupe.",
    },
  ];

  return (
    <section className="rounded-[28px] border border-ink/10 bg-ink p-5 text-white shadow-soft sm:p-6">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="max-w-2xl">
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-white/60">
            Crawl metrics
          </p>
          <h2 className="mt-3 text-2xl font-semibold capitalize">
            {result.crawlRun.status}
          </h2>
          <p className="mt-2 text-sm leading-6 text-white/70">
            Updated {formatRelativeMoment(result.search.updatedAt)}. Read the funnel left to right
            to see how discovery turned into saved jobs.
          </p>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 xl:w-[360px]">
          <SummaryBadge label="Validation mode" value={describeValidationMode(result.search.filters.crawlMode)} />
          <SummaryBadge
            label="Experience policy"
            value={result.search.filters.experienceMatchMode ?? "balanced"}
          />
          <SummaryBadge
            label="Unspecified levels"
            value={
              result.search.filters.includeUnspecifiedExperience
                ? "Included"
                : "Excluded"
            }
          />
          <SummaryBadge
            label="Provider failures"
            value={`${result.diagnostics.providerFailures}`}
          />
        </div>
      </div>

      <div className="mt-5">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-white/55">
          Discovery to save funnel
        </div>
        <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-[repeat(5,minmax(0,1fr))_1.15fr]">
          {flowMetrics.map((metric) => (
            <SummaryMetricCard
              key={metric.label}
              label={metric.label}
              value={metric.value}
              description={metric.description}
            />
          ))}

          <SummaryMetricCard
            label="Validation behavior"
            value={describeValidationMode(result.search.filters.crawlMode)}
            description="Fast defers checks, balanced validates the newest saved links inline, and deep validates every saved link before finishing."
          />
        </div>
      </div>
    </section>
  );
}

function SummaryMetricCard(props: {
  label: string;
  value: number | string;
  description: string;
}) {
  return (
    <div className="rounded-[24px] border border-white/10 bg-white/5 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-white/60">
        {props.label}
      </div>
      <div className="mt-2 text-2xl font-semibold text-white">{props.value}</div>
      <div className="mt-2 text-sm leading-6 text-white/70">{props.description}</div>
    </div>
  );
}

function SummaryBadge(props: { label: string; value: string }) {
  return (
    <div className="rounded-[22px] border border-white/10 bg-white/5 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-white/60">
        {props.label}
      </div>
      <div className="mt-2 text-sm font-semibold uppercase tracking-[0.16em] text-white">
        {props.value}
      </div>
    </div>
  );
}
