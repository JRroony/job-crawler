"use client";

import type { CrawlDiagnostics } from "@/lib/types";
import { cn } from "@/lib/utils";

type DiagnosticsBreakdownPanelProps = {
  diagnostics: CrawlDiagnostics;
};

export function DiagnosticsBreakdownPanel(
  props: DiagnosticsBreakdownPanelProps,
) {
  const metrics = [
    {
      key: "excludedByTitle",
      label: "Excluded by title",
      value: props.diagnostics.excludedByTitle,
      description: "Fetched jobs that never matched the requested role title.",
    },
    {
      key: "excludedByLocation",
      label: "Excluded by location",
      value: props.diagnostics.excludedByLocation,
      description: "Title matches removed by country, state, or city filters.",
    },
    {
      key: "excludedByExperience",
      label: "Excluded by experience",
      value: props.diagnostics.excludedByExperience,
      description: "Title and location matches removed by the experience policy.",
    },
    {
      key: "dedupedOut",
      label: "Deduped out",
      value: props.diagnostics.dedupedOut,
      description: "Overlapping matches merged away before the final save set.",
    },
    {
      key: "validationDeferred",
      label: "Validation deferred",
      value: props.diagnostics.validationDeferred,
      description: "Saved jobs still waiting on a separate link validation pass.",
    },
  ] as const;

  const notableSignals = metrics.filter((metric) => metric.value > 0);

  return (
    <section className="rounded-[28px] border border-ink/10 bg-white/88 p-5 shadow-soft backdrop-blur sm:p-6">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
            Drop-off diagnostics
          </p>
          <h2 className="mt-3 text-2xl font-semibold text-ink">
            Why jobs fell away between fetch and save
          </h2>
        </div>
        <p className="max-w-2xl text-sm leading-6 text-slate">
          Provider failures are tracked separately. These counters explain what happened after jobs
          were fetched: filtering pressure, dedupe, and deferred validation.
        </p>
      </div>

      <div className="mt-5 grid gap-4 xl:grid-cols-[0.8fr_1.2fr]">
        <div className="rounded-[24px] border border-ink/10 bg-[rgba(244,239,230,0.72)] px-4 py-4">
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ember">
            Read this panel as
          </div>
          <div className="mt-3 space-y-3 text-sm leading-6 text-slate">
            <p>
              High title, location, or experience counts usually mean the crawl found jobs, but
              the current filters were tighter than the public listings.
            </p>
            <p>
              High dedupe counts usually mean multiple sources converged on the same underlying
              posting.
            </p>
            <p>
              Deferred validation is operational, not a fetch failure. It simply reflects the
              chosen crawl mode.
            </p>
          </div>
        </div>

        <div className="rounded-[24px] border border-ink/10 bg-white/70 px-4 py-4">
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
            Strongest signals
          </div>
          {notableSignals.length > 0 ? (
            <div className="mt-3 grid gap-3 sm:grid-cols-2">
              {notableSignals.slice(0, 4).map((metric) => (
                <div
                  key={metric.label}
                  className="rounded-[20px] border border-ink/10 bg-sand/50 px-4 py-3"
                >
                  <div className="text-sm font-semibold text-ink">
                    {metric.label}
                  </div>
                  <div className="mt-1 text-sm leading-6 text-slate">
                    {metric.value} event{metric.value === 1 ? "" : "s"} recorded.
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-3 rounded-[20px] border border-pine/20 bg-pine/5 px-4 py-4 text-sm leading-6 text-slate">
              No major filter, dedupe, or deferred-validation pressure was recorded for this run.
            </div>
          )}
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {metrics.map((metric) => (
          <MetricCard
            key={metric.label}
            label={metric.label}
            value={metric.value}
            description={metric.description}
            tone={metricTone(metric.key, metric.value)}
          />
        ))}
      </div>
    </section>
  );
}

function MetricCard(props: {
  label: string;
  value: number;
  description: string;
  tone: "neutral" | "amber" | "tide";
}) {
  return (
    <div
      className={cn(
        "rounded-[22px] border px-4 py-4",
        props.tone === "amber" && "border-amber-200 bg-amber-50/70",
        props.tone === "tide" && "border-tide/20 bg-tide/5",
        props.tone === "neutral" && "border-ink/10 bg-sand/45",
      )}
    >
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
        {props.label}
      </div>
      <div className="mt-2 text-2xl font-semibold text-ink">{props.value}</div>
      <div className="mt-2 text-sm leading-6 text-slate">{props.description}</div>
    </div>
  );
}

function metricTone(
  key: "excludedByTitle" | "excludedByLocation" | "excludedByExperience" | "dedupedOut" | "validationDeferred",
  value: number,
) {
  if (value === 0) {
    return "neutral" as const;
  }

  if (key === "validationDeferred") {
    return "tide" as const;
  }

  return "amber" as const;
}
