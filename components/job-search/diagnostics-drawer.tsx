"use client";

import React from "react";

import type { CrawlResponse, SearchDocument } from "@/lib/types";
import type { SearchFilters } from "@/lib/types";
import { crawlModeOptions, experienceModeOptions, labelForProviderPlatform, labelForProviderStatus } from "@/components/job-crawler/ui-config";
import { describeSelectedPlatforms } from "@/components/job-search/helpers";
import { cn, formatRelativeMoment } from "@/lib/utils";
import type { AgentDiagnostics } from "@/lib/server/agent/types";

type DiagnosticsDrawerProps = {
  activeResult: CrawlResponse | null;
  agentDiagnostics?: AgentDiagnostics | null;
  recentSearches: SearchDocument[];
  filters: SearchFilters;
  onLoadSearch: (searchId: string) => void;
  onRerunSearch: (searchId?: string) => void;
  onSetCrawlMode: (value: SearchFilters["crawlMode"]) => void;
  onSetExperienceMatchMode: (value: SearchFilters["experienceMatchMode"]) => void;
  onToggleIncludeUnspecified: () => void;
};

export function DiagnosticsDrawer(props: DiagnosticsDrawerProps) {
  const sourceResults = props.activeResult?.sourceResults ?? [];
  const sessionDiagnostics = props.activeResult?.diagnostics.session;

  return (
    <details className="group rounded-[20px] border border-ink/8 bg-white/92 px-5 py-4 shadow-[0_18px_48px_rgba(15,23,42,0.05)]">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4">
        <div>
          <div className="text-sm font-semibold text-ink">Recent searches and debug details</div>
          <div className="mt-1 text-sm text-slate">
            Operational controls, history, and crawl diagnostics stay available here without dominating the main search flow.
          </div>
        </div>
        <span className="rounded-full border border-ink/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate transition group-open:bg-mist/45">
          Expand
        </span>
      </summary>

      <div className="mt-5 grid gap-6 border-t border-ink/8 pt-5">
        <section className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
          <div className="space-y-4">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
                Crawl mode
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {crawlModeOptions.map((option) => (
                  <InlineToggle
                    key={option.value}
                    label={option.label}
                    selected={(props.filters.crawlMode ?? "fast") === option.value}
                    onClick={() => props.onSetCrawlMode(option.value)}
                  />
                ))}
              </div>
            </div>

            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
                Experience matching
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {experienceModeOptions.map((option) => (
                  <InlineToggle
                    key={option.value}
                    label={option.label}
                    selected={(props.filters.experienceMatchMode ?? "balanced") === option.value}
                    onClick={() => props.onSetExperienceMatchMode(option.value)}
                  />
                ))}
                <InlineToggle
                  label="Include unspecified"
                  selected={
                    props.filters.includeUnspecifiedExperience === true ||
                    props.filters.experienceMatchMode === "broad"
                  }
                  onClick={props.onToggleIncludeUnspecified}
                />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-3">
              <StatCard
                label="Platforms"
                value={describeSelectedPlatforms(props.filters)}
              />
              <StatCard
                label="Recent searches"
                value={`${props.recentSearches.length}`}
              />
              <StatCard
                label="Current status"
                value={props.activeResult?.crawlRun.status ?? "idle"}
              />
            </div>
          </div>

          <div className="rounded-[24px] border border-ink/8 bg-mist/35 px-5 py-5">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
              Search history
            </div>
            <div className="mt-3 space-y-3">
              {props.recentSearches.length === 0 ? (
                <p className="text-sm leading-6 text-slate">
                  Searches will appear here after the first successful crawl.
                </p>
              ) : (
                props.recentSearches.slice(0, 5).map((search) => (
                  <div
                    key={search._id}
                    className="rounded-[20px] border border-ink/8 bg-white px-4 py-4"
                  >
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0">
                        <div className="truncate text-sm font-semibold text-ink">
                          {search.filters.title}
                        </div>
                        <div className="mt-1 text-sm text-slate">
                          Updated {formatRelativeMoment(search.updatedAt)}
                        </div>
                      </div>
                      <div className="flex gap-2">
                        <button
                          type="button"
                          onClick={() => props.onLoadSearch(search._id)}
                          className="rounded-full border border-ink/10 px-3 py-2 text-xs font-medium text-ink transition hover:border-ink/25 hover:bg-mist/45"
                        >
                          Load
                        </button>
                        <button
                          type="button"
                          onClick={() => props.onRerunSearch(search._id)}
                          className="rounded-full bg-ink px-3 py-2 text-xs font-medium text-white transition hover:bg-tide"
                        >
                          Rerun
                        </button>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </section>

        {props.agentDiagnostics?.agentEnabled ? (
          <AgentDiagnosticsSection agentDiagnostics={props.agentDiagnostics} />
        ) : null}

        {props.activeResult ? (
          <section className="space-y-4">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
                Crawl diagnostics
              </div>
              <div className="mt-2 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard
                  label="Indexed visible"
                  value={`${sessionDiagnostics?.indexedResultsCount ?? 0}`}
                />
                <StatCard
                  label="Supplemental visible"
                  value={`${sessionDiagnostics?.supplementalResultsCount ?? 0}`}
                />
                <StatCard
                  label="Supplemental status"
                  value={describeSupplementalStatus(sessionDiagnostics)}
                />
                <StatCard
                  label="Trigger"
                  value={describeSupplementalReason(sessionDiagnostics?.triggerReason)}
                />
                <StatCard
                  label="Discovered sources"
                  value={`${props.activeResult.diagnostics.discoveredSources}`}
                />
                <StatCard
                  label="Crawled sources"
                  value={`${props.activeResult.diagnostics.crawledSources}`}
                />
                <StatCard
                  label="Provider failures"
                  value={`${props.activeResult.diagnostics.providerFailures}`}
                />
                <StatCard
                  label="Validation deferred"
                  value={`${props.activeResult.diagnostics.validationDeferred}`}
                />
              </div>
              {sessionDiagnostics?.triggerExplanation ? (
                <p className="mt-3 text-sm leading-6 text-slate">
                  {sessionDiagnostics.triggerExplanation}
                </p>
              ) : null}
            </div>

            {sourceResults.length > 0 ? (
              <div className="grid gap-3 xl:grid-cols-2">
                {sourceResults.map((sourceResult) => (
                  <article
                    key={`${sourceResult.provider}-${sourceResult._id}`}
                    className="rounded-[24px] border border-ink/8 bg-white px-5 py-4"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="font-semibold text-ink">
                          {labelForProviderPlatform(sourceResult.provider)}
                        </div>
                        <div className="mt-1 text-sm text-slate">
                          {sourceResult.sourceCount} sources • {sourceResult.fetchedCount} fetched
                          • {sourceResult.savedCount} saved
                        </div>
                      </div>
                      <span
                        className={cn(
                          "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                          sourceResult.status === "running" && "bg-tide/10 text-tide",
                          sourceResult.status === "success" && "bg-pine/10 text-pine",
                          sourceResult.status === "partial" && "bg-amber-100 text-amber-900",
                          sourceResult.status === "timed_out" && "bg-amber-100 text-amber-900",
                          sourceResult.status === "failed" && "bg-red-100 text-red-700",
                          sourceResult.status === "unsupported" && "bg-tide/10 text-tide",
                        )}
                      >
                        {labelForProviderStatus(sourceResult.status)}
                      </span>
                    </div>
                    {sourceResult.errorMessage ? (
                      <p className="mt-3 text-sm leading-6 text-red-700">
                        {sourceResult.errorMessage}
                      </p>
                    ) : null}
                  </article>
                ))}
              </div>
            ) : null}
          </section>
        ) : null}
      </div>
    </details>
  );
}

function InlineToggle(props: {
  label: string;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      aria-pressed={props.selected}
      className={cn(
        "rounded-full border px-4 py-2 text-sm transition",
        props.selected
          ? "border-ink bg-ink text-white"
          : "border-ink/10 bg-white text-slate hover:border-ink/25 hover:bg-mist/45",
      )}
    >
      {props.label}
    </button>
  );
}

function StatCard(props: { label: string; value: string }) {
  return (
    <div className="rounded-[20px] border border-ink/8 bg-white px-4 py-4">
      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
        {props.label}
      </div>
      <div className="mt-2 text-sm font-medium leading-6 text-ink">{props.value}</div>
    </div>
  );
}

function describeSupplementalStatus(
  sessionDiagnostics: CrawlResponse["diagnostics"]["session"] | undefined,
) {
  if (sessionDiagnostics?.supplementalRunning) {
    return "running";
  }

  if (sessionDiagnostics?.supplementalQueued) {
    return "complete";
  }

  return "not queued";
}

function describeSupplementalReason(
  reason: NonNullable<CrawlResponse["diagnostics"]["session"]>["triggerReason"] | undefined,
) {
  if (reason === "indexed_coverage_sufficient") {
    return "strong index";
  }

  if (reason === "reused_completed_coverage") {
    return "reused coverage";
  }

  if (reason === "freshness_recovery") {
    return "freshness";
  }

  if (reason === "retry_incomplete_previous_run") {
    return "retry incomplete";
  }

  if (reason === "insufficient_indexed_coverage") {
    return "coverage gap";
  }

  return "n/a";
}

function AgentDiagnosticsSection(props: { agentDiagnostics: AgentDiagnostics }) {
  const d = props.agentDiagnostics;

  return (
    <section className="space-y-3 rounded-[20px] border border-tide/12 bg-tide/3 px-5 py-5">
      <div>
        <div className="flex items-center gap-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-tide/75">
            Agent Diagnostics
          </span>
          <span className="rounded-full bg-tide/10 px-2 py-0.5 text-[10px] font-semibold text-tide">
            {d.agentMode}
          </span>
          {d.crawlTriggered ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
              crawl triggered
            </span>
          ) : null}
        </div>
      </div>

      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
        <StatCard label="DB candidates" value={`${d.dbCandidateCount}`} />
        <StatCard label="DB fresh" value={`${d.dbFreshCount}`} />
        <StatCard
          label="Quality score"
          value={`${(d.qualityScore * 100).toFixed(0)}%`}
        />
        <StatCard
          label="Duplicates"
          value={`${(d.duplicateRate * 100).toFixed(0)}%`}
        />
        <StatCard label="Title variants" value={`${d.titleVariantsUsed.length}`} />
        <StatCard
          label="Location clauses"
          value={`${d.locationClausesUsed.length}`}
        />
        <StatCard
          label="Filtered: title"
          value={`${d.filteredByTitle}`}
        />
        <StatCard
          label="Filtered: location"
          value={`${d.filteredByLocation}`}
        />
        <StatCard
          label="Filtered: seniority"
          value={`${d.filteredBySeniority}`}
        />
      </div>

      {d.crawlReason ? (
        <div className="rounded-[16px] border border-ink/8 bg-white px-4 py-3">
          <div className="text-xs font-semibold text-ink">Crawl reason</div>
          <p className="mt-1 text-xs leading-5 text-slate">{d.crawlReason}</p>
        </div>
      ) : null}

      {d.plannedPlatforms.length > 0 ? (
        <div className="rounded-[16px] border border-ink/8 bg-white px-4 py-3">
          <div className="text-xs font-semibold text-ink">Planned platforms</div>
          <div className="mt-1 flex flex-wrap gap-1">
            {d.plannedPlatforms.map((p) => (
              <span
                key={p}
                className="rounded-full border border-ink/10 px-2 py-0.5 text-[10px] font-medium text-slate"
              >
                {p}{" "}
                {d.platformSupportStatuses[p] ? `(${d.platformSupportStatuses[p]})` : ""}
              </span>
            ))}
          </div>
        </div>
      ) : null}

      {d.decisionChain.length > 0 ? (
        <details className="rounded-[16px] border border-ink/8 bg-white px-4 py-3">
          <summary className="cursor-pointer text-xs font-semibold text-ink">
            Decision chain ({d.decisionChain.length} steps)
          </summary>
          <ul className="mt-2 space-y-1">
            {d.decisionChain.map((step, i) => (
              <li key={i} className="text-[11px] leading-5 text-slate">
                {i + 1}. {step}
              </li>
            ))}
          </ul>
        </details>
      ) : null}

      {d.warnings.length > 0 ? (
        <div className="rounded-[16px] border border-amber-200 bg-amber-50 px-4 py-3">
          <div className="text-xs font-semibold text-amber-800">
            Warnings ({d.warnings.length})
          </div>
          <ul className="mt-1 space-y-0.5">
            {d.warnings.map((w, i) => (
              <li key={i} className="text-[11px] leading-5 text-amber-700">
                {w}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="text-[10px] text-slate/60">
        Stage timings: intent={d.stageTimingsMs.intentParsing}ms,
        expansion={d.stageTimingsMs.queryExpansion}ms,
        db={d.stageTimingsMs.dbQuery}ms,
        quality={d.stageTimingsMs.qualityEvaluation}ms,
        total={d.stageTimingsMs.total}ms
      </div>
    </section>
  );
}
