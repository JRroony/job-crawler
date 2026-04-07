"use client";

import { useState } from "react";

import { ResultsTable } from "@/components/results-table";
import type { CrawlResponse, SearchDocument, SearchFilters } from "@/lib/types";
import { experienceLevels } from "@/lib/types";
import { cn, formatRelativeMoment, labelForExperience } from "@/lib/utils";

type JobCrawlerAppProps = {
  initialSearches: SearchDocument[];
  initialError?: string;
};

export type ViewState = "idle" | "loading" | "success" | "empty" | "partial" | "error";

type ZeroResultState = {
  title: string;
  description: string;
};

const initialFilters: SearchFilters = {
  title: "",
  country: "",
  state: "",
  city: "",
};

export function JobCrawlerApp({
  initialSearches,
  initialError,
}: JobCrawlerAppProps) {
  const [filters, setFilters] = useState<SearchFilters>(initialFilters);
  const [recentSearches, setRecentSearches] = useState(initialSearches);
  const [activeResult, setActiveResult] = useState<CrawlResponse | null>(null);
  const [viewState, setViewState] = useState<ViewState>(initialError ? "error" : "idle");
  const [message, setMessage] = useState(initialError ?? "");
  const [revalidatingIds, setRevalidatingIds] = useState<string[]>([]);

  async function submitSearch(nextFilters: SearchFilters) {
    setViewState("loading");
    setMessage("");

    try {
      const response = await fetch("/api/searches", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(nextFilters),
      });

      const payload = (await response.json()) as CrawlResponse & { error?: string; details?: unknown };
      if (!response.ok) {
        throw new Error(payload.error ?? "The crawl request failed.");
      }

      setActiveResult(payload);
      setRecentSearches((current) => dedupeSearches([payload.search, ...current]));
      setViewState(resolveViewState(payload));
    } catch (error) {
      setViewState("error");
      setMessage(error instanceof Error ? error.message : "The crawl request failed.");
    }
  }

  async function rerunActiveSearch(searchId?: string) {
    const id = searchId ?? activeResult?.search._id;
    if (!id) {
      await submitSearch(filters);
      return;
    }

    setViewState("loading");
    setMessage("");

    try {
      const response = await fetch(`/api/searches/${id}/rerun`, {
        method: "POST",
      });
      const payload = (await response.json()) as CrawlResponse & { error?: string };
      if (!response.ok) {
        throw new Error(payload.error ?? "The rerun request failed.");
      }

      setActiveResult(payload);
      setRecentSearches((current) => dedupeSearches([payload.search, ...current]));
      setViewState(resolveViewState(payload));
    } catch (error) {
      setViewState("error");
      setMessage(error instanceof Error ? error.message : "The rerun request failed.");
    }
  }

  async function loadSearch(searchId: string) {
    setViewState("loading");
    setMessage("");

    try {
      const response = await fetch(`/api/searches/${searchId}`);
      const payload = (await response.json()) as CrawlResponse & { error?: string };
      if (!response.ok) {
        throw new Error(payload.error ?? "The search could not be loaded.");
      }

      setActiveResult(payload);
      setFilters({
        ...payload.search.filters,
        country: payload.search.filters.country ?? "",
        state: payload.search.filters.state ?? "",
        city: payload.search.filters.city ?? "",
      });
      setViewState(resolveViewState(payload));
    } catch (error) {
      setViewState("error");
      setMessage(error instanceof Error ? error.message : "The search could not be loaded.");
    }
  }

  async function revalidateSingleJob(jobId: string) {
    setRevalidatingIds((current) => [...current, jobId]);

    try {
      const response = await fetch(`/api/jobs/${jobId}/revalidate`, {
        method: "POST",
      });
      const payload = (await response.json()) as { job?: CrawlResponse["jobs"][number]; error?: string };
      if (!response.ok || !payload.job) {
        throw new Error(payload.error ?? "The job could not be revalidated.");
      }

      setActiveResult((current) =>
        current
          ? {
              ...current,
              jobs: current.jobs
                .map((job) => (job._id === payload.job?._id ? payload.job : job))
                .sort(jobComparator),
            }
          : current,
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "The job could not be revalidated.");
    } finally {
      setRevalidatingIds((current) => current.filter((id) => id !== jobId));
    }
  }

  const metrics = activeResult
    ? [
        {
          label: "Fetched",
          value: activeResult.crawlRun.totalFetchedJobs,
        },
        {
          label: "Matched",
          value: activeResult.crawlRun.totalMatchedJobs,
        },
        {
          label: "Saved",
          value: activeResult.crawlRun.dedupedJobs,
        },
      ]
    : [];

  const selectedFilters = buildFilterBadges(filters);
  const providerSummary = activeResult
    ? summarizeSourceResults(activeResult.sourceResults)
    : null;

  return (
    <main className="relative overflow-hidden">
      <div className="mx-auto min-h-screen max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <section className="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
          <div className="relative overflow-hidden rounded-[40px] border border-white/70 bg-white/80 p-8 shadow-soft backdrop-blur xl:p-10">
            <div className="absolute inset-0 bg-grid-fade bg-[size:24px_24px] opacity-20" />
            <div className="absolute -left-20 top-0 h-52 w-52 rounded-full bg-ember/20 blur-3xl" />
            <div className="absolute right-0 top-0 h-56 w-56 rounded-full bg-tide/15 blur-3xl" />
            <div className="absolute bottom-0 left-1/3 h-40 w-40 rounded-full bg-pine/10 blur-3xl" />

            <div className="relative space-y-8">
              <div className="space-y-5">
                <div className="flex flex-wrap items-center gap-3">
                  <p className="font-mono text-xs uppercase tracking-[0.32em] text-ember">
                    Public-source job aggregation
                  </p>
                  <span className="rounded-full border border-ink/10 bg-white/80 px-4 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-ink/70">
                    Link validation + normalized storage
                  </span>
                </div>

                <div className="max-w-3xl space-y-4">
                  <h1 className="text-4xl font-bold leading-tight text-ink sm:text-5xl">
                    Set your filters and turn public job listings into a trustworthy review list.
                  </h1>
                  <p className="max-w-2xl text-base leading-7 text-slate">
                    Enter a role, add location and experience filters when they help, and the
                    crawler will collect public ATS results, normalize duplicates, and re-check
                    link health before saving everything into MongoDB.
                  </p>
                </div>

                <div className="grid gap-3 sm:grid-cols-3">
                  <HeroStat
                    label="Coverage"
                    value="4 provider paths"
                    description="Greenhouse, Lever, Ashby, plus configured company pages."
                  />
                  <HeroStat
                    label="Trust signal"
                    value="Redirect-aware links"
                    description="HEAD first, GET fallback, and stale-page detection on saved jobs."
                  />
                  <HeroStat
                    label="Memory"
                    value="Stored search history"
                    description="Searches, runs, jobs, and link validations stay queryable locally."
                  />
                </div>
              </div>

              <form
                className="grid gap-5 rounded-[32px] border border-ink/10 bg-[rgba(244,239,230,0.82)] p-5 md:p-6"
                onSubmit={(event) => {
                  event.preventDefault();
                  void submitSearch(cleanFilters(filters));
                }}
              >
                <div className="max-w-xl">
                  <p className="font-mono text-xs uppercase tracking-[0.28em] text-ember">
                    Search filters
                  </p>
                  <h2 className="mt-3 text-2xl font-semibold text-ink">
                    Tell the crawler what to look for
                  </h2>
                  <p className="mt-2 text-sm leading-6 text-slate">
                    Enter the role first, then add location and experience filters when you need to
                    narrow broad or noisy results.
                  </p>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  <Field
                    label="Target job title"
                    value={filters.title}
                    onChange={(value) => setFilters((current) => ({ ...current, title: value }))}
                    placeholder="Software Engineer"
                    required
                  />
                  <Field
                    label="Country"
                    value={filters.country ?? ""}
                    onChange={(value) => setFilters((current) => ({ ...current, country: value }))}
                    placeholder="United States"
                  />
                  <Field
                    label="State"
                    value={filters.state ?? ""}
                    onChange={(value) => setFilters((current) => ({ ...current, state: value }))}
                    placeholder="California"
                  />
                  <Field
                    label="City"
                    value={filters.city ?? ""}
                    onChange={(value) => setFilters((current) => ({ ...current, city: value }))}
                    placeholder="San Francisco"
                  />
                </div>

                <div className="space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="text-sm font-medium text-ink">Experience levels</span>
                    <span className="text-xs text-slate">
                      {filters.experienceLevels?.length
                        ? `${filters.experienceLevels.length} selected`
                        : "Any level"}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {experienceLevels.map((level) => {
                      const selected = Boolean(filters.experienceLevels?.includes(level));

                      return (
                        <button
                          key={level}
                          type="button"
                          aria-pressed={selected}
                          onClick={() =>
                            setFilters((current) => ({
                              ...current,
                              experienceLevels: toggleExperienceLevel(
                                current.experienceLevels,
                                level,
                              ),
                            }))
                          }
                          className={cn(
                            "rounded-full border px-4 py-2 text-sm font-medium transition",
                            selected
                              ? "border-ember bg-ember text-white shadow-[0_10px_24px_rgba(186,88,53,0.24)]"
                              : "border-ink/10 bg-white text-ink hover:border-ember/35 hover:bg-ember/5",
                          )}
                        >
                          {labelForExperience(level)}
                        </button>
                      );
                    })}
                  </div>
                  <p className="text-xs leading-5 text-slate">
                    Leave every level unselected to match any experience band.
                  </p>
                </div>

                <div className="flex flex-wrap gap-2">
                  {selectedFilters.length > 0 ? (
                    selectedFilters.map((filter) => (
                      <span
                        key={filter}
                        className="rounded-full border border-ink/10 bg-white px-3 py-1 text-xs font-medium text-ink/80"
                      >
                        {filter}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-slate">
                      Add a title, location, or experience level to preview the active filters.
                    </span>
                  )}
                </div>

                <div className="flex flex-wrap items-center gap-3">
                  <button
                    type="submit"
                    disabled={viewState === "loading"}
                    className="rounded-full bg-ink px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-tide disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {viewState === "loading" ? "Crawling..." : "Crawl Jobs"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setFilters(initialFilters)}
                    className="rounded-full border border-ink/15 px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-ink transition hover:border-ink hover:bg-white"
                  >
                    Reset
                  </button>
                  <p className="text-sm text-slate">
                    LinkedIn and Indeed remain honest limited providers unless a compliant public path exists.
                  </p>
                </div>
              </form>

              {message ? (
                <div className="rounded-3xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                  {message}
                </div>
              ) : null}
            </div>
          </div>

          <aside className="space-y-6">
            <div className="rounded-[32px] border border-ink/10 bg-ink p-6 text-white shadow-soft">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="font-mono text-xs uppercase tracking-[0.28em] text-white/60">
                    Recent searches
                  </p>
                  <h2 className="mt-3 text-2xl font-semibold">Resume a saved search.</h2>
                  <p className="mt-3 text-sm leading-6 text-white/70">
                    Load earlier crawls when you want to compare fresh results against a known role
                    and location mix.
                  </p>
                </div>
              </div>
              <div className="mt-6 space-y-3">
                {recentSearches.length === 0 ? (
                  <p className="text-sm text-white/70">
                    No searches have been stored yet. Run a crawl to seed the database.
                  </p>
                ) : (
                  recentSearches.map((search) => (
                    <button
                      key={search._id}
                      type="button"
                      onClick={() => void loadSearch(search._id)}
                      className="block w-full rounded-3xl border border-white/10 bg-white/5 px-4 py-4 text-left transition hover:bg-white/10"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-white">
                            {search.filters.title}
                          </div>
                          <div className="mt-1 text-xs text-white/60">
                            {describeSearchMeta(search.filters)}
                          </div>
                        </div>
                        <div className="font-mono text-xs uppercase tracking-[0.14em] text-white/60">
                          {formatRelativeMoment(search.updatedAt)}
                        </div>
                      </div>
                    </button>
                  ))
                )}
              </div>
            </div>

            <div className="rounded-[32px] border border-ink/10 bg-white/75 p-6 shadow-soft backdrop-blur">
              <p className="font-mono text-xs uppercase tracking-[0.28em] text-ember">
                Trust model
              </p>
              <div className="mt-4 grid gap-3 text-sm text-slate">
                <StatLine
                  label="Filter strategy"
                  value="Lead with the role title, then add location and experience only when they help narrow the search."
                />
                <StatLine
                  label="Link checks"
                  value="HEAD first, GET fallback, redirect-aware, with stale-page detection on saved jobs."
                />
                <StatLine
                  label="Deduping"
                  value="Canonical or resolved URL first, then normalized company + title + location."
                />
                <StatLine
                  label="Collections"
                  value="searches, jobs, crawlRuns, crawlSourceResults, and linkValidations."
                />
              </div>
            </div>
          </aside>
        </section>

        <section className="mt-8 space-y-6">
          {activeResult ? (
            <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
              <div className="rounded-[28px] border border-ink/10 bg-white/80 p-5 shadow-soft backdrop-blur">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div className="space-y-2">
                    <p className="font-mono text-xs uppercase tracking-[0.3em] text-ember">
                      Crawl run
                    </p>
                    <h2 className="text-2xl font-semibold text-ink">
                      {activeResult.search.filters.title}
                    </h2>
                    <p className="text-sm text-slate">
                      {activeResult.jobs.length} job{activeResult.jobs.length === 1 ? "" : "s"} in the latest run.
                    </p>
                    <p className="text-sm leading-6 text-slate">
                      {buildSearchBrief({
                        ...activeResult.search.filters,
                        country: activeResult.search.filters.country ?? "",
                        state: activeResult.search.filters.state ?? "",
                        city: activeResult.search.filters.city ?? "",
                      })}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-3">
                    <button
                      type="button"
                      onClick={() => void rerunActiveSearch()}
                      className="rounded-full bg-ember px-5 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-ember/90"
                    >
                      Rerun crawl
                    </button>
                    <button
                      type="button"
                      onClick={() =>
                        activeResult?.search._id
                          ? void loadSearch(activeResult.search._id)
                          : undefined
                      }
                      className="rounded-full border border-ink/15 px-5 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-ink transition hover:border-ink hover:bg-white"
                    >
                      Refresh results
                    </button>
                  </div>
                </div>
                <div className="mt-5 grid gap-3 sm:grid-cols-3">
                  {metrics.map((metric) => (
                    <div key={metric.label} className="rounded-3xl bg-sand/60 px-4 py-4">
                      <div className="font-mono text-xs uppercase tracking-[0.18em] text-slate">
                        {metric.label}
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-ink">{metric.value}</div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="rounded-[28px] border border-ink/10 bg-ink px-5 py-5 text-white shadow-soft">
                <div className="font-mono text-xs uppercase tracking-[0.22em] text-white/60">
                  Coverage snapshot
                </div>
                <div className="mt-2 text-xl font-semibold capitalize">
                  {activeResult.crawlRun.status}
                </div>
                <div className="mt-2 text-sm text-white/70">
                  Updated {formatRelativeMoment(activeResult.search.updatedAt)}
                </div>
                {providerSummary ? (
                  <div className="mt-6 grid gap-3 sm:grid-cols-3 xl:grid-cols-1">
                    <InverseMetricCard
                      label="Healthy"
                      value={`${providerSummary.healthy}/${providerSummary.total}`}
                    />
                    <InverseMetricCard
                      label="Degraded"
                      value={`${providerSummary.degraded}`}
                    />
                    <InverseMetricCard
                      label="Unavailable"
                      value={`${providerSummary.unavailable}`}
                    />
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}

          {activeResult && activeResult.sourceResults.length > 0 ? (
            <SourceCoveragePanel sourceResults={activeResult.sourceResults} />
          ) : null}

          {viewState === "partial" && activeResult && activeResult.jobs.length > 0 ? (
            <NoticeBanner
              title="Some providers had trouble, but partial results are available."
              description="Review the source cards below before rerunning so you can tell whether the gap came from provider failures or a narrow filter set."
            />
          ) : null}

          {viewState === "loading" ? (
            <LoadingPanel />
          ) : null}

          {viewState === "idle" ? (
            <StatePanel
              title="Run your first crawl"
              description="Start with a target role, then narrow by country, state, city, and experience level. Results are normalized, deduped, link-validated, and saved into MongoDB."
            />
          ) : null}

          {viewState === "error" && !activeResult ? (
            <StatePanel
              title="The crawl could not complete"
              description="Check that local MongoDB is running and that your environment can reach the public job board endpoints, then retry."
              actionLabel="Retry"
              onAction={() => void rerunActiveSearch()}
            />
          ) : null}

          {activeResult && activeResult.jobs.length === 0 && viewState !== "loading" ? (
            <StatePanel
              title={describeZeroResultState(activeResult).title}
              description={describeZeroResultState(activeResult).description}
              actionLabel="Retry crawl"
              onAction={() => void rerunActiveSearch()}
            />
          ) : null}

          {activeResult && activeResult.jobs.length > 0 ? (
            <ResultsTable
              jobs={activeResult.jobs}
              onRevalidate={revalidateSingleJob}
              revalidatingIds={revalidatingIds}
            />
          ) : null}
        </section>
      </div>
    </main>
  );
}

function Field(props: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  required?: boolean;
}) {
  return (
    <label className="space-y-2">
      <span className="text-sm font-medium text-ink">{props.label}</span>
      <input
        value={props.value}
        onChange={(event) => props.onChange(event.target.value)}
        placeholder={props.placeholder}
        required={props.required}
        className="w-full rounded-2xl border border-ink/10 bg-white px-4 py-3 text-sm text-ink outline-none transition focus:border-ember focus:ring-2 focus:ring-ember/20"
      />
    </label>
  );
}

function StatLine(props: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-ink/10 bg-white px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
        {props.label}
      </div>
      <div className="mt-2 leading-6 text-ink">{props.value}</div>
    </div>
  );
}

function HeroStat(props: { label: string; value: string; description: string }) {
  return (
    <div className="rounded-[24px] border border-white/80 bg-white/75 px-4 py-4 backdrop-blur">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
        {props.label}
      </div>
      <div className="mt-2 text-lg font-semibold text-ink">{props.value}</div>
      <div className="mt-2 text-sm leading-6 text-slate">{props.description}</div>
    </div>
  );
}

function InverseMetricCard(props: { label: string; value: string }) {
  return (
    <div className="rounded-3xl border border-white/10 bg-white/5 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-white/60">
        {props.label}
      </div>
      <div className="mt-2 text-3xl font-semibold text-white">{props.value}</div>
    </div>
  );
}

function StatePanel(props: {
  title: string;
  description: string;
  actionLabel?: string;
  onAction?: () => void;
}) {
  return (
    <div className="rounded-[28px] border border-ink/10 bg-white/80 p-8 text-center shadow-soft backdrop-blur">
      <h2 className="text-2xl font-semibold text-ink">{props.title}</h2>
      <p className="mx-auto mt-3 max-w-2xl text-base leading-7 text-slate">
        {props.description}
      </p>
      {props.actionLabel && props.onAction ? (
        <button
          type="button"
          onClick={props.onAction}
          className="mt-6 rounded-full bg-ink px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-tide"
        >
          {props.actionLabel}
        </button>
      ) : null}
    </div>
  );
}

function NoticeBanner(props: { title: string; description: string }) {
  return (
    <div className="rounded-[28px] border border-amber-200 bg-amber-50 px-5 py-4">
      <div className="text-base font-semibold text-amber-900">{props.title}</div>
      <p className="mt-2 text-sm leading-6 text-amber-800">{props.description}</p>
    </div>
  );
}

function LoadingPanel() {
  return (
    <div className="rounded-[28px] border border-ink/10 bg-white/80 p-8 shadow-soft backdrop-blur">
      <div className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
        Crawl in progress
      </div>
      <div className="mt-4 grid gap-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div
            key={index}
            className="h-16 rounded-2xl bg-[linear-gradient(90deg,rgba(79,93,117,0.08),rgba(79,93,117,0.18),rgba(79,93,117,0.08))] bg-[length:200%_100%] animate-shimmer"
          />
        ))}
      </div>
    </div>
  );
}

export function resolveViewState(result: CrawlResponse): ViewState {
  if (result.crawlRun.status === "failed") {
    return "error";
  }

  if (result.crawlRun.status === "partial") {
    return "partial";
  }

  if (result.jobs.length === 0) {
    return "empty";
  }

  return "success";
}

export function describeZeroResultState(result: CrawlResponse): ZeroResultState {
  if (result.crawlRun.status === "failed") {
    return {
      title: "Providers failed before results could be returned",
      description:
        "The crawl did not complete successfully. Retry the crawl after checking provider connectivity or endpoint configuration.",
    };
  }

  if (result.crawlRun.status === "partial") {
    return {
      title: "Providers had issues during the crawl",
      description:
        "Some providers failed and the remaining sources did not produce any saved matches. Retry the crawl or broaden the filters.",
    };
  }

  return {
    title: "No matching jobs yet",
    description:
      "The crawl finished, but none of the public-source results matched the current filters. Adjust the title or location and rerun.",
  };
}

function SourceCoveragePanel(props: {
  sourceResults: CrawlResponse["sourceResults"];
}) {
  return (
    <div className="rounded-[28px] border border-ink/10 bg-white/80 p-5 shadow-soft backdrop-blur">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.28em] text-ember">
            Source coverage
          </p>
          <h3 className="mt-3 text-2xl font-semibold text-ink">
            Provider status for this run
          </h3>
        </div>
        <p className="max-w-xl text-sm leading-6 text-slate">
          Each provider reports fetched, matched, and saved counts so it is easier to tell the
          difference between a narrow query and a provider outage.
        </p>
      </div>

      <div className="mt-5 grid gap-4 lg:grid-cols-2">
        {props.sourceResults.map((sourceResult) => {
          const tone = sourceResultTone(sourceResult.status);

          return (
            <div
              key={`${sourceResult.provider}-${sourceResult._id}`}
              className="rounded-[24px] border border-ink/10 bg-mist/55 px-5 py-5"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="text-lg font-semibold text-ink">
                    {labelForProvider(sourceResult.provider)}
                  </div>
                  <div className="mt-1 font-mono text-xs uppercase tracking-[0.16em] text-slate/75">
                    {labelForSourceStatus(sourceResult.status)}
                  </div>
                </div>
                <span
                  className={cn(
                    "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                    tone.badge,
                  )}
                >
                  {sourceResult.status}
                </span>
              </div>

              <div className="mt-4 grid gap-3 sm:grid-cols-3">
                <StatLine label="Fetched" value={`${sourceResult.fetchedCount}`} />
                <StatLine label="Matched" value={`${sourceResult.matchedCount}`} />
                <StatLine label="Saved" value={`${sourceResult.savedCount}`} />
              </div>

              {sourceResult.errorMessage ? (
                <p className="mt-4 text-sm leading-6 text-red-700">{sourceResult.errorMessage}</p>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function cleanFilters(filters: SearchFilters): SearchFilters {
  return {
    title: filters.title.trim(),
    country: filters.country?.trim() || undefined,
    state: filters.state?.trim() || undefined,
    city: filters.city?.trim() || undefined,
    experienceLevels: filters.experienceLevels?.length
      ? filters.experienceLevels
      : undefined,
  };
}

function buildSearchBrief(filters: SearchFilters) {
  const title = filters.title.trim();
  if (!title) {
    return "Pick a target role and any helpful filters. The crawler will normalize matches, dedupe overlaps, and re-check links before saving them.";
  }

  const levelSummary = describeExperienceLevels(filters.experienceLevels, "lowercase");
  const levelPrefix = levelSummary ? `${levelSummary} ` : "";
  const location = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
  const locationText = location ? `in ${location}` : "across any location";

  return `Looking for ${levelPrefix}${title} roles ${locationText}. Matching jobs will be deduped and link-validated before they are stored.`;
}

function buildFilterBadges(filters: SearchFilters) {
  const badges: string[] = [];

  if (filters.title.trim()) {
    badges.push(`Role: ${filters.title.trim()}`);
  }

  const location = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
  if (location) {
    badges.push(`Location: ${location}`);
  }

  const levelSummary = describeExperienceLevels(filters.experienceLevels);
  if (levelSummary) {
    badges.push(`Levels: ${levelSummary}`);
  }

  return badges;
}

function describeSearchMeta(filters: SearchFilters) {
  const parts = [];
  const location = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");

  const levelSummary = describeExperienceLevels(filters.experienceLevels);
  if (levelSummary) {
    parts.push(levelSummary);
  }

  parts.push(location || "Any location");
  return parts.join(" • ");
}

function summarizeSourceResults(sourceResults: CrawlResponse["sourceResults"]) {
  return sourceResults.reduce(
    (summary, sourceResult) => {
      if (sourceResult.status === "success") {
        summary.healthy += 1;
      } else if (sourceResult.status === "partial") {
        summary.degraded += 1;
      } else {
        summary.unavailable += 1;
      }

      return summary;
    },
    {
      healthy: 0,
      degraded: 0,
      unavailable: 0,
      total: sourceResults.length,
    },
  );
}

function labelForProvider(provider: CrawlResponse["sourceResults"][number]["provider"]) {
  const labels: Record<CrawlResponse["sourceResults"][number]["provider"], string> = {
    greenhouse: "Greenhouse",
    lever: "Lever",
    ashby: "Ashby",
    company_page: "Company page",
    linkedin_limited: "LinkedIn limited",
    indeed_limited: "Indeed limited",
  };

  return labels[provider];
}

function labelForSourceStatus(status: CrawlResponse["sourceResults"][number]["status"]) {
  if (status === "success") {
    return "Healthy provider";
  }

  if (status === "partial") {
    return "Partial provider";
  }

  if (status === "unsupported") {
    return "Limited provider";
  }

  return "Provider failed";
}

function sourceResultTone(status: CrawlResponse["sourceResults"][number]["status"]) {
  if (status === "success") {
    return {
      badge: "bg-pine/10 text-pine",
    };
  }

  if (status === "partial") {
    return {
      badge: "bg-amber-100 text-amber-800",
    };
  }

  if (status === "unsupported") {
    return {
      badge: "bg-tide/10 text-tide",
    };
  }

  return {
    badge: "bg-red-100 text-red-700",
  };
}

function dedupeSearches(searches: SearchDocument[]) {
  const map = new Map<string, SearchDocument>();
  searches.forEach((search) => {
    map.set(search._id, search);
  });
  return Array.from(map.values());
}

function toggleExperienceLevel(
  selectedLevels: SearchFilters["experienceLevels"],
  level: (typeof experienceLevels)[number],
) {
  const nextLevels = new Set(selectedLevels ?? []);

  if (nextLevels.has(level)) {
    nextLevels.delete(level);
  } else {
    nextLevels.add(level);
  }

  const normalized = experienceLevels.filter((candidate) => nextLevels.has(candidate));
  return normalized.length > 0 ? normalized : undefined;
}

function describeExperienceLevels(
  levels: SearchFilters["experienceLevels"],
  format: "default" | "lowercase" = "default",
) {
  if (!levels?.length) {
    return undefined;
  }

  const labels = levels.map((level) => {
    const label = labelForExperience(level);
    return format === "lowercase" ? label.toLowerCase() : label;
  });

  if (labels.length === 1) {
    return labels[0];
  }

  if (labels.length === 2) {
    return `${labels[0]} or ${labels[1]}`;
  }

  return `${labels.slice(0, -1).join(", ")}, or ${labels[labels.length - 1]}`;
}

function jobComparator(left: CrawlResponse["jobs"][number], right: CrawlResponse["jobs"][number]) {
  if (left.postedAt && right.postedAt && left.postedAt !== right.postedAt) {
    return left.postedAt > right.postedAt ? -1 : 1;
  }

  if (left.postedAt && !right.postedAt) {
    return -1;
  }

  if (!left.postedAt && right.postedAt) {
    return 1;
  }

  const sourceComparison = left.sourcePlatform.localeCompare(right.sourcePlatform);
  if (sourceComparison !== 0) {
    return sourceComparison;
  }

  return left.title.localeCompare(right.title);
}
