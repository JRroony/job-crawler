"use client";

import type { Dispatch, FormEventHandler, ReactNode, SetStateAction } from "react";

import type { ActiveCrawlerPlatform, SearchFilters } from "@/lib/types";
import { experienceLevels } from "@/lib/types";
import { cn, labelForExperience } from "@/lib/utils";
import {
  crawlModeOptions,
  experienceModeOptions,
  labelForCrawlerPlatform,
  passivePlatformOptions,
  resolveCrawlMode,
  resolveExperienceMode,
  resolveRequestedPlatforms,
  resolveSelectedPlatforms,
  selectablePlatformOptions,
  togglePlatformSelection,
} from "@/components/job-crawler/ui-config";

type SearchControlsPanelProps = {
  filters: SearchFilters;
  selectedFilters: string[];
  isLoading: boolean;
  setFilters: Dispatch<SetStateAction<SearchFilters>>;
  onSubmit: FormEventHandler<HTMLFormElement>;
  onReset: () => void;
};

export function SearchControlsPanel(props: SearchControlsPanelProps) {
  const activePlatforms = resolveSelectedPlatforms(props.filters.platforms);
  const selectedPlatforms = new Set(activePlatforms);
  const requestedPlatforms = resolveRequestedPlatforms(props.filters.platforms);
  const requestedButInactive = requestedPlatforms.filter(
    (platform) => !activePlatforms.includes(platform as ActiveCrawlerPlatform),
  );
  const selectedExperienceMode = resolveExperienceMode(props.filters.experienceMatchMode);
  const selectedCrawlMode = resolveCrawlMode(props.filters.crawlMode);
  const includeUnspecified =
    props.filters.includeUnspecifiedExperience === true || selectedExperienceMode === "broad";
  const locationSummary =
    [props.filters.city, props.filters.state, props.filters.country].filter(Boolean).join(", ") ||
    "Any location";

  return (
    <section className="rounded-[32px] border border-ink/10 bg-white/85 p-5 shadow-soft backdrop-blur sm:p-6 xl:p-7">
      <div className="flex flex-col gap-6">
        <div className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="max-w-3xl">
            <p className="font-mono text-xs uppercase tracking-[0.28em] text-ember">
              Operational search setup
            </p>
            <h1 className="mt-3 text-3xl font-semibold text-ink sm:text-4xl">
              Set the crawl scope before you spend time on the result set.
            </h1>
            <p className="mt-3 text-sm leading-7 text-slate sm:text-base">
              This form is organized the way a real crawler workflow works: define the target,
              choose how strict matching should be, decide which provider families are active, then
              pick how aggressively links should be validated during the run.
            </p>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
            <SummaryCallout
              label="Discovery scope"
              value="Configured seeds plus public ATS discovery"
              detail="Configured sources still seed coverage, but the crawler can now discover additional public ATS sources from the search intent."
            />
            <SummaryCallout
              label="Default behavior"
              value="All implemented platforms"
              detail="If you do not customize platform selection, the crawl runs every supported provider family."
            />
          </div>
        </div>

        <form className="grid gap-5" onSubmit={props.onSubmit}>
          <WorkflowSection
            step="1"
            title="Define the job target"
            description="Start with the role and geography you actually want saved."
          >
            <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
              <div className="space-y-4">
                <Field
                  label="Target role"
                  value={props.filters.title}
                  onChange={(value) =>
                    props.setFilters((current) => ({
                      ...current,
                      title: value,
                    }))
                  }
                  placeholder="Software Engineer"
                  required
                  emphasized
                />
                <div className="rounded-[22px] border border-ink/10 bg-sand/45 px-4 py-4 text-sm leading-6 text-slate">
                  Start with the role you want. If a user types platform or location hints into
                  this field, the backend now normalizes obvious cases before discovery and
                  matching begin.
                </div>
              </div>

              <div className="grid gap-4 sm:grid-cols-3">
                <Field
                  label="Country"
                  value={props.filters.country ?? ""}
                  onChange={(value) =>
                    props.setFilters((current) => ({
                      ...current,
                      country: value,
                    }))
                  }
                  placeholder="United States"
                />
                <Field
                  label="State"
                  value={props.filters.state ?? ""}
                  onChange={(value) =>
                    props.setFilters((current) => ({
                      ...current,
                      state: value,
                    }))
                  }
                  placeholder="California"
                />
                <Field
                  label="City"
                  value={props.filters.city ?? ""}
                  onChange={(value) =>
                    props.setFilters((current) => ({
                      ...current,
                      city: value,
                    }))
                  }
                  placeholder="San Francisco"
                />
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <InlineSummary label="Role" value={props.filters.title.trim() || "Not set yet"} />
              <InlineSummary label="Location" value={locationSummary} />
            </div>
          </WorkflowSection>

          <WorkflowSection
            step="2"
            title="Choose the matching policy"
            description="Control how hard the crawler should work to keep inferred or underspecified jobs."
          >
            <div className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
              <div className="space-y-5">
                <Subsection
                  title="Experience levels"
                  description="Leave all levels off to accept any level that passes the broader matching policy."
                >
                  <div className="flex flex-wrap gap-2">
                    {experienceLevels.map((level) => {
                      const selected = Boolean(props.filters.experienceLevels?.includes(level));

                      return (
                        <button
                          key={level}
                          type="button"
                          aria-pressed={selected}
                          onClick={() =>
                            props.setFilters((current) => ({
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
                              ? "border-ember bg-ember text-white shadow-[0_12px_24px_rgba(186,88,53,0.22)]"
                              : "border-ink/10 bg-white text-ink hover:border-ember/35 hover:bg-ember/5",
                          )}
                        >
                          {labelForExperience(level)}
                        </button>
                      );
                    })}
                  </div>
                </Subsection>

                <div className="grid gap-3 sm:grid-cols-3">
                  <InlineSummary
                    label="Selected levels"
                    value={
                      props.filters.experienceLevels?.length
                        ? `${props.filters.experienceLevels.length}`
                        : "Any"
                    }
                  />
                  <InlineSummary
                    label="Experience mode"
                    value={selectedExperienceMode}
                  />
                  <InlineSummary
                    label="Unspecified"
                    value={includeUnspecified ? "Included" : "Excluded"}
                  />
                </div>
              </div>

              <div className="space-y-5">
                <Subsection
                  title="Experience matching mode"
                  description="This controls how aggressively inferred experience is trusted."
                >
                  <div className="grid gap-3 sm:grid-cols-3 xl:grid-cols-1">
                    {experienceModeOptions.map((option) => {
                      const selected = selectedExperienceMode === option.value;

                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() =>
                            props.setFilters((current) => ({
                              ...current,
                              experienceMatchMode: option.value,
                              includeUnspecifiedExperience:
                                option.value === "broad"
                                  ? true
                                  : current.includeUnspecifiedExperience,
                            }))
                          }
                          className={cn(
                            "rounded-[24px] border px-4 py-4 text-left transition",
                            selected
                              ? "border-ember bg-[rgba(186,88,53,0.09)] shadow-[0_14px_28px_rgba(186,88,53,0.12)]"
                              : "border-ink/10 bg-white hover:border-ember/30 hover:bg-ember/5",
                          )}
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <div className="font-semibold text-ink">{option.label}</div>
                              <div className="mt-2 text-sm leading-6 text-slate">
                                {option.description}
                              </div>
                            </div>
                            <span
                              className={cn(
                                "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                                selected
                                  ? "bg-ember text-white"
                                  : "border border-ink/10 bg-white text-slate",
                              )}
                            >
                              {selected ? "Active" : "Available"}
                            </span>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </Subsection>

                <Subsection
                  title="Include unspecified experience"
                  description="Useful when a role and location fit, but the source did not expose a reliable level."
                >
                  <label className="flex items-start gap-3 rounded-[24px] border border-ink/10 bg-white px-4 py-4">
                    <input
                      type="checkbox"
                      checked={includeUnspecified}
                      disabled={selectedExperienceMode === "broad"}
                      onChange={(event) =>
                        props.setFilters((current) => ({
                          ...current,
                          includeUnspecifiedExperience: event.target.checked || undefined,
                        }))
                      }
                      className="mt-1 h-4 w-4 rounded border-ink/30 text-ember focus:ring-ember/25 disabled:cursor-not-allowed disabled:opacity-60"
                    />
                    <div>
                      <div className="font-semibold text-ink">
                        {selectedExperienceMode === "broad"
                          ? "Always included in broad mode"
                          : "Include unspecified jobs"}
                      </div>
                      <div className="mt-2 text-sm leading-6 text-slate">
                        {selectedExperienceMode === "broad"
                          ? "Broad mode already includes jobs with missing experience classification."
                          : "This can prevent useful public postings from being dropped only because the source omitted a clear level."}
                      </div>
                    </div>
                  </label>
                </Subsection>
              </div>
            </div>
          </WorkflowSection>

          <div className="grid gap-5 xl:grid-cols-[1.08fr_0.92fr]">
            <WorkflowSection
              step="3"
              title="Select provider families"
              description="Choose which implemented source families should run for this crawl."
            >
              <div className="flex flex-wrap gap-2">
                <InlineSummary
                  label="Active platforms"
                  value={`${activePlatforms.length} selected`}
                />
                <InlineSummary
                  label="Default scope"
                  value={
                    props.filters.platforms ? "Custom selection" : "All implemented providers"
                  }
                />
                {requestedButInactive.length > 0 ? (
                  <InlineSummary
                    label="Disabled in selection"
                    value={requestedButInactive
                      .map((platform) => labelForCrawlerPlatform(platform))
                      .join(", ")}
                    tone="amber"
                  />
                ) : null}
              </div>

              <div className="grid gap-3">
                {selectablePlatformOptions.map((option) => {
                  const selected = selectedPlatforms.has(option.platform);

                  return (
                    <button
                      key={option.platform}
                      type="button"
                      onClick={() =>
                        props.setFilters((current) => ({
                          ...current,
                          platforms: togglePlatformSelection(current.platforms, option.platform),
                        }))
                      }
                      className={cn(
                        "flex items-start justify-between gap-3 rounded-[24px] border px-4 py-4 text-left transition",
                        selected
                          ? "border-ember bg-[rgba(186,88,53,0.08)] shadow-[0_14px_28px_rgba(186,88,53,0.10)]"
                          : "border-ink/10 bg-white hover:border-ember/30 hover:bg-ember/5",
                      )}
                    >
                      <div>
                        <div className="font-semibold text-ink">{option.label}</div>
                        <div className="mt-1 text-sm leading-6 text-slate">{option.detail}</div>
                      </div>
                      <div
                        className={cn(
                          "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                          selected
                            ? "bg-ember text-white"
                            : "border border-ink/10 bg-white text-slate",
                        )}
                      >
                        {selected ? "Selected" : option.availability}
                      </div>
                    </button>
                  );
                })}
              </div>

              {requestedButInactive.length > 0 ? (
                <div className="rounded-[22px] border border-amber-200 bg-amber-50 px-4 py-4 text-sm leading-6 text-amber-900">
                  Saved filters include {requestedButInactive.map((platform) => labelForCrawlerPlatform(platform)).join(", ")}, but those paths are currently disabled and will not run in this crawler build.
                </div>
              ) : null}

              <Subsection
                title="Limited and planned platforms"
                description="Visible for transparency, but not active crawler targets."
              >
                <div className="grid gap-3 md:grid-cols-3">
                  {passivePlatformOptions.map((option) => (
                    <div
                      key={option.label}
                      className="rounded-[22px] border border-ink/10 bg-[rgba(244,239,230,0.75)] px-4 py-4"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-semibold text-ink">{option.label}</div>
                        <span
                          className={cn(
                            "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                            option.tone === "limited"
                              ? "bg-amber-100 text-amber-900"
                              : "bg-ink/8 text-ink/70",
                          )}
                        >
                          {option.tone === "limited" ? "Limited" : "Disabled"}
                        </span>
                      </div>
                      <div className="mt-2 text-sm leading-6 text-slate">{option.detail}</div>
                    </div>
                  ))}
                </div>
              </Subsection>
            </WorkflowSection>

            <WorkflowSection
              step="4"
              title="Set run behavior"
              description="Pick how much validation work should happen before the crawl finishes."
            >
              <div className="grid gap-3">
                {crawlModeOptions.map((option) => {
                  const selected = selectedCrawlMode === option.value;

                  return (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() =>
                        props.setFilters((current) => ({
                          ...current,
                          crawlMode: option.value,
                        }))
                      }
                      className={cn(
                        "rounded-[24px] border px-4 py-4 text-left transition",
                        selected
                          ? "border-tide bg-[rgba(63,114,175,0.08)] shadow-[0_14px_28px_rgba(63,114,175,0.10)]"
                          : "border-ink/10 bg-white hover:border-tide/30 hover:bg-tide/5",
                      )}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="font-semibold text-ink">{option.label}</div>
                          <div className="mt-2 text-sm leading-6 text-slate">
                            {option.description}
                          </div>
                        </div>
                        <span
                          className={cn(
                            "rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]",
                            selected
                              ? "bg-tide text-white"
                              : "border border-ink/10 bg-white text-slate",
                          )}
                        >
                          {selected ? "Active" : "Available"}
                        </span>
                      </div>
                      <div className="mt-3 text-xs uppercase tracking-[0.16em] text-slate/75">
                        {option.validationSummary}
                      </div>
                    </button>
                  );
                })}
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <SummaryCallout
                  label="Current validation plan"
                  value={
                    crawlModeOptions.find((option) => option.value === selectedCrawlMode)
                      ?.validationSummary ?? "Deferred validation"
                  }
                  detail="This affects link validation behavior, not source discovery."
                />
                <SummaryCallout
                  label="Selected platform scope"
                  value={
                    activePlatforms.length > 0
                      ? activePlatforms
                          .map((platform) => labelForCrawlerPlatform(platform))
                          .join(", ")
                      : "No enabled platforms selected"
                  }
                  detail="If you narrow this list, only those provider families will run."
                />
              </div>
            </WorkflowSection>
          </div>

          <div className="rounded-[28px] border border-ink/10 bg-[rgba(244,239,230,0.78)] px-4 py-4">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ember">
                  Active summary
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {props.selectedFilters.length > 0 ? (
                    props.selectedFilters.map((filter) => (
                      <span
                        key={filter}
                        className="rounded-full border border-ink/10 bg-white px-3 py-1 text-xs font-medium text-ink/85"
                      >
                        {filter}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-slate">
                      Start with a title, then refine scope, matching policy, and validation mode.
                    </span>
                  )}
                </div>
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  type="submit"
                  disabled={props.isLoading}
                  className="rounded-full bg-ink px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-tide disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {props.isLoading ? "Crawling..." : "Start crawl"}
                </button>
                <button
                  type="button"
                  onClick={props.onReset}
                  className="rounded-full border border-ink/15 bg-white px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-ink transition hover:border-ink hover:bg-sand/80"
                >
                  Reset filters
                </button>
              </div>
            </div>
          </div>
        </form>
      </div>
    </section>
  );
}

function WorkflowSection(props: {
  step: string;
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-[28px] border border-ink/10 bg-[rgba(255,255,255,0.82)] p-4 sm:p-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="inline-flex items-center gap-2 rounded-full border border-ink/10 bg-sand/50 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
            <span className="text-ember">Step {props.step}</span>
            <span>{props.title}</span>
          </div>
          <div className="mt-3 text-sm leading-6 text-slate">{props.description}</div>
        </div>
      </div>

      <div className="mt-5 space-y-5">{props.children}</div>
    </section>
  );
}

function SummaryCallout(props: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-[22px] border border-ink/10 bg-sand/55 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ember">
        {props.label}
      </div>
      <div className="mt-2 text-sm font-semibold leading-6 text-ink">{props.value}</div>
      <div className="mt-2 text-sm leading-6 text-slate">{props.detail}</div>
    </div>
  );
}

function Subsection(props: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-3">
      <div>
        <div className="text-sm font-semibold text-ink">{props.title}</div>
        <div className="mt-1 text-sm leading-6 text-slate">{props.description}</div>
      </div>
      {props.children}
    </div>
  );
}

function InlineSummary(props: {
  label: string;
  value: string;
  tone?: "neutral" | "amber";
}) {
  return (
    <div
      className={cn(
        "rounded-full border px-3 py-1 text-xs font-medium",
        props.tone === "amber"
          ? "border-amber-200 bg-amber-50 text-amber-900"
          : "border-ink/10 bg-white text-ink/85",
      )}
    >
      <span className="font-mono uppercase tracking-[0.16em] text-slate/75">{props.label}</span>
      <span className="ml-2">{props.value}</span>
    </div>
  );
}

function Field(props: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  required?: boolean;
  emphasized?: boolean;
}) {
  return (
    <label className="space-y-2">
      <span className="text-sm font-medium text-ink">{props.label}</span>
      <input
        value={props.value}
        onChange={(event) => props.onChange(event.target.value)}
        placeholder={props.placeholder}
        required={props.required}
        className={cn(
          "w-full rounded-[20px] border bg-white px-4 py-3 text-sm text-ink outline-none transition focus:ring-2",
          props.emphasized
            ? "border-ember/20 focus:border-ember focus:ring-ember/20"
            : "border-ink/10 focus:border-ember focus:ring-ember/20",
        )}
      />
    </label>
  );
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
