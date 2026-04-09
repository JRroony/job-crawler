"use client";

import React from "react";

import type { SearchFilters } from "@/lib/types";
import { cn } from "@/lib/utils";
import type { ClientResultFilters } from "@/components/job-search/helpers";
import {
  disabledPlatformFilterOptions,
  experienceFilterOptions,
  platformFilterOptions,
  postedDateFilterOptions,
} from "@/components/job-search/helpers";

type FilterBarProps = {
  filters: SearchFilters;
  resultFilters: ClientResultFilters;
  onTogglePlatform: (platform: (typeof platformFilterOptions)[number]["value"]) => void;
  onToggleExperience: (level: (typeof experienceFilterOptions)[number]["value"]) => void;
  onToggleRemoteOnly: () => void;
  onToggleVisaFriendlyOnly: () => void;
  onPostedDateChange: (value: ClientResultFilters["postedDate"]) => void;
  onClear: () => void;
};

export function FilterBar(props: FilterBarProps) {
  const activePlatformSet = new Set(
    props.filters.platforms?.length
      ? props.filters.platforms
      : platformFilterOptions.map((option) => option.value),
  );
  const activeExperienceSet = new Set(props.filters.experienceLevels ?? []);
  const hasAnyFilter =
    Boolean(props.filters.platforms?.length) ||
    activeExperienceSet.size > 0 ||
    props.resultFilters.remoteOnly ||
    props.resultFilters.visaFriendlyOnly ||
    props.resultFilters.postedDate !== "any";

  return (
    <section className="rounded-[28px] border border-ink/8 bg-white px-5 py-4 shadow-[0_18px_48px_rgba(15,23,42,0.05)] sm:px-6">
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-ink">Refine your results</h2>
            <p className="mt-1 text-sm text-slate">
              Keep the main workflow lightweight while still narrowing the job list quickly.
            </p>
          </div>
          <button
            type="button"
            onClick={props.onClear}
            className={cn(
              "self-start rounded-full px-4 py-2 text-sm font-medium transition",
              hasAnyFilter
                ? "border border-ink/10 text-slate hover:border-ink/25 hover:bg-mist/45"
                : "cursor-default text-slate/45",
            )}
            disabled={!hasAnyFilter}
          >
            Clear filters
          </button>
        </div>

        <FilterGroup label="Platform">
          {platformFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activePlatformSet.has(option.value)}
              onClick={() => props.onTogglePlatform(option.value)}
            />
          ))}
          {disabledPlatformFilterOptions.map((option) => (
            <FilterChip
              key={option.label}
              label={`${option.label} · ${option.detail}`}
              selected={false}
              disabled
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Experience">
          {experienceFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activeExperienceSet.has(option.value)}
              onClick={() => props.onToggleExperience(option.value)}
            />
          ))}
        </FilterGroup>

        <div className="grid gap-4 lg:grid-cols-[0.8fr_0.8fr_1fr]">
          <FilterGroup label="Work style">
            <FilterChip
              label="Remote only"
              selected={props.resultFilters.remoteOnly}
              onClick={props.onToggleRemoteOnly}
            />
          </FilterGroup>

          <FilterGroup label="Hiring support">
            <FilterChip
              label="Visa friendly"
              selected={props.resultFilters.visaFriendlyOnly}
              onClick={props.onToggleVisaFriendlyOnly}
            />
          </FilterGroup>

          <FilterGroup label="Posted date">
            {postedDateFilterOptions.map((option) => (
              <FilterChip
                key={option.value}
                label={option.label}
                selected={props.resultFilters.postedDate === option.value}
                onClick={() => props.onPostedDateChange(option.value)}
              />
            ))}
          </FilterGroup>
        </div>
      </div>
    </section>
  );
}

function FilterGroup(props: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
        {props.label}
      </div>
      <div className="flex flex-wrap gap-2">{props.children}</div>
    </div>
  );
}

function FilterChip(props: {
  label: string;
  selected: boolean;
  disabled?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      disabled={props.disabled}
      aria-pressed={props.selected}
      className={cn(
        "rounded-full border px-4 py-2 text-sm transition",
        props.disabled && "cursor-not-allowed border-ink/8 bg-white text-slate/45",
        !props.disabled &&
          props.selected &&
          "border-ink bg-ink text-white shadow-[0_10px_30px_rgba(16,24,32,0.14)]",
        !props.disabled &&
          !props.selected &&
          "border-ink/10 bg-white text-slate hover:border-ink/25 hover:bg-mist/45",
      )}
    >
      {props.label}
    </button>
  );
}
