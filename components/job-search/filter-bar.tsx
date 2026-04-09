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
    <section className="rounded-[18px] border border-ink/10 bg-white px-4 py-3 shadow-sm sm:px-5">
      <div className="flex flex-col gap-3">
        <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate/70">
              Filters
            </h2>
          </div>
          <button
            type="button"
            onClick={props.onClear}
            className={cn(
              "self-start rounded-full px-3 py-1.5 text-sm font-medium transition",
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

        <div className="grid gap-3 lg:grid-cols-[0.8fr_0.8fr_1fr]">
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
    <div className="space-y-1.5">
      <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate/60">
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
        "rounded-full border px-3 py-1.5 text-sm transition",
        props.disabled && "cursor-not-allowed border-ink/8 bg-white text-slate/45",
        !props.disabled &&
          props.selected &&
          "border-[#0a66c2] bg-[#0a66c2] text-white",
        !props.disabled &&
          !props.selected &&
          "border-ink/10 bg-white text-slate hover:border-ink/25 hover:bg-mist/45",
      )}
    >
      {props.label}
    </button>
  );
}
