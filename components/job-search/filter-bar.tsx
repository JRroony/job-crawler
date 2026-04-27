"use client";

import React from "react";

import type { EmploymentType, SearchFilters } from "@/lib/types";
import { cn } from "@/lib/utils";
import type {
  ClientResultFilters,
  SponsorshipFilter,
  WorkplaceFilter,
} from "@/components/job-search/helpers";
import {
  employmentTypeFilterOptions,
  experienceFilterOptions,
  platformFilterOptions,
  postedDateFilterOptions,
  sponsorshipFilterOptions,
  workplaceFilterOptions,
} from "@/components/job-search/helpers";

type JobFilterSidebarProps = {
  filters: SearchFilters;
  resultFilters: ClientResultFilters;
  mobileOpen?: boolean;
  onCloseMobile?: () => void;
  onTogglePlatform: (platform: (typeof platformFilterOptions)[number]["value"]) => void;
  onToggleExperience: (level: (typeof experienceFilterOptions)[number]["value"]) => void;
  onWorkplaceChange: (value: WorkplaceFilter) => void;
  onToggleEmploymentType: (type: EmploymentType) => void;
  onSponsorshipChange: (value: SponsorshipFilter) => void;
  onCompanyChange: (value: string) => void;
  onPostedDateChange: (value: ClientResultFilters["postedDate"]) => void;
  onClear: () => void;
};

export function JobFilterSidebar(props: JobFilterSidebarProps) {
  const panel = (
    <FilterPanel
      filters={props.filters}
      resultFilters={props.resultFilters}
      onTogglePlatform={props.onTogglePlatform}
      onToggleExperience={props.onToggleExperience}
      onWorkplaceChange={props.onWorkplaceChange}
      onToggleEmploymentType={props.onToggleEmploymentType}
      onSponsorshipChange={props.onSponsorshipChange}
      onCompanyChange={props.onCompanyChange}
      onPostedDateChange={props.onPostedDateChange}
      onClear={props.onClear}
    />
  );

  return (
    <>
      <aside className="hidden lg:block">{panel}</aside>

      {props.mobileOpen ? (
        <div className="fixed inset-0 z-40 lg:hidden" role="dialog" aria-modal="true">
          <button
            type="button"
            className="absolute inset-0 bg-ink/35"
            aria-label="Close filters"
            onClick={props.onCloseMobile}
          />
          <div className="absolute inset-x-0 bottom-0 max-h-[85vh] overflow-y-auto rounded-t-xl bg-white p-4 shadow-[0_-20px_60px_rgba(15,23,42,0.18)]">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-base font-semibold text-ink">Filters</h2>
              <button
                type="button"
                onClick={props.onCloseMobile}
                className="rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate hover:bg-slate-50"
              >
                Done
              </button>
            </div>
            {panel}
          </div>
        </div>
      ) : null}
    </>
  );
}

function FilterPanel(props: Omit<JobFilterSidebarProps, "mobileOpen" | "onCloseMobile">) {
  const activePlatformSet = new Set(
    props.filters.platforms?.length
      ? props.filters.platforms
      : platformFilterOptions.map((option) => option.value),
  );
  const activeExperienceSet = new Set(props.filters.experienceLevels ?? []);
  const activeEmploymentTypeSet = new Set(props.resultFilters.employmentTypes ?? []);
  const activeWorkplace = props.resultFilters.workplace ?? "any";
  const activeSponsorship = props.resultFilters.sponsorship ?? "any";
  const hasAnyFilter =
    Boolean(props.filters.platforms?.length) ||
    activeExperienceSet.size > 0 ||
    activeEmploymentTypeSet.size > 0 ||
    activeWorkplace !== "any" ||
    activeSponsorship !== "any" ||
    Boolean(props.resultFilters.company?.trim()) ||
    props.resultFilters.postedDate !== "any";

  return (
    <section className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm lg:sticky lg:top-24">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-base font-semibold text-ink">Filters</h2>
        <button
          type="button"
          onClick={props.onClear}
          className={cn(
            "rounded-md px-2.5 py-1.5 text-sm font-medium transition",
            hasAnyFilter
              ? "text-[#0a66c2] hover:bg-[#0a66c2]/10"
              : "cursor-default text-slate/45",
          )}
          disabled={!hasAnyFilter}
        >
          Clear
        </button>
      </div>

      <div className="space-y-5">
        <FilterGroup label="Date posted">
          {postedDateFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={props.resultFilters.postedDate === option.value}
              onClick={() => props.onPostedDateChange(option.value)}
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Experience level">
          {experienceFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activeExperienceSet.has(option.value)}
              onClick={() => props.onToggleExperience(option.value)}
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Workplace">
          {workplaceFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activeWorkplace === option.value}
              onClick={() => props.onWorkplaceChange(option.value)}
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Employment type">
          {employmentTypeFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activeEmploymentTypeSet.has(option.value)}
              onClick={() => props.onToggleEmploymentType(option.value)}
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Platform">
          {platformFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activePlatformSet.has(option.value)}
              onClick={() => props.onTogglePlatform(option.value)}
            />
          ))}
        </FilterGroup>

        <FilterGroup label="Sponsorship">
          {sponsorshipFilterOptions.map((option) => (
            <FilterChip
              key={option.value}
              label={option.label}
              selected={activeSponsorship === option.value}
              onClick={() => props.onSponsorshipChange(option.value)}
            />
          ))}
        </FilterGroup>

        <div>
          <label className="text-sm font-semibold text-ink" htmlFor="company-filter">
            Company
          </label>
          <input
            id="company-filter"
            value={props.resultFilters.company ?? ""}
            onChange={(event) => props.onCompanyChange(event.target.value)}
            placeholder="Filter by company"
            className="mt-2 h-10 w-full rounded-md border border-slate-300 bg-white px-3 text-sm text-ink outline-none transition placeholder:text-slate/50 focus:border-[#0a66c2] focus:ring-2 focus:ring-[#0a66c2]/15"
          />
        </div>
      </div>
    </section>
  );
}

function FilterGroup(props: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="mb-2 text-sm font-semibold text-ink">{props.label}</div>
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
        "rounded-full border px-3 py-1.5 text-xs font-medium transition",
        props.disabled && "cursor-not-allowed border-slate-200 bg-slate-50 text-slate/45",
        !props.disabled &&
          props.selected &&
          "border-[#0a66c2] bg-[#e7f3ff] text-[#0a66c2]",
        !props.disabled &&
          !props.selected &&
          "border-slate-200 bg-white text-slate hover:border-[#0a66c2]/35 hover:bg-[#e7f3ff]/60",
      )}
    >
      {props.label}
    </button>
  );
}

export const FilterBar = JobFilterSidebar;
