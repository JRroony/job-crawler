"use client";

import React, { type FormEventHandler } from "react";

type SearchBarProps = {
  keyword: string;
  location: string;
  isLoading: boolean;
  onKeywordChange: (value: string) => void;
  onLocationChange: (value: string) => void;
  onSubmit: FormEventHandler<HTMLFormElement>;
  onReset: () => void;
};

export function SearchBar(props: SearchBarProps) {
  return (
    <section className="overflow-hidden rounded-[28px] border border-ink/10 bg-[linear-gradient(140deg,rgba(255,255,255,0.96),rgba(247,242,233,0.96))] px-4 py-5 shadow-[0_22px_60px_rgba(15,23,42,0.07)] sm:px-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="max-w-2xl">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
            Job Search
          </div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight text-ink sm:text-[2.3rem]">
            Search public job boards without the clutter
          </h1>
          <p className="mt-2 text-sm leading-6 text-slate">
            Search across Greenhouse, Lever, Ashby, and configured company pages, then review a clean result list beside the original posting details.
          </p>
        </div>
        <div className="rounded-full border border-ink/10 bg-white/80 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate">
          Public listings only
        </div>
      </div>

      <form
        className="mt-6 grid gap-3 lg:grid-cols-[minmax(0,1.3fr)_minmax(280px,0.95fr)_auto]"
        onSubmit={props.onSubmit}
      >
        <label className="rounded-[20px] border border-ink/10 bg-white/88 px-4 py-3 shadow-sm">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/60">
            Role
          </div>
          <input
            value={props.keyword}
            onChange={(event) => props.onKeywordChange(event.target.value)}
            placeholder="Title, keyword, or company"
            className="mt-2 h-7 w-full border-none bg-transparent px-0 text-[15px] text-ink outline-none placeholder:text-slate/55"
            autoComplete="off"
          />
        </label>

        <label className="rounded-[20px] border border-ink/10 bg-white/88 px-4 py-3 shadow-sm">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/60">
            Location
          </div>
          <input
            value={props.location}
            onChange={(event) => props.onLocationChange(event.target.value)}
            placeholder="City, state, country, or remote"
            className="mt-2 h-7 w-full border-none bg-transparent px-0 text-[15px] text-ink outline-none placeholder:text-slate/55"
            autoComplete="off"
          />
        </label>

        <div className="flex gap-2 lg:self-stretch">
          <button
            type="submit"
            disabled={props.isLoading}
            className="min-h-[58px] min-w-[132px] rounded-[18px] bg-[#0a66c2] px-5 text-sm font-semibold text-white transition hover:bg-[#004182] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {props.isLoading ? "Searching..." : "Search"}
          </button>
          <button
            type="button"
            onClick={props.onReset}
            className="min-h-[58px] rounded-[18px] border border-ink/10 bg-white/75 px-4 text-sm font-medium text-slate transition hover:border-ink/25 hover:bg-mist/45"
          >
            Reset
          </button>
        </div>
      </form>
    </section>
  );
}
