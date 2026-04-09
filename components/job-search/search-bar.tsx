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
    <section className="rounded-[18px] border border-ink/10 bg-white px-4 py-4 shadow-sm sm:px-5">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
            Job Search
          </div>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight text-ink">
            Find public jobs in one view
          </h1>
        </div>
        <p className="max-w-xl text-sm text-slate">
          Search by role and location, then browse the results list beside a clean detail panel.
        </p>
      </div>

      <form
        className="mt-4 grid gap-2 lg:grid-cols-[minmax(0,1.25fr)_minmax(260px,0.9fr)_auto]"
        onSubmit={props.onSubmit}
      >
        <input
          value={props.keyword}
          onChange={(event) => props.onKeywordChange(event.target.value)}
          placeholder="Title, keyword, or company"
          className="h-12 rounded-[14px] border border-ink/10 bg-white px-4 text-sm text-ink outline-none transition focus:border-ink/30"
          autoComplete="off"
        />

        <input
          value={props.location}
          onChange={(event) => props.onLocationChange(event.target.value)}
          placeholder="City, state, country, or remote"
          className="h-12 rounded-[14px] border border-ink/10 bg-white px-4 text-sm text-ink outline-none transition focus:border-ink/30"
          autoComplete="off"
        />

        <div className="flex gap-2">
          <button
            type="submit"
            disabled={props.isLoading}
            className="h-12 min-w-[120px] rounded-[14px] bg-[#0a66c2] px-5 text-sm font-semibold text-white transition hover:bg-[#004182] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {props.isLoading ? "Searching..." : "Search"}
          </button>
          <button
            type="button"
            onClick={props.onReset}
            className="h-12 rounded-[14px] border border-ink/10 px-4 text-sm font-medium text-slate transition hover:border-ink/25 hover:bg-mist/45"
          >
            Reset
          </button>
        </div>
      </form>
    </section>
  );
}
