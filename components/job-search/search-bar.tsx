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
    <section className="rounded-[32px] border border-ink/8 bg-white px-5 py-5 shadow-[0_22px_60px_rgba(15,23,42,0.06)] sm:px-6 sm:py-6">
      <div className="max-w-3xl">
        <div className="text-sm font-medium uppercase tracking-[0.18em] text-slate/65">
          Public jobs search
        </div>
        <h1 className="mt-3 text-3xl font-semibold tracking-tight text-ink sm:text-4xl">
          Search roles, narrow the list, and inspect jobs without leaving the page.
        </h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-slate sm:text-base">
          A simpler job-search surface on top of the existing crawler so the primary flow stays
          focused on finding, filtering, and opening good roles.
        </p>
      </div>

      <form className="mt-6 grid gap-3 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.9fr)_auto]" onSubmit={props.onSubmit}>
        <div className="rounded-[24px] border border-ink/10 bg-white px-4 py-3">
          <label className="block text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
            Keywords
          </label>
          <input
            value={props.keyword}
            onChange={(event) => props.onKeywordChange(event.target.value)}
            placeholder="Job title, keyword, or company"
            className="mt-2 w-full border-0 bg-transparent p-0 text-base text-ink outline-none placeholder:text-slate/45"
            autoComplete="off"
          />
        </div>

        <div className="rounded-[24px] border border-ink/10 bg-white px-4 py-3">
          <label className="block text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
            Location
          </label>
          <input
            value={props.location}
            onChange={(event) => props.onLocationChange(event.target.value)}
            placeholder="City, state, or remote"
            className="mt-2 w-full border-0 bg-transparent p-0 text-base text-ink outline-none placeholder:text-slate/45"
            autoComplete="off"
          />
        </div>

        <div className="flex gap-2 lg:items-stretch">
          <button
            type="submit"
            disabled={props.isLoading}
            className="min-w-[132px] rounded-[24px] bg-ink px-6 py-4 text-sm font-semibold text-white transition hover:bg-tide disabled:cursor-not-allowed disabled:opacity-60"
          >
            {props.isLoading ? "Searching..." : "Search"}
          </button>
          <button
            type="button"
            onClick={props.onReset}
            className="rounded-[24px] border border-ink/10 px-5 py-4 text-sm font-medium text-slate transition hover:border-ink/25 hover:bg-mist/45"
          >
            Reset
          </button>
        </div>
      </form>
    </section>
  );
}
