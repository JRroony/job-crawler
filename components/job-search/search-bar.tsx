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
    <section className="rounded-[24px] border border-ink/10 bg-white/94 px-4 py-4 shadow-[0_18px_50px_rgba(15,23,42,0.06)] backdrop-blur sm:px-5">
      <div className="flex flex-col gap-3 border-b border-ink/8 pb-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/60">
            Job search
          </div>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight text-ink sm:text-[2rem]">
            Find relevant roles quickly
          </h1>
          <p className="mt-1 text-sm text-slate">
            Search public postings, narrow the list with lightweight filters, and inspect the original job details without leaving the page.
          </p>
        </div>
        <div className="rounded-full border border-ink/10 bg-mist/45 px-3 py-1.5 text-xs font-semibold text-slate">
          Public listings only
        </div>
      </div>

      <form
        className="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1.35fr)_minmax(250px,0.9fr)_auto]"
        onSubmit={props.onSubmit}
      >
        <label className="rounded-[18px] border border-ink/10 bg-white px-4 py-3 shadow-sm ring-1 ring-transparent transition focus-within:border-[#0a66c2]/40 focus-within:ring-[#0a66c2]/20">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/55">
            Role
          </div>
          <input
            value={props.keyword}
            onChange={(event) => props.onKeywordChange(event.target.value)}
            placeholder="Title, keyword, or company"
            className="mt-2 h-7 w-full border-none bg-transparent px-0 text-[15px] text-ink outline-none placeholder:text-slate/45"
            autoComplete="off"
          />
        </label>

        <label className="rounded-[18px] border border-ink/10 bg-white px-4 py-3 shadow-sm ring-1 ring-transparent transition focus-within:border-[#0a66c2]/40 focus-within:ring-[#0a66c2]/20">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/55">
            Location
          </div>
          <input
            value={props.location}
            onChange={(event) => props.onLocationChange(event.target.value)}
            placeholder="City, state, country, or remote"
            className="mt-2 h-7 w-full border-none bg-transparent px-0 text-[15px] text-ink outline-none placeholder:text-slate/45"
            autoComplete="off"
          />
        </label>

        <div className="flex gap-2 lg:self-stretch">
          <button
            type="submit"
            disabled={props.isLoading}
            className="min-h-[56px] min-w-[132px] rounded-[16px] bg-[#0a66c2] px-5 text-sm font-semibold text-white transition hover:bg-[#004182] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {props.isLoading ? "Searching..." : "Search"}
          </button>
          <button
            type="button"
            onClick={props.onReset}
            className="min-h-[56px] rounded-[16px] border border-ink/10 bg-white px-4 text-sm font-medium text-slate transition hover:border-ink/25 hover:bg-mist/45"
          >
            Reset
          </button>
        </div>
      </form>
    </section>
  );
}
