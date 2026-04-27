"use client";

import React, { type FormEventHandler } from "react";

type JobSearchHeaderProps = {
  keyword: string;
  location: string;
  isLoading: boolean;
  onKeywordChange: (value: string) => void;
  onLocationChange: (value: string) => void;
  onSubmit: FormEventHandler<HTMLFormElement>;
  onReset: () => void;
};

export function JobSearchHeader(props: JobSearchHeaderProps) {
  return (
    <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/95 shadow-sm backdrop-blur">
      <div className="mx-auto flex max-w-[1440px] flex-col gap-3 px-4 py-3 sm:px-6 lg:flex-row lg:items-center lg:gap-5 lg:px-8">
        <div className="flex shrink-0 items-center gap-2">
          <div className="flex h-9 w-9 items-center justify-center rounded-md bg-[#0a66c2] text-sm font-bold text-white">
            J
          </div>
          <div className="text-lg font-semibold tracking-tight text-ink">JobSearch</div>
        </div>

        <form
          className="grid flex-1 gap-2 lg:grid-cols-[minmax(220px,1fr)_minmax(220px,0.8fr)_auto_auto]"
          onSubmit={props.onSubmit}
        >
          <label className="flex min-h-11 items-center gap-2 rounded-md border border-slate-300 bg-white px-3 text-sm ring-1 ring-transparent transition focus-within:border-[#0a66c2] focus-within:ring-[#0a66c2]/20">
            <span className="shrink-0 font-medium text-slate/70">Title</span>
            <span className="sr-only">Job title, keywords, or company</span>
            <input
              value={props.keyword}
              onChange={(event) => props.onKeywordChange(event.target.value)}
              placeholder="Job title, keywords, or company"
              className="h-10 w-full border-none bg-transparent px-0 text-sm text-ink outline-none placeholder:text-slate/55"
              autoComplete="off"
            />
          </label>

          <label className="flex min-h-11 items-center gap-2 rounded-md border border-slate-300 bg-white px-3 text-sm ring-1 ring-transparent transition focus-within:border-[#0a66c2] focus-within:ring-[#0a66c2]/20">
            <span className="shrink-0 font-medium text-slate/70">Location</span>
            <span className="sr-only">City, state, country, or remote</span>
            <input
              value={props.location}
              onChange={(event) => props.onLocationChange(event.target.value)}
              placeholder="City, state, country, or remote"
              className="h-10 w-full border-none bg-transparent px-0 text-sm text-ink outline-none placeholder:text-slate/55"
              autoComplete="off"
            />
          </label>

          <button
            type="submit"
            className="min-h-11 rounded-md bg-[#0a66c2] px-6 text-sm font-semibold text-white transition hover:bg-[#004182]"
          >
            {props.isLoading ? "Searching..." : "Search"}
          </button>
          <button
            type="button"
            onClick={props.onReset}
            className="min-h-11 rounded-md border border-slate-300 bg-white px-4 text-sm font-medium text-slate transition hover:border-slate-400 hover:bg-slate-50"
          >
            Clear
          </button>
        </form>
      </div>
    </header>
  );
}

export const SearchBar = JobSearchHeader;
