"use client";

import type { SearchDocument } from "@/lib/types";
import { cn, formatRelativeMoment } from "@/lib/utils";

type RecentSearchesPanelProps = {
  searches: SearchDocument[];
  activeSearchId?: string;
  onLoad: (searchId: string) => void;
  onRerun: (searchId: string) => void;
  describeSearchMeta: (filters: SearchDocument["filters"]) => string;
};

export function RecentSearchesPanel(props: RecentSearchesPanelProps) {
  return (
    <section className="rounded-[28px] border border-ink/10 bg-white/88 p-5 shadow-soft backdrop-blur">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
            Recent searches
          </p>
          <h2 className="mt-3 text-2xl font-semibold text-ink">Reload or rerun a saved search.</h2>
        </div>
      </div>

      <div className="mt-4 space-y-3">
        {props.searches.length === 0 ? (
          <div className="rounded-[24px] border border-ink/10 bg-sand/55 px-4 py-4 text-sm leading-6 text-slate">
            No saved searches yet. The first crawl will populate this list.
          </div>
        ) : (
          props.searches.map((search) => {
            const active = search._id === props.activeSearchId;

            return (
              <div
                key={search._id}
                className={cn(
                  "rounded-[24px] border px-4 py-4 transition",
                  active
                    ? "border-ember/40 bg-[rgba(186,88,53,0.06)]"
                    : "border-ink/10 bg-sand/45 hover:border-ink/20 hover:bg-sand/70",
                )}
              >
                <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="truncate text-base font-semibold text-ink">
                        {search.filters.title}
                      </div>
                      {search.lastStatus ? (
                        <span className="rounded-full border border-ink/10 bg-white px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate">
                          {search.lastStatus}
                        </span>
                      ) : null}
                    </div>
                    <div className="mt-2 text-sm leading-6 text-slate">
                      {props.describeSearchMeta(search.filters)}
                    </div>
                    <div className="mt-2 text-xs uppercase tracking-[0.16em] text-slate/75">
                      Updated {formatRelativeMoment(search.updatedAt)}
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      onClick={() => props.onLoad(search._id)}
                      className="rounded-full border border-ink/15 bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-ink transition hover:border-ink hover:bg-ink hover:text-white"
                    >
                      Load
                    </button>
                    <button
                      type="button"
                      onClick={() => props.onRerun(search._id)}
                      className="rounded-full bg-ember px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-white transition hover:bg-ember/90"
                    >
                      Rerun
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </section>
  );
}
