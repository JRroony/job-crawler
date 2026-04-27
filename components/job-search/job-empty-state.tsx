"use client";

import React from "react";

type JobEmptyStateProps = {
  backgroundUpdating?: boolean;
};

export function JobEmptyState(props: JobEmptyStateProps) {
  return (
    <section className="rounded-lg border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
      <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-[#e7f3ff] text-base font-semibold text-[#0a66c2]">
        0
      </div>
      <h2 className="mt-4 text-xl font-semibold text-ink">No matching jobs found yet.</h2>
      <p className="mt-2 text-sm text-slate">Try a broader title or location.</p>
      {props.backgroundUpdating ? (
        <p className="mt-3 text-sm text-slate/80">We&apos;re updating results in the background.</p>
      ) : null}
    </section>
  );
}
