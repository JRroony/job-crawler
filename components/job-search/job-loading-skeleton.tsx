"use client";

import React from "react";

type JobLoadingSkeletonProps = {
  count?: number;
};

export function JobLoadingSkeleton({ count = 5 }: JobLoadingSkeletonProps) {
  return (
    <div className="space-y-3" aria-label="Loading jobs">
      {Array.from({ length: count }, (_, index) => (
        <article
          key={`job-loading-${index}`}
          className="overflow-hidden rounded-lg border border-slate-200 bg-white p-4 shadow-sm"
        >
          <div className="animate-pulse space-y-4">
            <div className="h-5 w-3/4 rounded bg-slate-200" />
            <div className="h-4 w-1/2 rounded bg-slate-200" />
            <div className="flex gap-2">
              <div className="h-6 w-20 rounded-full bg-slate-200" />
              <div className="h-6 w-24 rounded-full bg-slate-200" />
            </div>
            <div className="space-y-2">
              <div className="h-3 w-full rounded bg-slate-200" />
              <div className="h-3 w-4/5 rounded bg-slate-200" />
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}
