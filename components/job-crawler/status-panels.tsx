"use client";

import React from "react";

import { labelForProviderPlatform, labelForProviderStatus } from "@/components/job-crawler/ui-config";
import type { CrawlResponse } from "@/lib/types";
import { cn } from "@/lib/utils";

type NoticeTone = "amber" | "tide";
type StateTone = "neutral" | "amber" | "red";

type MessageTone = "error" | "info";

export function MessageBanner(props: { message: string; tone?: MessageTone }) {
  const tone = props.tone ?? "error";

  return (
    <div className={cn(
      "rounded-[18px] px-4 py-3",
      tone === "error"
        ? "border border-red-200/80 bg-red-50/90"
        : "border border-tide/20 bg-[rgba(63,114,175,0.07)]",
    )}>
      <div className={cn(
        "font-mono text-[11px] uppercase tracking-[0.18em]",
        tone === "error" ? "text-red-700" : "text-tide",
      )}>
        {tone === "error" ? "Search issue" : "Notice"}
      </div>
      <div className={cn(
        "mt-1 text-sm leading-6",
        tone === "error" ? "text-red-800" : "text-slate",
      )}>{props.message}</div>
    </div>
  );
}

export function StatePanel(props: {
  title: string;
  description: string;
  highlights?: string[];
  actionLabel?: string;
  onAction?: () => void;
  tone?: StateTone;
}) {
  const tone = props.tone ?? "neutral";

  return (
    <section
      className={cn(
        "rounded-[22px] border p-6 shadow-[0_18px_48px_rgba(15,23,42,0.05)]",
        tone === "neutral" && "border-ink/8 bg-white",
        tone === "amber" && "border-amber-200/80 bg-amber-50/80",
        tone === "red" && "border-red-200/80 bg-red-50/85",
      )}
    >
      <div className="mx-auto max-w-4xl">
        <div
          className={cn(
            "inline-flex rounded-full px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em]",
            tone === "neutral" && "border border-ink/8 bg-mist/50 text-slate",
            tone === "amber" && "border border-amber-200 bg-white/80 text-amber-900",
            tone === "red" && "border border-red-200 bg-white/80 text-red-700",
          )}
        >
          Search state
        </div>
        <h2 className="mt-3 text-2xl font-semibold text-ink">{props.title}</h2>
        <p className="mt-2 max-w-3xl text-sm leading-7 text-slate">
          {props.description}
        </p>
      </div>

      {props.highlights?.length ? (
        <div className="mx-auto mt-6 grid max-w-4xl gap-3 text-left sm:grid-cols-2">
          {props.highlights.map((highlight, index) => (
            <div
              key={`state-highlight-${index}`}
              className="rounded-[22px] border border-ink/8 bg-white px-4 py-4 text-sm leading-6 text-ink"
            >
              {highlight}
            </div>
          ))}
        </div>
      ) : null}

      {props.actionLabel && props.onAction ? (
        <div className="mt-5">
          <button
            type="button"
            onClick={props.onAction}
            className="rounded-full bg-ink px-6 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-tide"
          >
            {props.actionLabel}
          </button>
        </div>
      ) : null}
    </section>
  );
}

export function NoticeBanner(props: {
  title: string;
  description: string;
  tone: NoticeTone;
  highlights?: string[];
}) {
  return (
    <section
      className={cn(
        "rounded-[18px] border px-4 py-3",
        props.tone === "amber"
          ? "border-amber-200/80 bg-amber-50/85"
          : "border-tide/20 bg-[rgba(63,114,175,0.07)]",
      )}
    >
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <div
            className={cn(
              "inline-flex rounded-full px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em]",
              props.tone === "amber"
                ? "bg-white/80 text-amber-900"
                : "bg-white/80 text-tide",
            )}
          >
            {props.tone === "amber" ? "Partial coverage" : "Validation note"}
          </div>
          <div
            className={cn(
              "mt-2 text-base font-semibold",
              props.tone === "amber" ? "text-amber-900" : "text-tide",
            )}
          >
            {props.title}
          </div>
          <p
            className={cn(
              "mt-2 text-sm leading-6",
              props.tone === "amber" ? "text-amber-800" : "text-slate",
            )}
          >
            {props.description}
          </p>
        </div>
      </div>

      {props.highlights?.length ? (
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          {props.highlights.map((highlight, index) => (
            <div
              key={`notice-highlight-${index}`}
              className="rounded-[20px] border border-white/70 bg-white/70 px-4 py-3 text-sm leading-6 text-ink"
            >
              {highlight}
            </div>
          ))}
        </div>
      ) : null}
    </section>
  );
}

export function LoadingPanel(props: {
  stage?: CrawlResponse["crawlRun"]["stage"];
  foundCount?: number;
  fetchedCount?: number;
  matchedCount?: number;
  providerSummary?: CrawlResponse["crawlRun"]["providerSummary"];
  actionButton?: React.ReactNode;
  stopButton?: React.ReactNode;
}) {
  const stageLabel = describeStage(props.stage);
  const providerSummary = props.providerSummary ?? [];
  const activeProviders = providerSummary.filter((provider) => provider.sourceCount > 0);
  const hasVisibleResults = (props.foundCount ?? 0) > 0;

  // When results are already visible, show a compact inline bar instead of the full panel.
  if (hasVisibleResults) {
    return (
      <section className="rounded-[16px] border border-ink/8 bg-white/90 px-4 py-3 shadow-[0_12px_32px_rgba(15,23,42,0.04)]">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="flex h-2.5 w-2.5 items-center justify-center">
              <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-ember" />
            </div>
            <span className="text-sm text-slate">
              {stageLabel} &middot; {props.foundCount} saved
            </span>
            <span className="hidden text-xs text-slate/60 sm:inline">
              ({props.fetchedCount} fetched, {props.matchedCount} matched)
            </span>
          </div>
          <div className="flex items-center gap-2">
            {props.stopButton}
            {props.actionButton}
          </div>
        </div>
      </section>
    );
  }

  return (
    <section className="rounded-[20px] border border-ink/8 bg-white p-5 shadow-[0_18px_48px_rgba(15,23,42,0.05)]">
      <div className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
        Search in progress
      </div>
      <h2 className="mt-2 text-xl font-semibold text-ink">
        Gathering fresh job results
      </h2>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-slate">
        {stageLabel}
      </p>
      <div className="mt-4 flex flex-wrap gap-2 text-sm text-slate">
        <span className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5">
          {props.fetchedCount ?? 0} fetched
        </span>
        <span className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5">
          {props.matchedCount ?? 0} matched
        </span>
        <span className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5">
          {props.foundCount ?? 0} saved
        </span>
      </div>
      {props.stopButton || props.actionButton ? (
        <div className="mt-4 flex flex-wrap gap-2">
          {props.stopButton}
          {props.actionButton}
        </div>
      ) : null}
      <div className="mt-4 grid gap-4 lg:grid-cols-[0.9fr_1.1fr]">
        <div className="space-y-3">
          {(activeProviders.length > 0 ? activeProviders : providerSummary).slice(0, 4).map((provider) => (
            <div
              key={`${provider.provider}-${provider.status}`}
              className="rounded-[22px] border border-ink/10 bg-sand/45 px-4 py-4"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
                  {labelForProviderPlatform(provider.provider)}
                </div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/75">
                  {labelForProviderStatus(provider.status)}
                </div>
              </div>
              <div className="mt-2 text-sm font-medium leading-6 text-ink">
                {provider.sourceCount} source{provider.sourceCount === 1 ? "" : "s"} • {provider.fetchedCount} fetched • {provider.savedCount} saved
              </div>
            </div>
          ))}
        </div>

        <div className="grid gap-4">
          {Array.from({ length: 4 }).map((_, index) => (
            <div
              key={index}
              className="h-16 rounded-[20px] bg-[linear-gradient(90deg,rgba(79,93,117,0.08),rgba(79,93,117,0.18),rgba(79,93,117,0.08))] bg-[length:200%_100%] animate-shimmer"
            />
          ))}
        </div>
      </div>
    </section>
  );
}

// Compact indicator shown when supplemental background work is still running
// but initial results are already visible to the user.
export function BackgroundSupplementIndicator(props: {
  stage?: CrawlResponse["crawlRun"]["stage"];
  foundCount?: number;
  onStop?: () => void;
}) {
  const stageLabel = describeStage(props.stage);

  return (
    <div className="flex items-center gap-2 rounded-full border border-ink/10 bg-white/80 px-3 py-1.5 text-xs text-slate">
      <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />
      <span>
        {stageLabel ?? "Refining results"} &middot; {props.foundCount ?? 0} saved
      </span>
      {props.onStop ? (
        <button
          type="button"
          onClick={props.onStop}
          className="ml-1 rounded-full px-1.5 py-0.5 text-[11px] font-medium text-slate/70 transition hover:bg-ink/5"
          title="Stop background refinement"
        >
          Stop
        </button>
      ) : null}
    </div>
  );
}

function describeStage(stage?: CrawlResponse["crawlRun"]["stage"]) {
  if (stage === "discovering") {
    return "Discovering runnable sources and recovering direct job URLs.";
  }

  if (stage === "crawling") {
    return "Fetching provider boards and saving matching jobs in batches.";
  }

  if (stage === "validating") {
    return "Validating the newest saved links before the run wraps up.";
  }

  if (stage === "finalizing") {
    return "Finalizing counts and preparing the latest saved result set.";
  }

  return "Queueing the crawl and preparing the first provider batch.";
}
