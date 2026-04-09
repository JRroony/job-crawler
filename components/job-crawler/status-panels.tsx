"use client";

import { cn } from "@/lib/utils";

type NoticeTone = "amber" | "tide";
type StateTone = "neutral" | "amber" | "red";

export function MessageBanner(props: { message: string }) {
  return (
    <div className="rounded-[24px] border border-red-200/80 bg-red-50/85 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-red-700">
        Search issue
      </div>
      <div className="mt-2 text-sm leading-6 text-red-800">{props.message}</div>
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
        "rounded-[28px] border p-8 shadow-[0_18px_48px_rgba(15,23,42,0.05)]",
        tone === "neutral" && "border-ink/8 bg-white",
        tone === "amber" && "border-amber-200/80 bg-amber-50/80",
        tone === "red" && "border-red-200/80 bg-red-50/85",
      )}
    >
      <div className="mx-auto max-w-4xl text-center">
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
        <h2 className="mt-4 text-2xl font-semibold text-ink">{props.title}</h2>
        <p className="mx-auto mt-3 max-w-3xl text-base leading-7 text-slate">
          {props.description}
        </p>
      </div>

      {props.highlights?.length ? (
        <div className="mx-auto mt-6 grid max-w-4xl gap-3 text-left sm:grid-cols-2">
          {props.highlights.map((highlight) => (
            <div
              key={highlight}
              className="rounded-[22px] border border-ink/8 bg-white px-4 py-4 text-sm leading-6 text-ink"
            >
              {highlight}
            </div>
          ))}
        </div>
      ) : null}

      {props.actionLabel && props.onAction ? (
        <div className="mt-6 text-center">
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
        "rounded-[28px] border px-5 py-4",
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
              "mt-3 text-base font-semibold",
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
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {props.highlights.map((highlight) => (
            <div
              key={highlight}
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

export function LoadingPanel() {
  const steps = [
    "Discovering public job sources",
    "Fetching matching jobs from enabled platforms",
    "Normalizing and preparing the result list",
  ];

  return (
    <section className="rounded-[28px] border border-ink/8 bg-white p-8 shadow-[0_18px_48px_rgba(15,23,42,0.05)]">
      <div className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
        Search in progress
      </div>
      <h2 className="mt-3 text-2xl font-semibold text-ink">
        Gathering fresh job results
      </h2>
      <div className="mt-5 grid gap-4 lg:grid-cols-[0.9fr_1.1fr]">
        <div className="space-y-3">
          {steps.map((step, index) => (
            <div
              key={step}
              className="rounded-[22px] border border-ink/10 bg-sand/45 px-4 py-4"
            >
              <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
                Step {index + 1}
              </div>
              <div className="mt-2 text-sm font-medium leading-6 text-ink">{step}</div>
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
