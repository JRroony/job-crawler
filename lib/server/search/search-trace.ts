import "server-only";

import { randomUUID } from "node:crypto";

import type { JobListing, SearchFilters } from "@/lib/types";

export const searchTraceSampleLimit = 10;

export type SearchTraceStageName =
  | "start"
  | "intent"
  | "candidate-query"
  | "candidate-db-result"
  | "final-filter"
  | "index-first-decision"
  | "response";

export type SearchTraceDiagnostics = {
  traceId: string;
  start?: Record<string, unknown>;
  intent?: Record<string, unknown>;
  candidateQuery?: Record<string, unknown>;
  candidateDbResult?: Record<string, unknown>;
  candidateChannelBreakdown?: Record<string, unknown>;
  finalFilter?: Record<string, unknown>;
  indexFirstDecision?: Record<string, unknown>;
  response?: Record<string, unknown>;
};

export function createSearchTraceId() {
  return randomUUID();
}

export function createEmptySearchTrace(traceId: string): SearchTraceDiagnostics {
  return { traceId };
}

export function emitSearchTraceStage<TPayload extends { traceId: string }>(
  stage: SearchTraceStageName,
  payload: TPayload,
) {
  const safePayload = toJsonSafeRecord(payload);
  console.info(`[search:trace:${stage}]`, safePayload);
  return safePayload;
}

export function attachSearchTraceStage(
  trace: SearchTraceDiagnostics,
  stage: SearchTraceStageName,
  payload: Record<string, unknown>,
): SearchTraceDiagnostics {
  return {
    ...trace,
    [toTraceDiagnosticsKey(stage)]: payload,
  };
}

export function buildSearchIntentTracePayload(input: {
  traceId: string;
  filters: SearchFilters;
}) {
  return {
    traceId: input.traceId,
    title: input.filters.title,
    country: input.filters.country,
    state: input.filters.state,
    city: input.filters.city,
    platforms: input.filters.platforms,
    experienceLevels: input.filters.experienceLevels,
    crawlMode: input.filters.crawlMode ?? "balanced",
  };
}

export function sampleCandidateIds(jobs: JobListing[]) {
  return jobs.slice(0, searchTraceSampleLimit).map((job) => job._id);
}

export function sampleCandidateTitles(jobs: JobListing[]) {
  return jobs.slice(0, searchTraceSampleLimit).map((job) => job.title);
}

export function sampleCandidateLocations(jobs: JobListing[]) {
  return jobs.slice(0, searchTraceSampleLimit).map((job) => ({
    locationText: job.locationText,
    country: job.country,
    state: job.state,
    city: job.city,
  }));
}

export function toJsonSafeRecord(value: unknown): Record<string, unknown> {
  const safeValue = toJsonSafeValue(value);
  return isRecord(safeValue) ? safeValue : {};
}

function toTraceDiagnosticsKey(stage: SearchTraceStageName) {
  if (stage === "candidate-query") {
    return "candidateQuery";
  }
  if (stage === "candidate-db-result") {
    return "candidateDbResult";
  }
  if (stage === "final-filter") {
    return "finalFilter";
  }
  if (stage === "index-first-decision") {
    return "indexFirstDecision";
  }
  return stage;
}

function toJsonSafeValue(value: unknown): unknown {
  if (value === undefined) {
    return undefined;
  }

  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof RegExp) {
    return {
      pattern: value.source,
      flags: value.flags,
    };
  }

  if (Array.isArray(value)) {
    return value.map((item) => toJsonSafeValue(item) ?? null);
  }

  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value).flatMap(([key, item]) => {
        const safeItem = toJsonSafeValue(item);
        return typeof safeItem === "undefined" ? [] : [[key, safeItem]];
      }),
    );
  }

  return String(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
