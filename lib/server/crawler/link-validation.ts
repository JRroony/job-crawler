import "server-only";

import type { LinkStatus, LinkValidationResult } from "@/lib/types";

import { canonicalizeUrl, createId } from "@/lib/server/crawler/helpers";
import {
  safeFetch,
  safeFetchText,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";

export type LinkValidationDraft = Omit<LinkValidationResult, "_id" | "jobId">;

const staleMarkers = [
  "job no longer available",
  "this job is no longer available",
  "position has been filled",
  "position closed",
  "role has been closed",
  "no longer accepting applications",
  "job expired",
];

export async function validateJobLink(
  applyUrl: string,
  fetchImpl: typeof fetch,
  now = new Date(),
): Promise<LinkValidationDraft> {
  const checkedAt = now.toISOString();

  let parsed: URL;
  try {
    parsed = new URL(applyUrl);
  } catch {
    return {
      applyUrl,
      status: "invalid",
      method: "GET",
      checkedAt,
      errorMessage: "Malformed URL.",
    };
  }

  const headResult = await safeFetch(parsed.toString(), {
    fetchImpl,
    method: "HEAD",
    redirect: "follow",
    cache: "no-store",
  });

  if (!headResult.ok && shouldTreatHeadFailureAsInvalid(headResult)) {
    return buildDraftFromFailure({
      applyUrl,
      result: headResult,
      checkedAt,
      method: "HEAD",
      status: "invalid",
    });
  }

  const getResult = await safeFetchText(parsed.toString(), {
    fetchImpl,
    method: "GET",
    redirect: "follow",
    cache: "no-store",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
  });

  if (!getResult.ok) {
    return buildDraftFromFailure({
      applyUrl,
      result: getResult,
      checkedAt,
      method: "GET",
      status: resolveLinkStatusFromFailure(getResult),
    });
  }

  const body = (getResult.data ?? "").toLowerCase();
  const detectedMarkers = staleMarkers.filter((marker) => body.includes(marker));

  if (detectedMarkers.length > 0) {
    return buildDraft({
      applyUrl,
      response: getResult.response,
      checkedAt,
      method: "GET",
      status: "stale",
      staleMarkers: detectedMarkers,
    });
  }

  return buildDraft({
    applyUrl,
    response: getResult.response,
    checkedAt,
    method: "GET",
    status: "valid",
  });
}

function shouldTreatHeadFailureAsInvalid(result: Extract<SafeFetchResult, { ok: false }>) {
  return (
    result.errorType === "http" &&
    (result.statusCode === 404 || result.statusCode === 410)
  );
}

function resolveLinkStatusFromFailure(
  result: Extract<SafeFetchResult, { ok: false }>,
) {
  if (
    result.errorType === "http" &&
    result.statusCode !== undefined &&
    result.statusCode >= 400 &&
    result.statusCode < 500
  ) {
    return "invalid" satisfies LinkStatus;
  }

  return "unknown" satisfies LinkStatus;
}

export function toStoredValidation(
  jobId: string,
  validation: LinkValidationDraft,
): LinkValidationResult {
  return {
    _id: createId(),
    jobId,
    ...validation,
  };
}

function buildDraft(input: {
  applyUrl: string;
  response: Response;
  checkedAt: string;
  method: "HEAD" | "GET";
  status: LinkStatus;
  staleMarkers?: string[];
}) {
  return {
    applyUrl: input.applyUrl,
    resolvedUrl: input.response.url || undefined,
    canonicalUrl: canonicalizeUrl(input.response.url || input.applyUrl),
    status: input.status,
    method: input.method,
    httpStatus: input.response.status,
    checkedAt: input.checkedAt,
    staleMarkers: input.staleMarkers,
  } satisfies LinkValidationDraft;
}

function buildDraftFromFailure(input: {
  applyUrl: string;
  result: Extract<SafeFetchResult, { ok: false }>;
  checkedAt: string;
  method: "HEAD" | "GET";
  status: LinkStatus;
}) {
  return {
    applyUrl: input.applyUrl,
    resolvedUrl: input.result.response?.url || undefined,
    canonicalUrl: canonicalizeUrl(input.result.response?.url || input.applyUrl),
    status: input.status,
    method: input.method,
    httpStatus: input.result.statusCode,
    checkedAt: input.checkedAt,
    errorMessage: input.result.message,
  } satisfies LinkValidationDraft;
}
