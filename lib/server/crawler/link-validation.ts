import "server-only";

import type { LinkStatus, LinkValidationResult } from "@/lib/types";

import { canonicalizeUrl, createId } from "@/lib/server/crawler/helpers";

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

  try {
    const headResponse = await fetchImpl(parsed.toString(), {
      method: "HEAD",
      redirect: "follow",
      cache: "no-store",
    });

    if (headResponse.status >= 400) {
      return buildDraft({
        applyUrl,
        response: headResponse,
        checkedAt,
        method: "HEAD",
        status: "invalid",
      });
    }

    const getResponse = await fetchImpl(parsed.toString(), {
      method: "GET",
      redirect: "follow",
      cache: "no-store",
      headers: {
        Accept: "text/html,application/xhtml+xml",
      },
    });

    const body = await safeReadBody(getResponse);
    const detectedMarkers = staleMarkers.filter((marker) =>
      body.toLowerCase().includes(marker),
    );

    if (getResponse.status >= 400) {
      return buildDraft({
        applyUrl,
        response: getResponse,
        checkedAt,
        method: "GET",
        status: "invalid",
      });
    }

    if (detectedMarkers.length > 0) {
      return buildDraft({
        applyUrl,
        response: getResponse,
        checkedAt,
        method: "GET",
        status: "stale",
        staleMarkers: detectedMarkers,
      });
    }

    return buildDraft({
      applyUrl,
      response: getResponse,
      checkedAt,
      method: "GET",
      status: "valid",
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Validation failed.";
    const status: LinkStatus = /redirect/i.test(message) ? "invalid" : "unknown";

    return {
      applyUrl,
      status,
      method: "GET",
      checkedAt,
      errorMessage: message,
      canonicalUrl: canonicalizeUrl(applyUrl),
    };
  }
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

async function safeReadBody(response: Response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}
