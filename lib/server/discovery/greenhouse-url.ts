import "server-only";

const greenhouseHosts = new Set([
  "boards.greenhouse.io",
  "job-boards.greenhouse.io",
  "boards-api.greenhouse.io",
]);

export type ParsedGreenhouseUrl = {
  normalizedHost: string;
  kind: "board" | "job" | "api" | "embed";
  boardSlug?: string;
  jobId?: string;
  canonicalBoardUrl?: string;
  canonicalJobUrl?: string;
  canonicalApiUrl?: string;
};

export function parseGreenhouseUrl(value: string | URL): ParsedGreenhouseUrl | undefined {
  const url = toUrl(value);
  if (!url) {
    return undefined;
  }

  const normalizedHost = normalizeGreenhouseHostname(url.hostname);
  if (!greenhouseHosts.has(normalizedHost)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const embeddedBoardSlug = normalizeGreenhouseSlug(url.searchParams.get("for"));
  let boardSlug: string | undefined;
  let jobId: string | undefined;
  let kind: ParsedGreenhouseUrl["kind"];

  if (segments[0] === "embed" && segments[1] === "job_board") {
    boardSlug = embeddedBoardSlug;
    kind = "embed";
  } else if (normalizedHost === "boards-api.greenhouse.io") {
    boardSlug =
      segments[0] === "v1" && segments[1] === "boards"
        ? normalizeGreenhouseSlug(segments[2])
        : undefined;
    jobId =
      segments[3] === "jobs"
        ? normalizeGreenhouseJobId(segments[4])
        : normalizeGreenhouseJobId(url.searchParams.get("gh_jid"));
    kind = "api";
  } else {
    boardSlug = normalizeGreenhouseSlug(segments[0]);
    jobId =
      segments[1] === "jobs"
        ? normalizeGreenhouseJobId(segments[2])
        : normalizeGreenhouseJobId(url.searchParams.get("gh_jid"));
    kind = jobId ? "job" : "board";
  }

  if (!boardSlug) {
    return undefined;
  }

  const canonicalBoardUrl = buildCanonicalGreenhouseBoardUrl(boardSlug);
  const canonicalApiUrl = buildCanonicalGreenhouseApiUrl(boardSlug);

  return {
    normalizedHost,
    kind,
    boardSlug,
    jobId,
    canonicalBoardUrl,
    canonicalJobUrl: jobId
      ? buildCanonicalGreenhouseJobUrl(boardSlug, jobId)
      : undefined,
    canonicalApiUrl,
  };
}

export function canonicalizeGreenhouseUrl(value: string) {
  const parsed = parseGreenhouseUrl(value);
  if (!parsed) {
    return undefined;
  }

  if (parsed.jobId && parsed.canonicalJobUrl) {
    return parsed.canonicalJobUrl;
  }

  if (parsed.kind === "api" && parsed.canonicalApiUrl) {
    return parsed.canonicalApiUrl;
  }

  return parsed.canonicalBoardUrl;
}

export function buildCanonicalGreenhouseBoardUrl(boardSlug: string) {
  return `https://boards.greenhouse.io/${boardSlug}`;
}

export function buildCanonicalGreenhouseApiUrl(boardSlug: string) {
  return `https://boards-api.greenhouse.io/v1/boards/${boardSlug}/jobs?content=true`;
}

export function buildCanonicalGreenhouseJobUrl(boardSlug: string, jobId: string) {
  return `https://job-boards.greenhouse.io/${boardSlug}/jobs/${jobId}`;
}

export function buildCanonicalGreenhouseJobApiUrl(boardSlug: string, jobId: string) {
  return `https://boards-api.greenhouse.io/v1/boards/${boardSlug}/jobs/${jobId}?content=true`;
}

export function normalizeGreenhouseHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

export function normalizeGreenhouseSlug(value?: string | null) {
  const normalized = value?.trim().toLowerCase();
  return normalized ? normalized : undefined;
}

function normalizeGreenhouseJobId(value?: string | null) {
  const normalized = value?.trim();
  return normalized ? normalized.replace(/\/+$/, "") : undefined;
}

function toUrl(value: string | URL) {
  try {
    return value instanceof URL ? value : new URL(value);
  } catch {
    return undefined;
  }
}
