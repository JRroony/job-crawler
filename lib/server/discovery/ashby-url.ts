import "server-only";

const ashbyHosts = new Set([
  "jobs.ashbyhq.com",
]);

export type ParsedAshbyUrl = {
  normalizedHost: string;
  kind: "board" | "job";
  companyToken?: string;
  jobPath?: string;
  canonicalBoardUrl?: string;
  canonicalJobUrl?: string;
};

export function parseAshbyUrl(value: string | URL): ParsedAshbyUrl | undefined {
  const url = toUrl(value);
  if (!url) {
    return undefined;
  }

  const normalizedHost = normalizeAshbyHostname(url.hostname);
  if (!ashbyHosts.has(normalizedHost)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const companyToken = normalizeAshbySegment(segments[0]);
  const jobPath = segments.slice(1).map(normalizeAshbySegment).filter(Boolean).join("/");

  if (!companyToken) {
    return undefined;
  }

  const canonicalBoardUrl = buildCanonicalAshbyBoardUrl(companyToken);

  return {
    normalizedHost,
    kind: jobPath ? "job" : "board",
    companyToken,
    jobPath: jobPath || undefined,
    canonicalBoardUrl,
    canonicalJobUrl: jobPath
      ? buildCanonicalAshbyJobUrl(companyToken, jobPath)
      : undefined,
  };
}

export function buildCanonicalAshbyBoardUrl(companyToken: string) {
  return `https://jobs.ashbyhq.com/${companyToken}`;
}

export function buildCanonicalAshbyJobUrl(companyToken: string, jobPath: string) {
  return `https://jobs.ashbyhq.com/${companyToken}/${jobPath}`;
}

export function normalizeAshbyHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

function normalizeAshbySegment(value?: string | null) {
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
