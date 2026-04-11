import "server-only";

const leverHosts = new Set([
  "jobs.lever.co",
  "api.lever.co",
]);

export type ParsedLeverUrl = {
  normalizedHost: string;
  kind: "board" | "job" | "api";
  siteToken?: string;
  jobId?: string;
  canonicalHostedUrl?: string;
  canonicalJobUrl?: string;
  canonicalApiUrl?: string;
};

export function parseLeverUrl(value: string | URL): ParsedLeverUrl | undefined {
  const url = toUrl(value);
  if (!url) {
    return undefined;
  }

  const normalizedHost = normalizeLeverHostname(url.hostname);
  if (!leverHosts.has(normalizedHost)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  let siteToken: string | undefined;
  let jobId: string | undefined;
  let kind: ParsedLeverUrl["kind"];

  if (normalizedHost === "api.lever.co") {
    siteToken =
      segments[0] === "v0" && segments[1] === "postings"
        ? normalizeLeverSegment(segments[2])
        : undefined;
    jobId =
      segments[0] === "v0" && segments[1] === "postings"
        ? normalizeLeverSegment(segments[3])
        : undefined;
    kind = jobId ? "job" : "api";
  } else {
    siteToken = normalizeLeverSegment(segments[0]);
    jobId = normalizeLeverSegment(segments[1]);
    kind = jobId ? "job" : "board";
  }

  if (!siteToken) {
    return undefined;
  }

  const canonicalHostedUrl = buildCanonicalLeverHostedUrl(siteToken);

  return {
    normalizedHost,
    kind,
    siteToken,
    jobId,
    canonicalHostedUrl,
    canonicalJobUrl: jobId
      ? buildCanonicalLeverJobUrl(siteToken, jobId)
      : undefined,
    canonicalApiUrl: buildCanonicalLeverApiUrl(siteToken, jobId),
  };
}

export function buildCanonicalLeverHostedUrl(siteToken: string) {
  return `https://jobs.lever.co/${siteToken}`;
}

export function buildCanonicalLeverJobUrl(siteToken: string, jobId: string) {
  return `https://jobs.lever.co/${siteToken}/${jobId}`;
}

export function buildCanonicalLeverApiUrl(siteToken: string, jobId?: string) {
  return `https://api.lever.co/v0/postings/${siteToken}${jobId ? `/${jobId}` : ""}?mode=json`;
}

export function normalizeLeverHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

function normalizeLeverSegment(value?: string | null) {
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
