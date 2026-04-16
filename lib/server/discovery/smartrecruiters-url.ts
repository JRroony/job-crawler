import "server-only";

const smartRecruitersHosts = new Set([
  "jobs.smartrecruiters.com",
  "careers.smartrecruiters.com",
]);

const smartRecruitersCareersHostPattern = /(^|\.)smartrecruiterscareers\.com$/i;

export type ParsedSmartRecruitersUrl = {
  normalizedHost: string;
  kind: "board" | "job";
  companyToken?: string;
  jobPath?: string;
  jobId?: string;
  canonicalBoardUrl?: string;
  canonicalJobUrl?: string;
};

export function parseSmartRecruitersUrl(
  value: string | URL,
): ParsedSmartRecruitersUrl | undefined {
  const url = toUrl(value);
  if (!url) {
    return undefined;
  }

  const normalizedHost = normalizeSmartRecruitersHostname(url.hostname);
  if (
    !smartRecruitersHosts.has(normalizedHost) &&
    !smartRecruitersCareersHostPattern.test(normalizedHost)
  ) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const companyToken =
    normalizeSmartRecruitersSegment(segments[0]) ??
    normalizeSmartRecruitersSegment(url.searchParams.get("company"));
  const jobPath = segments.slice(1).map(normalizeSmartRecruitersSegment).filter(Boolean).join("/");

  if (!companyToken) {
    return undefined;
  }

  const canonicalBoardUrl = buildCanonicalSmartRecruitersBoardUrl(companyToken);
  const normalizedJobPath = jobPath || undefined;
  const jobId = normalizeSmartRecruitersJobId(normalizedJobPath);

  return {
    normalizedHost,
    kind: normalizedJobPath ? "job" : "board",
    companyToken,
    jobPath: normalizedJobPath,
    jobId,
    canonicalBoardUrl,
    canonicalJobUrl: normalizedJobPath
      ? buildCanonicalSmartRecruitersJobUrl(companyToken, normalizedJobPath)
      : undefined,
  };
}

export function buildCanonicalSmartRecruitersBoardUrl(companyToken: string) {
  return `https://careers.smartrecruiters.com/${companyToken}`;
}

export function buildCanonicalSmartRecruitersJobUrl(
  companyToken: string,
  jobPath: string,
) {
  return `https://jobs.smartrecruiters.com/${companyToken}/${jobPath}`;
}

export function normalizeSmartRecruitersHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

function normalizeSmartRecruitersSegment(value?: string | null) {
  const normalized = value?.trim();
  return normalized ? normalized.replace(/^\/+|\/+$/g, "") : undefined;
}

function normalizeSmartRecruitersJobId(jobPath?: string) {
  const candidate = jobPath?.split("/").at(-1)?.trim();
  if (!candidate) {
    return undefined;
  }

  const prefixed = candidate.match(/^([0-9a-f-]{8,})-/i);
  if (prefixed) {
    return prefixed[1];
  }

  return candidate;
}

function toUrl(value: string | URL) {
  try {
    return value instanceof URL ? value : new URL(value);
  } catch {
    return undefined;
  }
}
