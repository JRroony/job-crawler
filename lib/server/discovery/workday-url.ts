import "server-only";

const workdayHostPattern = /(^|\.)myworkdayjobs\.com$/i;
const workdayApiPrefix = ["wday", "cxs"] as const;
const localeSegmentPattern = /^[a-z]{2}(?:-[a-z]{2})?$/i;

export type ParsedWorkdayUrl = {
  normalizedHost: string;
  kind: "site" | "job" | "api";
  tenant?: string;
  locale?: string;
  sitePath?: string;
  careerSitePath?: string;
  jobPath?: string;
  canonicalSourceUrl?: string;
  canonicalJobUrl?: string;
  canonicalApiUrl?: string;
  canonicalApiJobUrl?: string;
  token?: string;
};

export function parseWorkdayUrl(value: string | URL): ParsedWorkdayUrl | undefined {
  const url = toUrl(value);
  if (!url) {
    return undefined;
  }

  const normalizedHost = normalizeWorkdayHostname(url.hostname);
  if (!workdayHostPattern.test(normalizedHost)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  if (segments.length === 0) {
    return undefined;
  }

  if (isWorkdayApiPath(segments)) {
    return parseWorkdayApiUrl(url, normalizedHost, segments);
  }

  return parseWorkdayDisplayUrl(url, normalizedHost, segments);
}

export function normalizeWorkdayHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

export function buildCanonicalWorkdaySourceUrl(origin: string, sitePath?: string) {
  return sitePath ? `${origin}/${encodeWorkdayPath(sitePath)}` : origin;
}

export function buildCanonicalWorkdayJobUrl(origin: string, sitePath: string, jobPath: string) {
  return `${buildCanonicalWorkdaySourceUrl(origin, sitePath)}/job/${encodeWorkdayPath(jobPath)}`;
}

export function buildCanonicalWorkdayApiListUrl(
  origin: string,
  tenant: string,
  careerSitePath: string,
) {
  return `${origin}/wday/cxs/${encodeWorkdayPath(tenant)}/${encodeWorkdayPath(careerSitePath)}/jobs`;
}

export function buildCanonicalWorkdayApiJobUrl(
  origin: string,
  tenant: string,
  careerSitePath: string,
  jobPath: string,
) {
  return `${buildCanonicalWorkdayApiListUrl(origin, tenant, careerSitePath)}/${encodeWorkdayPath(jobPath)}`;
}

function parseWorkdayDisplayUrl(
  url: URL,
  normalizedHost: string,
  segments: string[],
): ParsedWorkdayUrl | undefined {
  const jobIndex = segments.findIndex((segment) => segment.toLowerCase() === "job");
  const siteSegments = jobIndex >= 0 ? segments.slice(0, jobIndex) : segments;
  const jobSegments = jobIndex >= 0 ? segments.slice(jobIndex + 1) : [];
  const locale = looksLikeLocaleSegment(siteSegments[0]) ? siteSegments[0] : undefined;
  const careerSiteSegments = locale ? siteSegments.slice(1) : siteSegments;
  const tenant = normalizedHost.split(".")[0];
  const sitePath = siteSegments.join("/");
  const careerSitePath = careerSiteSegments.join("/");
  const jobPath = jobSegments.join("/");

  if (!tenant || !careerSitePath) {
    return undefined;
  }

  return {
    normalizedHost,
    kind: jobIndex >= 0 && jobPath ? "job" : "site",
    tenant,
    locale,
    sitePath: sitePath || undefined,
    careerSitePath,
    jobPath: jobPath || undefined,
    canonicalSourceUrl: buildCanonicalWorkdaySourceUrl(url.origin, sitePath),
    canonicalJobUrl:
      jobIndex >= 0 && sitePath && jobPath
        ? buildCanonicalWorkdayJobUrl(url.origin, sitePath, jobPath)
        : undefined,
    canonicalApiUrl: buildCanonicalWorkdayApiListUrl(url.origin, tenant, careerSitePath),
    canonicalApiJobUrl:
      jobIndex >= 0 && jobPath
        ? buildCanonicalWorkdayApiJobUrl(url.origin, tenant, careerSitePath, jobPath)
        : undefined,
    token: normalizeWorkdayToken(tenant, careerSitePath),
  };
}

function parseWorkdayApiUrl(
  url: URL,
  normalizedHost: string,
  segments: string[],
): ParsedWorkdayUrl | undefined {
  const tenant = normalizeWorkdaySegment(segments[2]);
  const jobsIndex = segments.findIndex((segment, index) =>
    index >= 3 && segment.toLowerCase() === "jobs",
  );
  const careerSiteSegments =
    jobsIndex >= 0 ? segments.slice(3, jobsIndex) : segments.slice(3);
  const jobSegments = jobsIndex >= 0 ? segments.slice(jobsIndex + 1) : [];
  const careerSitePath = careerSiteSegments.join("/");
  const jobPath = jobSegments.join("/");

  if (!tenant || !careerSitePath) {
    return undefined;
  }

  return {
    normalizedHost,
    kind: jobPath ? "job" : "api",
    tenant,
    careerSitePath,
    jobPath: jobPath || undefined,
    canonicalSourceUrl: undefined,
    canonicalJobUrl: undefined,
    canonicalApiUrl: buildCanonicalWorkdayApiListUrl(url.origin, tenant, careerSitePath),
    canonicalApiJobUrl: jobPath
      ? buildCanonicalWorkdayApiJobUrl(url.origin, tenant, careerSitePath, jobPath)
      : undefined,
    token: normalizeWorkdayToken(tenant, careerSitePath),
  };
}

function normalizeWorkdayToken(tenant?: string, careerSitePath?: string) {
  const normalizedTenant = normalizeWorkdaySegment(tenant)?.toLowerCase();
  const normalizedCareerSite = careerSitePath
    ?.split("/")
    .map((segment) => normalizeWorkdaySegment(segment)?.toLowerCase())
    .filter(Boolean)
    .join("/");

  if (normalizedTenant && normalizedCareerSite) {
    return `${normalizedTenant}:${normalizedCareerSite}`;
  }

  return normalizedTenant || normalizedCareerSite || undefined;
}

function isWorkdayApiPath(segments: string[]) {
  return workdayApiPrefix.every(
    (segment, index) => segments[index]?.toLowerCase() === segment,
  );
}

function looksLikeLocaleSegment(value?: string) {
  return typeof value === "string" && localeSegmentPattern.test(value);
}

function normalizeWorkdaySegment(value?: string | null) {
  const normalized = value?.trim();
  return normalized ? normalized.replace(/^\/+|\/+$/g, "") : undefined;
}

function encodeWorkdayPath(path: string) {
  return path
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function toUrl(value: string | URL) {
  try {
    return value instanceof URL ? value : new URL(value);
  } catch {
    return undefined;
  }
}
