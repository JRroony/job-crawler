import "server-only";

const workdayHostPattern = /(^|\.)myworkdayjobs\.com$/i;

export type ParsedWorkdayUrl = {
  normalizedHost: string;
  kind: "site" | "job";
  tenant?: string;
  sitePath?: string;
  jobPath?: string;
  canonicalSourceUrl?: string;
  canonicalJobUrl?: string;
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
  const jobIndex = segments.findIndex((segment) => segment.toLowerCase() === "job");
  const siteSegments = jobIndex >= 0 ? segments.slice(0, jobIndex) : segments;
  const jobSegments = jobIndex >= 0 ? segments.slice(jobIndex + 1) : [];
  const sitePath = siteSegments.join("/");
  const jobPath = jobSegments.join("/");
  const tenant = normalizedHost.split(".")[0];
  const canonicalSourceUrl = buildCanonicalWorkdaySourceUrl(url.origin, sitePath);

  return {
    normalizedHost,
    kind: jobIndex >= 0 && jobPath ? "job" : "site",
    tenant: tenant || undefined,
    sitePath: sitePath || undefined,
    jobPath: jobPath || undefined,
    canonicalSourceUrl,
    canonicalJobUrl:
      jobIndex >= 0 && jobPath
        ? buildCanonicalWorkdayJobUrl(url.origin, sitePath, jobPath)
        : undefined,
    token: normalizeWorkdayToken(tenant, sitePath),
  };
}

export function normalizeWorkdayHostname(value: string) {
  return value.replace(/^www\./i, "").toLowerCase();
}

function buildCanonicalWorkdaySourceUrl(origin: string, sitePath?: string) {
  return sitePath ? `${origin}/${sitePath}` : origin;
}

function buildCanonicalWorkdayJobUrl(origin: string, sitePath: string, jobPath: string) {
  return `${buildCanonicalWorkdaySourceUrl(origin, sitePath)}/job/${jobPath}`;
}

function normalizeWorkdayToken(tenant?: string, sitePath?: string) {
  const left = tenant?.trim().toLowerCase();
  const right = sitePath?.trim().toLowerCase();

  if (left && right) {
    return `${left}:${right}`;
  }

  return left || right || undefined;
}

function toUrl(value: string | URL) {
  try {
    return value instanceof URL ? value : new URL(value);
  } catch {
    return undefined;
  }
}
