import "server-only";

import {
  normalizeComparableText,
  slugToLabel,
} from "@/lib/server/crawler/helpers";
import type {
  DiscoveredSource,
  DiscoveryConfidence,
  SourceClassificationCandidate,
} from "@/lib/server/discovery/types";

const greenhouseHosts = new Set([
  "boards.greenhouse.io",
  "job-boards.greenhouse.io",
  "boards-api.greenhouse.io",
]);

const leverHosts = new Set([
  "jobs.lever.co",
  "api.lever.co",
]);

const ashbyHosts = new Set([
  "jobs.ashbyhq.com",
]);

const workdayHostPattern = /(^|\.)myworkdayjobs\.com$/i;

const publicCareerSubdomains = new Set([
  "apply",
  "boards",
  "careers",
  "jobs",
  "join",
  "opportunities",
  "roles",
  "talent",
  "team",
  "work",
  "www",
]);

export function classifySourceCandidate(
  input: SourceClassificationCandidate,
): DiscoveredSource {
  const parsedUrl = safeParseUrl(input.url);
  if (!parsedUrl) {
    return {
      id: buildSourceId("unknown", input.url),
      platform: "unknown",
      url: input.url,
      token: input.token,
      companyHint: cleanString(input.companyHint),
      confidence: input.confidence ?? "low",
      discoveryMethod: input.discoveryMethod,
    };
  }

  const greenhouseToken = extractGreenhouseToken(parsedUrl);
  if (greenhouseToken) {
    const boardUrl = `https://boards.greenhouse.io/${greenhouseToken}`;

    return {
      id: buildSourceId("greenhouse", greenhouseToken),
      platform: "greenhouse",
      url: boardUrl,
      boardUrl,
      apiUrl: `https://boards-api.greenhouse.io/v1/boards/${greenhouseToken}/jobs?content=true`,
      token: greenhouseToken,
      companyHint: resolveCompanyHint(input.companyHint, greenhouseToken, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  const leverToken = extractLeverToken(parsedUrl);
  if (leverToken) {
    const hostedUrl = `https://jobs.lever.co/${leverToken}`;

    return {
      id: buildSourceId("lever", leverToken),
      platform: "lever",
      url: hostedUrl,
      hostedUrl,
      apiUrl: `https://api.lever.co/v0/postings/${leverToken}?mode=json`,
      token: leverToken,
      companyHint: resolveCompanyHint(input.companyHint, leverToken, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  const ashbyToken = extractAshbyToken(parsedUrl);
  if (ashbyToken) {
    const boardUrl = `https://jobs.ashbyhq.com/${ashbyToken}`;

    return {
      id: buildSourceId("ashby", ashbyToken),
      platform: "ashby",
      url: boardUrl,
      boardUrl,
      token: ashbyToken,
      companyHint: resolveCompanyHint(input.companyHint, ashbyToken, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  if (isWorkdayUrl(parsedUrl)) {
    return {
      id: buildSourceId("workday", parsedUrl.toString()),
      platform: "workday",
      url: parsedUrl.toString(),
      token: input.token,
      companyHint: resolveCompanyHint(input.companyHint, input.token, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "medium"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  return {
    id: buildSourceId("company_page", parsedUrl.toString()),
    platform: "company_page",
    url: parsedUrl.toString(),
    token: input.token,
    companyHint:
      resolveCompanyHint(input.companyHint, input.token, parsedUrl) ??
      "Unknown company",
    pageType: input.pageType ?? "html_page",
    confidence: resolveCompanyPageConfidence(input),
    discoveryMethod: input.discoveryMethod,
  };
}

function safeParseUrl(value: string) {
  try {
    const parsed = new URL(value);
    parsed.hash = "";
    return parsed;
  } catch {
    return null;
  }
}

function extractGreenhouseToken(url: URL) {
  if (!isGreenhouseUrl(url)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const embeddedToken = normalizeGreenhouseToken(url.searchParams.get("for"));

  if (segments[0] === "embed") {
    return embeddedToken;
  }

  if (url.hostname === "boards.greenhouse.io") {
    return normalizeGreenhouseToken(segments[0]);
  }

  if (url.hostname === "job-boards.greenhouse.io") {
    return normalizeGreenhouseToken(segments[0]);
  }

  if (segments[0] === "v1" && segments[1] === "boards") {
    return normalizeGreenhouseToken(segments[2]);
  }

  return undefined;
}

function extractLeverToken(url: URL) {
  if (!isLeverUrl(url)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);

  if (url.hostname === "jobs.lever.co") {
    return cleanString(segments[0]);
  }

  if (segments[0] === "v0" && segments[1] === "postings") {
    return cleanString(segments[2]);
  }

  return undefined;
}

function extractAshbyToken(url: URL) {
  if (!isAshbyUrl(url)) {
    return undefined;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  return cleanString(segments[0]);
}

function isWorkdayUrl(url: URL) {
  return workdayHostPattern.test(url.hostname);
}

function isGreenhouseUrl(url: URL) {
  return greenhouseHosts.has(url.hostname);
}

function isLeverUrl(url: URL) {
  return leverHosts.has(url.hostname);
}

function isAshbyUrl(url: URL) {
  return ashbyHosts.has(url.hostname);
}

function resolveCompanyHint(
  explicitHint?: string,
  token?: string,
  parsedUrl?: URL,
) {
  const cleanedHint = cleanString(explicitHint);
  if (cleanedHint) {
    return cleanedHint;
  }

  const cleanedToken = cleanString(token);
  if (cleanedToken) {
    return slugToLabel(cleanedToken);
  }

  if (!parsedUrl) {
    return undefined;
  }

  return inferCompanyHintFromUrl(parsedUrl);
}

function inferCompanyHintFromUrl(url: URL) {
  const hostParts = url.hostname
    .split(".")
    .map((part) => part.trim())
    .filter(Boolean);

  const meaningful = hostParts.filter((part) => !publicCareerSubdomains.has(part.toLowerCase()));
  const companySlug = meaningful.length >= 2
    ? meaningful[meaningful.length - 2]
    : meaningful[0];

  return companySlug ? slugToLabel(companySlug) : undefined;
}

function resolvePlatformConfidence(
  explicitConfidence: DiscoveryConfidence | undefined,
  fallback: DiscoveryConfidence,
) {
  return explicitConfidence ?? fallback;
}

function resolveCompanyPageConfidence(
  input: SourceClassificationCandidate,
): DiscoveryConfidence {
  if (input.confidence) {
    return input.confidence;
  }

  if (input.pageType === "json_feed") {
    return "high";
  }

  if (input.pageType) {
    return "medium";
  }

  return "low";
}

function cleanString(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function normalizeGreenhouseToken(value?: string | null) {
  const trimmed = value?.trim().toLowerCase();
  return trimmed ? trimmed : undefined;
}

function buildSourceId(platform: DiscoveredSource["platform"], key: string) {
  const comparable = normalizeComparableText(key).replace(/\s+/g, "-");
  return `${platform}:${comparable || "source"}`;
}
