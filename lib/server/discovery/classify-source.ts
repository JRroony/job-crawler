import "server-only";

import {
  parseAshbyUrl,
} from "@/lib/server/discovery/ashby-url";
import {
  normalizeComparableText,
  slugToLabel,
} from "@/lib/server/crawler/helpers";
import {
  parseGreenhouseUrl,
} from "@/lib/server/discovery/greenhouse-url";
import {
  buildCanonicalLeverApiUrl,
  parseLeverUrl,
} from "@/lib/server/discovery/lever-url";
import type {
  DiscoveredSource,
  DiscoveryConfidence,
  SourceClassificationCandidate,
} from "@/lib/server/discovery/types";
import {
  parseWorkdayUrl,
} from "@/lib/server/discovery/workday-url";

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

  const greenhouseUrl = parseGreenhouseUrl(parsedUrl);
  if (greenhouseUrl?.boardSlug) {
    const boardUrl = greenhouseUrl.canonicalBoardUrl;

    return {
      id: buildSourceId("greenhouse", greenhouseUrl.boardSlug),
      platform: "greenhouse",
      url: boardUrl ?? parsedUrl.toString(),
      boardUrl,
      apiUrl: greenhouseUrl.canonicalApiUrl,
      token: greenhouseUrl.boardSlug,
      jobId: greenhouseUrl.jobId,
      companyHint: resolveCompanyHint(input.companyHint, greenhouseUrl.boardSlug, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  const leverUrl = parseLeverUrl(parsedUrl);
  if (leverUrl?.siteToken) {
    const hostedUrl = leverUrl.canonicalHostedUrl ?? `https://jobs.lever.co/${leverUrl.siteToken}`;

    return {
      id: buildSourceId("lever", leverUrl.siteToken),
      platform: "lever",
      url: hostedUrl,
      hostedUrl,
      apiUrl: buildCanonicalLeverApiUrl(leverUrl.siteToken),
      token: leverUrl.siteToken,
      jobId: leverUrl.jobId,
      companyHint: resolveCompanyHint(input.companyHint, leverUrl.siteToken, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  const ashbyUrl = parseAshbyUrl(parsedUrl);
  if (ashbyUrl?.companyToken) {
    const boardUrl = ashbyUrl.canonicalBoardUrl ?? `https://jobs.ashbyhq.com/${ashbyUrl.companyToken}`;

    return {
      id: buildSourceId("ashby", ashbyUrl.companyToken),
      platform: "ashby",
      url: boardUrl,
      boardUrl,
      token: ashbyUrl.companyToken,
      jobId: ashbyUrl.jobPath,
      companyHint: resolveCompanyHint(input.companyHint, ashbyUrl.companyToken, parsedUrl),
      confidence: resolvePlatformConfidence(input.confidence, "high"),
      discoveryMethod: input.discoveryMethod,
    };
  }

  const workdayUrl = parseWorkdayUrl(parsedUrl);
  if (workdayUrl) {
    return {
      id: buildSourceId("workday", workdayUrl.token ?? workdayUrl.canonicalSourceUrl ?? parsedUrl.toString()),
      platform: "workday",
      url: workdayUrl.canonicalSourceUrl ?? parsedUrl.toString(),
      token: workdayUrl.token ?? input.token,
      jobId: workdayUrl.jobPath,
      sitePath: workdayUrl.sitePath,
      companyHint: resolveCompanyHint(input.companyHint, workdayUrl.token ?? input.token, parsedUrl),
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

function buildSourceId(platform: DiscoveredSource["platform"], key: string) {
  const comparable = normalizeComparableText(key).replace(/\s+/g, "-");
  return `${platform}:${comparable || "source"}`;
}
