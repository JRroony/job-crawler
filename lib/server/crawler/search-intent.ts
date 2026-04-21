import "server-only";

import {
  sanitizeSearchFiltersInput,
  type CrawlerPlatform,
} from "@/lib/types";

type SearchIntentNormalization = {
  title: string;
  platforms?: CrawlerPlatform[];
  country?: string;
};

const platformPhraseMatchers: Array<{
  platform: CrawlerPlatform;
  pattern: RegExp;
}> = [
  {
    platform: "greenhouse",
    pattern: /\bgreenhouse\b/gi,
  },
  {
    platform: "lever",
    pattern: /\blever\b/gi,
  },
  {
    platform: "ashby",
    pattern: /\bashby\b/gi,
  },
  {
    platform: "smartrecruiters",
    pattern: /\bsmart ?recruiters?\b/gi,
  },
  {
    platform: "company_page",
    pattern: /\bcompany(?: |-)?pages?\b/gi,
  },
  {
    platform: "workday",
    pattern: /\bworkday\b/gi,
  },
];

const countryPhraseMatchers: Array<{
  country: string;
  pattern: RegExp;
}> = [
  {
    country: "United States",
    pattern: /\b(?:united states(?: of america)?|u\.?\s*s\.?\s*a?\.?)\b/gi,
  },
  {
    country: "Canada",
    pattern: /\b(?:canada|canadian)\b/gi,
  },
];

const wrapperWordPattern =
  /\b(?:jobs?|roles?|positions?|openings?|listings?)\b/gi;

const leadingTrailingNoisePatterns = [
  /^(?:in|at|for|on|with)(?: the)?\b[\s,:-]*/i,
  /[\s,:-]*(?:in|at|for|on|with)(?: the)?$/i,
  /[\s,:-]*the$/i,
];

export function normalizeSearchIntentInput(rawFilters: unknown) {
  const normalizedFilters = sanitizeSearchFiltersInput(rawFilters);
  if (
    !normalizedFilters ||
    typeof normalizedFilters !== "object" ||
    Array.isArray(normalizedFilters)
  ) {
    return normalizedFilters;
  }

  const candidate = { ...(normalizedFilters as Record<string, unknown>) };
  const rawTitle = typeof candidate.title === "string" ? candidate.title.trim() : "";
  if (!rawTitle) {
    return candidate;
  }

  const normalized = normalizeSearchIntent(rawTitle);

  return {
    ...candidate,
    title: normalized.title,
    ...(hasPopulatedArray(candidate.platforms)
      ? {}
      : normalized.platforms?.length
        ? {
            platforms: normalized.platforms,
          }
        : {}),
    ...(hasPopulatedString(candidate.country) || !normalized.country
      ? {}
      : {
          country: normalized.country,
        }),
  };
}

export function normalizeSearchIntent(title: string): SearchIntentNormalization {
  let workingTitle = title.trim();
  const inferredPlatforms = new Set<CrawlerPlatform>();
  let inferredCountry: string | undefined;

  for (const matcher of platformPhraseMatchers) {
    if (!workingTitle.match(matcher.pattern)) {
      continue;
    }

    inferredPlatforms.add(matcher.platform);
    workingTitle = workingTitle.replace(matcher.pattern, " ");
  }

  for (const matcher of countryPhraseMatchers) {
    if (!workingTitle.match(matcher.pattern)) {
      continue;
    }

    inferredCountry ??= matcher.country;
    workingTitle = workingTitle.replace(matcher.pattern, " ");
  }

  workingTitle = workingTitle.replace(wrapperWordPattern, " ");
  workingTitle = collapseWhitespace(workingTitle);

  for (const pattern of leadingTrailingNoisePatterns) {
    workingTitle = workingTitle.replace(pattern, "");
    workingTitle = collapseWhitespace(workingTitle);
  }

  return {
    title: workingTitle.length >= 2 ? workingTitle : title.trim(),
    ...(inferredPlatforms.size > 0
      ? {
          platforms: Array.from(inferredPlatforms),
        }
      : {}),
    ...(inferredCountry
      ? {
          country: inferredCountry,
        }
      : {}),
  };
}

function collapseWhitespace(value: string) {
  return value
    .replace(/[|/]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hasPopulatedString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function hasPopulatedArray(value: unknown): value is unknown[] {
  return Array.isArray(value) && value.length > 0;
}
