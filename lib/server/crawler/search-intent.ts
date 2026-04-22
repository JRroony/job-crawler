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

type LocationFieldNormalization = {
  country?: string;
  state?: string;
  city?: string;
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
  const normalizedLocation = normalizeStructuredLocationFields(candidate);
  const {
    country: _country,
    state: _state,
    city: _city,
    ...candidateWithoutLocation
  } = candidate;

  return {
    ...candidateWithoutLocation,
    ...normalizedLocation,
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

function normalizeStructuredLocationFields(
  candidate: Record<string, unknown>,
): LocationFieldNormalization {
  const country = readOptionalString(candidate.country);
  const state = readOptionalString(candidate.state);
  const city = readOptionalString(candidate.city);

  if (!city || state || country) {
    return {
      ...(country ? { country } : {}),
      ...(state ? { state } : {}),
      ...(city ? { city } : {}),
    };
  }

  const cityAsCountry = resolveCountryLabel(city);
  if (!cityAsCountry) {
    return {
      city,
    };
  }

  return {
    country: cityAsCountry,
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

function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function resolveCountryLabel(value: string) {
  const normalized = value.trim().toLowerCase().replace(/\./g, "").replace(/\s+/g, " ");

  if (!normalized) {
    return undefined;
  }

  if (["us", "usa", "u s", "u s a", "united states", "united states of america"].includes(normalized)) {
    return "United States";
  }

  const countryLabels: Record<string, string> = {
    canada: "Canada",
    canadian: "Canada",
    "united kingdom": "United Kingdom",
    uk: "United Kingdom",
    "great britain": "United Kingdom",
    britain: "United Kingdom",
    germany: "Germany",
    deutschland: "Germany",
    india: "India",
    france: "France",
    europe: "Europe",
  };

  return countryLabels[normalized];
}
