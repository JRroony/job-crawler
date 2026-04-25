"use client";

import { buildStableJobRenderIdentity } from "@/lib/job-identity";
import {
  evaluateStoredExperienceMatch,
  resolveExperienceLevel,
} from "@/lib/experience";
import type {
  ActiveCrawlerPlatform,
  ExperienceLevel,
  JobListing,
  SearchFilters,
} from "@/lib/types";
import { activeCrawlerPlatforms, experienceLevels } from "@/lib/types";
import { labelForCrawlerPlatform } from "@/components/job-crawler/ui-config";
import { labelForExperience } from "@/lib/utils";
import { parseGeoIntent } from "@/lib/geo/parse";
import { matchJobLocationAgainstGeoIntent } from "@/lib/geo/match";
import { normalizeJobGeoLocation } from "@/lib/geo/location";

export type PostedDateFilter = "any" | "24h" | "7d" | "30d";

export type ClientResultFilters = {
  remoteOnly: boolean;
  visaFriendlyOnly: boolean;
  postedDate: PostedDateFilter;
};

export const defaultClientResultFilters: ClientResultFilters = {
  remoteOnly: false,
  visaFriendlyOnly: false,
  postedDate: "any",
};

export const platformFilterOptions: Array<{
  value: ActiveCrawlerPlatform;
  label: string;
}> = activeCrawlerPlatforms.map((platform) => ({
  value: platform,
  label: labelForCrawlerPlatform(platform),
}));

export const disabledPlatformFilterOptions = [
  {
    label: "LinkedIn",
    detail: "Limited",
  },
  {
    label: "Indeed",
    detail: "Limited",
  },
];

export const experienceFilterOptions: Array<{
  value: ExperienceLevel;
  label: string;
}> = experienceLevels.map((level) => ({
  value: level,
  label: labelForExperience(level),
}));

export const postedDateFilterOptions: Array<{
  value: PostedDateFilter;
  label: string;
}> = [
  {
    value: "any",
    label: "Any time",
  },
  {
    value: "24h",
    label: "24 hours",
  },
  {
    value: "7d",
    label: "7 days",
  },
  {
    value: "30d",
    label: "30 days",
  },
];

export function buildLocationInputValue(filters: SearchFilters) {
  return [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
}

export function parseLocationInput(
  value: string,
): Pick<SearchFilters, "city" | "state" | "country"> {
  const cleaned = value.trim();
  if (!cleaned) {
    return {
      city: "",
      state: "",
      country: "",
    };
  }

  const parts = cleaned
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length === 0) {
    return {
      city: "",
      state: "",
      country: "",
    };
  }

  if (parts.length === 1) {
    const intent = parseGeoIntent(parts[0]);
    if (intent.scope === "remote_country" && intent.country) {
      return {
        city: "Remote",
        state: "",
        country: intent.country.name,
      };
    }

    if (intent.scope === "global_remote") {
      return {
        city: "Remote",
        state: "",
        country: "",
      };
    }

    if (intent.scope === "country" && intent.country) {
      return {
        city: "",
        state: "",
        country: intent.country.name,
      };
    }

    return {
      city: intent.city?.name ?? parts[0],
      state: intent.region?.name ?? "",
      country: intent.country?.name ?? "",
    };
  }

  if (parts.length === 2) {
    if (isRemoteTerm(parts[0])) {
      return {
        city: "Remote",
        state: "",
        country: normalizeCountryLabel(parts[1]),
      };
    }

    if (isCountryLike(parts[1])) {
      return {
        city: parts[0],
        state: "",
        country: normalizeCountryLabel(parts[1]),
      };
    }

    return {
      city: parts[0],
      state: parts[1],
      country: "",
    };
  }

  return {
    city: parts[0],
    state: parts[1],
    country: normalizeCountryLabel(parts.slice(2).join(", ")),
  };
}

export function filterJobsForDisplay(
  jobs: JobListing[],
  searchFilters: SearchFilters,
  resultFilters: ClientResultFilters,
) {
  const selectedPlatforms = new Set(
    searchFilters.platforms?.length
      ? searchFilters.platforms.filter((platform): platform is ActiveCrawlerPlatform =>
          activeCrawlerPlatforms.includes(platform as ActiveCrawlerPlatform),
        )
      : activeCrawlerPlatforms,
  );
  const selectedExperienceLevels = new Set(searchFilters.experienceLevels ?? []);
  const includeUnspecified =
    searchFilters.includeUnspecifiedExperience === true ||
    searchFilters.experienceMatchMode === "broad";

  return jobs.filter((job) => {
    if (!matchesActiveSearchTitle(job, searchFilters)) {
      return false;
    }

    if (!matchesActiveSearchLocation(job, searchFilters)) {
      return false;
    }

    if (selectedPlatforms.size > 0 && !selectedPlatforms.has(job.sourcePlatform as ActiveCrawlerPlatform)) {
      return false;
    }

    if (selectedExperienceLevels.size > 0) {
      const classification = job.experienceClassification ?? {
        explicitLevel: job.experienceLevel,
        inferredLevel: undefined,
        confidence: "high" as const,
        isUnspecified: !job.experienceLevel,
        reasons: job.experienceLevel ? ["Resolved stored experience level."] : [],
      };
      const experienceMatch = evaluateStoredExperienceMatch({
        classification,
        selectedLevels: Array.from(selectedExperienceLevels),
        mode: searchFilters.experienceMatchMode ?? "balanced",
        includeUnspecified,
      });

      if (!experienceMatch.matches) {
        return false;
      }
    }

    if (resultFilters.remoteOnly && !isRemoteJob(job)) {
      return false;
    }

    if (resultFilters.visaFriendlyOnly && !isVisaFriendlyJob(job)) {
      return false;
    }

    if (!matchesPostedDateFilter(job, resultFilters.postedDate)) {
      return false;
    }

    return true;
  });
}

function matchesActiveSearchTitle(job: JobListing, filters: SearchFilters) {
  const query = normalizeSearchText(filters.title);
  if (!query) {
    return true;
  }

  const title = normalizeSearchText(job.normalizedTitle || job.title);
  if (!title) {
    return false;
  }

  if (title.includes(query) || query.includes(title)) {
    return true;
  }

  if (query === "machine learning engineer") {
    return [
      "machine learning engineer",
      "ml engineer",
      "ai engineer",
      "applied ai engineer",
      "applied ml engineer",
      "machine learning infrastructure engineer",
      "deep learning engineer",
      "computer vision engineer",
      "nlp engineer",
      "llm engineer",
      "generative ai engineer",
      "research engineer ml",
      "ai research engineer",
    ].some((variant) => title.includes(variant));
  }

  const queryTerms = query.split(" ").filter((term) => term.length > 2);
  return queryTerms.length > 0 && queryTerms.every((term) => title.includes(term));
}

function matchesActiveSearchLocation(job: JobListing, filters: SearchFilters) {
  const intent = parseGeoIntent(buildLocationInputValue(filters));
  if (intent.scope === "none") {
    return true;
  }
  const geoLocation = job.geoLocation ?? normalizeJobGeoLocation(job);
  return matchJobLocationAgainstGeoIntent(geoLocation, intent).matches;
}

function parseRemoteCountryTerm(value: string) {
  const normalized = normalizeSearchText(value);
  const match = normalized.match(/^remote(?:\s+(?:in|within))?\s+(.+)$/);
  if (match) {
    return normalizeCountryLabel(match[1] ?? "");
  }

  const reverseMatch = normalized.match(/^(.+)\s+remote$/);
  return reverseMatch ? normalizeCountryLabel(reverseMatch[1] ?? "") : undefined;
}

export function getJobTags(job: JobListing) {
  const tags: string[] = [];

  const workplaceLabel = getWorkplaceLabel(job);
  if (workplaceLabel) {
    tags.push(workplaceLabel);
  }

  if (isVisaFriendlyJob(job)) {
    tags.push("Visa");
  }

  const experienceTag = getExperienceLabel(job);
  if (experienceTag) {
    tags.push(experienceTag);
  }

  if (isNewJob(job)) {
    tags.push("New");
  }

  return tags;
}

export function getWorkplaceLabel(job: JobListing) {
  if (job.remoteType === "hybrid") {
    return "Hybrid";
  }

  if (job.remoteType === "remote") {
    return "Remote";
  }

  if (job.remoteType === "onsite") {
    return undefined;
  }

  return isRemoteJob(job) ? "Remote" : undefined;
}

export function getExperienceLabel(job: JobListing) {
  const level =
    job.experienceLevel ?? resolveExperienceLevel(job.experienceClassification);

  return level ? labelForExperience(level) : undefined;
}

export function isRemoteJob(job: JobListing) {
  if (job.remoteType === "remote" || job.remoteType === "hybrid") {
    return true;
  }

  if (job.remoteType === "onsite") {
    return false;
  }

  const searchable = buildSearchableJobText(job);
  return /\b(remote|work from home|distributed)\b/i.test(searchable);
}

export function isVisaFriendlyJob(job: JobListing) {
  if (job.sponsorshipHint === "supported") {
    return true;
  }

  if (job.sponsorshipHint === "not_supported") {
    return false;
  }

  const searchable = buildSearchableJobText(job);

  if (
    /\b(no|not|without|unable to|cannot)\s+(offer |provide |support )?(visa|immigration|sponsorship)\b/i.test(
      searchable,
    ) ||
    /\b(visa|immigration) sponsorship is not available\b/i.test(searchable)
  ) {
    return false;
  }

  return /\b(visa sponsorship|immigration support|h-1b|h1b|will sponsor|can sponsor|sponsorship available|open to visa)\b/i.test(
    searchable,
  );
}

export function isNewJob(job: JobListing) {
  const postedAt = toTimestamp(job.postingDate ?? job.postedAt);
  if (!postedAt) {
    return false;
  }

  return Date.now() - postedAt <= 3 * 24 * 60 * 60 * 1000;
}

export function matchesPostedDateFilter(
  job: JobListing,
  postedDate: PostedDateFilter,
) {
  if (postedDate === "any") {
    return true;
  }

  const normalizedPostedAt = toTimestamp(job.postingDate ?? job.postedAt);
  if (!normalizedPostedAt) {
    return false;
  }

  const diffMs = Date.now() - normalizedPostedAt;
  const windowMs =
    postedDate === "24h"
      ? 24 * 60 * 60 * 1000
      : postedDate === "7d"
        ? 7 * 24 * 60 * 60 * 1000
        : 30 * 24 * 60 * 60 * 1000;

  return diffMs <= windowMs;
}

export function describeSelectedPlatforms(filters: SearchFilters) {
  const selected = filters.platforms?.length
    ? filters.platforms
        .filter((platform): platform is ActiveCrawlerPlatform =>
          activeCrawlerPlatforms.includes(platform as ActiveCrawlerPlatform),
        )
        .map((platform) => labelForCrawlerPlatform(platform))
    : platformFilterOptions.map((platform) => platform.label);

  return selected.join(", ");
}

export function buildStableJobRenderKeys(jobs: JobListing[]) {
  const seen = new Map<string, number>();

  return jobs.map((job) => {
    const baseKey = buildStableJobRenderIdentity(job);
    const occurrence = seen.get(baseKey) ?? 0;
    seen.set(baseKey, occurrence + 1);

    return occurrence === 0 ? baseKey : `${baseKey}::${occurrence + 1}`;
  });
}

function buildSearchableJobText(job: JobListing) {
  return [
    job.title,
    job.company,
    job.locationRaw,
    job.locationText,
    job.descriptionSnippet,
    safeStringify(job.rawSourceMetadata),
  ]
    .filter(Boolean)
    .join(" ");
}

function safeStringify(value: unknown) {
  try {
    return JSON.stringify(value) ?? "";
  } catch {
    return "";
  }
}

function toTimestamp(value?: string) {
  if (!value) {
    return undefined;
  }

  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? undefined : timestamp;
}

function isRemoteTerm(value: string) {
  return /\bremote\b/i.test(value);
}

function isCountryLike(value: string) {
  return Boolean(resolveCountryLabel(value));
}

function isUnitedStatesAlias(value: string) {
  return /^(us|usa|u\.s\.a\.|united states|united states of america)$/i.test(value.trim());
}

function normalizeCountryLabel(value: string) {
  return resolveCountryLabel(value) ?? value.trim();
}

function normalizeSearchText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
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
