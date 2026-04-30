"use client";

import { buildStableJobRenderIdentity } from "@/lib/job-identity";
import {
  evaluateStoredExperienceMatch,
  resolveExperienceLevel,
} from "@/lib/experience";
import type {
  ActiveCrawlerPlatform,
  EmploymentType,
  ExperienceLevel,
  JobListing,
  RemoteType,
  SearchFilters,
} from "@/lib/types";
import { activeCrawlerPlatforms, employmentTypes, experienceLevels } from "@/lib/types";
import { labelForCrawlerPlatform } from "@/components/job-crawler/ui-config";
import { labelForExperience } from "@/lib/utils";
import { parseGeoIntent } from "@/lib/geo/parse";
import { matchJobLocationAgainstGeoIntent } from "@/lib/geo/match";
import { normalizeJobGeoLocation } from "@/lib/geo/location";

export type PostedDateFilter = "any" | "24h" | "7d" | "30d";
export type WorkplaceFilter = "any" | Extract<RemoteType, "onsite" | "hybrid" | "remote">;
export type SponsorshipFilter = "any" | "supported" | "not_supported" | "unknown";

export type ClientResultFilters = {
  remoteOnly: boolean;
  visaFriendlyOnly: boolean;
  postedDate: PostedDateFilter;
  workplace?: WorkplaceFilter;
  employmentTypes?: EmploymentType[];
  sponsorship?: SponsorshipFilter;
  company?: string;
};

export const defaultClientResultFilters: ClientResultFilters = {
  remoteOnly: false,
  visaFriendlyOnly: false,
  postedDate: "any",
  workplace: "any",
  employmentTypes: undefined,
  sponsorship: "any",
  company: "",
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

export const workplaceFilterOptions: Array<{
  value: WorkplaceFilter;
  label: string;
}> = [
  {
    value: "any",
    label: "Any",
  },
  {
    value: "onsite",
    label: "On-site",
  },
  {
    value: "hybrid",
    label: "Hybrid",
  },
  {
    value: "remote",
    label: "Remote",
  },
];

export const employmentTypeFilterOptions: Array<{
  value: EmploymentType;
  label: string;
}> = employmentTypes
  .filter((type) => type !== "unknown")
  .map((type) => ({
    value: type,
    label: labelForEmploymentType(type),
  }));

export const sponsorshipFilterOptions: Array<{
  value: SponsorshipFilter;
  label: string;
}> = [
  {
    value: "any",
    label: "Any",
  },
  {
    value: "supported",
    label: "Sponsorship available",
  },
  {
    value: "not_supported",
    label: "No sponsorship",
  },
  {
    value: "unknown",
    label: "Not specified",
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

    if (!matchesWorkplaceFilter(job, resultFilters.workplace, resultFilters.remoteOnly)) {
      return false;
    }

    if (!matchesEmploymentTypeFilter(job, resultFilters.employmentTypes)) {
      return false;
    }

    if (!matchesSponsorshipFilter(job, resultFilters.sponsorship, resultFilters.visaFriendlyOnly)) {
      return false;
    }

    if (!matchesCompanyFilter(job, resultFilters.company)) {
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

  const querySignal = buildClientTitleSignal(query);
  const jobSignal = buildClientTitleSignal(title);
  const sharedTerms = querySignal.meaningfulTokens.filter((token) =>
    jobSignal.expandedTokens.includes(token),
  );
  const sharedModifierTerms = sharedTerms.filter((token) => token !== querySignal.headWord);
  const sameFamily =
    Boolean(querySignal.family && jobSignal.family) &&
    querySignal.family === jobSignal.family;
  const compatibleHead = areClientTitleHeadsCompatible(
    querySignal.headWord,
    jobSignal.headWord,
  );

  if (sameFamily && compatibleHead) {
    return true;
  }

  if (sameFamily && sharedModifierTerms.length > 0) {
    return true;
  }

  if (compatibleHead && sharedModifierTerms.length > 0) {
    return true;
  }

  const requiredTerms = querySignal.meaningfulTokens.filter((term) => term.length > 2);
  return requiredTerms.length > 0 && requiredTerms.every((term) => jobSignal.expandedTokens.includes(term));
}

type ClientTitleFamily =
  | "software_engineering"
  | "data_platform"
  | "data_analytics"
  | "ai_ml_science"
  | "product"
  | "program_management"
  | "cloud_devops_security"
  | "architecture_solutions"
  | "design_content_marketing"
  | "qa_support_it"
  | "business_operations_people";

type ClientTitleSignal = {
  normalized: string;
  tokens: string[];
  expandedTokens: string[];
  meaningfulTokens: string[];
  headWord?: string;
  family?: ClientTitleFamily;
};

const clientTitleStopTerms = new Set([
  "and",
  "for",
  "in",
  "of",
  "the",
  "to",
  "with",
  "job",
  "jobs",
  "role",
  "roles",
  "senior",
  "sr",
  "junior",
  "jr",
  "lead",
  "staff",
  "principal",
  "associate",
  "ii",
  "iii",
  "iv",
  "v",
]);

const clientTitleHeadWords = [
  "engineer",
  "developer",
  "scientist",
  "analyst",
  "manager",
  "owner",
  "designer",
  "writer",
  "architect",
  "consultant",
  "administrator",
  "specialist",
  "coordinator",
  "recruiter",
  "tester",
];

const clientTitleFamilyKeywords: Array<{
  family: ClientTitleFamily;
  phrases: string[];
  terms: string[];
}> = [
  {
    family: "software_engineering",
    phrases: ["software", "backend", "back end", "frontend", "front end", "full stack", "web", "mobile", "ios", "android", "platform", "api", "distributed systems"],
    terms: ["software", "backend", "frontend", "fullstack", "developer", "web", "mobile", "ios", "android", "platform", "api", "systems"],
  },
  {
    family: "data_platform",
    phrases: ["data engineer", "analytics engineer", "data platform", "data warehouse", "etl", "database", "big data", "data integration"],
    terms: ["data", "analytics", "warehouse", "etl", "database", "pipeline", "spark", "dbt", "bigquery"],
  },
  {
    family: "data_analytics",
    phrases: ["data analyst", "business analyst", "product analyst", "business intelligence", "reporting analyst", "financial analyst", "marketing analyst", "insights"],
    terms: ["analyst", "analytics", "business", "product", "intelligence", "reporting", "financial", "marketing", "insights", "strategy"],
  },
  {
    family: "ai_ml_science",
    phrases: ["machine learning", "artificial intelligence", "applied scientist", "research scientist", "data scientist", "deep learning", "computer vision", "natural language", "generative ai", "large language model"],
    terms: ["ml", "ai", "llm", "nlp", "vision", "scientist", "research", "applied", "learning", "model", "models", "mlops", "data"],
  },
  {
    family: "product",
    phrases: ["product manager", "technical product", "product owner", "growth product", "platform product", "associate product"],
    terms: ["product", "pm", "apm", "owner", "growth", "platform"],
  },
  {
    family: "program_management",
    phrases: ["program manager", "technical program", "project manager", "delivery manager", "scrum master"],
    terms: ["program", "project", "delivery", "scrum", "tpm"],
  },
  {
    family: "cloud_devops_security",
    phrases: ["devops", "site reliability", "cloud", "infrastructure", "security", "devsecops", "network"],
    terms: ["devops", "sre", "cloud", "infrastructure", "security", "devsecops", "network", "reliability"],
  },
  {
    family: "architecture_solutions",
    phrases: ["solutions engineer", "solution engineer", "solutions architect", "sales engineer", "pre sales", "customer engineer"],
    terms: ["solutions", "solution", "sales", "customer", "architect", "presales"],
  },
  {
    family: "design_content_marketing",
    phrases: ["product designer", "ux", "ui", "technical writer", "content designer", "documentation", "marketing"],
    terms: ["designer", "design", "ux", "ui", "writer", "writing", "content", "documentation", "marketing"],
  },
  {
    family: "qa_support_it",
    phrases: ["qa", "quality assurance", "test engineer", "sdet", "support engineer", "it support"],
    terms: ["qa", "quality", "test", "tester", "sdet", "support", "it"],
  },
  {
    family: "business_operations_people",
    phrases: ["business operations", "operations", "recruiter", "people", "hr", "talent"],
    terms: ["business", "operations", "recruiter", "people", "hr", "talent"],
  },
];

function buildClientTitleSignal(normalized: string): ClientTitleSignal {
  const expanded = expandClientTitleText(normalized);
  const tokens = expanded.split(" ").filter(Boolean);
  const meaningfulTokens = tokens.filter((token) => !clientTitleStopTerms.has(token));
  const headWord = [...meaningfulTokens].reverse().find((token) =>
    clientTitleHeadWords.includes(token),
  );

  return {
    normalized,
    tokens,
    expandedTokens: Array.from(new Set(tokens)),
    meaningfulTokens: Array.from(new Set(meaningfulTokens)),
    headWord,
    family: inferClientTitleFamily(expanded, meaningfulTokens, headWord),
  };
}

function expandClientTitleText(normalized: string) {
  return normalizeSearchText(
    normalized
      .replace(/\bml\b/g, "machine learning ml")
      .replace(/\bai\b/g, "artificial intelligence ai")
      .replace(/\bllm\b/g, "large language model llm")
      .replace(/\bnlp\b/g, "natural language processing nlp")
      .replace(/\bsre\b/g, "site reliability engineer sre")
      .replace(/\bsdet\b/g, "software development engineer test sdet")
      .replace(/\bux\b/g, "user experience ux")
      .replace(/\bui\b/g, "user interface ui")
      .replace(/\bpm\b/g, "product manager pm")
      .replace(/\btpm\b/g, "technical program manager tpm"),
  );
}

function inferClientTitleFamily(
  normalized: string,
  tokens: string[],
  headWord?: string,
): ClientTitleFamily | undefined {
  const tokenSet = new Set(tokens);
  const scored = clientTitleFamilyKeywords
    .map((definition) => {
      const phraseScore = definition.phrases.filter((phrase) =>
        normalized.includes(normalizeSearchText(phrase)),
      ).length * 3;
      const termScore = definition.terms.filter((term) => tokenSet.has(term)).length;
      const headScore =
        headWord && definition.terms.includes(headWord)
          ? 1
          : 0;
      return {
        family: definition.family,
        score: phraseScore + termScore + headScore,
      };
    })
    .filter((entry) => entry.score > 0)
    .sort((left, right) => right.score - left.score);

  return scored[0]?.family;
}

function areClientTitleHeadsCompatible(left?: string, right?: string) {
  if (!left || !right) {
    return false;
  }

  if (left === right) {
    return true;
  }

  const pairs = [
    ["engineer", "developer"],
    ["manager", "owner"],
    ["architect", "consultant"],
    ["tester", "engineer"],
    ["designer", "writer"],
  ];

  return pairs.some(([first, second]) =>
    (left === first && right === second) || (left === second && right === first),
  );
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
    return "On-site";
  }

  return isRemoteJob(job) ? "Remote" : undefined;
}

export function getSponsorshipLabel(job: JobListing) {
  if (job.sponsorshipHint === "supported") {
    return "Sponsorship available";
  }

  if (job.sponsorshipHint === "not_supported") {
    return "No sponsorship";
  }

  return isVisaFriendlyJob(job) ? "Sponsorship likely" : "Sponsorship not specified";
}

export function labelForEmploymentType(type?: EmploymentType) {
  const labels: Record<EmploymentType, string> = {
    full_time: "Full-time",
    part_time: "Part-time",
    contract: "Contract",
    temporary: "Temporary",
    internship: "Internship",
    apprenticeship: "Apprenticeship",
    seasonal: "Seasonal",
    freelance: "Freelance",
    unknown: "Not specified",
  };

  return type ? labels[type] : labels.unknown;
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

export function toggleEmploymentTypeSelection(
  selectedTypes: ClientResultFilters["employmentTypes"],
  type: EmploymentType,
) {
  const nextTypes = new Set(selectedTypes ?? []);

  if (nextTypes.has(type)) {
    nextTypes.delete(type);
  } else {
    nextTypes.add(type);
  }

  const normalized = employmentTypes.filter(
    (candidate): candidate is EmploymentType =>
      candidate !== "unknown" && nextTypes.has(candidate),
  );

  return normalized.length > 0 ? normalized : undefined;
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

function matchesWorkplaceFilter(
  job: JobListing,
  workplace: WorkplaceFilter | undefined,
  legacyRemoteOnly: boolean,
) {
  if (legacyRemoteOnly) {
    return isRemoteJob(job);
  }

  if (!workplace || workplace === "any") {
    return true;
  }

  if (workplace === "remote") {
    return job.remoteType === "remote" || isRemoteJob(job);
  }

  return job.remoteType === workplace;
}

function matchesEmploymentTypeFilter(
  job: JobListing,
  selectedTypes: ClientResultFilters["employmentTypes"],
) {
  if (!selectedTypes?.length) {
    return true;
  }

  return Boolean(job.employmentType && selectedTypes.includes(job.employmentType));
}

function matchesSponsorshipFilter(
  job: JobListing,
  sponsorship: SponsorshipFilter | undefined,
  legacyVisaFriendlyOnly: boolean,
) {
  if (legacyVisaFriendlyOnly) {
    return isVisaFriendlyJob(job);
  }

  if (!sponsorship || sponsorship === "any") {
    return true;
  }

  if (sponsorship === "supported") {
    return isVisaFriendlyJob(job);
  }

  return job.sponsorshipHint === sponsorship;
}

function matchesCompanyFilter(job: JobListing, company: string | undefined) {
  const query = normalizeSearchText(company);
  if (!query) {
    return true;
  }

  return normalizeSearchText(job.normalizedCompany || job.company).includes(query);
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
