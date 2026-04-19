import "server-only";

import { resolveUsState, resolveUsStateCode, isUnitedStatesValue } from "@/lib/server/locations/us";
import {
  findSupportedCountryConceptInText,
  findSupportedCountryRegionInText,
  getSupportedCountryCanonicalName,
  resolveSupportedCountryConcept,
  resolveSupportedCountryRegion,
} from "@/lib/server/locations/world";
import {
  analyzeTitle,
  buildTitleQueryVariants,
  extractMeaningfulPhrases,
  normalizeTitleText,
} from "@/lib/server/title-retrieval";
import type { SearchFilters, JobListing, JobSearchIndex } from "@/lib/types";
import { resolveOperationalCrawlerPlatforms } from "@/lib/types";

type CandidateQuerySort = Record<string, 1 | -1>;

export type IndexedJobCandidateQuery = {
  filter: Record<string, unknown>;
  limit: number;
  sort: CandidateQuerySort;
  diagnostics: {
    strategy: "coarse_prefilter";
    titleFamily?: string;
    titleConceptIds: string[];
    titleSearchTerms: string[];
    usedPlatformPrefilter: boolean;
    usedLocationPrefilter: boolean;
    usedExperiencePrefilter: boolean;
    usedNormalizedTitleRegex: boolean;
  };
};

const genericSingleWordTitleTerms = new Set([
  "analyst",
  "developer",
  "engineer",
  "manager",
  "owner",
  "specialist",
]);

function dedupeStrings(values: Array<string | undefined>) {
  const seen = new Set<string>();
  const deduped: string[] = [];

  for (const value of values) {
    const normalized = value?.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    deduped.push(normalized);
  }

  return deduped;
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildNormalizedPhraseRegex(phrases: string[]) {
  const normalized = dedupeStrings(phrases.map((phrase) => normalizeTitleText(phrase)))
    .filter((phrase) => phrase.length >= 2)
    .sort((left, right) => right.length - left.length);

  if (normalized.length === 0) {
    return undefined;
  }

  return new RegExp(`(^| )(${normalized.map((phrase) => escapeRegExp(phrase)).join("|")})(?= |$)`, "i");
}

function buildTitleConceptIds(title: string) {
  const analysis = analyzeTitle(title);
  return dedupeStrings([
    analysis.primaryConceptId,
    ...analysis.matchedConceptIds,
    ...analysis.candidateConceptIds.slice(0, 6),
  ]);
}

function buildTitleSearchTerms(title: string) {
  const analysis = analyzeTitle(title);
  const variants = buildTitleQueryVariants(title, { maxQueries: 12 }).slice(0, 12);
  const phraseSource = analysis.strippedNormalized || analysis.normalized;
  const phraseTerms = extractMeaningfulPhrases(phraseSource, {
    maxLength: Math.min(3, Math.max(1, analysis.meaningfulTokens.length)),
  });

  return dedupeStrings([
    analysis.normalized,
    analysis.strippedNormalized,
    analysis.canonicalTitle,
    ...variants.map((variant) => variant.normalized),
    ...phraseTerms,
    ...analysis.meaningfulTokens.filter(
      (term) => term.length > 2 && !genericSingleWordTitleTerms.has(term),
    ),
  ]);
}

export function buildJobSearchIndex(
  job: Pick<JobListing, "title" | "normalizedTitle">,
): JobSearchIndex {
  const analysis = analyzeTitle(job.title);
  const titleNormalized = normalizeTitleText(job.normalizedTitle || job.title);
  const titleStrippedNormalized =
    normalizeTitleText(analysis.strippedNormalized || titleNormalized || job.title) ||
    titleNormalized;

  return {
    titleNormalized: titleNormalized || normalizeTitleText(job.title),
    titleStrippedNormalized,
    titleFamily: analysis.family,
    titleRoleGroup: analysis.roleGroup,
    titleConceptIds: dedupeStrings([
      analysis.primaryConceptId,
      ...analysis.matchedConceptIds,
    ]),
    titleSearchTerms: buildTitleSearchTerms(job.title),
  };
}

function buildLocationCandidateClause(filters: SearchFilters) {
  const clauses: Record<string, unknown>[] = [];

  if (filters.country) {
    if (isUnitedStatesValue(filters.country)) {
      clauses.push({
        $or: [
          { "resolvedLocation.isUnitedStates": true },
          { country: "United States" },
        ],
      });
    } else {
      const countryConcept =
        resolveSupportedCountryConcept(filters.country) ??
        findSupportedCountryConceptInText(filters.country);
      const canonicalCountry =
        getSupportedCountryCanonicalName(countryConcept) ?? filters.country;

      clauses.push({
        $or: [
          { country: canonicalCountry },
          { "resolvedLocation.country": canonicalCountry },
        ],
      });
    }
  }

  if (filters.state) {
    const supportedRegion =
      resolveSupportedCountryRegion(filters.state) ??
      findSupportedCountryRegionInText(filters.state);
    const state = resolveUsState(filters.state) ?? supportedRegion?.name;
    const stateCode =
      resolveUsStateCode(filters.state) ??
      (state ? resolveUsStateCode(state) : undefined) ??
      supportedRegion?.code;
    const stateClauses = dedupeStrings([state, stateCode, filters.state]).flatMap((value) => [
      { "resolvedLocation.state": value },
      { "resolvedLocation.stateCode": value },
      { state: value },
    ]);

    if (stateClauses.length > 0) {
      clauses.push({ $or: stateClauses });
    }
  }

  if (filters.city) {
    clauses.push({
      $or: [
        { "resolvedLocation.city": filters.city },
        { city: filters.city },
      ],
    });
  }

  if (clauses.length === 0) {
    return undefined;
  }

  return clauses.length === 1 ? clauses[0] : { $and: clauses };
}

function buildExperienceCandidateClause(filters: SearchFilters) {
  if (!filters.experienceLevels?.length) {
    return undefined;
  }

  const clauses: Record<string, unknown>[] = [
    { experienceLevel: { $in: filters.experienceLevels } },
    { "experienceClassification.explicitLevel": { $in: filters.experienceLevels } },
  ];

  if (filters.experienceMatchMode !== "strict") {
    clauses.push({
      "experienceClassification.inferredLevel": { $in: filters.experienceLevels },
    });
  }

  if (filters.includeUnspecifiedExperience === true) {
    clauses.push({
      "experienceClassification.isUnspecified": true,
    });
  }

  return { $or: clauses };
}

export function buildIndexedJobCandidateQuery(
  filters: SearchFilters,
): IndexedJobCandidateQuery {
  const allowedPlatforms = filters.platforms?.length
    ? resolveOperationalCrawlerPlatforms(filters.platforms)
    : undefined;
  const titleAnalysis = analyzeTitle(filters.title);
  const titleConceptIds = buildTitleConceptIds(filters.title);
  const titleSearchTerms = buildTitleSearchTerms(filters.title);
  const normalizedTitleRegex = buildNormalizedPhraseRegex(titleSearchTerms);
  const titleClauses: Record<string, unknown>[] = [];

  if (titleConceptIds.length > 0) {
    titleClauses.push({
      "searchIndex.titleConceptIds": {
        $in: titleConceptIds,
      },
    });
  }

  if (titleAnalysis.family && titleSearchTerms.length > 0) {
    titleClauses.push({
      $and: [
        { "searchIndex.titleFamily": titleAnalysis.family },
        { "searchIndex.titleSearchTerms": { $in: titleSearchTerms } },
      ],
    });
  }

  if (normalizedTitleRegex) {
    titleClauses.push({
      $or: [
        {
          normalizedTitle: {
            $regex: normalizedTitleRegex,
          },
        },
        {
          titleNormalized: {
            $regex: normalizedTitleRegex,
          },
        },
      ],
    });
  }

  const clauses: Record<string, unknown>[] = [{ isActive: true }];

  if (allowedPlatforms?.length) {
    clauses.push({
      sourcePlatform: { $in: allowedPlatforms },
    });
  }

  const locationClause = buildLocationCandidateClause(filters);
  if (locationClause) {
    clauses.push(locationClause);
  }

  const experienceClause = buildExperienceCandidateClause(filters);
  if (experienceClause) {
    clauses.push(experienceClause);
  }

  if (titleClauses.length > 0) {
    clauses.push(titleClauses.length === 1 ? titleClauses[0] : { $or: titleClauses });
  }

  const filter = clauses.length === 1 ? clauses[0] : { $and: clauses };
  const limit =
    filters.state || filters.city || filters.experienceLevels?.length || filters.platforms?.length
      ? 250
      : titleAnalysis.primaryConceptId || titleAnalysis.family
        ? 300
        : 400;

  return {
    filter,
    limit,
    sort: {
      postingDate: -1,
      postedAt: -1,
      lastSeenAt: -1,
      crawledAt: -1,
      discoveredAt: -1,
      title: 1,
    },
    diagnostics: {
      strategy: "coarse_prefilter",
      titleFamily: titleAnalysis.family,
      titleConceptIds,
      titleSearchTerms,
      usedPlatformPrefilter: Boolean(allowedPlatforms?.length),
      usedLocationPrefilter: Boolean(locationClause),
      usedExperiencePrefilter: Boolean(experienceClause),
      usedNormalizedTitleRegex: Boolean(normalizedTitleRegex),
    },
  };
}
