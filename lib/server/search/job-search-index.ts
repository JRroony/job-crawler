import "server-only";

import {
  analyzeUsLocation,
  getUsMetroCatalog,
  getUsStateCatalog,
  isUnitedStatesValue,
  normalizeLocationText,
  resolveUsState,
  resolveUsStateCode,
} from "@/lib/server/locations/us";
import {
  analyzeSupportedCountryLocation,
  findSupportedCountryConceptInText,
  findSupportedCountryRegionInText,
  getSupportedCountryLocationAliases,
  getSupportedCountryCanonicalName,
  resolveSupportedCountryConcept,
  resolveSupportedCountryRegion,
} from "@/lib/server/locations/world";
import { normalizeJobGeoLocation } from "@/lib/server/geo/match";
import { parseGeoIntentFromFilters } from "@/lib/server/geo/parse";
import { normalizeGeoText } from "@/lib/server/geo/normalize";
import { countryLocationAliases, geoCatalog } from "@/lib/server/geo/catalog";
import {
  areTitleConceptsAdjacent,
  analyzeTitle,
  buildTitleQueryVariants,
  extractMeaningfulPhrases,
  getTitleConcept,
  listTitleConcepts,
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
  usedLocationTextFallback: boolean;
  usedExperiencePrefilter: boolean;
  usedNormalizedTitleRegex: boolean;
  usedFamilyRoleFallback: boolean;
  usedSearchReadyTitleKeys: boolean;
  usedSearchReadyLocationKeys: boolean;
  usedSearchReadyExperienceKeys: boolean;
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

function buildNormalizedTermRegex(terms: string[]) {
  const normalized = dedupeStrings(terms.map((term) => normalizeLocationText(term)))
    .filter((term) => term.length >= 2)
    .sort((left, right) => right.length - left.length);

  if (normalized.length === 0) {
    return undefined;
  }

  return new RegExp(`(^| )(${normalized.map((term) => escapeRegExp(term)).join("|")})(?= |$)`, "i");
}

function buildTitleConceptIds(title: string) {
  const analysis = analyzeTitle(title);
  const seedConceptIds = dedupeStrings([
    analysis.primaryConceptId,
    ...analysis.matchedConceptIds,
    ...analysis.candidateConceptIds.slice(0, 6),
  ]);

  const adjacentConceptIds = seedConceptIds.flatMap((conceptId) => {
    const concept = getTitleConcept(conceptId);
    return concept?.adjacentConceptIds ?? [];
  });
  const familyRoleConceptIds = listTitleConcepts()
    .filter((concept) => {
      if (!analysis.family || concept.family !== analysis.family) {
        return false;
      }

      const sameRoleGroup =
        !analysis.roleGroup ||
        !concept.roleGroup ||
        concept.roleGroup === analysis.roleGroup;
      const relatedToSeed = seedConceptIds.some(
        (seedConceptId) =>
          seedConceptId === concept.id ||
          areTitleConceptsAdjacent(seedConceptId, concept.id),
      );

      return sameRoleGroup || relatedToSeed;
    })
    .map((concept) => concept.id);

  return dedupeStrings([
    ...seedConceptIds,
    ...adjacentConceptIds,
    ...familyRoleConceptIds,
  ]);
}

function buildTitleSearchTerms(title: string) {
  const analysis = analyzeTitle(title);
  const variants = buildTitleQueryVariants(title, { maxQueries: 24 }).slice(0, 24);
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
  job: Pick<
    JobListing,
    | "title"
    | "normalizedTitle"
    | "country"
    | "state"
    | "city"
    | "locationText"
    | "normalizedLocation"
    | "locationNormalized"
    | "resolvedLocation"
    | "geoLocation"
    | "experienceLevel"
    | "experienceClassification"
  >,
): JobSearchIndex {
  const analysis = analyzeTitle(job.title);
  const titleNormalized = normalizeTitleText(job.normalizedTitle || job.title);
  const titleStrippedNormalized =
    normalizeTitleText(analysis.strippedNormalized || titleNormalized || job.title) ||
    titleNormalized;
  const titleConceptIds = dedupeStrings([
    analysis.primaryConceptId,
    ...analysis.matchedConceptIds,
  ]);
  const titleSearchTerms = buildTitleSearchTerms(job.title);
  const locationKeys = buildJobLocationSearchKeys(job);
  const experienceSearchKeys = dedupeStrings([
    job.experienceLevel ? `experience:${job.experienceLevel}` : undefined,
    job.experienceClassification?.explicitLevel
      ? `experience:${job.experienceClassification.explicitLevel}`
      : undefined,
    job.experienceClassification?.inferredLevel
      ? `experience:${job.experienceClassification.inferredLevel}`
      : undefined,
    job.experienceClassification?.isUnspecified ? "experience:unspecified" : undefined,
  ]);

  return {
    titleNormalized: titleNormalized || normalizeTitleText(job.title),
    titleStrippedNormalized,
    titleFamily: analysis.family,
    titleRoleGroup: analysis.roleGroup,
    titleConceptIds,
    titleSearchTerms,
    titleSearchKeys: buildJobTitleSearchKeys({
      family: analysis.family,
      roleGroup: analysis.roleGroup,
      conceptIds: titleConceptIds,
      searchTerms: titleSearchTerms,
    }),
    locationCountryKeys: locationKeys.countryKeys,
    locationRegionKeys: locationKeys.regionKeys,
    locationCityKeys: locationKeys.cityKeys,
    locationSearchKeys: locationKeys.searchKeys,
    experienceSearchKeys,
  };
}

function buildJobTitleSearchKeys(input: {
  family?: string;
  roleGroup?: string;
  conceptIds: string[];
  searchTerms: string[];
}) {
  return dedupeStrings([
    input.family ? `family:${input.family}` : undefined,
    input.family && input.roleGroup ? `family_role:${input.family}:${input.roleGroup}` : undefined,
    ...input.conceptIds.map((conceptId) => `concept:${conceptId}`),
    ...input.searchTerms.map((term) => `term:${term}`),
  ]);
}

function buildFilterTitleSearchKeys(title: string) {
  const analysis = analyzeTitle(title);
  const titleConceptIds = buildTitleConceptIds(title);
  const titleSearchTerms = buildTitleSearchTerms(title);

  return buildJobTitleSearchKeys({
    family: analysis.family,
    roleGroup: analysis.roleGroup,
    conceptIds: titleConceptIds,
    searchTerms: titleSearchTerms,
  });
}

function buildJobLocationSearchKeys(
  job: Pick<
    JobListing,
    | "country"
    | "state"
    | "city"
    | "locationText"
    | "normalizedLocation"
    | "locationNormalized"
    | "resolvedLocation"
    | "geoLocation"
  >,
) {
  const geoLocation = job.geoLocation ?? normalizeJobGeoLocation(job);
  const countryKeys = geoLocation.searchKeys.filter((key) => key.startsWith("country:") || key.startsWith("country_code:"));
  const regionKeys = geoLocation.searchKeys.filter((key) => key.startsWith("region:") || key.startsWith("region_code:"));
  const cityKeys = geoLocation.searchKeys.filter((key) => key.startsWith("city:"));

  const dedupedCountryKeys = dedupeStrings(countryKeys);
  const dedupedRegionKeys = dedupeStrings(regionKeys);
  const dedupedCityKeys = dedupeStrings(cityKeys);

  return {
    countryKeys: dedupedCountryKeys,
    regionKeys: dedupedRegionKeys,
    cityKeys: dedupedCityKeys,
    searchKeys: dedupeStrings([
      ...dedupedCountryKeys,
      ...dedupedRegionKeys,
      ...dedupedCityKeys,
    ]),
  };
}

function normalizeCountryName(value?: string) {
  if (!value) {
    return undefined;
  }

  if (isUnitedStatesValue(value)) {
    return "United States";
  }

  const concept =
    resolveSupportedCountryConcept(value) ??
    findSupportedCountryConceptInText(value);

  return getSupportedCountryCanonicalName(concept) ?? value;
}

function normalizeSearchKey(value?: string) {
  return normalizeLocationText(value);
}

function buildLocationCandidateClause(filters: SearchFilters) {
  const geoIntent = parseGeoIntentFromFilters(filters);
  const legacyClauses: Record<string, unknown>[] = [];
  const searchReadyKeys = geoIntent.searchKeys;
  const searchReadyClause =
    searchReadyKeys.length > 0
      ? {
          $or: [
            { "searchIndex.locationSearchKeys": { $in: searchReadyKeys } },
            { "geoLocation.searchKeys": { $in: searchReadyKeys } },
          ],
        }
      : undefined;

  if (geoIntent.country) {
    const catalogCountry = geoCatalog.find(
      (country) => country.code === geoIntent.country?.code || country.name === geoIntent.country?.name,
    );
    const aliases = dedupeStrings([
      geoIntent.country.name,
      geoIntent.country.code,
      ...geoIntent.country.aliases,
      ...(catalogCountry ? countryLocationAliases(catalogCountry) : []),
    ]);
    const locationTextFallback = buildGenericLocationTextFallbackClause(aliases);
    legacyClauses.push({
      $or: [
        { country: geoIntent.country.name },
        { "resolvedLocation.country": geoIntent.country.name },
        { "resolvedLocation.physicalLocations.country": geoIntent.country.name },
        { "resolvedLocation.eligibilityCountries": geoIntent.country.name },
        { "geoLocation.physicalLocations.country": geoIntent.country.name },
        { "geoLocation.remoteEligibility.country": geoIntent.country.name },
        ...(locationTextFallback ? [locationTextFallback] : []),
      ],
    });
  }

  if (geoIntent.region) {
    const stateClauses = dedupeStrings([
      geoIntent.region.name,
      geoIntent.region.code,
      ...geoIntent.region.aliases,
    ]).flatMap((value) => [
      { "resolvedLocation.state": value },
      { "resolvedLocation.stateCode": value },
      { "resolvedLocation.physicalLocations.state": value },
      { "resolvedLocation.physicalLocations.stateCode": value },
      { "geoLocation.physicalLocations.region": value },
      { "geoLocation.physicalLocations.regionCode": value },
      { "geoLocation.remoteEligibility.region": value },
      { "geoLocation.remoteEligibility.regionCode": value },
      { state: value },
    ]);

    if (stateClauses.length > 0) {
      legacyClauses.push({ $or: stateClauses });
    }
  }

  if (geoIntent.city) {
    legacyClauses.push({
      $or: [
        { "resolvedLocation.city": geoIntent.city.name },
        { "resolvedLocation.physicalLocations.city": geoIntent.city.name },
        { "geoLocation.physicalLocations.city": geoIntent.city.name },
        { "geoLocation.remoteEligibility.city": geoIntent.city.name },
        { city: geoIntent.city.name },
        ...(geoIntent.confidence === "low" ? [buildGenericLocationTextFallbackClause(geoIntent.city.aliases)] : []),
      ],
    });
  }

  const legacyClause =
    legacyClauses.length === 0
      ? undefined
      : legacyClauses.length === 1
        ? legacyClauses[0]
        : { $and: legacyClauses };

  if (!searchReadyClause && !legacyClause) {
    return undefined;
  }

  if (!searchReadyClause) {
    return legacyClause;
  }

  if (!legacyClause) {
    return searchReadyClause;
  }

  return { $or: [searchReadyClause, legacyClause] };
}

function buildGenericLocationTextFallbackClause(aliases: string[]) {
  const regex = buildNormalizedTermRegex(aliases.map(normalizeGeoText));

  if (!regex) {
    return undefined;
  }

  return {
    $or: [
      { normalizedLocation: { $regex: regex } },
      { locationNormalized: { $regex: regex } },
      { locationText: { $regex: regex } },
    ],
  };
}

function buildExperienceCandidateClause(filters: SearchFilters) {
  if (!filters.experienceLevels?.length) {
    return undefined;
  }

  const searchReadyKeys = filters.experienceLevels.map((level) => `experience:${level}`);

  const clauses: Record<string, unknown>[] = [
    { "searchIndex.experienceSearchKeys": { $in: searchReadyKeys } },
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
      "searchIndex.experienceSearchKeys": "experience:unspecified",
    });
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
  const titleSearchKeys = buildFilterTitleSearchKeys(filters.title);
  const normalizedTitleRegex = buildNormalizedPhraseRegex(titleSearchTerms);
  const familyRoleFallback = buildFamilyRoleFallbackClause(filters.title);
  const titleClauses: Record<string, unknown>[] = [];

  if (titleSearchKeys.length > 0) {
    titleClauses.push({
      "searchIndex.titleSearchKeys": {
        $in: titleSearchKeys,
      },
    });
  }

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

  if (familyRoleFallback) {
    titleClauses.push(familyRoleFallback);
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
      usedLocationTextFallback: Boolean(parseGeoIntentFromFilters(filters).rawInput),
      usedExperiencePrefilter: Boolean(experienceClause),
      usedNormalizedTitleRegex: Boolean(normalizedTitleRegex),
      usedFamilyRoleFallback: Boolean(familyRoleFallback),
      usedSearchReadyTitleKeys: titleSearchKeys.length > 0,
      usedSearchReadyLocationKeys: parseGeoIntentFromFilters(filters).searchKeys.length > 0,
      usedSearchReadyExperienceKeys: Boolean(filters.experienceLevels?.length),
    },
  };
}

function buildFilterLocationSearchKeys(filters: Pick<SearchFilters, "country" | "state" | "city">) {
  return parseGeoIntentFromFilters(filters).searchKeys;
}

function buildFamilyRoleFallbackClause(title: string) {
  const analysis = analyzeTitle(title);
  if (!analysis.family || !analysis.roleGroup) {
    return undefined;
  }

  const relatedConceptIds = buildTitleConceptIds(title);
  const genericRoleTerms = dedupeStrings([
    analysis.headWord,
    analysis.headWord === "engineer" ? "developer" : undefined,
    analysis.headWord === "developer" ? "engineer" : undefined,
    analysis.headWord === "manager" ? "owner" : undefined,
    analysis.headWord === "owner" ? "manager" : undefined,
    analysis.headWord === "analyst" ? "scientist" : undefined,
  ]);

  return {
    $and: [
      { "searchIndex.titleFamily": analysis.family },
      {
        $or: [
          { "searchIndex.titleRoleGroup": analysis.roleGroup },
          ...(relatedConceptIds.length > 0
            ? [{ "searchIndex.titleConceptIds": { $in: relatedConceptIds } }]
            : []),
          ...(genericRoleTerms.length > 0
            ? [
                { "searchIndex.titleStrippedNormalized": { $in: genericRoleTerms } },
                { "searchIndex.titleNormalized": { $in: genericRoleTerms } },
                { normalizedTitle: { $in: genericRoleTerms } },
                { titleNormalized: { $in: genericRoleTerms } },
              ]
            : []),
        ],
      },
    ],
  };
}
