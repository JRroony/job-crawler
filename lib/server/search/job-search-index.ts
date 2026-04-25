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
  getSupportedCountryCanonicalName,
  resolveSupportedCountryConcept,
  resolveSupportedCountryRegion,
} from "@/lib/server/locations/world";
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
  >,
) {
  const countryKeys: string[] = [];
  const regionKeys: string[] = [];
  const cityKeys: string[] = [];

  const addLocation = (input: {
    country?: string;
    state?: string;
    stateCode?: string;
    city?: string;
    isUnitedStates?: boolean;
  }) => {
    const country = input.isUnitedStates
      ? "United States"
      : normalizeCountryName(input.country);
    const normalizedCountry = normalizeSearchKey(country);
    if (normalizedCountry) {
      countryKeys.push(`country:${normalizedCountry}`);
    }

    for (const region of dedupeStrings([input.state, input.stateCode])) {
      const normalizedRegion = normalizeSearchKey(region);
      if (normalizedCountry && normalizedRegion) {
        regionKeys.push(`region:${normalizedCountry}:${normalizedRegion}`);
      }
    }

    const normalizedCity = normalizeSearchKey(input.city);
    if (normalizedCountry && normalizedCity) {
      cityKeys.push(`city:${normalizedCountry}:${normalizedCity}`);
    }
  };

  addLocation({
    country: job.country,
    state: job.state,
    city: job.city,
    isUnitedStates: job.country ? isUnitedStatesValue(job.country) : undefined,
  });
  addLocation({
    country: job.resolvedLocation?.country,
    state: job.resolvedLocation?.state,
    stateCode: job.resolvedLocation?.stateCode,
    city: job.resolvedLocation?.city,
    isUnitedStates: job.resolvedLocation?.isUnitedStates,
  });

  for (const point of job.resolvedLocation?.physicalLocations ?? []) {
    addLocation({
      country: point.country,
      state: point.state,
      stateCode: point.stateCode,
      city: point.city,
      isUnitedStates: point.country === "United States",
    });
  }

  for (const country of job.resolvedLocation?.eligibilityCountries ?? []) {
    addLocation({
      country,
      isUnitedStates: isUnitedStatesValue(country),
    });
  }

  for (const text of dedupeStrings([
    job.locationText,
    job.normalizedLocation,
    job.locationNormalized,
  ])) {
    const us = analyzeUsLocation(text);
    if (us.isUnitedStates) {
      addLocation({
        country: "United States",
        state: us.stateName,
        stateCode: us.stateCode,
        city: us.city,
        isUnitedStates: true,
      });
      continue;
    }

    const supported = analyzeSupportedCountryLocation(text);
    if (supported.country) {
      addLocation({
        country: supported.country,
        state: supported.state,
        stateCode: supported.stateCode,
        city: supported.city,
      });
    }
  }

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
  const legacyClauses: Record<string, unknown>[] = [];
  const searchReadyKeys = buildFilterLocationSearchKeys(filters);
  const searchReadyClause =
    searchReadyKeys.length > 0
      ? {
          "searchIndex.locationSearchKeys": { $in: searchReadyKeys },
        }
      : undefined;

  if (filters.country) {
    if (isUnitedStatesValue(filters.country)) {
      const locationTextFallback = buildUnitedStatesLocationTextFallbackClause();
      legacyClauses.push({
        $or: [
          { "resolvedLocation.isUnitedStates": true },
          { "resolvedLocation.physicalLocations.country": "United States" },
          { "resolvedLocation.eligibilityCountries": "United States" },
          { country: "United States" },
          ...(locationTextFallback ? [locationTextFallback] : []),
        ],
      });
    } else {
      const countryConcept =
        resolveSupportedCountryConcept(filters.country) ??
        findSupportedCountryConceptInText(filters.country);
      const canonicalCountry =
        getSupportedCountryCanonicalName(countryConcept) ?? filters.country;

      legacyClauses.push({
        $or: [
          { country: canonicalCountry },
          { "resolvedLocation.country": canonicalCountry },
          { "resolvedLocation.physicalLocations.country": canonicalCountry },
          { "resolvedLocation.eligibilityCountries": canonicalCountry },
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
      { "resolvedLocation.physicalLocations.state": value },
      { "resolvedLocation.physicalLocations.stateCode": value },
      { state: value },
    ]);

    if (stateClauses.length > 0) {
      legacyClauses.push({ $or: stateClauses });
    }
  }

  if (filters.city) {
    legacyClauses.push({
      $or: [
        { "resolvedLocation.city": filters.city },
        { "resolvedLocation.physicalLocations.city": filters.city },
        { city: filters.city },
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

function buildUnitedStatesLocationTextFallbackClause() {
  const stateTerms = getUsStateCatalog().flatMap((state) => [
    state.name,
    state.code,
  ]);
  const metroTerms = getUsMetroCatalog().flatMap((metro) => [
    metro.city,
    `${metro.city} ${metro.stateCode}`,
    `${metro.city} ${metro.stateName}`,
  ]);
  const regex = buildNormalizedTermRegex([
    "united states",
    "usa",
    "remote us",
    "remote united states",
    ...stateTerms,
    ...metroTerms,
  ]);

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
      usedLocationTextFallback: Boolean(filters.country && isUnitedStatesValue(filters.country)),
      usedExperiencePrefilter: Boolean(experienceClause),
      usedNormalizedTitleRegex: Boolean(normalizedTitleRegex),
      usedFamilyRoleFallback: Boolean(familyRoleFallback),
      usedSearchReadyTitleKeys: titleSearchKeys.length > 0,
      usedSearchReadyLocationKeys: buildFilterLocationSearchKeys(filters).length > 0,
      usedSearchReadyExperienceKeys: Boolean(filters.experienceLevels?.length),
    },
  };
}

function buildFilterLocationSearchKeys(filters: Pick<SearchFilters, "country" | "state" | "city">) {
  const country = normalizeCountryName(filters.country);
  const normalizedCountry = normalizeSearchKey(country);
  if (!normalizedCountry) {
    return [];
  }

  const region =
    country && country === "United States"
      ? resolveUsState(filters.state) ?? resolveUsStateCode(filters.state) ?? filters.state
      : (resolveSupportedCountryRegion(filters.state, resolveSupportedCountryConcept(country))?.name ??
        resolveSupportedCountryRegion(filters.state, resolveSupportedCountryConcept(country))?.code ??
        filters.state);

  return dedupeStrings([
    `country:${normalizedCountry}`,
    filters.state && region ? `region:${normalizedCountry}:${normalizeSearchKey(region)}` : undefined,
    filters.city ? `city:${normalizedCountry}:${normalizeSearchKey(filters.city)}` : undefined,
  ]);
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
