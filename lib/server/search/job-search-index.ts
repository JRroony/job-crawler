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

export const indexedJobCandidateChannelNames = [
  "exactTitleChannel",
  "aliasTitleChannel",
  "conceptChannel",
  "familyChannel",
  "geoChannel",
  "legacyTitleFallbackChannel",
  "legacyLocationFallbackChannel",
] as const;

export type IndexedJobCandidateChannelName =
  (typeof indexedJobCandidateChannelNames)[number];

export type IndexedJobCandidateChannelBreakdown = {
  exactTitleCount: number;
  aliasTitleCount: number;
  conceptCount: number;
  familyCount: number;
  geoCount: number;
  legacyTitleFallbackCount: number;
  legacyLocationFallbackCount: number;
  mergedCandidateCount: number;
  finalMatchedCount: number;
  returnedCount: number;
};

export type IndexedJobCandidateChannel = {
  name: IndexedJobCandidateChannelName;
  filter: Record<string, unknown>;
  limit: number;
  sort: CandidateQuerySort;
  diagnostics: {
    strategy: "multi_channel_prefilter";
    channel: IndexedJobCandidateChannelName;
    titleTerms?: string[];
    titleConceptIds?: string[];
    titleFamily?: string;
    titleRoleGroup?: string;
    locationSearchKeys?: string[];
    usesRegexFallback?: boolean;
    requiresLocation?: boolean;
  };
};

export type IndexedJobCandidateQuery = {
  filter: Record<string, unknown>;
  limit: number;
  sort: CandidateQuerySort;
  channels: IndexedJobCandidateChannel[];
  mergedCandidateLimit: number;
  diagnostics: {
    strategy: "coarse_prefilter";
    titleFamily?: string;
    titleRoleGroup?: string;
    titleConceptIds: string[];
    titleSearchTerms: string[];
    exactTitleTerms: string[];
    aliasTitleTerms: string[];
    locationSearchKeys: string[];
    hasLocationFilter: boolean;
    hasTitleFilter: boolean;
    hasTitleConstraint: boolean;
    hasLocationConstraint: boolean;
    queryShape: string;
    channelNames: IndexedJobCandidateChannelName[];
    titleChannels: IndexedJobCandidateChannelName[];
    titleChannelsRequireLocation: boolean;
    locationConstraintAppliedToEveryTitleChannel: boolean;
    usesGeoOnlyChannelForVisibleResults: boolean;
    usesTitleOnlyChannelForLocationSearch: boolean;
    channelLimits: Record<IndexedJobCandidateChannelName, number>;
    candidateChannelBreakdown?: IndexedJobCandidateChannelBreakdown;
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

export const emptyIndexedJobCandidateChannelBreakdown =
  (): IndexedJobCandidateChannelBreakdown => ({
    exactTitleCount: 0,
    aliasTitleCount: 0,
    conceptCount: 0,
    familyCount: 0,
    geoCount: 0,
    legacyTitleFallbackCount: 0,
    legacyLocationFallbackCount: 0,
    mergedCandidateCount: 0,
    finalMatchedCount: 0,
    returnedCount: 0,
  });

const defaultMergedCandidateLimit = 5_000;
const defaultChannelCandidateLimit = 5_000;

const genericSingleWordTitleTerms = new Set([
  "analyst",
  "developer",
  "engineer",
  "manager",
  "owner",
  "specialist",
]);

function readPositiveIntegerEnv(name: string, fallback: number) {
  const parsed = Number.parseInt(process.env[name] ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function resolveIndexedCandidateLimits() {
  const mergedCandidateLimit = readPositiveIntegerEnv(
    "INDEXED_SEARCH_MERGED_CANDIDATE_LIMIT",
    defaultMergedCandidateLimit,
  );
  const channelLimit = readPositiveIntegerEnv(
    "INDEXED_SEARCH_CHANNEL_CANDIDATE_LIMIT",
    defaultChannelCandidateLimit,
  );

  return {
    mergedCandidateLimit,
    channelLimits: Object.fromEntries(
      indexedJobCandidateChannelNames.map((name) => [name, channelLimit]),
    ) as Record<IndexedJobCandidateChannelName, number>,
  };
}

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

function buildExactTitleTerms(title: string) {
  const analysis = analyzeTitle(title);

  return dedupeStrings([
    analysis.normalized,
    analysis.strippedNormalized,
    normalizeTitleText(analysis.canonicalTitle),
  ]);
}

function buildAliasTitleTerms(title: string) {
  return dedupeStrings(
    buildTitleQueryVariants(title, { maxQueries: 64 })
      .filter((variant) =>
        variant.kind === "synonym" ||
        variant.kind === "abbreviation" ||
        variant.kind === "adjacent_concept",
      )
      .map((variant) => variant.normalized),
  );
}

function titleTermSearchKeys(terms: string[]) {
  return terms.map((term) => `term:${term}`);
}

function titleConceptSearchKeys(conceptIds: string[]) {
  return conceptIds.map((conceptId) => `concept:${conceptId}`);
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
  > &
    Partial<
      Pick<
        JobListing,
        | "sourcePlatform"
        | "linkStatus"
        | "isActive"
        | "postingDate"
        | "postedAt"
        | "lastSeenAt"
        | "crawledAt"
        | "discoveredAt"
        | "indexedAt"
      >
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
    statusSearchKeys: buildJobStatusSearchKeys(job),
    rankingTimestamps: {
      ...(job.postingDate ? { postingDate: job.postingDate } : {}),
      ...(job.postedAt ? { postedAt: job.postedAt } : {}),
      ...(job.lastSeenAt ? { lastSeenAt: job.lastSeenAt } : {}),
      ...(job.crawledAt ? { crawledAt: job.crawledAt } : {}),
      ...(job.discoveredAt ? { discoveredAt: job.discoveredAt } : {}),
      ...(job.indexedAt ? { indexedAt: job.indexedAt } : {}),
    },
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
      ...geoLocation.searchKeys,
      ...dedupedCountryKeys,
      ...dedupedRegionKeys,
      ...dedupedCityKeys,
    ]),
  };
}

function buildJobStatusSearchKeys(
  job: Partial<Pick<JobListing, "sourcePlatform" | "linkStatus" | "isActive">>,
) {
  return dedupeStrings([
    typeof job.isActive === "boolean"
      ? `status:${job.isActive ? "active" : "inactive"}`
      : undefined,
    job.linkStatus ? `link_status:${job.linkStatus}` : undefined,
    job.sourcePlatform ? `platform:${job.sourcePlatform}` : undefined,
  ]);
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

function buildSearchReadyLocationCandidateClause(filters: SearchFilters) {
  const geoIntent = parseGeoIntentFromFilters(filters);
  const searchReadyKeys = geoIntent.searchKeys;
  const resolvedLocationClause = buildResolvedLocationCandidateClauseFromIntent(geoIntent);
  const clauses = [
    ...(searchReadyKeys.length > 0
      ? [
          { "searchIndex.locationSearchKeys": { $in: searchReadyKeys } },
          { "geoLocation.searchKeys": { $in: searchReadyKeys } },
        ]
      : []),
    ...(resolvedLocationClause ? [resolvedLocationClause] : []),
  ];

  if (clauses.length === 0) {
    return undefined;
  }

  return {
    $or: clauses,
  };
}

function buildResolvedLocationCandidateClause(filters: SearchFilters) {
  const geoIntent = parseGeoIntentFromFilters(filters);
  return buildResolvedLocationCandidateClauseFromIntent(geoIntent);
}

function buildResolvedLocationCandidateClauseFromIntent(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (geoIntent.scope === "none" || geoIntent.scope === "ambiguous") {
    return undefined;
  }

  const scopedLocationClause =
    geoIntent.city
      ? buildCityResolvedLocationClause(geoIntent)
      : geoIntent.region
        ? buildRegionResolvedLocationClause(geoIntent)
        : geoIntent.country
          ? buildCountryResolvedLocationClause(geoIntent)
          : undefined;

  if (!geoIntent.isRemote) {
    return scopedLocationClause;
  }

  const remoteClause = {
    $or: [
      { "resolvedLocation.isRemote": true },
      { remoteType: "remote" },
      { "geoLocation.workplaceType": "remote" },
    ],
  };

  if (!scopedLocationClause) {
    return remoteClause;
  }

  return { $and: [remoteClause, scopedLocationClause] };
}

function buildCountryResolvedLocationClause(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (!geoIntent.country) {
    return undefined;
  }

  const countryAliases = dedupeStrings([
    geoIntent.country.name,
    geoIntent.country.code,
    ...geoIntent.country.aliases,
  ]);
  const clauses: Record<string, unknown>[] = [
    { "resolvedLocation.country": { $in: countryAliases } },
    { country: { $in: countryAliases } },
  ];

  if (isUnitedStatesValue(geoIntent.country.name) || isUnitedStatesValue(geoIntent.country.code)) {
    clauses.push({ "resolvedLocation.isUnitedStates": true });
  }

  return { $or: clauses };
}

function buildRegionResolvedLocationClause(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (!geoIntent.region) {
    return undefined;
  }

  const regionAliases = dedupeStrings([
    geoIntent.region.name,
    geoIntent.region.code,
    ...geoIntent.region.aliases,
  ]);

  return {
    $or: [
      { "resolvedLocation.state": { $in: regionAliases } },
      { "resolvedLocation.stateCode": { $in: regionAliases } },
      { state: { $in: regionAliases } },
    ],
  };
}

function buildCityResolvedLocationClause(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (!geoIntent.city) {
    return undefined;
  }

  const cityAliases = dedupeStrings([
    geoIntent.city.name,
    ...geoIntent.city.aliases,
  ]);

  return {
    $or: [
      { "resolvedLocation.city": { $in: cityAliases } },
      { city: { $in: cityAliases } },
    ],
  };
}

function buildLegacyLocationFallbackClause(filters: SearchFilters) {
  const geoIntent = parseGeoIntentFromFilters(filters);
  const terms = buildLegacyLocationFallbackTerms(geoIntent);

  return buildGenericLocationTextFallbackClause(terms);
}

function buildLegacyLocationFallbackTerms(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (geoIntent.scope === "none") {
    return [];
  }

  if (geoIntent.isRemote) {
    return buildRemoteLegacyLocationFallbackTerms(geoIntent);
  }

  if (geoIntent.city) {
    return buildCityLegacyLocationFallbackTerms(geoIntent);
  }

  if (geoIntent.region) {
    return buildRegionLegacyLocationFallbackTerms(geoIntent);
  }

  if (geoIntent.country) {
    const terms: string[] = [];
    const catalogCountry = geoCatalog.find(
      (country) => country.code === geoIntent.country?.code || country.name === geoIntent.country?.name,
    );
    const countryTerms = dedupeStrings([
      geoIntent.country.name,
      geoIntent.country.code,
      ...geoIntent.country.aliases,
      ...(catalogCountry ? countryLocationAliases(catalogCountry) : []),
    ]);
    const isUnitedStatesCountry =
      isUnitedStatesValue(geoIntent.country.name) || isUnitedStatesValue(geoIntent.country.code);

    terms.push(
      ...(
        isUnitedStatesCountry
          ? countryTerms.filter((term) => !isAmbiguousShortLocationTerm(term))
          : countryTerms
      ),
    );

    if (isUnitedStatesCountry) {
      terms.push(...buildBroadUnitedStatesLegacyFallbackTerms());
    }

    return dedupeStrings(terms);
  }

  return geoIntent.normalizedInput ? [geoIntent.normalizedInput] : [];
}

function buildRemoteLegacyLocationFallbackTerms(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  const baseTerms =
    geoIntent.city
      ? buildCityLegacyLocationFallbackTerms(geoIntent)
      : geoIntent.region
        ? buildRegionLegacyLocationFallbackTerms(geoIntent)
        : geoIntent.country
          ? dedupeStrings([
              geoIntent.country.name,
              geoIntent.country.code,
              ...geoIntent.country.aliases,
            ])
          : [geoIntent.normalizedInput];

  return buildRemoteQualifiedLocationTerms(baseTerms);
}

function buildRegionLegacyLocationFallbackTerms(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (!geoIntent.region) {
    return [];
  }

  const terms = dedupeStrings([
    geoIntent.region.name,
    geoIntent.region.code,
    ...geoIntent.region.aliases,
  ]);
  const usState = resolveUsState(geoIntent.region.name) ?? resolveUsState(geoIntent.region.code);
  const usStateCode =
    resolveUsStateCode(geoIntent.region.name) ?? resolveUsStateCode(geoIntent.region.code);

  return dedupeStrings([
    ...terms,
    usState,
    usStateCode,
  ]);
}

function buildCityLegacyLocationFallbackTerms(
  geoIntent: ReturnType<typeof parseGeoIntentFromFilters>,
) {
  if (!geoIntent.city) {
    return [];
  }

  const analyzedCity = analyzeUsLocation(geoIntent.city.name);
  const cityTerms = dedupeStrings([
    geoIntent.city.name,
    ...geoIntent.city.aliases,
    analyzedCity.city,
    analyzedCity.stateName,
    analyzedCity.stateCode,
    [analyzedCity.city, analyzedCity.stateCode].filter(Boolean).join(" "),
    [analyzedCity.city, analyzedCity.stateName].filter(Boolean).join(" "),
  ]);

  return cityTerms;
}

function buildRemoteQualifiedLocationTerms(terms: string[]) {
  return dedupeStrings(
    terms
      .filter((term) => term.trim().length > 0)
      .flatMap((term) => [
        `remote ${term}`,
        `${term} remote`,
        `remote in ${term}`,
        `remote within ${term}`,
      ]),
  );
}

function buildBroadUnitedStatesLegacyFallbackTerms() {
  return dedupeStrings([
    ...getUsStateCatalog().flatMap((state) => [
      state.name,
      isAmbiguousShortLocationTerm(state.code) ? undefined : state.code,
    ]),
    ...getUsMetroCatalog().flatMap((metro) => [
      metro.city,
      `${metro.city} ${metro.stateCode}`,
      `${metro.city} ${metro.stateName}`,
    ]),
  ]);
}

function isAmbiguousShortLocationTerm(value: string) {
  return new Set(["AS", "CA", "HI", "ID", "IN", "ME", "OR"]).has(value.toUpperCase());
}

function buildLocationCandidateClause(filters: SearchFilters) {
  const searchReadyClause = buildSearchReadyLocationCandidateClause(filters);
  const legacyClause = buildLegacyLocationFallbackClause(filters);

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
  const geoIntent = parseGeoIntentFromFilters(filters);
  const hasLocationFilter = geoIntent.scope !== "none";
  const hasTitleFilter = normalizeTitleText(filters.title).length > 0;
  const exactTitleTerms = buildExactTitleTerms(filters.title);
  const aliasTitleTerms = buildAliasTitleTerms(filters.title);
  const titleConceptIds = buildTitleConceptIds(filters.title);
  const titleSearchTerms = buildTitleSearchTerms(filters.title);
  const titleSearchKeys = buildFilterTitleSearchKeys(filters.title);
  const normalizedTitleRegex = buildNormalizedPhraseRegex(titleSearchTerms);
  const familyRoleFallback = buildFamilyRoleFallbackClause(filters.title);
  const locationCandidateClause = buildLocationCandidateClause(filters);
  const searchReadyLocationClause = buildSearchReadyLocationCandidateClause(filters);
  const legacyLocationFallbackClause = buildLegacyLocationFallbackClause(filters);
  const experienceClause = buildExperienceCandidateClause(filters);
  const limits = resolveIndexedCandidateLimits();
  const baseClauses: Record<string, unknown>[] = [
    {
      $or: [
        { isActive: true },
        { isActive: { $exists: false } },
      ],
    },
  ];

  if (allowedPlatforms?.length) {
    baseClauses.push({
      sourcePlatform: { $in: allowedPlatforms },
    });
  }

  if (experienceClause) {
    baseClauses.push(experienceClause);
  }

  const titleChannelClauses: Record<string, unknown>[] = [
    buildExactTitleChannelClause(exactTitleTerms),
    buildAliasTitleChannelClause(aliasTitleTerms),
    buildConceptChannelClause(titleConceptIds),
    familyRoleFallback,
    buildLegacyTitleFallbackClause(normalizedTitleRegex),
  ].flatMap((clause) => (clause ? [clause as Record<string, unknown>] : []));
  const titleConstraint =
    titleChannelClauses.length === 0
      ? undefined
      : titleChannelClauses.length === 1
        ? titleChannelClauses[0]
        : { $or: titleChannelClauses };
  const titleChannelBaseClauses = locationCandidateClause
    ? [...baseClauses, locationCandidateClause]
    : baseClauses;
  const titleChannelsRequireLocation =
    hasTitleFilter && hasLocationFilter && Boolean(locationCandidateClause);
  const channels = [
    buildCandidateChannel({
      name: "exactTitleChannel",
      baseClauses: titleChannelBaseClauses,
      channelClause: buildExactTitleChannelClause(exactTitleTerms),
      limit: limits.channelLimits.exactTitleChannel,
      diagnostics: {
        titleTerms: exactTitleTerms,
        locationSearchKeys: geoIntent.searchKeys,
        requiresLocation: titleChannelsRequireLocation,
      },
    }),
    buildCandidateChannel({
      name: "aliasTitleChannel",
      baseClauses: titleChannelBaseClauses,
      channelClause: buildAliasTitleChannelClause(aliasTitleTerms),
      limit: limits.channelLimits.aliasTitleChannel,
      diagnostics: {
        titleTerms: aliasTitleTerms,
        locationSearchKeys: geoIntent.searchKeys,
        requiresLocation: titleChannelsRequireLocation,
      },
    }),
    buildCandidateChannel({
      name: "conceptChannel",
      baseClauses: titleChannelBaseClauses,
      channelClause: buildConceptChannelClause(titleConceptIds),
      limit: limits.channelLimits.conceptChannel,
      diagnostics: {
        titleConceptIds,
        locationSearchKeys: geoIntent.searchKeys,
        requiresLocation: titleChannelsRequireLocation,
      },
    }),
    buildCandidateChannel({
      name: "familyChannel",
      baseClauses: titleChannelBaseClauses,
      channelClause: familyRoleFallback,
      limit: limits.channelLimits.familyChannel,
      diagnostics: {
        titleFamily: titleAnalysis.family,
        titleRoleGroup: titleAnalysis.roleGroup,
        locationSearchKeys: geoIntent.searchKeys,
        requiresLocation: titleChannelsRequireLocation,
      },
    }),
    buildCandidateChannel({
      name: "legacyTitleFallbackChannel",
      baseClauses: titleChannelBaseClauses,
      channelClause: buildLegacyTitleFallbackClause(normalizedTitleRegex),
      limit: limits.channelLimits.legacyTitleFallbackChannel,
      diagnostics: {
        titleTerms: titleSearchTerms,
        locationSearchKeys: geoIntent.searchKeys,
        usesRegexFallback: true,
        requiresLocation: titleChannelsRequireLocation,
      },
    }),
  ].filter((channel): channel is IndexedJobCandidateChannel => Boolean(channel));
  const channelNames = channels.map((channel) => channel.name);
  const titleChannels = channels
    .filter((channel) => channel.name !== "geoChannel" && channel.name !== "legacyLocationFallbackChannel")
    .map((channel) => channel.name);
  const hasTitleConstraint = hasTitleFilter && Boolean(titleConstraint);
  const hasLocationConstraint = hasLocationFilter && Boolean(locationCandidateClause);
  const locationConstraintAppliedToEveryTitleChannel =
    !hasTitleFilter ||
    !hasLocationFilter ||
    (hasLocationConstraint &&
      titleChannels.length > 0 &&
      channels
        .filter((channel) => titleChannels.includes(channel.name))
        .every((channel) => channel.diagnostics.requiresLocation === true));
  const usesGeoOnlyChannelForVisibleResults = false;
  const usesTitleOnlyChannelForLocationSearch =
    hasTitleFilter &&
    hasLocationFilter &&
    channels
      .filter((channel) => titleChannels.includes(channel.name))
      .some((channel) => channel.diagnostics.requiresLocation !== true);
  const queryShape = describeCandidateQueryShape({
    hasTitleFilter,
    hasLocationFilter,
    hasTitleConstraint,
    hasLocationConstraint,
    titleChannels,
  });
  const filterClauses = [...baseClauses];
  if (hasLocationConstraint && locationCandidateClause) {
    filterClauses.push(locationCandidateClause);
  }
  if (hasTitleFilter) {
    filterClauses.push(titleConstraint ?? buildNoCandidateClause());
  }
  const filter = composeCandidateClause(filterClauses);

  return {
    filter,
    limit: limits.mergedCandidateLimit,
    sort: indexedJobCandidateSort,
    channels,
    mergedCandidateLimit: limits.mergedCandidateLimit,
    diagnostics: {
      strategy: "coarse_prefilter",
      titleFamily: titleAnalysis.family,
      titleRoleGroup: titleAnalysis.roleGroup,
      titleConceptIds,
      titleSearchTerms,
      exactTitleTerms,
      aliasTitleTerms,
      locationSearchKeys: geoIntent.searchKeys,
      hasLocationFilter,
      hasTitleFilter,
      hasTitleConstraint,
      hasLocationConstraint,
      queryShape,
      channelNames,
      titleChannels,
      titleChannelsRequireLocation,
      locationConstraintAppliedToEveryTitleChannel,
      usesGeoOnlyChannelForVisibleResults,
      usesTitleOnlyChannelForLocationSearch,
      channelLimits: limits.channelLimits,
      usedPlatformPrefilter: Boolean(allowedPlatforms?.length),
      usedLocationPrefilter: Boolean(locationCandidateClause),
      usedLocationTextFallback: Boolean(legacyLocationFallbackClause),
      usedExperiencePrefilter: Boolean(experienceClause),
      usedNormalizedTitleRegex: Boolean(normalizedTitleRegex),
      usedFamilyRoleFallback: Boolean(familyRoleFallback),
      usedSearchReadyTitleKeys: titleSearchKeys.length > 0,
      usedSearchReadyLocationKeys: Boolean(searchReadyLocationClause),
      usedSearchReadyExperienceKeys: Boolean(filters.experienceLevels?.length),
    },
  };
}

function describeCandidateQueryShape(input: {
  hasTitleFilter: boolean;
  hasLocationFilter: boolean;
  hasTitleConstraint: boolean;
  hasLocationConstraint: boolean;
  titleChannels: IndexedJobCandidateChannelName[];
}) {
  if (input.hasTitleFilter && input.hasLocationFilter) {
    const channelExpression =
      input.titleChannels.length > 0
        ? input.titleChannels.join(" OR ")
        : "noTitleCandidateChannel";
    return input.hasLocationConstraint && input.hasTitleConstraint
      ? `base AND locationConstraint AND (${channelExpression})`
      : "base AND noMatchingTitleGeoIntersection";
  }

  if (input.hasTitleFilter) {
    const channelExpression =
      input.titleChannels.length > 0
        ? input.titleChannels.join(" OR ")
        : "noTitleCandidateChannel";
    return input.hasTitleConstraint
      ? `base AND (${channelExpression})`
      : "base AND noMatchingTitleConstraint";
  }

  if (input.hasLocationFilter) {
    return input.hasLocationConstraint
      ? "base AND locationConstraint"
      : "base AND noMatchingLocationConstraint";
  }

  return "base";
}

function buildNoCandidateClause() {
  return { _id: "__job_crawler_no_indexed_candidates__" };
}

const indexedJobCandidateSort: CandidateQuerySort = {
  postingDate: -1,
  postedAt: -1,
  lastSeenAt: -1,
  crawledAt: -1,
  discoveredAt: -1,
  title: 1,
};

function buildCandidateChannel(input: {
  name: IndexedJobCandidateChannelName;
  baseClauses: Record<string, unknown>[];
  channelClause?: Record<string, unknown>;
  limit: number;
  diagnostics: Omit<
    IndexedJobCandidateChannel["diagnostics"],
    "strategy" | "channel"
  >;
}): IndexedJobCandidateChannel | undefined {
  if (!input.channelClause) {
    return undefined;
  }

  return {
    name: input.name,
    filter: composeCandidateClause(input.baseClauses, input.channelClause),
    limit: input.limit,
    sort: indexedJobCandidateSort,
    diagnostics: {
      strategy: "multi_channel_prefilter",
      channel: input.name,
      ...input.diagnostics,
    },
  };
}

function composeCandidateClause(
  baseClauses: Record<string, unknown>[],
  channelClause?: Record<string, unknown>,
) {
  const clauses = [...baseClauses, ...(channelClause ? [channelClause] : [])];

  if (clauses.length === 1) {
    return clauses[0] ?? {};
  }

  return { $and: clauses };
}

function buildExactTitleChannelClause(terms: string[]) {
  if (terms.length === 0) {
    return undefined;
  }

  return buildTitleFieldClause(terms);
}

function buildAliasTitleChannelClause(terms: string[]) {
  if (terms.length === 0) {
    return undefined;
  }

  return buildTitleFieldClause(terms);
}

function buildTitleFieldClause(terms: string[]) {
  const titleSearchKeys = titleTermSearchKeys(terms);

  return {
    $or: [
      { "searchIndex.titleNormalized": { $in: terms } },
      { "searchIndex.titleStrippedNormalized": { $in: terms } },
      { "searchIndex.titleSearchTerms": { $in: terms } },
      { "searchIndex.titleSearchKeys": { $in: titleSearchKeys } },
      { normalizedTitle: { $in: terms } },
      { titleNormalized: { $in: terms } },
    ],
  };
}

function buildConceptChannelClause(conceptIds: string[]) {
  if (conceptIds.length === 0) {
    return undefined;
  }

  return {
    $or: [
      { "searchIndex.titleConceptIds": { $in: conceptIds } },
      { "searchIndex.titleSearchKeys": { $in: titleConceptSearchKeys(conceptIds) } },
    ],
  };
}

function buildLegacyTitleFallbackClause(regex?: RegExp) {
  if (!regex) {
    return undefined;
  }

  return {
    $or: [
      { normalizedTitle: { $regex: regex } },
      { titleNormalized: { $regex: regex } },
      { title: { $regex: regex } },
    ],
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
