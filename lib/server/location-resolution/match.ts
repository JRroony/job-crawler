import "server-only";

import {
  analyzeUsLocation,
  isUnitedStatesValue,
  normalizeLocationText,
  resolveUsState,
  resolveUsStateCode,
} from "@/lib/server/locations/us";
import {
  findSupportedCountryConceptInText,
  findSupportedCountryMetroInText,
  findSupportedCountryRegionInText,
  getSupportedCountryAliases,
  getSupportedCountryCanonicalName,
  resolveSupportedCountryConcept,
  resolveSupportedCountryMetro,
  resolveSupportedCountryRegion,
} from "@/lib/server/locations/world";
import type { JobListing, ResolvedLocation, SearchFilters } from "@/lib/types";

type LocationWorkplaceMode = "any" | "remote" | "hybrid";
type JobWorkplaceMode = "remote" | "hybrid" | "onsite" | "unknown";
type ResolvedLocationPoint = NonNullable<ResolvedLocation["physicalLocations"]>[number];
type ResolvedLocationConflict = NonNullable<ResolvedLocation["conflicts"]>[number];

export type LocationMatchDiagnostics = {
  original: string;
  normalized: string;
  country?: string;
  state?: string;
  city?: string;
  workplaceMode: LocationWorkplaceMode;
  scopesApplied: Array<"country" | "state" | "city">;
  expandedTerms: string[];
};

export type JobLocationDiagnostics = {
  raw: string;
  normalized: string;
  country?: string;
  state?: string;
  stateCode?: string;
  city?: string;
  workplaceMode: JobWorkplaceMode;
  isRemote: boolean;
  isUnitedStates: boolean;
  physicalLocations: ResolvedLocationPoint[];
  eligibilityCountries: string[];
  conflicts: ResolvedLocationConflict[];
  aliasesUsed: string[];
};

export type LocationMatchResult = {
  matches: boolean;
  explanation: string;
  matchedTerms: string[];
  queryDiagnostics: LocationMatchDiagnostics;
  jobDiagnostics: JobLocationDiagnostics;
};

type LocationFilterableJob = Pick<
  JobListing,
  "country" | "state" | "city" | "locationText"
>;

type LocationScope = "country" | "state" | "city";

type LocationQueryTermSet = {
  scope: LocationScope;
  raw: string;
  normalized: string;
  workplaceMode: LocationWorkplaceMode;
  canonicalCountry?: string;
  canonicalState?: string;
  canonicalStateCode?: string;
  canonicalCity?: string;
  expandedTerms: string[];
};

type ScopeMatchResult = {
  matches: boolean;
  explanation: string;
  matchedTerms: string[];
};

const remoteMarkers = ["remote", "work from home"];
const hybridMarkers = ["hybrid"];
const onsiteMarkers = ["onsite", "on site", "in office", "office based"];
const unitedStatesQueryAliases = [
  "united states",
  "usa",
  "us",
];

export function getLocationMatchResult(
  job: LocationFilterableJob,
  filters: Pick<SearchFilters, "country" | "state" | "city">,
  resolvedLocation: ResolvedLocation,
): LocationMatchResult | undefined {
  const countryQuery = filters.country
    ? buildLocationQueryTermSet("country", filters.country)
    : undefined;
  const stateQuery = filters.state
    ? buildLocationQueryTermSet("state", filters.state)
    : undefined;
  const cityQuery = filters.city
    ? buildLocationQueryTermSet("city", filters.city)
    : undefined;
  const activeQueries = [countryQuery, stateQuery, cityQuery].filter(
    (value): value is LocationQueryTermSet => Boolean(value),
  );

  if (activeQueries.length === 0) {
    return undefined;
  }

  const jobDiagnostics = buildJobLocationDiagnostics(job, resolvedLocation);
  const matchedTerms: string[] = [];
  const explanations: string[] = [];

  for (const query of activeQueries) {
    const scopeResult = matchQueryScope(query, jobDiagnostics);
    if (!scopeResult.matches) {
      return {
        matches: false,
        explanation: scopeResult.explanation,
        matchedTerms,
        queryDiagnostics: buildLocationQueryDiagnostics(activeQueries),
        jobDiagnostics,
      };
    }

    matchedTerms.push(...scopeResult.matchedTerms);
    explanations.push(scopeResult.explanation);
  }

  return {
    matches: true,
    explanation: explanations.join(" "),
    matchedTerms: dedupeTerms(matchedTerms),
    queryDiagnostics: buildLocationQueryDiagnostics(activeQueries),
    jobDiagnostics,
  };
}

function matchQueryScope(
  query: LocationQueryTermSet,
  jobDiagnostics: JobLocationDiagnostics,
): ScopeMatchResult {
  if (!workplaceModeMatches(query.workplaceMode, jobDiagnostics.workplaceMode)) {
    return {
      matches: false,
      explanation: `${labelScope(query.scope)} filter "${query.raw}" requires a ${query.workplaceMode} role, but the job is classified as ${jobDiagnostics.workplaceMode}.`,
      matchedTerms: [],
    };
  }

  if (query.scope === "country") {
    if (query.canonicalCountry) {
      const physicalMatch = findCountryPoint(
        jobDiagnostics.physicalLocations,
        query.canonicalCountry,
      );
      const eligibilityMatches = countryListIncludes(
        jobDiagnostics.eligibilityCountries,
        query.canonicalCountry,
      );
      const hasPhysicalLocations = jobDiagnostics.physicalLocations.length > 0;

      if (physicalMatch) {
        return {
          matches: true,
          explanation: `Country filter "${query.raw}" matched a physical job location in ${query.canonicalCountry}${physicalMatch.state ? ` via ${physicalMatch.state}` : ""}${physicalMatch.city ? ` and ${physicalMatch.city}` : ""}.`,
          matchedTerms: matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms),
        };
      }

      if (query.workplaceMode === "remote" && eligibilityMatches) {
        return {
          matches: true,
          explanation: `Country filter "${query.raw}" matched remote eligibility for ${query.canonicalCountry}; no physical location was treated as ${query.canonicalCountry}.`,
          matchedTerms: matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms),
        };
      }

      if (!hasPhysicalLocations && eligibilityMatches) {
        return {
          matches: true,
          explanation: `Country filter "${query.raw}" matched because the job only provides remote eligibility for ${query.canonicalCountry}; no physical office location was available.`,
          matchedTerms: matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms),
        };
      }

      return {
        matches: false,
        explanation: `Country filter "${query.raw}" did not match any physical job location in ${query.canonicalCountry}${eligibilityMatches ? "; matching remote eligibility requires an explicit remote country filter when physical evidence points elsewhere." : ""}.`,
        matchedTerms: [],
      };
    }

    return {
      matches: false,
      explanation: `Country filter "${query.raw}" did not match the job location "${jobDiagnostics.raw}".`,
      matchedTerms: [],
    };
  }

  if (query.scope === "state") {
    const matchedByResolvedState =
      Boolean(query.canonicalState) &&
      normalizeLocationText(jobDiagnostics.state) === normalizeLocationText(query.canonicalState);
    const matchedTerms = matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms);

    if (matchedByResolvedState || matchedTerms.length > 0) {
      return {
        matches: true,
        explanation: `State filter "${query.raw}" matched ${matchedByResolvedState ? `the resolved state "${jobDiagnostics.state}"` : `the normalized location text`}.`,
        matchedTerms,
      };
    }

    return {
      matches: false,
      explanation: `State filter "${query.raw}" did not match the resolved state or normalized location text.`,
      matchedTerms: [],
    };
  }

  const matchedByResolvedCity =
    Boolean(query.canonicalCity) &&
    normalizeLocationText(jobDiagnostics.city) === normalizeLocationText(query.canonicalCity);
  const matchedTerms = matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms);

  if (matchedByResolvedCity || matchedTerms.length > 0) {
    return {
      matches: true,
      explanation: `City filter "${query.raw}" matched ${matchedByResolvedCity ? `the resolved city "${jobDiagnostics.city}"` : `the normalized location text`}.`,
      matchedTerms,
    };
  }

  return {
    matches: false,
    explanation: `City filter "${query.raw}" did not match the resolved city or normalized location text.`,
    matchedTerms: [],
  };
}

function buildLocationQueryDiagnostics(activeQueries: LocationQueryTermSet[]): LocationMatchDiagnostics {
  return {
    original: activeQueries.map((query) => query.raw).join(", "),
    normalized: normalizeLocationText(activeQueries.map((query) => query.normalized).join(" ")),
    country: activeQueries.find((query) => query.scope === "country")?.raw,
    state: activeQueries.find((query) => query.scope === "state")?.raw,
    city: activeQueries.find((query) => query.scope === "city")?.raw,
    workplaceMode: activeQueries.find((query) => query.workplaceMode !== "any")?.workplaceMode ?? "any",
    scopesApplied: activeQueries.map((query) => query.scope),
    expandedTerms: dedupeTerms(activeQueries.flatMap((query) => query.expandedTerms)),
  };
}

function buildJobLocationDiagnostics(
  job: LocationFilterableJob,
  resolvedLocation: ResolvedLocation,
): JobLocationDiagnostics {
  const raw = job.locationText?.trim() || "Location unavailable";
  const physicalLocations = getPhysicalLocations(job, resolvedLocation);
  const eligibilityCountries = getEligibilityCountries(resolvedLocation);
  const normalized = normalizeLocationText(
    [
      resolvedLocation.city,
      resolvedLocation.state,
      resolvedLocation.stateCode,
      resolvedLocation.country,
      ...physicalLocations.flatMap((location) => [
        location.city,
        location.state,
        location.stateCode,
        location.country,
      ]),
      ...eligibilityCountries,
      job.city,
      job.state,
      job.country,
      job.locationText,
    ]
      .filter(Boolean)
      .join(" "),
  );

  return {
    raw,
    normalized,
    country: resolvedLocation.country ?? job.country,
    state: resolvedLocation.state ?? job.state,
    stateCode: resolvedLocation.stateCode,
    city: resolvedLocation.city ?? job.city,
    workplaceMode: inferJobWorkplaceMode(raw, resolvedLocation),
    isRemote: resolvedLocation.isRemote,
    isUnitedStates: resolvedLocation.isUnitedStates,
    physicalLocations,
    eligibilityCountries,
    conflicts: resolvedLocation.conflicts ?? [],
    aliasesUsed: dedupeTerms([
      ...(resolvedLocation.country && isUnitedStatesValue(resolvedLocation.country)
        ? unitedStatesQueryAliases
        : []),
      ...getSupportedCountryAliases(
        resolveSupportedCountryConcept(resolvedLocation.country),
      ),
      resolvedLocation.state ? normalizeLocationText(resolvedLocation.state) : "",
      resolvedLocation.stateCode ? normalizeLocationText(resolvedLocation.stateCode) : "",
      resolvedLocation.city ? normalizeLocationText(resolvedLocation.city) : "",
      ...matchedAliasTerms(normalized, buildLocationQueryAliasesFromJob(raw, resolvedLocation)),
    ]),
  };
}

function getPhysicalLocations(
  job: LocationFilterableJob,
  resolvedLocation: ResolvedLocation,
) {
  if (resolvedLocation.physicalLocations) {
    return resolvedLocation.physicalLocations;
  }

  const country = resolvedLocation.country ?? job.country;
  if (!country) {
    return [];
  }

  return [
    {
      country,
      ...(resolvedLocation.state ?? job.state
        ? { state: resolvedLocation.state ?? job.state }
        : {}),
      ...(resolvedLocation.stateCode ? { stateCode: resolvedLocation.stateCode } : {}),
      ...(resolvedLocation.city ?? job.city ? { city: resolvedLocation.city ?? job.city } : {}),
      confidence: resolvedLocation.confidence,
      evidence: resolvedLocation.evidence,
    },
  ];
}

function getEligibilityCountries(resolvedLocation: ResolvedLocation) {
  if (resolvedLocation.eligibilityCountries?.length) {
    return resolvedLocation.eligibilityCountries;
  }

  return resolvedLocation.isRemote && resolvedLocation.country
    ? [resolvedLocation.country]
    : [];
}

function findCountryPoint(
  points: ResolvedLocationPoint[],
  country: string,
) {
  return points.find(
    (point) => normalizeLocationText(point.country) === normalizeLocationText(country),
  );
}

function countryListIncludes(countries: string[], country: string) {
  return countries.some(
    (candidate) => normalizeLocationText(candidate) === normalizeLocationText(country),
  );
}

function buildLocationQueryTermSet(scope: LocationScope, raw: string): LocationQueryTermSet {
  const normalized = normalizeLocationText(raw);
  const workplaceMode = inferQueryWorkplaceMode(raw);
  const analysis = analyzeUsLocation(raw);
  const supportedMetro =
    resolveSupportedCountryMetro(raw) ?? findSupportedCountryMetroInText(raw);
  const supportedRegion =
    resolveSupportedCountryRegion(raw, supportedMetro?.countryConcept) ??
    findSupportedCountryRegionInText(raw, supportedMetro?.countryConcept);
  const supportedCountryConcept =
    supportedMetro?.countryConcept ??
    supportedRegion?.countryConcept ??
    resolveSupportedCountryConcept(raw) ??
    findSupportedCountryConceptInText(raw);
  const canonicalState =
    resolveUsState(raw) ?? analysis.stateName ?? supportedRegion?.name ?? supportedMetro?.regionName;
  const canonicalStateCode =
    (canonicalState ? resolveUsStateCode(canonicalState) : undefined) ??
    analysis.stateCode ??
    supportedRegion?.code ??
    supportedMetro?.regionCode;
  const canonicalCity = analysis.city ?? supportedMetro?.city;
  const canonicalCountry =
    analysis.isUnitedStates || isUnitedStatesValue(raw)
      ? "United States"
      : getSupportedCountryCanonicalName(supportedCountryConcept);

  if (scope === "country" && canonicalCountry === "United States") {
    return {
      scope,
      raw,
      normalized,
      workplaceMode,
      canonicalCountry,
      expandedTerms: dedupeTerms([
        ...unitedStatesQueryAliases,
        ...buildBroadWorkplaceTerms(unitedStatesQueryAliases),
        ...buildWorkplaceQualifiedTerms(workplaceMode, unitedStatesQueryAliases),
      ]),
    };
  }

  if (scope === "country" && canonicalCountry) {
    const countryAliases = dedupeTerms([
      ...getSupportedCountryAliases(supportedCountryConcept),
      normalized,
    ]);

    return {
      scope,
      raw,
      normalized,
      workplaceMode,
      canonicalCountry,
      expandedTerms: dedupeTerms([
        ...countryAliases,
        ...buildBroadWorkplaceTerms(countryAliases),
        ...buildWorkplaceQualifiedTerms(workplaceMode, countryAliases),
      ]),
    };
  }

  if (scope === "state" && canonicalState) {
    const stateAliases = dedupeTerms([
      normalizeLocationText(canonicalState),
      canonicalStateCode ? normalizeLocationText(canonicalStateCode) : "",
      canonicalStateCode ? normalizeLocationText(`${canonicalStateCode} usa`) : "",
    ]);

    return {
      scope,
      raw,
      normalized,
      workplaceMode,
      canonicalState,
      canonicalStateCode,
      canonicalCountry: "United States",
      expandedTerms: dedupeTerms([
        ...stateAliases,
        ...buildBroadWorkplaceTerms([normalizeLocationText(canonicalState)]),
        ...buildWorkplaceQualifiedTerms(workplaceMode, [normalizeLocationText(canonicalState)]),
      ]),
    };
  }

  if (scope === "state" && supportedRegion) {
    const countryName = getSupportedCountryCanonicalName(supportedRegion.countryConcept);
    const regionAliases = dedupeTerms([
      ...supportedRegion.aliases,
      supportedRegion.name,
      supportedRegion.code ?? "",
      countryName ? `${supportedRegion.name} ${countryName}` : "",
      supportedRegion.code && countryName ? `${supportedRegion.code} ${countryName}` : "",
    ]);

    return {
      scope,
      raw,
      normalized,
      workplaceMode,
      canonicalCountry: countryName,
      canonicalState: supportedRegion.name,
      canonicalStateCode: supportedRegion.code,
      expandedTerms: dedupeTerms([
        ...regionAliases,
        ...buildBroadWorkplaceTerms(regionAliases),
        ...buildWorkplaceQualifiedTerms(workplaceMode, regionAliases),
      ]),
    };
  }

  if (scope === "city" && canonicalCity) {
    const cityAliases = dedupeTerms([
      normalizeLocationText(canonicalCity),
      canonicalStateCode ? normalizeLocationText(`${canonicalCity} ${canonicalStateCode}`) : "",
      canonicalState ? normalizeLocationText(`${canonicalCity} ${canonicalState}`) : "",
      canonicalCountry ? normalizeLocationText(`${canonicalCity} ${canonicalCountry}`) : "",
    ]);

    return {
      scope,
      raw,
      normalized,
      workplaceMode,
      canonicalCity,
      canonicalState,
      canonicalStateCode,
      canonicalCountry: analysis.isUnitedStates ? "United States" : undefined,
      expandedTerms: dedupeTerms([
        ...cityAliases,
        ...buildBroadWorkplaceTerms(cityAliases),
        ...buildWorkplaceQualifiedTerms(workplaceMode, cityAliases),
      ]),
    };
  }

  return {
    scope,
    raw,
    normalized,
    workplaceMode,
    canonicalCountry,
    canonicalState,
    canonicalStateCode,
    canonicalCity,
    expandedTerms: dedupeTerms([
      normalized,
      ...buildWorkplaceQualifiedTerms(workplaceMode, [normalized]),
    ]),
  };
}

function buildLocationQueryAliasesFromJob(raw: string, resolvedLocation: ResolvedLocation) {
  const terms = [
    normalizeLocationText(raw),
    resolvedLocation.city ? normalizeLocationText(resolvedLocation.city) : "",
    resolvedLocation.state ? normalizeLocationText(resolvedLocation.state) : "",
    resolvedLocation.stateCode ? normalizeLocationText(resolvedLocation.stateCode) : "",
    resolvedLocation.country ? normalizeLocationText(resolvedLocation.country) : "",
    ...(resolvedLocation.physicalLocations ?? []).flatMap((location) => [
      normalizeLocationText(location.city),
      normalizeLocationText(location.state),
      normalizeLocationText(location.stateCode),
      normalizeLocationText(location.country),
    ]),
    ...(resolvedLocation.eligibilityCountries ?? []).map(normalizeLocationText),
  ];

  if (resolvedLocation.city && resolvedLocation.stateCode) {
    terms.push(normalizeLocationText(`${resolvedLocation.city} ${resolvedLocation.stateCode}`));
  }

  if (resolvedLocation.city && resolvedLocation.state) {
    terms.push(normalizeLocationText(`${resolvedLocation.city} ${resolvedLocation.state}`));
  }

  if (resolvedLocation.isUnitedStates) {
    terms.push(...unitedStatesQueryAliases);
  } else {
    terms.push(
      ...getSupportedCountryAliases(
        resolveSupportedCountryConcept(resolvedLocation.country),
      ),
    );
  }

  return dedupeTerms(terms);
}

function inferQueryWorkplaceMode(value?: string): LocationWorkplaceMode {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return "any";
  }

  if (remoteMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))) {
    return "remote";
  }

  if (hybridMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))) {
    return "hybrid";
  }

  return "any";
}

function inferJobWorkplaceMode(
  rawLocation: string,
  resolvedLocation: ResolvedLocation,
): JobWorkplaceMode {
  const normalized = normalizeLocationText(rawLocation);

  if (
    resolvedLocation.isRemote ||
    (resolvedLocation.eligibilityCountries?.length ?? 0) > 0 ||
    remoteMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))
  ) {
    return "remote";
  }

  if (hybridMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))) {
    return "hybrid";
  }

  if (onsiteMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))) {
    return "onsite";
  }

  return "unknown";
}

function workplaceModeMatches(
  wanted: LocationWorkplaceMode,
  actual: JobWorkplaceMode,
) {
  if (wanted === "any") {
    return true;
  }

  return actual === wanted;
}

function buildWorkplaceQualifiedTerms(
  workplaceMode: LocationWorkplaceMode,
  baseTerms: string[],
) {
  if (workplaceMode === "any") {
    return [];
  }

  return baseTerms.flatMap((term) => {
    if (!term) {
      return [];
    }

    return dedupeTerms([
      `${workplaceMode} ${term}`,
      `${term} ${workplaceMode}`,
      workplaceMode === "remote" ? `remote in ${term}` : `hybrid in ${term}`,
    ]);
  });
}

function buildBroadWorkplaceTerms(baseTerms: string[]) {
  return dedupeTerms([
    ...baseTerms.flatMap((term) => [
      `remote ${term}`,
      `${term} remote`,
      `remote in ${term}`,
    ]),
    ...baseTerms.flatMap((term) => [
      `hybrid ${term}`,
      `${term} hybrid`,
      `hybrid in ${term}`,
    ]),
  ]);
}

function matchedAliasTerms(haystack: string, aliases: string[]) {
  return dedupeTerms(
    aliases.filter((alias) => alias && containsNormalizedTerm(haystack, alias)),
  );
}

function containsNormalizedTerm(haystack: string, term: string) {
  return (
    haystack === term ||
    haystack.startsWith(`${term} `) ||
    haystack.endsWith(` ${term}`) ||
    haystack.includes(` ${term} `)
  );
}

function dedupeTerms(values: string[]) {
  const seen = new Set<string>();
  const deduped: string[] = [];

  values
    .map((value) => normalizeLocationText(value))
    .filter(Boolean)
    .forEach((value) => {
      if (seen.has(value)) {
        return;
      }

      seen.add(value);
      deduped.push(value);
    });

  return deduped;
}

function labelScope(scope: LocationScope) {
  switch (scope) {
    case "country":
      return "Country";
    case "state":
      return "State";
    default:
      return "City";
  }
}
