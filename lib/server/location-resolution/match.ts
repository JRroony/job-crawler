import "server-only";

import {
  analyzeUsLocation,
  isUnitedStatesValue,
  normalizeLocationText,
  resolveUsState,
  resolveUsStateCode,
} from "@/lib/server/locations/us";
import type { JobListing, ResolvedLocation, SearchFilters } from "@/lib/types";

type LocationWorkplaceMode = "any" | "remote" | "hybrid";
type JobWorkplaceMode = "remote" | "hybrid" | "onsite" | "unknown";

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
    if (query.canonicalCountry === "United States") {
      if (!jobDiagnostics.isUnitedStates) {
        return {
          matches: false,
          explanation: `Country filter "${query.raw}" requires a United States location, but the job did not resolve to the United States.`,
          matchedTerms: [],
        };
      }

      return {
        matches: true,
        explanation: `Country filter "${query.raw}" matched because the job resolved to the United States${jobDiagnostics.state ? ` via ${jobDiagnostics.state}` : ""}${jobDiagnostics.city ? ` and ${jobDiagnostics.city}` : ""}.`,
        matchedTerms: matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms),
      };
    }

    if (
      query.canonicalCountry &&
      normalizeLocationText(jobDiagnostics.country) === normalizeLocationText(query.canonicalCountry)
    ) {
      return {
        matches: true,
        explanation: `Country filter "${query.raw}" matched the resolved country "${jobDiagnostics.country}".`,
        matchedTerms: matchedAliasTerms(jobDiagnostics.normalized, query.expandedTerms),
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
  const normalized = normalizeLocationText(
    [
      resolvedLocation.city,
      resolvedLocation.state,
      resolvedLocation.stateCode,
      resolvedLocation.country,
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
    aliasesUsed: dedupeTerms([
      ...(resolvedLocation.country && isUnitedStatesValue(resolvedLocation.country)
        ? unitedStatesQueryAliases
        : []),
      resolvedLocation.state ? normalizeLocationText(resolvedLocation.state) : "",
      resolvedLocation.stateCode ? normalizeLocationText(resolvedLocation.stateCode) : "",
      resolvedLocation.city ? normalizeLocationText(resolvedLocation.city) : "",
      ...matchedAliasTerms(normalized, buildLocationQueryAliasesFromJob(raw, resolvedLocation)),
    ]),
  };
}

function buildLocationQueryTermSet(scope: LocationScope, raw: string): LocationQueryTermSet {
  const normalized = normalizeLocationText(raw);
  const workplaceMode = inferQueryWorkplaceMode(raw);
  const analysis = analyzeUsLocation(raw);
  const canonicalState = resolveUsState(raw) ?? analysis.stateName;
  const canonicalStateCode =
    (canonicalState ? resolveUsStateCode(canonicalState) : undefined) ?? analysis.stateCode;
  const canonicalCity = analysis.city;
  const canonicalCountry =
    analysis.isUnitedStates || isUnitedStatesValue(raw) ? "United States" : undefined;

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

  if (scope === "city" && canonicalCity) {
    const cityAliases = dedupeTerms([
      normalizeLocationText(canonicalCity),
      canonicalStateCode ? normalizeLocationText(`${canonicalCity} ${canonicalStateCode}`) : "",
      canonicalState ? normalizeLocationText(`${canonicalCity} ${canonicalState}`) : "",
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
  ];

  if (resolvedLocation.city && resolvedLocation.stateCode) {
    terms.push(normalizeLocationText(`${resolvedLocation.city} ${resolvedLocation.stateCode}`));
  }

  if (resolvedLocation.city && resolvedLocation.state) {
    terms.push(normalizeLocationText(`${resolvedLocation.city} ${resolvedLocation.state}`));
  }

  if (resolvedLocation.isUnitedStates) {
    terms.push(...unitedStatesQueryAliases);
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

  if (resolvedLocation.isRemote || remoteMarkers.some((marker) => containsNormalizedTerm(normalized, normalizeLocationText(marker)))) {
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
