import "server-only";

import type { SearchFilters } from "@/lib/types";

export type UsMetro = {
  city: string;
  stateName: string;
  stateCode: string;
  priority: number;
};

export type UsLocationIntent =
  | { kind: "none" }
  | { kind: "non_us" }
  | { kind: "broad_us" }
  | { kind: "us_state"; stateName: string; stateCode: string }
  | { kind: "us_city"; city: string; stateName?: string; stateCode?: string };

const unitedStatesAliases = new Set(
  [
    "united states",
    "united states of america",
    "usa",
    "us",
    "u s a",
    "u s",
  ].map(normalizeLocationText),
);

const usStatePairs = [
  ["AL", "Alabama"],
  ["AK", "Alaska"],
  ["AZ", "Arizona"],
  ["AR", "Arkansas"],
  ["CA", "California"],
  ["CO", "Colorado"],
  ["CT", "Connecticut"],
  ["DE", "Delaware"],
  ["FL", "Florida"],
  ["GA", "Georgia"],
  ["HI", "Hawaii"],
  ["ID", "Idaho"],
  ["IL", "Illinois"],
  ["IN", "Indiana"],
  ["IA", "Iowa"],
  ["KS", "Kansas"],
  ["KY", "Kentucky"],
  ["LA", "Louisiana"],
  ["ME", "Maine"],
  ["MD", "Maryland"],
  ["MA", "Massachusetts"],
  ["MI", "Michigan"],
  ["MN", "Minnesota"],
  ["MS", "Mississippi"],
  ["MO", "Missouri"],
  ["MT", "Montana"],
  ["NE", "Nebraska"],
  ["NV", "Nevada"],
  ["NH", "New Hampshire"],
  ["NJ", "New Jersey"],
  ["NM", "New Mexico"],
  ["NY", "New York"],
  ["NC", "North Carolina"],
  ["ND", "North Dakota"],
  ["OH", "Ohio"],
  ["OK", "Oklahoma"],
  ["OR", "Oregon"],
  ["PA", "Pennsylvania"],
  ["RI", "Rhode Island"],
  ["SC", "South Carolina"],
  ["SD", "South Dakota"],
  ["TN", "Tennessee"],
  ["TX", "Texas"],
  ["UT", "Utah"],
  ["VT", "Vermont"],
  ["VA", "Virginia"],
  ["WA", "Washington"],
  ["WV", "West Virginia"],
  ["WI", "Wisconsin"],
  ["WY", "Wyoming"],
  ["DC", "District of Columbia"],
] as const;

const usStateByAlias = new Map(
  usStatePairs.flatMap(([stateCode, stateName]) => [
    [normalizeLocationText(stateCode), stateName],
    [normalizeLocationText(stateName), stateName],
  ] as const),
);

const usStateCodeByName = new Map(
  usStatePairs.map(([stateCode, stateName]) => [stateName, stateCode] as const),
);

const usMetros: UsMetro[] = [
  { city: "Seattle", stateName: "Washington", stateCode: "WA", priority: 1 },
  { city: "Bellevue", stateName: "Washington", stateCode: "WA", priority: 2 },
  { city: "San Francisco", stateName: "California", stateCode: "CA", priority: 3 },
  { city: "San Jose", stateName: "California", stateCode: "CA", priority: 4 },
  { city: "Mountain View", stateName: "California", stateCode: "CA", priority: 5 },
  { city: "Sunnyvale", stateName: "California", stateCode: "CA", priority: 6 },
  { city: "Palo Alto", stateName: "California", stateCode: "CA", priority: 7 },
  { city: "Austin", stateName: "Texas", stateCode: "TX", priority: 8 },
  { city: "New York", stateName: "New York", stateCode: "NY", priority: 9 },
  { city: "Boston", stateName: "Massachusetts", stateCode: "MA", priority: 10 },
  { city: "Chicago", stateName: "Illinois", stateCode: "IL", priority: 11 },
  { city: "Washington", stateName: "District of Columbia", stateCode: "DC", priority: 12 },
  { city: "Arlington", stateName: "Virginia", stateCode: "VA", priority: 13 },
  { city: "Reston", stateName: "Virginia", stateCode: "VA", priority: 14 },
  { city: "Herndon", stateName: "Virginia", stateCode: "VA", priority: 15 },
  { city: "Denver", stateName: "Colorado", stateCode: "CO", priority: 16 },
  { city: "Atlanta", stateName: "Georgia", stateCode: "GA", priority: 17 },
  { city: "Los Angeles", stateName: "California", stateCode: "CA", priority: 18 },
  { city: "Irvine", stateName: "California", stateCode: "CA", priority: 19 },
  { city: "Santa Clara", stateName: "California", stateCode: "CA", priority: 20 },
  { city: "Redwood City", stateName: "California", stateCode: "CA", priority: 21 },
  { city: "Oakland", stateName: "California", stateCode: "CA", priority: 22 },
  { city: "San Diego", stateName: "California", stateCode: "CA", priority: 23 },
  { city: "Jersey City", stateName: "New Jersey", stateCode: "NJ", priority: 24 },
  { city: "Portland", stateName: "Oregon", stateCode: "OR", priority: 25 },
  { city: "Dallas", stateName: "Texas", stateCode: "TX", priority: 26 },
  { city: "Houston", stateName: "Texas", stateCode: "TX", priority: 27 },
  { city: "Miami", stateName: "Florida", stateCode: "FL", priority: 28 },
  { city: "Nashville", stateName: "Tennessee", stateCode: "TN", priority: 29 },
  { city: "Philadelphia", stateName: "Pennsylvania", stateCode: "PA", priority: 30 },
  { city: "Phoenix", stateName: "Arizona", stateCode: "AZ", priority: 31 },
  { city: "Pittsburgh", stateName: "Pennsylvania", stateCode: "PA", priority: 32 },
  { city: "Raleigh", stateName: "North Carolina", stateCode: "NC", priority: 33 },
  { city: "Salt Lake City", stateName: "Utah", stateCode: "UT", priority: 34 },
  { city: "Las Vegas", stateName: "Nevada", stateCode: "NV", priority: 35 },
];

const metrosByPriority = [...usMetros].sort(
  (left, right) => left.priority - right.priority || left.city.localeCompare(right.city),
);

const metrosByNormalizedCity = metrosByPriority.reduce<Map<string, UsMetro[]>>((map, metro) => {
  const key = normalizeLocationText(metro.city);
  const current = map.get(key) ?? [];
  current.push(metro);
  map.set(key, current);
  return map;
}, new Map());

const metroAliases = metrosByPriority.reduce<Map<string, UsMetro>>((map, metro) => {
  for (const alias of buildMetroAliases(metro)) {
    if (!map.has(alias)) {
      map.set(alias, metro);
    }
  }

  return map;
}, new Map());

export function getUsMetroCatalog() {
  return metrosByPriority;
}

export function resolveUsState(value?: string) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return usStateByAlias.get(normalized);
}

export function resolveUsStateCode(value?: string) {
  const stateName = resolveUsState(value);
  return stateName ? usStateCodeByName.get(stateName) : undefined;
}

export function isUnitedStatesValue(value?: string) {
  const normalized = normalizeLocationText(value);
  return Boolean(normalized) && unitedStatesAliases.has(normalized);
}

export function isRecognizedUsCity(value?: string) {
  const normalized = normalizeLocationText(value);
  return Boolean(normalized) && metroAliases.has(normalized);
}

export function resolveUsMetro(value?: string) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  const aliasedMetro = metroAliases.get(normalized);
  if (aliasedMetro) {
    return aliasedMetro;
  }

  const matchingMetros = metrosByNormalizedCity.get(normalized);
  if (!matchingMetros?.length) {
    return undefined;
  }

  return matchingMetros[0];
}

export function planUsLocationIntent(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
): UsLocationIntent {
  const explicitStateName = resolveUsState(filters.state);
  const explicitStateCode = explicitStateName
    ? usStateCodeByName.get(explicitStateName)
    : undefined;
  const explicitMetro =
    filters.city && explicitStateCode
      ? resolveUsMetroFromCityAndState(filters.city, explicitStateCode)
      : resolveUsMetro(filters.city);

  if (filters.country && !isUnitedStatesValue(filters.country) && !explicitStateName && !explicitMetro) {
    return {
      kind: "non_us",
    };
  }

  if (filters.city && (explicitMetro || isUnitedStatesValue(filters.country) || explicitStateName)) {
    return {
      kind: "us_city",
      city: filters.city.trim(),
      stateName: explicitStateName ?? explicitMetro?.stateName,
      stateCode: explicitStateCode ?? explicitMetro?.stateCode,
    };
  }

  if (explicitStateName && explicitStateCode) {
    return {
      kind: "us_state",
      stateName: explicitStateName,
      stateCode: explicitStateCode,
    };
  }

  if (isUnitedStatesValue(filters.country)) {
    return {
      kind: "broad_us",
    };
  }

  return {
    kind: "none",
  };
}

export function buildUsDiscoveryLocationClauses(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
  options: {
    maxClauses?: number;
  } = {},
) {
  const intent = planUsLocationIntent(filters);
  const maxClauses = options.maxClauses ?? 12;

  if (intent.kind === "none" || intent.kind === "non_us") {
    return {
      intent,
      clauses: [] as string[],
    };
  }

  if (intent.kind === "broad_us") {
    const metroClauses = metrosByPriority
      .slice(0, Math.max(0, maxClauses - 4))
      .map((metro) => formatMetroClause(metro.city, metro.stateCode));

    return {
      intent,
      clauses: dedupeLocationClauses([
        "",
        "remote us",
        "remote usa",
        "remote united states",
        ...metroClauses,
      ]).slice(0, maxClauses),
    };
  }

  if (intent.kind === "us_state") {
    const stateMetros = metrosByPriority
      .filter((metro) => metro.stateCode === intent.stateCode)
      .map((metro) => formatMetroClause(metro.city, metro.stateCode));

    return {
      intent,
      clauses: dedupeLocationClauses([
        "",
        formatStateClause(intent.stateName),
        ...stateMetros,
      ]).slice(0, maxClauses),
    };
  }

  return {
    intent,
    clauses: dedupeLocationClauses([
      "",
      formatCityClause(intent.city, intent.stateCode),
    ]).slice(0, maxClauses),
  };
}

function resolveUsMetroFromCityAndState(city: string, stateCode: string) {
  const normalizedCity = normalizeLocationText(city);
  const matchingMetros = metrosByNormalizedCity.get(normalizedCity) ?? [];

  return matchingMetros.find((metro) => metro.stateCode === stateCode) ?? matchingMetros[0];
}

function buildMetroAliases(metro: UsMetro) {
  return dedupeLocationClauses([
    normalizeLocationText(metro.city),
    normalizeLocationText(`${metro.city} ${metro.stateCode}`),
    normalizeLocationText(`${metro.city} ${metro.stateName}`),
    normalizeLocationText(`${metro.city}, ${metro.stateCode}`),
    normalizeLocationText(`${metro.city}, ${metro.stateName}`),
  ]);
}

function dedupeLocationClauses(clauses: string[]) {
  const seen = new Set<string>();
  const deduped: string[] = [];

  for (const clause of clauses) {
    const normalized = clause === "" ? "" : normalizeLocationText(clause);
    if (seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    deduped.push(normalized);
  }

  return deduped;
}

function formatMetroClause(city: string, stateCode: string) {
  return normalizeLocationText(`${city} ${stateCode}`);
}

function formatCityClause(city: string, stateCode?: string) {
  return normalizeLocationText(stateCode ? `${city} ${stateCode}` : city);
}

function formatStateClause(stateName: string) {
  if (stateName === "Washington") {
    return "washington state";
  }

  if (stateName === "New York") {
    return "new york state";
  }

  return normalizeLocationText(stateName);
}

function normalizeLocationText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
