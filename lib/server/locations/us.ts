import "server-only";

import type { SearchFilters } from "@/lib/types";

export type UsState = {
  code: string;
  name: string;
};

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

export type UsLocationAnalysis = {
  isUnitedStates: boolean;
  isRemote: boolean;
  city?: string;
  stateName?: string;
  stateCode?: string;
  normalized: string;
};

export type UsDiscoveryLocationToken = {
  kind: "country" | "remote" | "state" | "remote_state" | "metro";
  value: string;
  priority: number;
};

export type UsDiscoveryLocationClause = {
  clause: string;
  kind: UsDiscoveryLocationToken["kind"] | "blank";
  priority: number;
};

const unitedStatesAliasList = [
  "united states",
  "united states of america",
  "usa",
  "us",
  "u s a",
  "u s",
];

const remoteUnitedStatesAliasList = [
  "remote united states",
  "remote usa",
  "remote us",
  "united states remote",
  "usa remote",
  "us remote",
  "remote united states only",
  "remote usa only",
  "remote us only",
];

const ambiguousStateCodes = new Set(["AS", "HI", "ID", "IN", "ME", "OR"]);

const usStateEntries: UsState[] = [
  { code: "AL", name: "Alabama" },
  { code: "AK", name: "Alaska" },
  { code: "AZ", name: "Arizona" },
  { code: "AR", name: "Arkansas" },
  { code: "CA", name: "California" },
  { code: "CO", name: "Colorado" },
  { code: "CT", name: "Connecticut" },
  { code: "DE", name: "Delaware" },
  { code: "FL", name: "Florida" },
  { code: "GA", name: "Georgia" },
  { code: "HI", name: "Hawaii" },
  { code: "ID", name: "Idaho" },
  { code: "IL", name: "Illinois" },
  { code: "IN", name: "Indiana" },
  { code: "IA", name: "Iowa" },
  { code: "KS", name: "Kansas" },
  { code: "KY", name: "Kentucky" },
  { code: "LA", name: "Louisiana" },
  { code: "ME", name: "Maine" },
  { code: "MD", name: "Maryland" },
  { code: "MA", name: "Massachusetts" },
  { code: "MI", name: "Michigan" },
  { code: "MN", name: "Minnesota" },
  { code: "MS", name: "Mississippi" },
  { code: "MO", name: "Missouri" },
  { code: "MT", name: "Montana" },
  { code: "NE", name: "Nebraska" },
  { code: "NV", name: "Nevada" },
  { code: "NH", name: "New Hampshire" },
  { code: "NJ", name: "New Jersey" },
  { code: "NM", name: "New Mexico" },
  { code: "NY", name: "New York" },
  { code: "NC", name: "North Carolina" },
  { code: "ND", name: "North Dakota" },
  { code: "OH", name: "Ohio" },
  { code: "OK", name: "Oklahoma" },
  { code: "OR", name: "Oregon" },
  { code: "PA", name: "Pennsylvania" },
  { code: "RI", name: "Rhode Island" },
  { code: "SC", name: "South Carolina" },
  { code: "SD", name: "South Dakota" },
  { code: "TN", name: "Tennessee" },
  { code: "TX", name: "Texas" },
  { code: "UT", name: "Utah" },
  { code: "VT", name: "Vermont" },
  { code: "VA", name: "Virginia" },
  { code: "WA", name: "Washington" },
  { code: "WV", name: "West Virginia" },
  { code: "WI", name: "Wisconsin" },
  { code: "WY", name: "Wyoming" },
  { code: "DC", name: "District of Columbia" },
];

const unitedStatesAliases = new Set(unitedStatesAliasList.map(normalizeLocationText));
const remoteUnitedStatesAliases = new Set(
  remoteUnitedStatesAliasList.map(normalizeLocationText),
);

const usStateByAlias = new Map(
  usStateEntries.flatMap((state) => {
    const aliases: Array<[string, string]> = [
      [normalizeLocationText(state.code), state.name],
      [normalizeLocationText(state.name), state.name],
    ];

    if (state.name === "District of Columbia") {
      aliases.push([normalizeLocationText("Washington DC"), state.name]);
      aliases.push([normalizeLocationText("Washington D C"), state.name]);
    }

    return aliases;
  }),
);

const usStateCodeByName = new Map(
  usStateEntries.map((state) => [state.name, state.code] as const),
);

const stateEntriesByPriority = prioritizeStates(usStateEntries);

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
  { city: "Sacramento", stateName: "California", stateCode: "CA", priority: 36 },
  { city: "San Antonio", stateName: "Texas", stateCode: "TX", priority: 37 },
  { city: "Tempe", stateName: "Arizona", stateCode: "AZ", priority: 38 },
  { city: "Tampa", stateName: "Florida", stateCode: "FL", priority: 39 },
  { city: "Minneapolis", stateName: "Minnesota", stateCode: "MN", priority: 40 },
  { city: "Detroit", stateName: "Michigan", stateCode: "MI", priority: 41 },
  { city: "Cincinnati", stateName: "Ohio", stateCode: "OH", priority: 42 },
  { city: "Columbus", stateName: "Ohio", stateCode: "OH", priority: 43 },
  { city: "St Louis", stateName: "Missouri", stateCode: "MO", priority: 44 },
  { city: "Charlotte", stateName: "North Carolina", stateCode: "NC", priority: 45 },
  { city: "Madison", stateName: "Wisconsin", stateCode: "WI", priority: 46 },
  { city: "Boulder", stateName: "Colorado", stateCode: "CO", priority: 47 },
  { city: "Cambridge", stateName: "Massachusetts", stateCode: "MA", priority: 48 },
  { city: "Ann Arbor", stateName: "Michigan", stateCode: "MI", priority: 49 },
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

const metroAliasEntries = Array.from(metroAliases.entries()).sort(
  (left, right) => right[0].length - left[0].length || left[0].localeCompare(right[0]),
);

const orderedStateMatchers = buildOrderedStateMatchers();

export function getUsStateCatalog() {
  return [...usStateEntries];
}

export function getUsMetroCatalog() {
  return [...metrosByPriority];
}

export function normalizeLocationText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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

export function analyzeUsLocation(value?: string): UsLocationAnalysis {
  const cleaned = value?.trim() ?? "";
  const normalized = normalizeLocationText(cleaned);

  if (!normalized) {
    return {
      isUnitedStates: false,
      isRemote: false,
      normalized,
    };
  }

  const directMetro = resolveUsMetro(cleaned) ?? findContainedUsMetro(normalized);
  const stateName = directMetro?.stateName ?? findContainedUsStateName(normalized);
  const stateCode = directMetro?.stateCode ?? (stateName ? usStateCodeByName.get(stateName) : undefined);
  const city = directMetro?.city ?? inferCityFromText(cleaned, normalized, stateName);
  const isRemote = normalized === "remote" || containsNormalizedTerm(normalized, "remote");
  const isUnitedStates =
    Boolean(directMetro) ||
    Boolean(stateName) ||
    matchesUnitedStatesAlias(normalized) ||
    matchesRemoteUnitedStatesAlias(normalized);

  return {
    isUnitedStates,
    isRemote,
    city,
    stateName,
    stateCode,
    normalized,
  };
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
  const analyzedCity = analyzeUsLocation(filters.city);

  if (
    filters.country &&
    !isUnitedStatesValue(filters.country) &&
    !explicitStateName &&
    !explicitMetro &&
    !analyzedCity.isUnitedStates
  ) {
    return {
      kind: "non_us",
    };
  }

  if (filters.city && (explicitMetro || analyzedCity.isUnitedStates || isUnitedStatesValue(filters.country) || explicitStateName)) {
    return {
      kind: "us_city",
      city: (explicitMetro?.city ?? analyzedCity.city ?? filters.city).trim(),
      stateName: explicitStateName ?? explicitMetro?.stateName ?? analyzedCity.stateName,
      stateCode: explicitStateCode ?? explicitMetro?.stateCode ?? analyzedCity.stateCode,
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

export function buildUsDiscoveryLocationTokens(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
) {
  const intent = planUsLocationIntent(filters);

  if (intent.kind === "none" || intent.kind === "non_us") {
    return {
      intent,
      tokens: [] as UsDiscoveryLocationToken[],
    };
  }

  if (intent.kind === "us_city") {
    const metro = intent.stateCode
      ? resolveUsMetroFromCityAndState(intent.city, intent.stateCode)
      : resolveUsMetro(intent.city);

    const tokens = dedupeDiscoveryLocationTokens([
      { kind: "metro", value: normalizeLocationText(intent.city), priority: 1 },
      intent.stateCode
        ? { kind: "metro", value: normalizeLocationText(`${intent.city} ${intent.stateCode}`), priority: 2 }
        : undefined,
      intent.stateName
        ? { kind: "metro", value: normalizeLocationText(`${intent.city} ${intent.stateName}`), priority: 3 }
        : undefined,
      metro
        ? { kind: "metro", value: normalizeLocationText(`${metro.city} ${metro.stateCode}`), priority: 4 }
        : undefined,
      intent.stateName
        ? { kind: "state", value: formatStateClause(intent.stateName), priority: 5 }
        : undefined,
      intent.stateCode
        ? { kind: "state", value: buildStateCodeDiscoveryClause(intent.stateCode), priority: 6 }
        : undefined,
      intent.stateName
        ? { kind: "remote_state", value: formatRemoteStateClause(intent.stateName), priority: 7 }
        : undefined,
      intent.stateName
        ? { kind: "remote_state", value: formatStateRemoteClause(intent.stateName), priority: 8 }
        : undefined,
      { kind: "country", value: "united states", priority: 9 },
    ]);

    return {
      intent,
      tokens,
    };
  }

  if (intent.kind === "us_state") {
    const metros = metrosByPriority
      .filter((metro) => metro.stateCode === intent.stateCode)
      .flatMap((metro) => [
        { kind: "metro", value: formatMetroClause(metro.city, metro.stateCode), priority: metro.priority } satisfies UsDiscoveryLocationToken,
        { kind: "metro", value: normalizeLocationText(`${metro.city} ${metro.stateName}`), priority: metro.priority + 100 } satisfies UsDiscoveryLocationToken,
      ]);

    return {
      intent,
      tokens: dedupeDiscoveryLocationTokens([
        { kind: "state", value: formatStateClause(intent.stateName), priority: 1 },
        { kind: "state", value: buildStateCodeDiscoveryClause(intent.stateCode), priority: 2 },
        { kind: "remote_state", value: formatRemoteStateClause(intent.stateName), priority: 3 },
        { kind: "remote_state", value: formatStateRemoteClause(intent.stateName), priority: 4 },
        ...metros,
      ]),
    };
  }

  const countryTokens = [
    { kind: "country", value: "united states", priority: 1 },
    { kind: "country", value: "usa", priority: 2 },
    { kind: "country", value: "us", priority: 3 },
  ] satisfies UsDiscoveryLocationToken[];

  const remoteTokens = [
    { kind: "remote", value: "remote united states", priority: 4 },
    { kind: "remote", value: "remote usa", priority: 5 },
    { kind: "remote", value: "remote us", priority: 6 },
    { kind: "remote", value: "united states remote", priority: 7 },
    { kind: "remote", value: "usa remote", priority: 8 },
    { kind: "remote", value: "us remote", priority: 9 },
  ] satisfies UsDiscoveryLocationToken[];

  const stateTokens = stateEntriesByPriority.flatMap((state, index) => [
    {
      kind: "state",
      value: formatStateClause(state.name),
      priority: 20 + index * 2,
    } satisfies UsDiscoveryLocationToken,
    {
      kind: "state",
      value: buildStateCodeDiscoveryClause(state.code),
      priority: 21 + index * 2,
    } satisfies UsDiscoveryLocationToken,
  ]);

  const remoteStateTokens = stateEntriesByPriority.flatMap((state, index) => [
    {
      kind: "remote_state",
      value: formatRemoteStateClause(state.name),
      priority: 120 + index * 2,
    } satisfies UsDiscoveryLocationToken,
    {
      kind: "remote_state",
      value: formatStateRemoteClause(state.name),
      priority: 121 + index * 2,
    } satisfies UsDiscoveryLocationToken,
  ]);

  const metroTokens = metrosByPriority.flatMap((metro) => [
    {
      kind: "metro",
      value: formatMetroClause(metro.city, metro.stateCode),
      priority: 200 + metro.priority * 2,
    } satisfies UsDiscoveryLocationToken,
    {
      kind: "metro",
      value: normalizeLocationText(`${metro.city} ${metro.stateName}`),
      priority: 201 + metro.priority * 2,
    } satisfies UsDiscoveryLocationToken,
  ]);

  return {
    intent,
    tokens: dedupeDiscoveryLocationTokens([
      ...countryTokens,
      ...remoteTokens,
      ...remoteStateTokens,
      ...stateTokens,
      ...metroTokens,
    ]),
  };
}

export function buildUsDiscoveryLocationClauses(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
  options: {
    maxClauses?: number;
  } = {},
) {
  const tokenPlan = buildUsDiscoveryLocationTokens(filters);
  const maxClauses = options.maxClauses ?? 48;

  if (tokenPlan.intent.kind === "none" || tokenPlan.intent.kind === "non_us") {
    return {
      intent: tokenPlan.intent,
      clauses: [] as string[],
      detailedClauses: [] as UsDiscoveryLocationClause[],
    };
  }

  if (tokenPlan.intent.kind === "broad_us") {
    const countryClauses = tokenPlan.tokens
      .filter((token) => token.kind === "country")
      .map((token) => token.value);
    const remoteClauses = tokenPlan.tokens
      .filter((token) => token.kind === "remote")
      .map((token) => token.value);
    const remoteStateClauses = tokenPlan.tokens
      .filter((token) => token.kind === "remote_state")
      .map((token) => token.value);
    const metroClauses = tokenPlan.tokens
      .filter((token) => token.kind === "metro")
      .map((token) => token.value);
    const stateClauses = tokenPlan.tokens
      .filter((token) => token.kind === "state")
      .map((token) => token.value);
    const remainingBudget = Math.max(0, maxClauses - 1 - countryClauses.length - remoteClauses.length);
    const prioritizedRemoteStateCount = Math.min(
      remoteStateClauses.length,
      Math.max(2, Math.floor(remainingBudget / 4)),
    );
    const prioritizedMetroCount = Math.min(
      metroClauses.length,
      Math.max(4, Math.ceil((remainingBudget - prioritizedRemoteStateCount) / 3)),
    );
    const prioritizedStateCount = Math.max(
      0,
      remainingBudget - prioritizedRemoteStateCount - prioritizedMetroCount,
    );

    const orderedClauses = finalizeDiscoveryLocationClauses(
      [
        { clause: "", kind: "blank", priority: 0 },
        ...tokenPlan.tokens
          .filter((token) => token.kind === "country")
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "remote")
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "remote_state")
          .slice(0, prioritizedRemoteStateCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "metro")
          .slice(0, prioritizedMetroCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "state")
          .slice(0, prioritizedStateCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "remote_state")
          .slice(prioritizedRemoteStateCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "metro")
          .slice(prioritizedMetroCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
        ...tokenPlan.tokens
          .filter((token) => token.kind === "state")
          .slice(prioritizedStateCount)
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
      ],
      maxClauses,
    );

    return {
      intent: tokenPlan.intent,
      clauses: orderedClauses.map((entry) => entry.clause),
      detailedClauses: orderedClauses,
    };
  }

  if (tokenPlan.intent.kind === "us_state") {
    const orderedClauses = finalizeDiscoveryLocationClauses(
      [
        { clause: "", kind: "blank", priority: 0 },
        ...tokenPlan.tokens
          .sort((left, right) => left.priority - right.priority || left.value.localeCompare(right.value))
          .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
      ],
      Math.min(maxClauses, 23),
    );

    return {
      intent: tokenPlan.intent,
      clauses: orderedClauses.map((entry) => entry.clause),
      detailedClauses: orderedClauses,
    };
  }

  const orderedClauses = finalizeDiscoveryLocationClauses(
    [
      { clause: "", kind: "blank", priority: 0 },
      ...tokenPlan.tokens
        .sort((left, right) => left.priority - right.priority || left.value.localeCompare(right.value))
        .map((token) => ({ clause: token.value, kind: token.kind, priority: token.priority })),
    ],
    maxClauses,
  );

  return {
    intent: tokenPlan.intent,
    clauses: orderedClauses.map((entry) => entry.clause),
    detailedClauses: orderedClauses,
  };
}

function prioritizeStates(states: UsState[]) {
  const preferredOrder = [
    "California",
    "Texas",
    "New York",
    "Washington",
    "Massachusetts",
    "Illinois",
    "Florida",
    "Georgia",
    "Virginia",
    "Colorado",
    "Pennsylvania",
    "North Carolina",
    "New Jersey",
    "Arizona",
    "Utah",
    "Oregon",
  ];

  return [...states].sort((left, right) => {
    const leftPriority = preferredOrder.indexOf(left.name);
    const rightPriority = preferredOrder.indexOf(right.name);
    const normalizedLeftPriority = leftPriority === -1 ? preferredOrder.length : leftPriority;
    const normalizedRightPriority = rightPriority === -1 ? preferredOrder.length : rightPriority;

    return normalizedLeftPriority - normalizedRightPriority || left.name.localeCompare(right.name);
  });
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

function buildOrderedStateMatchers() {
  return stateEntriesByPriority.flatMap((state) => {
    const normalizedCode = normalizeLocationText(state.code);
    const normalizedName = normalizeLocationText(state.name);
    return [
      {
        alias: normalizedName,
        stateName: state.name,
        stateCode: state.code,
        allowContainedMatch: true,
      },
      {
        alias: normalizedCode,
        stateName: state.name,
        stateCode: state.code,
        allowContainedMatch: !ambiguousStateCodes.has(state.code),
      },
    ];
  });
}

function findContainedUsMetro(normalized: string) {
  for (const [alias, metro] of metroAliasEntries) {
    if (containsNormalizedTerm(normalized, alias)) {
      return metro;
    }
  }

  return undefined;
}

function findContainedUsStateName(normalized: string) {
  for (const matcher of orderedStateMatchers) {
    if (normalized === matcher.alias) {
      return matcher.stateName;
    }

    if (normalized.endsWith(` ${matcher.alias}`)) {
      return matcher.stateName;
    }

    if (matcher.allowContainedMatch && containsNormalizedTerm(normalized, matcher.alias)) {
      return matcher.stateName;
    }
  }

  return undefined;
}

function inferCityFromText(cleaned: string, normalized: string, stateName?: string) {
  if (!stateName) {
    return undefined;
  }

  const normalizedStateName = normalizeLocationText(stateName);
  const stateCode = usStateCodeByName.get(stateName);
  const normalizedStateCode = normalizeLocationText(stateCode);
  const normalizedCountryAliases = [...unitedStatesAliases, ...remoteUnitedStatesAliases];

  let working = normalized;

  normalizedCountryAliases.forEach((alias) => {
    if (containsNormalizedTerm(working, alias)) {
      working = removeNormalizedTerm(working, alias);
    }
  });

  if (normalizedStateName && working.endsWith(` ${normalizedStateName}`)) {
    working = working.slice(0, -(` ${normalizedStateName}`).length).trim();
  } else if (normalizedStateCode && working.endsWith(` ${normalizedStateCode}`)) {
    working = working.slice(0, -(` ${normalizedStateCode}`).length).trim();
  }

  working = removeNormalizedTerm(working, "remote");
  if (!working) {
    return undefined;
  }

  const parts = cleaned
    .split(/[,/|]| - /g)
    .map((part) => part.trim())
    .filter(Boolean);

  const directCityPart = parts.find((part) => {
    const normalizedPart = normalizeLocationText(part);
    return Boolean(normalizedPart) && !resolveUsState(normalizedPart) && !matchesUnitedStatesAlias(normalizedPart) && normalizedPart !== "remote";
  });

  if (directCityPart && normalizeLocationText(directCityPart) === working) {
    return directCityPart;
  }

  return titleCaseNormalizedText(working);
}

function matchesUnitedStatesAlias(normalized: string) {
  return [...unitedStatesAliases].some((alias) => containsNormalizedTerm(normalized, alias));
}

function matchesRemoteUnitedStatesAlias(normalized: string) {
  return [...remoteUnitedStatesAliases].some((alias) => containsNormalizedTerm(normalized, alias));
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

function finalizeDiscoveryLocationClauses(
  clauses: UsDiscoveryLocationClause[],
  maxClauses: number,
) {
  const seen = new Set<string>();
  const deduped: UsDiscoveryLocationClause[] = [];

  for (const clause of clauses) {
    const normalized = clause.clause === "" ? "" : normalizeLocationText(clause.clause);
    if (seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    deduped.push({
      ...clause,
      clause: normalized,
    });
  }

  return deduped.slice(0, maxClauses);
}

function dedupeDiscoveryLocationTokens(tokens: Array<UsDiscoveryLocationToken | undefined>) {
  const map = new Map<string, UsDiscoveryLocationToken>();

  tokens
    .filter((token): token is UsDiscoveryLocationToken => Boolean(token))
    .forEach((token) => {
      const normalizedValue = normalizeLocationText(token.value);
      const existing = map.get(normalizedValue);

      if (!existing || token.priority < existing.priority) {
        map.set(normalizedValue, {
          ...token,
          value: normalizedValue,
        });
      }
    });

  return Array.from(map.values());
}

function buildStateCodeDiscoveryClause(stateCode: string) {
  return normalizeLocationText(`${stateCode} usa`);
}

function formatMetroClause(city: string, stateCode: string) {
  return normalizeLocationText(`${city} ${stateCode}`);
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

function formatRemoteStateClause(stateName: string) {
  return normalizeLocationText(`remote ${formatStateClause(stateName)}`);
}

function formatStateRemoteClause(stateName: string) {
  return normalizeLocationText(`${formatStateClause(stateName)} remote`);
}

function containsNormalizedTerm(haystack: string, term: string) {
  return (
    haystack === term ||
    haystack.startsWith(`${term} `) ||
    haystack.endsWith(` ${term}`) ||
    haystack.includes(` ${term} `)
  );
}

function removeNormalizedTerm(haystack: string, term: string) {
  if (haystack === term) {
    return "";
  }

  return haystack
    .replace(new RegExp(`(^| )${escapeRegExp(term)}(?= |$)`, "g"), " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function titleCaseNormalizedText(value: string) {
  return value
    .split(" ")
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}
