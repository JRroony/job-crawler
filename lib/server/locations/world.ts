import "server-only";

import type { SearchFilters } from "@/lib/types";
import { normalizeLocationText } from "@/lib/server/locations/us";

export type SupportedCountryConcept =
  | "canada"
  | "germany"
  | "israel"
  | "united kingdom";

type SupportedCountryRegion = {
  countryConcept: SupportedCountryConcept;
  name: string;
  code?: string;
  aliases: string[];
  priority: number;
};

type SupportedCountryMetro = {
  countryConcept: SupportedCountryConcept;
  city: string;
  regionName?: string;
  regionCode?: string;
  aliases: string[];
  priority: number;
};

type SupportedCountryDefinition = {
  concept: SupportedCountryConcept;
  canonicalName: string;
  aliases: string[];
  regions: SupportedCountryRegion[];
  metros: SupportedCountryMetro[];
};

export type CountryLocationIntent =
  | { kind: "none" }
  | { kind: "country"; countryConcept?: SupportedCountryConcept; countryName: string }
  | {
      kind: "country_region";
      countryConcept?: SupportedCountryConcept;
      countryName: string;
      regionName: string;
      regionCode?: string;
    }
  | {
      kind: "country_city";
      countryConcept?: SupportedCountryConcept;
      countryName: string;
      city: string;
      regionName?: string;
      regionCode?: string;
    };

export type CountryDiscoveryLocationClause = {
  clause: string;
  kind: "country" | "remote" | "state" | "remote_state" | "metro" | "blank";
  priority: number;
};

export type CountryLocationAnalysis = {
  normalized: string;
  isRemote: boolean;
  countryConcept?: SupportedCountryConcept;
  country?: string;
  state?: string;
  stateCode?: string;
  city?: string;
};

const supportedCountryDefinitions: SupportedCountryDefinition[] = [
  {
    concept: "canada",
    canonicalName: "Canada",
    aliases: ["canada", "ca canada"],
    regions: [
      {
        countryConcept: "canada",
        name: "Ontario",
        code: "ON",
        aliases: ["ontario", "on", "ontario canada", "on canada"],
        priority: 1,
      },
      {
        countryConcept: "canada",
        name: "British Columbia",
        code: "BC",
        aliases: [
          "british columbia",
          "bc",
          "british columbia canada",
          "bc canada",
        ],
        priority: 2,
      },
      {
        countryConcept: "canada",
        name: "Quebec",
        code: "QC",
        aliases: ["quebec", "qc", "quebec canada", "qc canada"],
        priority: 3,
      },
      {
        countryConcept: "canada",
        name: "Alberta",
        code: "AB",
        aliases: ["alberta", "ab", "alberta canada", "ab canada"],
        priority: 4,
      },
    ],
    metros: [
      {
        countryConcept: "canada",
        city: "Toronto",
        regionName: "Ontario",
        regionCode: "ON",
        aliases: ["toronto", "toronto on", "toronto ontario", "toronto canada"],
        priority: 1,
      },
      {
        countryConcept: "canada",
        city: "Vancouver",
        regionName: "British Columbia",
        regionCode: "BC",
        aliases: [
          "vancouver",
          "vancouver bc",
          "vancouver british columbia",
          "vancouver canada",
        ],
        priority: 2,
      },
      {
        countryConcept: "canada",
        city: "Montreal",
        regionName: "Quebec",
        regionCode: "QC",
        aliases: [
          "montreal",
          "montreal qc",
          "montreal quebec",
          "montreal canada",
        ],
        priority: 3,
      },
      {
        countryConcept: "canada",
        city: "Calgary",
        regionName: "Alberta",
        regionCode: "AB",
        aliases: ["calgary", "calgary ab", "calgary alberta", "calgary canada"],
        priority: 4,
      },
    ],
  },
  {
    concept: "germany",
    canonicalName: "Germany",
    aliases: ["germany", "deutschland"],
    regions: [
      {
        countryConcept: "germany",
        name: "Berlin",
        aliases: ["berlin", "berlin germany"],
        priority: 1,
      },
      {
        countryConcept: "germany",
        name: "Bavaria",
        code: "BY",
        aliases: ["bavaria", "bayern", "bavaria germany"],
        priority: 2,
      },
    ],
    metros: [
      {
        countryConcept: "germany",
        city: "Berlin",
        regionName: "Berlin",
        aliases: ["berlin", "berlin germany"],
        priority: 1,
      },
      {
        countryConcept: "germany",
        city: "Munich",
        regionName: "Bavaria",
        regionCode: "BY",
        aliases: ["munich", "munich bavaria", "munich germany", "munchen"],
        priority: 2,
      },
    ],
  },
  {
    concept: "israel",
    canonicalName: "Israel",
    aliases: ["israel", "il israel"],
    regions: [
      {
        countryConcept: "israel",
        name: "Tel Aviv District",
        aliases: ["tel aviv district", "tel aviv israel"],
        priority: 1,
      },
      {
        countryConcept: "israel",
        name: "Jerusalem District",
        aliases: ["jerusalem district", "jerusalem israel"],
        priority: 2,
      },
      {
        countryConcept: "israel",
        name: "Haifa District",
        aliases: ["haifa district", "haifa israel"],
        priority: 3,
      },
    ],
    metros: [
      {
        countryConcept: "israel",
        city: "Tel Aviv",
        regionName: "Tel Aviv District",
        aliases: ["tel aviv", "tel aviv yafo", "tel aviv israel"],
        priority: 1,
      },
      {
        countryConcept: "israel",
        city: "Jerusalem",
        regionName: "Jerusalem District",
        aliases: ["jerusalem", "jerusalem israel"],
        priority: 2,
      },
      {
        countryConcept: "israel",
        city: "Haifa",
        regionName: "Haifa District",
        aliases: ["haifa", "haifa israel"],
        priority: 3,
      },
    ],
  },
  {
    concept: "united kingdom",
    canonicalName: "United Kingdom",
    aliases: ["united kingdom", "uk", "great britain", "britain"],
    regions: [
      {
        countryConcept: "united kingdom",
        name: "England",
        aliases: ["england", "england uk", "england united kingdom"],
        priority: 1,
      },
      {
        countryConcept: "united kingdom",
        name: "Scotland",
        aliases: ["scotland", "scotland uk", "scotland united kingdom"],
        priority: 2,
      },
    ],
    metros: [
      {
        countryConcept: "united kingdom",
        city: "London",
        regionName: "England",
        aliases: ["london", "london england", "london uk", "london united kingdom"],
        priority: 1,
      },
      {
        countryConcept: "united kingdom",
        city: "Manchester",
        regionName: "England",
        aliases: [
          "manchester",
          "manchester england",
          "manchester uk",
          "manchester united kingdom",
        ],
        priority: 2,
      },
    ],
  },
];

const supportedCountriesByConcept = new Map(
  supportedCountryDefinitions.map((definition) => [definition.concept, definition] as const),
);

const countryAliasEntries = supportedCountryDefinitions
  .flatMap((definition) =>
    definition.aliases.map((alias) => [normalizeLocationText(alias), definition.concept] as const),
  )
  .sort((left, right) => right[0].length - left[0].length || left[0].localeCompare(right[0]));

const regionAliasEntries = supportedCountryDefinitions
  .flatMap((definition) =>
    definition.regions.flatMap((region) =>
      region.aliases.map((alias) => [normalizeLocationText(alias), region] as const),
    ),
  )
  .sort((left, right) => right[0].length - left[0].length || left[0].localeCompare(right[0]));

const metroAliasEntries = supportedCountryDefinitions
  .flatMap((definition) =>
    definition.metros.flatMap((metro) =>
      metro.aliases.map((alias) => [normalizeLocationText(alias), metro] as const),
    ),
  )
  .sort((left, right) => right[0].length - left[0].length || left[0].localeCompare(right[0]));

export function resolveSupportedCountryConcept(value?: string) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return countryAliasEntries.find(([alias]) => alias === normalized)?.[1];
}

export function findSupportedCountryConceptInText(value?: string) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return countryAliasEntries.find(([alias]) => containsNormalizedTerm(normalized, alias))?.[1];
}

export function getSupportedCountryCanonicalName(concept?: SupportedCountryConcept) {
  return concept ? supportedCountriesByConcept.get(concept)?.canonicalName : undefined;
}

export function getSupportedCountryAliases(concept?: SupportedCountryConcept) {
  const definition = concept ? supportedCountriesByConcept.get(concept) : undefined;
  return definition ? definition.aliases.map((alias) => normalizeLocationText(alias)) : [];
}

export function resolveSupportedCountryRegion(
  value?: string,
  countryConcept?: SupportedCountryConcept,
) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return regionAliasEntries.find(
    ([alias, region]) =>
      alias === normalized && (!countryConcept || region.countryConcept === countryConcept),
  )?.[1];
}

export function findSupportedCountryRegionInText(
  value?: string,
  countryConcept?: SupportedCountryConcept,
) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return regionAliasEntries.find(
    ([alias, region]) =>
      containsNormalizedTerm(normalized, alias) &&
      (!countryConcept || region.countryConcept === countryConcept),
  )?.[1];
}

export function resolveSupportedCountryMetro(
  value?: string,
  countryConcept?: SupportedCountryConcept,
) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return metroAliasEntries.find(
    ([alias, metro]) =>
      alias === normalized && (!countryConcept || metro.countryConcept === countryConcept),
  )?.[1];
}

export function findSupportedCountryMetroInText(
  value?: string,
  countryConcept?: SupportedCountryConcept,
) {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return undefined;
  }

  return metroAliasEntries.find(
    ([alias, metro]) =>
      containsNormalizedTerm(normalized, alias) &&
      (!countryConcept || metro.countryConcept === countryConcept),
  )?.[1];
}

export function analyzeSupportedCountryLocation(value?: string): CountryLocationAnalysis {
  const normalized = normalizeLocationText(value);
  if (!normalized) {
    return {
      normalized,
      isRemote: false,
    };
  }

  const metro = findSupportedCountryMetroInText(normalized);
  const region =
    findSupportedCountryRegionInText(normalized, metro?.countryConcept) ??
    (metro?.regionName
      ? resolveSupportedCountryRegion(metro.regionName, metro.countryConcept)
      : undefined);
  const countryConcept =
    metro?.countryConcept ??
    region?.countryConcept ??
    findSupportedCountryConceptInText(normalized);
  const country = getSupportedCountryCanonicalName(countryConcept);

  return {
    normalized,
    isRemote: containsNormalizedTerm(normalized, "remote"),
    countryConcept,
    country,
    state: region?.name ?? metro?.regionName,
    stateCode: region?.code ?? metro?.regionCode,
    city: metro?.city,
  };
}

export function planSupportedCountryLocationIntent(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
): CountryLocationIntent {
  const metro =
    resolveSupportedCountryMetro(filters.city) ??
    findSupportedCountryMetroInText(filters.city);
  const region =
    resolveSupportedCountryRegion(filters.state, metro?.countryConcept) ??
    findSupportedCountryRegionInText(filters.state, metro?.countryConcept) ??
    resolveSupportedCountryRegion(filters.city, metro?.countryConcept) ??
    findSupportedCountryRegionInText(filters.city, metro?.countryConcept);
  const countryConcept =
    resolveSupportedCountryConcept(filters.country) ??
    findSupportedCountryConceptInText(filters.country) ??
    metro?.countryConcept ??
    region?.countryConcept;
  const countryName =
    getSupportedCountryCanonicalName(countryConcept) ?? filters.country?.trim();

  if (!countryName) {
    return { kind: "none" };
  }

  if (filters.city) {
    return {
      kind: "country_city",
      countryConcept,
      countryName,
      city: metro?.city ?? filters.city.trim(),
      regionName: metro?.regionName ?? region?.name,
      regionCode: metro?.regionCode ?? region?.code,
    };
  }

  if (region) {
    return {
      kind: "country_region",
      countryConcept,
      countryName,
      regionName: region.name,
      regionCode: region.code,
    };
  }

  return {
    kind: "country",
    countryConcept,
    countryName,
  };
}

export function buildSupportedCountryDiscoveryLocationClauses(
  filters: Pick<SearchFilters, "country" | "state" | "city">,
  options: { maxClauses?: number } = {},
) {
  const intent = planSupportedCountryLocationIntent(filters);
  const maxClauses = options.maxClauses ?? 24;

  if (intent.kind === "none") {
    return {
      intent,
      clauses: [] as string[],
      detailedClauses: [] as CountryDiscoveryLocationClause[],
    };
  }

  const detailedClauses =
    intent.kind === "country_city"
      ? buildCityClauses(intent)
      : intent.kind === "country_region"
        ? buildRegionClauses(intent)
        : buildCountryClauses(intent);

  const finalized = finalizeClauses(
    [{ clause: "", kind: "blank", priority: 0 }, ...detailedClauses],
    maxClauses,
  );

  return {
    intent,
    clauses: finalized.map((entry) => entry.clause),
    detailedClauses: finalized,
  };
}

function buildCountryClauses(
  intent: Extract<CountryLocationIntent, { kind: "country" }>,
): CountryDiscoveryLocationClause[] {
  const aliases = intent.countryConcept
    ? getSupportedCountryAliases(intent.countryConcept)
    : [normalizeLocationText(intent.countryName)];
  const definition = intent.countryConcept
    ? supportedCountriesByConcept.get(intent.countryConcept)
    : undefined;
  const topRegions = (definition?.regions ?? [])
    .sort((left, right) => left.priority - right.priority)
    .slice(0, 4);
  const topMetros = (definition?.metros ?? [])
    .sort((left, right) => left.priority - right.priority)
    .slice(0, 4);

  return dedupeClauses([
    ...aliases.map((alias, index) => ({
      clause: alias,
      kind: "country" as const,
      priority: 1 + index,
    })),
    ...aliases.flatMap((alias, index) => [
      {
        clause: `remote ${alias}`,
        kind: "remote" as const,
        priority: 10 + index * 2,
      },
      {
        clause: `${alias} remote`,
        kind: "remote" as const,
        priority: 11 + index * 2,
      },
    ]),
    ...topRegions.flatMap((region, index) => [
      {
        clause: normalizeLocationText(region.name),
        kind: "state" as const,
        priority: regionPriority(index, 0),
      },
      region.code
        ? {
            clause: normalizeLocationText(`${region.code} ${intent.countryName}`),
            kind: "state" as const,
            priority: regionPriority(index, 1),
          }
        : undefined,
      {
        clause: normalizeLocationText(`remote ${region.name}`),
        kind: "remote_state" as const,
        priority: regionPriority(index, 2),
      },
    ]),
    ...topMetros.flatMap((metro, index) => [
      {
        clause: normalizeLocationText(metro.city),
        kind: "metro" as const,
        priority: 60 + index * 3,
      },
      metro.regionCode
        ? {
            clause: normalizeLocationText(`${metro.city} ${metro.regionCode}`),
            kind: "metro" as const,
            priority: 61 + index * 3,
          }
        : undefined,
      metro.regionName
        ? {
            clause: normalizeLocationText(`${metro.city} ${metro.regionName}`),
            kind: "metro" as const,
            priority: 62 + index * 3,
          }
        : undefined,
    ]),
  ]);
}

function regionPriority(index: number, offset: number) {
  return index < 3 ? 30 + index * 3 + offset : 80 + index * 3 + offset;
}

function buildRegionClauses(
  intent: Extract<CountryLocationIntent, { kind: "country_region" }>,
): CountryDiscoveryLocationClause[] {
  const baseCountryAliases = intent.countryConcept
    ? getSupportedCountryAliases(intent.countryConcept)
    : [normalizeLocationText(intent.countryName)];
  const definition = intent.countryConcept
    ? supportedCountriesByConcept.get(intent.countryConcept)
    : undefined;
  const metros = (definition?.metros ?? [])
    .filter((metro) => metro.regionName === intent.regionName)
    .sort((left, right) => left.priority - right.priority)
    .slice(0, 3);

  return dedupeClauses([
    {
      clause: normalizeLocationText(intent.regionName),
      kind: "state",
      priority: 1,
    },
    intent.regionCode
      ? {
          clause: normalizeLocationText(`${intent.regionCode} ${intent.countryName}`),
          kind: "state",
          priority: 2,
        }
      : undefined,
    {
      clause: normalizeLocationText(`remote ${intent.regionName}`),
      kind: "remote_state",
      priority: 3,
    },
    {
      clause: normalizeLocationText(`${intent.regionName} remote`),
      kind: "remote_state",
      priority: 4,
    },
    ...baseCountryAliases.slice(0, 2).map((alias, index) => ({
      clause: alias,
      kind: "country" as const,
      priority: 10 + index,
    })),
    ...metros.flatMap((metro, index) => [
      {
        clause: normalizeLocationText(metro.city),
        kind: "metro" as const,
        priority: 20 + index * 3,
      },
      metro.regionCode
        ? {
            clause: normalizeLocationText(`${metro.city} ${metro.regionCode}`),
            kind: "metro" as const,
            priority: 21 + index * 3,
          }
        : undefined,
      {
        clause: normalizeLocationText(`${metro.city} ${intent.regionName}`),
        kind: "metro" as const,
        priority: 22 + index * 3,
      },
    ]),
  ]);
}

function buildCityClauses(
  intent: Extract<CountryLocationIntent, { kind: "country_city" }>,
): CountryDiscoveryLocationClause[] {
  const countryAliases = intent.countryConcept
    ? getSupportedCountryAliases(intent.countryConcept)
    : [normalizeLocationText(intent.countryName)];

  return dedupeClauses([
    {
      clause: normalizeLocationText(intent.city),
      kind: "metro",
      priority: 1,
    },
    intent.regionCode
      ? {
          clause: normalizeLocationText(`${intent.city} ${intent.regionCode}`),
          kind: "metro",
          priority: 2,
        }
      : undefined,
    intent.regionName
      ? {
          clause: normalizeLocationText(`${intent.city} ${intent.regionName}`),
          kind: "metro",
          priority: 3,
        }
      : undefined,
    intent.regionName
      ? {
          clause: normalizeLocationText(intent.regionName),
          kind: "state",
          priority: 4,
        }
      : undefined,
    ...countryAliases.slice(0, 2).map((alias, index) => ({
      clause: alias,
      kind: "country" as const,
      priority: 8 + index,
    })),
  ]);
}

function finalizeClauses(
  clauses: CountryDiscoveryLocationClause[],
  maxClauses: number,
) {
  return dedupeClauses(clauses).slice(0, maxClauses);
}

function dedupeClauses(
  clauses: Array<CountryDiscoveryLocationClause | undefined>,
) {
  const seen = new Set<string>();
  const deduped: CountryDiscoveryLocationClause[] = [];

  clauses
    .filter((clause): clause is CountryDiscoveryLocationClause => Boolean(clause))
    .sort((left, right) => left.priority - right.priority || left.clause.localeCompare(right.clause))
    .forEach((clause) => {
      const normalizedClause = clause.clause === "" ? "" : normalizeLocationText(clause.clause);
      if (seen.has(normalizedClause)) {
        return;
      }

      seen.add(normalizedClause);
      deduped.push({
        ...clause,
        clause: normalizedClause,
      });
    });

  return deduped;
}

function containsNormalizedTerm(haystack: string, term: string) {
  return (
    haystack === term ||
    haystack.startsWith(`${term} `) ||
    haystack.endsWith(` ${term}`) ||
    haystack.includes(` ${term} `)
  );
}
