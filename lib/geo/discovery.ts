import {
  cityAliases,
  countryAliases,
  findCityByAlias,
  findRegionByAlias,
  geoCatalog,
  regionAliases,
  type GeoCatalogCountry,
  type GeoCatalogRegion,
} from "@/lib/geo/catalog";
import type { GeoIntent } from "@/lib/geo/types";
import { uniqueStrings } from "@/lib/geo/normalize";

export function buildDiscoveryClausesFromGeoIntent(intent: GeoIntent, options: { maxClauses?: number; includeRemoteExpansion?: boolean } = {}) {
  const maxClauses = options.maxClauses ?? 32;
  const includeRemote = options.includeRemoteExpansion ?? true;
  const clauses: string[] = [];
  const push = (...values: Array<string | undefined>) =>
    clauses.push(...values.filter((value): value is string => typeof value === "string"));

  if (intent.scope === "none") {
    return [];
  }
  if (intent.scope === "global_remote") {
    return ["Remote", "Remote Worldwide", "Worldwide Remote"];
  }
  if (intent.isRemote && intent.country) {
    push(`Remote ${intent.country.name}`, `Remote in ${intent.country.name}`, `${intent.country.name} Remote`);
    return uniqueStrings(clauses).slice(0, maxClauses);
  }
  if (intent.city) {
    const cityMatch = findCityByAlias(intent.city.name, intent.country?.code, intent.region?.code)[0];
    if (cityMatch) {
      push(...cityAliases(cityMatch.country, cityMatch.city));
      const region = cityMatch.country.regions?.find((entry) => entry.code === cityMatch.city.regionCode);
      push(...buildRegionDiscoveryAliases(cityMatch.country, region));
      push(...countryAliases(cityMatch.country));
    } else {
      push(intent.city.name, intent.country ? `${intent.city.name} ${intent.country.name}` : undefined);
    }
    if (includeRemote && intent.country) push(`Remote ${intent.country.name}`, `Remote in ${intent.country.name}`);
    return uniqueStrings(clauses).slice(0, maxClauses);
  }
  if (intent.region) {
    const regionMatch = findRegionByAlias(intent.region.name, intent.country?.code)[0];
    if (regionMatch) {
      push(...buildCompactRegionDiscoveryAliases(regionMatch.country, regionMatch.region));
      for (const city of regionMatch.country.cities?.filter((entry) => entry.regionCode === regionMatch.region.code) ?? []) {
        push(...cityAliases(regionMatch.country, city));
      }
    } else {
      push(intent.region.name, intent.country ? `${intent.region.name} ${intent.country.name}` : undefined);
    }
    if (includeRemote && intent.country) push(`Remote ${intent.region.name}`, `Remote in ${intent.region.name}`, `${intent.region.name} Remote`);
    return uniqueStrings(clauses).slice(0, maxClauses);
  }
  if (intent.country) {
    const country = geoCatalog.find((entry) => entry.code === intent.country?.code || entry.name === intent.country?.name);
    const aliases = country ? countryAliases(country) : intent.country.aliases;
    const primaryAliases = country?.discoveryAliases?.length
      ? country.discoveryAliases
      : [intent.country.name, ...aliases.filter((alias) => ["usa", "uk"].includes(alias.toLowerCase()))];
    const remoteAliases = country?.remoteDiscoveryAliases?.length ? country.remoteDiscoveryAliases : primaryAliases;
    push("", ...primaryAliases);
    if (includeRemote) {
      remoteAliases.forEach((alias) => push(`Remote ${alias}`));
      push(`Remote in ${primaryAliases[0]}`, `${primaryAliases[0]} Remote`);
    }
    if (includeRemote) {
      country?.regions?.slice(0, 4).forEach((region) => push(`Remote ${region.discoveryAliases?.[0] ?? region.name}`));
    }
    country?.cities?.slice(0, 2).forEach((city) => push(...buildCompactCityDiscoveryAliases(country, city)));
    country?.regions?.slice(0, 4).forEach((region) => push(...buildCompactRegionDiscoveryAliases(country, region)));
    country?.cities?.slice(2, 12).forEach((city) => push(...buildCompactCityDiscoveryAliases(country, city)));
    country?.regions?.slice(4, 8).forEach((region) => push(...buildCompactRegionDiscoveryAliases(country, region)));
  }
  return uniqueWithBlank(clauses).slice(0, maxClauses).map((clause) => clause.toLowerCase());
}

function buildRegionDiscoveryAliases(country: GeoCatalogCountry, region?: GeoCatalogRegion) {
  if (!region) {
    return [];
  }
  return uniqueStrings([
    ...regionAliases(country, region),
    region.code ? `${region.code} ${country.code}` : undefined,
    ...countryAliases(country).flatMap((alias) => (region.code ? [`${region.code} ${alias}`] : [])),
  ]);
}

function buildCompactRegionDiscoveryAliases(country: GeoCatalogCountry, region: GeoCatalogRegion) {
  const countryAlias = country.discoveryAliases?.find((alias) => alias.length <= 6) ?? country.name;
  return uniqueStrings([
    region.discoveryAliases?.[0],
    region.name,
    region.code ? `${region.code} ${countryAlias}` : undefined,
    ...(region.aliases ?? []),
  ]);
}

function buildCompactCityDiscoveryAliases(country: GeoCatalogCountry, city: { name: string; regionCode?: string; regionName?: string; aliases?: string[] }) {
  return uniqueStrings([
    city.name,
    city.regionCode ? `${city.name} ${city.regionCode}` : undefined,
    city.regionName ? `${city.name} ${city.regionName}` : undefined,
    `${city.name} ${country.name}`,
    ...(city.aliases ?? []),
  ]);
}

function uniqueWithBlank(values: string[]) {
  const seen = new Set<string>();
  const output: string[] = [];
  values.forEach((value) => {
    const key = value.trim().toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    output.push(value.trim());
  });
  return output;
}
