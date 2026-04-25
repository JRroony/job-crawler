import { cityAliases, countryAliases, findCityByAlias, findCountryByAlias, findRegionByAlias, regionAliases } from "@/lib/geo/catalog";
import { buildCityKeys, buildCountryKeys, buildRegionKeys, buildRemoteKeys } from "@/lib/geo/keys";
import { buildDiscoveryClausesFromGeoIntent } from "@/lib/geo/discovery";
import { normalizeGeoText, toTitleCase, uniqueStrings } from "@/lib/geo/normalize";
import type { GeoIntent } from "@/lib/geo/types";

const remotePattern = /\b(remote|remotely|work from home|worldwide|global)\b/i;

export function parseGeoIntentFromFilters(filters: { country?: string; state?: string; city?: string }) {
  const raw = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
  return parseGeoIntent(raw);
}

export function parseGeoIntent(rawInput?: string): GeoIntent {
  const raw = rawInput?.trim() ?? "";
  const normalizedInput = normalizeGeoText(raw);
  const base = (intent: Omit<GeoIntent, "discoveryClauses">): GeoIntent => {
    const withClauses = { ...intent, discoveryClauses: [] };
    return { ...withClauses, discoveryClauses: buildDiscoveryClausesFromGeoIntent(withClauses) };
  };
  if (!normalizedInput) {
    return base({ rawInput: raw, normalizedInput, scope: "none", isRemote: false, isCountryWide: false, confidence: "high", searchKeys: [] });
  }

  const isRemote = remotePattern.test(raw);
  const stripped = normalizeGeoText(raw.replace(/\b(remote|remotely|work from home|in|within|only)\b/gi, " "));
  if (isRemote && (!stripped || stripped === "global" || stripped === "worldwide")) {
    return base({ rawInput: raw, normalizedInput, scope: "global_remote", isRemote: true, isCountryWide: false, confidence: "high", searchKeys: ["remote_global"] });
  }

  const parts = raw.split(/[,/|;]|\s+-\s+/g).map((part) => part.trim()).filter(Boolean);
  const countryFromPart = [...parts].reverse().map(findCountryByAlias).find(Boolean);
  const country = countryFromPart ?? findCountryByAlias(stripped || raw);
  const regionMatches = parts.flatMap((part) => findRegionByAlias(part, country?.code));
  const region = regionMatches[0];
  const cityMatches = parts.flatMap((part) => findCityByAlias(part, country?.code, region?.region.code));
  const directCityMatches = findCityByAlias(stripped || raw, country?.code, region?.region.code);
  const cities = cityMatches.length ? cityMatches : directCityMatches;

  if (cities.length > 1 && !country && !region) {
    return base({
      rawInput: raw,
      normalizedInput,
      scope: "ambiguous",
      isRemote,
      isCountryWide: false,
      confidence: "ambiguous",
      ambiguityReason: `Location "${raw}" matches multiple catalog cities.`,
      searchKeys: cities.flatMap(({ country: cityCountry, city }) => buildCityKeys({ name: cityCountry.name }, { name: city.regionName, code: city.regionCode }, city)),
    });
  }

  const city = cities[0];
  if (city) {
    const geoCountry = city.country;
    const geoRegion = geoCountry.regions?.find((candidate) =>
      candidate.code === city.city.regionCode || candidate.name === city.city.regionName,
    );
    const searchKeys = uniqueStrings([
      ...buildCityKeys(geoCountry, geoRegion, city.city),
      ...(isRemote ? buildRemoteKeys(geoCountry, geoRegion, city.city) : []),
    ]);
    return base({
      rawInput: raw,
      normalizedInput,
      scope: isRemote ? "remote_city" : geoRegion ? "city_region" : "city_country",
      country: { code: geoCountry.code, name: geoCountry.name, aliases: countryAliases(geoCountry) },
      ...(geoRegion ? { region: { code: geoRegion.code, name: geoRegion.name, aliases: regionAliases(geoCountry, geoRegion) } } : {}),
      city: { name: city.city.name, aliases: cityAliases(geoCountry, city.city) },
      isRemote,
      isCountryWide: false,
      confidence: "high",
      searchKeys,
    });
  }

  if (region) {
    const countryAliasSet = new Set(countryAliases(region.country).map(normalizeGeoText));
    const requestedCity = parts.find((part) =>
      normalizeGeoText(part) !== normalizeGeoText(region.region.name) &&
      normalizeGeoText(part) !== normalizeGeoText(region.region.code) &&
      !countryAliasSet.has(normalizeGeoText(part)),
    );
    if (requestedCity && parts.length > 1) {
      return base({
        rawInput: raw,
        normalizedInput,
        scope: isRemote ? "remote_city" : "city_region",
        country: { code: region.country.code, name: region.country.name, aliases: countryAliases(region.country) },
        region: { code: region.region.code, name: region.region.name, aliases: regionAliases(region.country, region.region) },
        city: { name: toTitleCase(requestedCity), aliases: [requestedCity] },
        isRemote,
        isCountryWide: false,
        confidence: "low",
        ambiguityReason: "City and region combination was not found in the geo catalog; requiring the requested city text.",
        searchKeys: [`text:${normalizeGeoText(`${requestedCity} ${region.region.name}`)}`],
      });
    }
    const searchKeys = uniqueStrings([
      ...buildRegionKeys(region.country, region.region),
      ...(isRemote ? buildRemoteKeys(region.country, region.region) : []),
    ]);
    return base({
      rawInput: raw,
      normalizedInput,
      scope: isRemote ? "remote_region" : "region",
      country: { code: region.country.code, name: region.country.name, aliases: countryAliases(region.country) },
      region: { code: region.region.code, name: region.region.name, aliases: regionAliases(region.country, region.region) },
      isRemote,
      isCountryWide: false,
      confidence: "high",
      searchKeys,
    });
  }

  if (country) {
    const searchKeys = uniqueStrings(isRemote ? buildRemoteKeys(country) : buildCountryKeys(country));
    return base({
      rawInput: raw,
      normalizedInput,
      scope: isRemote ? "remote_country" : "country",
      country: { code: country.code, name: country.name, aliases: countryAliases(country) },
      isRemote,
      isCountryWide: true,
      confidence: "high",
      searchKeys,
    });
  }

  return base({
    rawInput: raw,
    normalizedInput,
    scope: isRemote ? "ambiguous" : "city",
    city: { name: toTitleCase(stripped || normalizedInput), aliases: [stripped || normalizedInput] },
    isRemote,
    isCountryWide: false,
    confidence: "low",
    ambiguityReason: "Location was not found in the geo catalog; using exact normalized text fallback only.",
    searchKeys: [`text:${stripped || normalizedInput}`],
  });
}
