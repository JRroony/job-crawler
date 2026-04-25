import type { GeoCatalogCity, GeoCatalogCountry, GeoCatalogRegion } from "@/lib/geo/catalog";
import type { GeoLocationPoint } from "@/lib/geo/types";
import { normalizeGeoText, uniqueStrings } from "@/lib/geo/normalize";

export function keyPart(value?: string) {
  return normalizeGeoText(value);
}

export function buildCountryKeys(country?: Pick<GeoCatalogCountry, "name" | "code"> | { name?: string; code?: string }) {
  const name = keyPart(country?.name);
  const code = keyPart(country?.code);
  return uniqueStrings([
    name ? `country:${name}` : undefined,
    code ? `country_code:${code}` : undefined,
  ]);
}

export function buildRegionKeys(country?: { name?: string }, region?: { name?: string; code?: string }) {
  const countryName = keyPart(country?.name);
  const regionName = keyPart(region?.name);
  const regionCode = keyPart(region?.code);
  return uniqueStrings([
    countryName && regionName ? `region:${countryName}:${regionName}` : undefined,
    countryName && regionCode ? `region_code:${countryName}:${regionCode}` : undefined,
  ]);
}

export function buildCityKeys(country?: { name?: string }, region?: { name?: string; code?: string }, city?: { name?: string }) {
  const countryName = keyPart(country?.name);
  const regionName = keyPart(region?.name);
  const cityName = keyPart(city?.name);
  return uniqueStrings([
    countryName && regionName && cityName ? `city:${countryName}:${regionName}:${cityName}` : undefined,
    countryName && cityName ? `city:${countryName}:${cityName}` : undefined,
  ]);
}

export function buildRemoteKeys(country?: { name?: string }, region?: { name?: string }, city?: { name?: string }) {
  const countryName = keyPart(country?.name);
  const regionName = keyPart(region?.name);
  const cityName = keyPart(city?.name);
  return uniqueStrings([
    !countryName ? "remote_global" : undefined,
    countryName ? `remote_country:${countryName}` : undefined,
    countryName && regionName ? `remote_region:${countryName}:${regionName}` : undefined,
    countryName && regionName && cityName ? `remote_city:${countryName}:${regionName}:${cityName}` : undefined,
    countryName && cityName ? `remote_city:${countryName}:${cityName}` : undefined,
  ]);
}

export function buildPointSearchKeys(point: GeoLocationPoint, remote = false) {
  const country = { name: point.country, code: point.countryCode };
  const region = { name: point.region, code: point.regionCode };
  const city = { name: point.city ?? "" };
  return uniqueStrings([
    ...buildCountryKeys(country),
    ...buildRegionKeys(country, region),
    ...buildCityKeys(country, region, city),
    ...(remote ? buildRemoteKeys(country, region, city) : []),
  ]);
}
