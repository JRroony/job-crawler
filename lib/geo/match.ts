import { parseGeoIntent } from "@/lib/geo/parse";
import { normalizeJobGeoLocation } from "@/lib/geo/location";
import { normalizeGeoText } from "@/lib/geo/normalize";
import type { GeoIntent, GeoLocation, GeoLocationMatchResult, GeoLocationPoint } from "@/lib/geo/types";

export type GeoMatchOptions = {
  includeCountryRemoteForCity?: boolean;
  includeCountryRemoteForRegion?: boolean;
  includePhysicalForRemoteCountry?: boolean;
};

export function matchJobLocationAgainstGeoIntent(
  geoLocation: GeoLocation,
  intent: GeoIntent,
  options: GeoMatchOptions = {},
): GeoLocationMatchResult {
  if (intent.scope === "none") {
    return { matches: true, explanation: "No location intent was requested.", matchedKeys: [], intent };
  }
  if (intent.scope === "global_remote") {
    const matched = geoLocation.isGlobalRemote || geoLocation.remoteEligibility.length > 0 || geoLocation.workplaceType === "remote";
    return { matches: matched, explanation: matched ? "Matched global remote/remote job." : "Global remote intent excludes non-remote job.", matchedKeys: geoLocation.searchKeys.filter((key) => key.startsWith("remote_")), intent };
  }

  if (intent.scope === "ambiguous" || intent.confidence === "low") {
    const phrase = normalizeGeoText(intent.city?.name ?? intent.normalizedInput);
    const matched = Boolean(phrase) && containsExactPhrase(geoLocation.normalizedText, phrase);
    return { matches: matched, explanation: matched ? `Matched exact fallback location phrase "${phrase}".` : `Ambiguous/low-confidence location did not match exact phrase "${phrase}".`, matchedKeys: matched ? [`text:${phrase}`] : [], intent };
  }

  if (intent.isRemote) {
    const remoteMatches =
      intent.city
        ? pointMatchesCity(geoLocation.remoteEligibility, intent.city.name, intent.region?.name, intent.country?.name)
        : intent.region
          ? pointMatchesRegion(geoLocation.remoteEligibility, intent.region.name, intent.country?.name)
          : intent.country
            ? pointMatchesCountry(geoLocation.remoteEligibility, intent.country.name)
            : false;
    if (remoteMatches) {
      return { matches: true, explanation: intent.country ? `Matched remote eligibility for ${intent.country.name}.` : `Matched explicit remote eligibility for "${intent.rawInput}".`, matchedKeys: intent.searchKeys, intent };
    }
    if (intent.country && options.includePhysicalForRemoteCountry && pointMatchesCountry(geoLocation.physicalLocations, intent.country.name)) {
      return { matches: true, explanation: `Matched physical location in ${intent.country.name} under remote-country policy.`, matchedKeys: [], intent };
    }
    return { matches: false, explanation: `Remote intent "${intent.rawInput}" requires a remote role with explicit matching eligibility.`, matchedKeys: [], intent };
  }

  if (intent.city) {
    if (pointMatchesCity(geoLocation.physicalLocations, intent.city.name, intent.region?.name, intent.country?.name)) {
      return { matches: true, explanation: `Matched physical city ${intent.city.name}.`, matchedKeys: intent.searchKeys, intent };
    }
    return { matches: false, explanation: `City intent "${intent.rawInput}" did not match the job city.`, matchedKeys: [], intent };
  }
  if (intent.region) {
    if (pointMatchesRegion(geoLocation.physicalLocations, intent.region.name, intent.country?.name)) {
      return { matches: true, explanation: `Matched physical region ${intent.region.name}.`, matchedKeys: intent.searchKeys, intent };
    }
    return { matches: false, explanation: `Region intent "${intent.rawInput}" did not match the job region.`, matchedKeys: [], intent };
  }
  if (intent.country && pointMatchesCountry(geoLocation.physicalLocations, intent.country.name)) {
    return { matches: true, explanation: `Matched physical job location in ${intent.country.name}.`, matchedKeys: intent.searchKeys, intent };
  }
  if (intent.country && geoLocation.physicalLocations.length === 0 && pointMatchesCountry(geoLocation.remoteEligibility, intent.country.name)) {
    return { matches: true, explanation: `Matched explicit remote eligibility for ${intent.country.name}.`, matchedKeys: intent.searchKeys, intent };
  }

  const hasRemoteEligibilityForCountry =
    Boolean(intent.country) && pointMatchesCountry(geoLocation.remoteEligibility, intent.country?.name ?? "");
  return {
    matches: false,
    explanation: hasRemoteEligibilityForCountry
      ? `Job location "${geoLocation.rawText}" has remote eligibility for ${intent.country?.name}, but matching it requires an explicit remote country filter because physical evidence points elsewhere.`
      : `Job location "${geoLocation.rawText}" does not satisfy ${intent.scope} intent "${intent.rawInput}".`,
    matchedKeys: [],
    intent,
  };
}

export function matchJobAgainstLocationInput(
  job: Parameters<typeof normalizeJobGeoLocation>[0] & { geoLocation?: GeoLocation },
  rawLocation?: string,
  options?: GeoMatchOptions,
) {
  return matchJobLocationAgainstGeoIntent(
    job.geoLocation ?? normalizeJobGeoLocation(job),
    parseGeoIntent(rawLocation),
    options,
  );
}

function pointMatchesCountry(points: GeoLocationPoint[], country: string) {
  const normalized = normalizeGeoText(country);
  return points.some((point) => normalizeGeoText(point.country) === normalized);
}

function pointMatchesRegion(points: GeoLocationPoint[], region: string, country?: string) {
  const normalizedRegion = normalizeGeoText(region);
  const normalizedCountry = normalizeGeoText(country);
  return points.some((point) =>
    normalizeGeoText(point.region) === normalizedRegion &&
    (!normalizedCountry || normalizeGeoText(point.country) === normalizedCountry),
  );
}

function pointMatchesCity(points: GeoLocationPoint[], city: string, region?: string, country?: string) {
  const normalizedCity = normalizeGeoText(city);
  const normalizedRegion = normalizeGeoText(region);
  const normalizedCountry = normalizeGeoText(country);
  return points.some((point) =>
    normalizeGeoText(point.city) === normalizedCity &&
    (!normalizedRegion || normalizeGeoText(point.region) === normalizedRegion) &&
    (!normalizedCountry || normalizeGeoText(point.country) === normalizedCountry),
  );
}

function containsExactPhrase(haystack: string, phrase: string) {
  return haystack === phrase || haystack.startsWith(`${phrase} `) || haystack.endsWith(` ${phrase}`) || haystack.includes(` ${phrase} `);
}
