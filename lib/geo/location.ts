import { findCityByAlias, findCountryByAlias, findRegionByAlias } from "@/lib/geo/catalog";
import { buildPointSearchKeys } from "@/lib/geo/keys";
import { normalizeGeoText, uniqueStrings } from "@/lib/geo/normalize";
import type { GeoLocation, GeoLocationPoint } from "@/lib/geo/types";

const remotePattern = /\b(remote|remotely|work from home|worldwide|global)\b/i;
const hybridPattern = /\b(hybrid)\b/i;
const onsitePattern = /\b(onsite|on site|in office|office based)\b/i;

export function normalizeJobGeoLocation(input: {
  country?: string;
  state?: string;
  city?: string;
  locationText?: string;
  locationRaw?: string;
  normalizedLocation?: string;
  locationNormalized?: string;
  resolvedLocation?: {
    country?: string;
    state?: string;
    stateCode?: string;
    city?: string;
    isRemote?: boolean;
    physicalLocations?: Array<{ country?: string; state?: string; stateCode?: string; city?: string; confidence?: string }>;
    eligibilityCountries?: string[];
  };
  rawSourceMetadata?: Record<string, unknown>;
}): GeoLocation {
  const rawText = input.locationText || input.locationRaw || [input.city, input.state, input.country].filter(Boolean).join(", ") || "Location unavailable";
  const normalizedText = normalizeGeoText([rawText, input.normalizedLocation, input.locationNormalized].filter(Boolean).join(" "));
  const workplaceType = input.resolvedLocation?.isRemote || remotePattern.test(rawText)
    ? "remote"
    : hybridPattern.test(rawText)
      ? "hybrid"
      : onsitePattern.test(rawText)
        ? "onsite"
        : "unknown";
  const isGlobalRemote = workplaceType === "remote" && /\b(worldwide|global|anywhere)\b/i.test(rawText);
  const physicalLocations: GeoLocationPoint[] = [];
  const remoteEligibility: GeoLocationPoint[] = [];
  const addPoint = (target: GeoLocationPoint[], point: Omit<GeoLocationPoint, "searchKeys" | "confidence" | "evidence"> & { confidence?: "high" | "medium" | "low"; evidence?: string[] }) => {
    if (!point.country && !point.region && !point.city) return;
    const normalizedPoint = resolvePoint(point);
    const finalPoint: GeoLocationPoint = {
      ...normalizedPoint,
      confidence: point.confidence ?? (normalizedPoint.country ? "high" : "low"),
      evidence: point.evidence ?? [rawText],
      searchKeys: [],
    };
    finalPoint.searchKeys = buildPointSearchKeys(finalPoint, target === remoteEligibility);
    const key = finalPoint.searchKeys.join("|") || normalizeGeoText([finalPoint.city, finalPoint.region, finalPoint.country].filter(Boolean).join(" "));
    if (!target.some((existing) => (existing.searchKeys.join("|") || normalizeGeoText(existing.city)) === key)) {
      target.push(finalPoint);
    }
  };

  addPoint(physicalLocations, {
    country: input.country ?? input.resolvedLocation?.country,
    region: input.state ?? input.resolvedLocation?.state,
    regionCode: input.resolvedLocation?.stateCode,
    city: input.city ?? input.resolvedLocation?.city,
    confidence: input.country || input.resolvedLocation?.country ? "high" : "low",
    evidence: ["structured_fields"],
  });
  for (const point of input.resolvedLocation?.physicalLocations ?? []) {
    addPoint(physicalLocations, {
      country: point.country,
      region: point.state,
      regionCode: point.stateCode,
      city: point.city,
      confidence: point.confidence === "high" || point.confidence === "medium" ? point.confidence : "low",
      evidence: ["resolvedLocation.physicalLocations"],
    });
  }
  for (const country of input.resolvedLocation?.eligibilityCountries ?? []) {
    addPoint(remoteEligibility, { country, confidence: "high", evidence: ["resolvedLocation.eligibilityCountries"] });
  }
  const textPoint = resolveTextPoint(rawText);
  if (textPoint) {
    addPoint(workplaceType === "remote" ? remoteEligibility : physicalLocations, { ...textPoint, confidence: "high", evidence: ["locationText"] });
  }
  if (workplaceType === "remote" && input.resolvedLocation?.country) {
    addPoint(remoteEligibility, { country: input.resolvedLocation.country, confidence: "high", evidence: ["resolvedLocation.remote"] });
  }

  const searchKeys = uniqueStrings([
    ...physicalLocations.flatMap((point) => point.searchKeys),
    ...remoteEligibility.flatMap((point) => point.searchKeys),
    ...(isGlobalRemote ? ["remote_global"] : []),
  ]);

  return {
    rawText,
    normalizedText,
    physicalLocations,
    remoteEligibility,
    workplaceType,
    isGlobalRemote,
    unresolvedTerms: searchKeys.length ? [] : [normalizedText].filter(Boolean),
    conflicts: collectConflicts(physicalLocations, remoteEligibility),
    searchKeys,
  };
}

function resolveTextPoint(value: string) {
  const strippedValue = value.replace(/\b(remote|remotely|work from home|in|within|only|onsite|on site|hybrid)\b/gi, " ");
  const parts = strippedValue.split(/[,/|;]|\s+-\s+/g).map((part) => part.trim()).filter(Boolean);
  const country = [...parts].reverse().map(findCountryByAlias).find(Boolean) ?? findCountryByAlias(strippedValue);
  const region = parts.flatMap((part) => findRegionByAlias(part, country?.code))[0];
  const city = parts.flatMap((part) => findCityByAlias(part, country?.code, region?.region.code))[0] ?? findCityByAlias(strippedValue, country?.code, region?.region.code)[0];
  if (city) {
    return {
      country: city.country.name,
      countryCode: city.country.code,
      region: city.city.regionName,
      regionCode: city.city.regionCode,
      city: city.city.name,
    };
  }
  if (region) {
    return { country: region.country.name, countryCode: region.country.code, region: region.region.name, regionCode: region.region.code };
  }
  if (country) {
    return { country: country.name, countryCode: country.code };
  }
  return undefined;
}

function resolvePoint(point: Omit<GeoLocationPoint, "searchKeys" | "confidence" | "evidence">) {
  const country = findCountryByAlias(point.country) ?? (point.countryCode ? findCountryByAlias(point.countryCode) : undefined);
  const region = findRegionByAlias(point.region ?? point.regionCode, country?.code)[0];
  const city = findCityByAlias(point.city, country?.code, region?.region.code)[0];
  return {
    country: city?.country.name ?? region?.country.name ?? country?.name ?? point.country,
    countryCode: city?.country.code ?? region?.country.code ?? country?.code ?? point.countryCode,
    region: city?.city.regionName ?? region?.region.name ?? point.region,
    regionCode: city?.city.regionCode ?? region?.region.code ?? point.regionCode,
    city: city?.city.name ?? point.city,
  };
}

function collectConflicts(physical: GeoLocationPoint[], remote: GeoLocationPoint[]) {
  const physicalCountries = uniqueStrings(physical.map((point) => point.country));
  const remoteCountries = uniqueStrings(remote.map((point) => point.country));
  return physicalCountries.length > 1
    ? [`Multiple physical countries: ${physicalCountries.join(", ")}`]
    : physicalCountries.length && remoteCountries.some((country) => country && !physicalCountries.includes(country))
      ? [`Physical and remote eligibility countries differ: ${uniqueStrings([...physicalCountries, ...remoteCountries]).join(", ")}`]
      : [];
}
