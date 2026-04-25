import "server-only";

import type {
  ResolvedLocation,
  ResolvedLocationConfidence,
  ResolvedLocationEvidence,
  ResolvedLocationEvidenceSource,
} from "@/lib/types";
import {
  analyzeUsLocation,
  isUnitedStatesValue,
  normalizeLocationText,
  resolveUsState,
  resolveUsStateCode,
} from "@/lib/server/locations/us";
import {
  analyzeSupportedCountryLocation,
  getSupportedCountryCanonicalName,
  resolveSupportedCountryConcept,
} from "@/lib/server/locations/world";

type ResolveJobLocationInput = {
  country?: string;
  state?: string;
  city?: string;
  locationText?: string;
  rawSourceMetadata?: Record<string, unknown>;
};

type LocationCandidate = {
  source: ResolvedLocationEvidenceSource;
  value: string;
  priority: number;
};

type ResolvedCandidate = {
  candidate: LocationCandidate;
  location: ResolvedLocation;
  score: number;
  role: "physical" | "eligibility";
};
type ResolvedLocationPoint = NonNullable<ResolvedLocation["physicalLocations"]>[number];
type ResolvedLocationConflict = NonNullable<ResolvedLocation["conflicts"]>[number];

const directLocationKeys = [
  "location",
  "locationName",
  "locationText",
  "jobLocationText",
  "formattedLocation",
  "formattedAddress",
  "address",
  "allLocations",
  "workplaceType",
] as const;

const cityKeys = ["city", "addressLocality", "locality"] as const;
const stateKeys = ["state", "stateName", "region", "addressRegion"] as const;
const countryKeys = ["country", "countryName", "addressCountry"] as const;

const locationPathPattern =
  /(location|address|city|state|country|region|office|workplace|remote)/i;
const officePathPattern = /(office|offices)/i;
const descriptionPathPattern =
  /(description|summary|overview|requirements|qualifications|content)/i;
const descriptionLocationHintPattern =
  /(remote|located|location|based|reside|eligible|must be in|within)/i;

export function resolveJobLocation(input: ResolveJobLocationInput): ResolvedLocation {
  const candidates = collectLocationCandidates(input);
  const resolvedCandidates = candidates
    .map(resolveCandidate)
    .filter((candidate): candidate is ResolvedCandidate => Boolean(candidate))
    .sort((left, right) => right.score - left.score || right.candidate.priority - left.candidate.priority);
  const bestCandidate = resolvedCandidates[0];

  if (bestCandidate) {
    const physicalLocations = collectResolvedLocationPoints(
      resolvedCandidates.filter((candidate) => candidate.role === "physical"),
    );
    const eligibilityCountries = collectEligibilityCountries(resolvedCandidates);
    const primaryPoint = physicalLocations[0] ?? locationToPoint(bestCandidate.location);
    const conflicts = collectLocationConflicts(physicalLocations, eligibilityCountries);

    return compactResolvedLocation({
      ...bestCandidate.location,
      country: primaryPoint?.country ?? bestCandidate.location.country,
      state: primaryPoint?.state ?? bestCandidate.location.state,
      stateCode: primaryPoint?.stateCode ?? bestCandidate.location.stateCode,
      city: primaryPoint?.city ?? bestCandidate.location.city,
      isRemote: resolvedCandidates.some((candidate) => candidate.location.isRemote),
      isUnitedStates: primaryPoint?.country === "United States",
      confidence: primaryPoint?.confidence ?? bestCandidate.location.confidence,
      evidence: collectSupportingEvidence(resolvedCandidates),
      physicalLocations,
      eligibilityCountries,
      conflicts,
    });
  }

  const normalizedCountry = normalizeCountryValue(input.country);
  const normalizedState = normalizeStateValue(input.state);
  const normalizedCity = normalizeFreeformValue(input.city);
  const isRemote = candidates.some((candidate) =>
    normalizeLocationText(candidate.value).includes("remote"),
  );
  const fallbackEvidence = collectSupportingEvidence(resolvedCandidates);

  return compactResolvedLocation({
    country: normalizedCountry,
    state: normalizedState,
    stateCode: normalizedState ? resolveUsStateCode(normalizedState) : undefined,
    city: normalizedCity,
    isRemote,
    isUnitedStates: normalizedCountry === "United States",
    confidence: normalizedCountry || normalizedState || normalizedCity ? "low" : "none",
    evidence: fallbackEvidence,
    physicalLocations: normalizedCountry
      ? [
          {
            country: normalizedCountry,
            ...(normalizedState ? { state: normalizedState } : {}),
            ...(normalizedState && resolveUsStateCode(normalizedState)
              ? { stateCode: resolveUsStateCode(normalizedState) }
              : {}),
            ...(normalizedCity ? { city: normalizedCity } : {}),
            confidence: "low",
            evidence: fallbackEvidence,
          },
        ]
      : [],
    eligibilityCountries: [],
    conflicts: [],
  });
}

export function resolveLocationText(value?: string) {
  return resolveJobLocation({
    locationText: value,
  });
}

function collectLocationCandidates(input: ResolveJobLocationInput) {
  const candidates: LocationCandidate[] = [];
  const seen = new Set<string>();

  const push = (
    source: ResolvedLocationEvidenceSource,
    value: string | undefined,
    priority: number,
  ) => {
    const trimmed = value?.trim();
    if (!trimmed) {
      return;
    }

    const key = `${source}:${normalizeLocationText(trimmed)}`;
    if (seen.has(key)) {
      return;
    }

    seen.add(key);
    candidates.push({
      source,
      value: trimmed,
      priority,
    });
  };

  const structuredLocation = buildLocationValue(input.city, input.state, input.country);
  push("structured_fields", structuredLocation, 100);
  push("structured_fields", buildLocationValue(input.city, input.state), 96);
  push("structured_fields", input.country, 88);
  push("structured_fields", input.state, 82);
  push("structured_fields", input.city, 78);
  push("location_text", input.locationText, 94);

  collectMetadataCandidates(input.rawSourceMetadata).forEach((candidate) =>
    push(candidate.source, candidate.value, candidate.priority),
  );

  return candidates;
}

function collectMetadataCandidates(rawSourceMetadata?: Record<string, unknown>) {
  const candidates: LocationCandidate[] = [];

  visitNode(rawSourceMetadata, []);

  return candidates;

  function visitNode(node: unknown, path: string[]) {
    if (!node) {
      return;
    }

    if (Array.isArray(node)) {
      node.forEach((value) => visitNode(value, path));
      return;
    }

    if (typeof node === "string") {
      const pathLabel = path.join(".");
      if (descriptionPathPattern.test(pathLabel)) {
        extractDescriptionLocationHints(node).forEach((hint) =>
          candidates.push({
            source: /\bremote\b/i.test(hint) ? "remote_hint" : "description",
            value: hint,
            priority: /\bremote\b/i.test(hint) ? 62 : 58,
          }),
        );
      } else if (locationPathPattern.test(pathLabel)) {
        candidates.push({
          source: officePathPattern.test(pathLabel) ? "office_metadata" : "metadata",
          value: node,
          priority: officePathPattern.test(pathLabel) ? 78 : 72,
        });
      }
      return;
    }

    if (typeof node !== "object") {
      return;
    }

    const record = node as Record<string, unknown>;
    const pathLabel = path.join(".");
    const source = officePathPattern.test(pathLabel) ? "office_metadata" : "metadata";
    const structuredCandidate = buildStructuredLocationFromRecord(record);
    if (structuredCandidate) {
      candidates.push({
        source,
        value: structuredCandidate,
        priority: source === "office_metadata" ? 80 : 74,
      });
    }

    Object.entries(record).forEach(([key, value]) => visitNode(value, [...path, key]));
  }
}

function buildStructuredLocationFromRecord(record: Record<string, unknown>) {
  const directLocation = firstString(record, directLocationKeys);
  if (directLocation) {
    return directLocation;
  }

  const city = firstString(record, cityKeys);
  const state = firstString(record, stateKeys);
  const country = firstString(record, countryKeys);

  return buildLocationValue(city, state, country);
}

function resolveCandidate(candidate: LocationCandidate): ResolvedCandidate | undefined {
  const analysis = analyzeUsLocation(candidate.value);
  const internationalAnalysis = analyzeSupportedCountryLocation(candidate.value);
  const eligibilityHint = isEligibilityCandidate(candidate);
  const structuredParts = eligibilityHint
    ? { city: undefined, state: undefined, stateCode: undefined }
    : inferStructuredUsParts(candidate.value, analysis.isRemote);
  const state = structuredParts.state ?? analysis.stateName;
  const stateCode =
    structuredParts.stateCode ??
    analysis.stateCode ??
    (state ? resolveUsStateCode(state) : undefined);
  const city = structuredParts.city ?? analysis.city;
  const isUnitedStates =
    analysis.isUnitedStates ||
    Boolean(state) ||
    isUnitedStatesValue(candidate.value);

  if (!isUnitedStates) {
    const normalizedCountry =
      internationalAnalysis.country ?? normalizeCountryValue(candidate.value);
    if (!normalizedCountry || normalizedCountry === "United States") {
      return undefined;
    }

    const confidence = resolveConfidence(candidate.source, {
      city: internationalAnalysis.city,
      state: internationalAnalysis.state,
      isRemote: internationalAnalysis.isRemote,
      explicitUsAlias: false,
    });

    const location = compactResolvedLocation({
      country: normalizedCountry,
      state: internationalAnalysis.state,
      stateCode: internationalAnalysis.stateCode,
      city: internationalAnalysis.city,
      isRemote: internationalAnalysis.isRemote,
      isUnitedStates: false,
      confidence,
      evidence: [{ source: candidate.source, value: candidate.value }],
    });

    return {
      candidate,
      location,
      role: eligibilityHint || internationalAnalysis.isRemote ? "eligibility" : "physical",
      score:
        candidate.priority +
        confidenceScore(confidence) +
        (internationalAnalysis.city ? 12 : 0) +
        (internationalAnalysis.state ? 14 : 0),
    };
  }

  const confidence = resolveConfidence(candidate.source, {
    city,
    state,
    isRemote: analysis.isRemote,
    explicitUsAlias: isUnitedStatesValue(candidate.value),
  });

  const location = compactResolvedLocation({
    country: "United States",
    state,
    stateCode,
    city,
    isRemote: analysis.isRemote,
    isUnitedStates: true,
    confidence,
    evidence: [{ source: candidate.source, value: candidate.value }],
  });

  return {
    candidate,
    location,
    role: eligibilityHint || analysis.isRemote ? "eligibility" : "physical",
    score: candidate.priority + confidenceScore(confidence) + (city ? 12 : 0) + (state ? 14 : 0),
  };
}

function isEligibilityCandidate(candidate: LocationCandidate) {
  return (
    candidate.source === "remote_hint" ||
    (candidate.source === "description" &&
      /\b(remote|remotely|eligible|eligibility|work within|within)\b/i.test(candidate.value))
  );
}

function collectResolvedLocationPoints(resolvedCandidates: ResolvedCandidate[]) {
  const points: ResolvedLocationPoint[] = [];
  const seen = new Set<string>();

  resolvedCandidates.forEach((candidate) => {
    const point = locationToPoint(candidate.location);
    if (!point) {
      return;
    }

    const key = [
      normalizeLocationText(point.country),
      normalizeLocationText(point.state),
      normalizeLocationText(point.stateCode),
      normalizeLocationText(point.city),
    ].join("|");
    if (seen.has(key)) {
      return;
    }

    seen.add(key);
    points.push(point);
  });

  return points;
}

function locationToPoint(location: ResolvedLocation) {
  if (!location.country) {
    return undefined;
  }

  return {
    country: location.country,
    ...(location.state ? { state: location.state } : {}),
    ...(location.stateCode ? { stateCode: location.stateCode } : {}),
    ...(location.city ? { city: location.city } : {}),
    confidence: location.confidence,
    evidence: location.evidence,
  } satisfies ResolvedLocationPoint;
}

function collectEligibilityCountries(resolvedCandidates: ResolvedCandidate[]) {
  const countries = resolvedCandidates
    .filter((candidate) => candidate.role === "eligibility")
    .map((candidate) => candidate.location.country)
    .filter((country): country is string => Boolean(country));

  return dedupeCountries(countries);
}

function collectLocationConflicts(
  physicalLocations: ResolvedLocationPoint[],
  eligibilityCountries: string[],
) {
  const physicalCountries = dedupeCountries(physicalLocations.map((location) => location.country));
  const allCountries = dedupeCountries([...physicalCountries, ...eligibilityCountries]);
  const conflicts: ResolvedLocationConflict[] = [];

  if (physicalCountries.length > 1) {
    conflicts.push({
      kind: "country_conflict",
      countries: physicalCountries,
      evidence: physicalLocations.flatMap((location) => location.evidence).slice(0, 6),
    });
  }

  const remoteOnlyCountries = eligibilityCountries.filter(
    (country) => !physicalCountries.includes(country),
  );
  if (physicalCountries.length > 0 && remoteOnlyCountries.length > 0) {
    conflicts.push({
      kind: "physical_remote_conflict",
      countries: allCountries,
      evidence: physicalLocations.flatMap((location) => location.evidence).slice(0, 4),
    });
  }

  return conflicts;
}

function dedupeCountries(values: string[]) {
  const seen = new Set<string>();
  const deduped: string[] = [];

  values.forEach((value) => {
    const key = normalizeLocationText(value);
    if (!key || seen.has(key)) {
      return;
    }

    seen.add(key);
    deduped.push(value);
  });

  return deduped;
}

function inferStructuredUsParts(value: string, isRemote: boolean) {
  const parts = splitLocationParts(value);
  const statePart = parts.find((part) => resolveUsState(part));
  const state = statePart ? resolveUsState(statePart) : undefined;
  const stateCode = state ? resolveUsStateCode(state) : undefined;
  const cityPart = parts.find((part, index) => {
    if (index !== 0) {
      return false;
    }

    const normalized = normalizeLocationText(stripLeadingWorkplaceDescriptor(part));
    return Boolean(normalized) &&
      normalized !== "remote" &&
      normalized !== "hybrid" &&
      !resolveUsState(part) &&
      !isUnitedStatesValue(part);
  });

  return {
    city: !isRemote && cityPart ? stripLeadingWorkplaceDescriptor(cityPart) : undefined,
    state,
    stateCode,
  };
}

function collectSupportingEvidence(resolvedCandidates: ResolvedCandidate[]) {
  const evidence: ResolvedLocationEvidence[] = [];
  const seen = new Set<string>();

  resolvedCandidates.forEach((candidate) => {
    candidate.location.evidence.forEach((item) => {
      const key = `${item.source}:${normalizeLocationText(item.value)}`;
      if (seen.has(key) || evidence.length >= 4) {
        return;
      }

      seen.add(key);
      evidence.push(item);
    });
  });

  return evidence;
}

function extractDescriptionLocationHints(value: string) {
  return value
    .split(/[.\n;]+/g)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0 && segment.length <= 180)
    .filter((segment) => descriptionLocationHintPattern.test(segment))
    .filter((segment) => {
      const usAnalysis = analyzeUsLocation(segment);
      const internationalAnalysis = analyzeSupportedCountryLocation(segment);
      return usAnalysis.isUnitedStates || Boolean(internationalAnalysis.country);
    });
}

function resolveConfidence(
  source: ResolvedLocationEvidenceSource,
  input: {
    city?: string;
    state?: string;
    isRemote: boolean;
    explicitUsAlias: boolean;
  },
): ResolvedLocationConfidence {
  if (
    source === "structured_fields" &&
    (input.city || input.state || input.explicitUsAlias)
  ) {
    return "high";
  }

  if (
    (source === "location_text" || source === "office_metadata") &&
    (input.city || input.state || input.explicitUsAlias)
  ) {
    return "high";
  }

  if (input.state || input.explicitUsAlias || (input.isRemote && source !== "description")) {
    return "medium";
  }

  return source === "description" ? "low" : "medium";
}

function confidenceScore(confidence: ResolvedLocationConfidence) {
  switch (confidence) {
    case "high":
      return 48;
    case "medium":
      return 24;
    case "low":
      return 8;
    default:
      return 0;
  }
}

function buildLocationValue(...parts: Array<string | undefined>) {
  const unique = parts
    .map(normalizeFreeformValue)
    .filter(Boolean) as string[];

  return unique.length > 0 ? unique.join(", ") : undefined;
}

function normalizeCountryValue(value?: string) {
  const trimmed = normalizeFreeformValue(value);
  if (!trimmed) {
    return undefined;
  }

  if (isUnitedStatesValue(trimmed)) {
    return "United States";
  }

  const supportedCountry = getSupportedCountryCanonicalName(
    resolveSupportedCountryConcept(trimmed),
  );
  return supportedCountry ?? trimmed;
}

function normalizeStateValue(value?: string) {
  const trimmed = normalizeFreeformValue(value);
  if (!trimmed) {
    return undefined;
  }

  return resolveUsState(trimmed) ?? trimmed;
}

function normalizeFreeformValue(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function splitLocationParts(value: string) {
  return value
    .split(/[,/|;]|\n| - /g)
    .map((part) => sanitizeLocationPart(part))
    .filter(Boolean);
}

function stripLeadingWorkplaceDescriptor(value: string) {
  return value
    .replace(/^(?:remote|hybrid|onsite|on site)\s+(?:in|within)\s+/i, "")
    .replace(/^(?:remote|hybrid|onsite|on site)\s+/i, "")
    .trim();
}

function sanitizeLocationPart(value: string) {
  return stripLeadingWorkplaceDescriptor(
    value
      .replace(/\((?:remote|hybrid|onsite|on site)[^)]+\)/gi, " ")
      .replace(/\s+/g, " ")
      .trim(),
  );
}

function firstString(
  record: Record<string, unknown>,
  keys: readonly string[],
) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }

    if (Array.isArray(value)) {
      const stringValue = value.find(
        (entry): entry is string => typeof entry === "string" && entry.trim().length > 0,
      );
      if (stringValue) {
        return stringValue.trim();
      }
    }
  }

  return undefined;
}

function compactResolvedLocation(
  location: Omit<ResolvedLocation, "physicalLocations" | "eligibilityCountries" | "conflicts"> &
    Partial<Pick<ResolvedLocation, "physicalLocations" | "eligibilityCountries" | "conflicts">>,
): ResolvedLocation {
  return {
    ...(location.country ? { country: location.country } : {}),
    ...(location.state ? { state: location.state } : {}),
    ...(location.stateCode ? { stateCode: location.stateCode } : {}),
    ...(location.city ? { city: location.city } : {}),
    isRemote: location.isRemote,
    isUnitedStates: location.isUnitedStates,
    confidence: location.confidence,
    evidence: location.evidence,
    physicalLocations: location.physicalLocations ?? [],
    eligibilityCountries: location.eligibilityCountries ?? [],
    conflicts: location.conflicts ?? [],
  };
}
