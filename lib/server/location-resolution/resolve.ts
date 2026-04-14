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
};

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
  const bestUnitedStatesCandidate = resolvedCandidates.find(
    (candidate) => candidate.location.isUnitedStates,
  );

  if (bestUnitedStatesCandidate) {
    return compactResolvedLocation({
      ...bestUnitedStatesCandidate.location,
      evidence: collectSupportingEvidence(
        resolvedCandidates,
        (candidate) => candidate.location.isUnitedStates,
      ),
    });
  }

  const normalizedCountry = normalizeCountryValue(input.country);
  const normalizedState = normalizeStateValue(input.state);
  const normalizedCity = normalizeFreeformValue(input.city);
  const isRemote = candidates.some((candidate) =>
    normalizeLocationText(candidate.value).includes("remote"),
  );
  const fallbackEvidence = collectSupportingEvidence(
    resolvedCandidates,
    () => true,
  );

  return compactResolvedLocation({
    country: normalizedCountry,
    state: normalizedState,
    stateCode: normalizedState ? resolveUsStateCode(normalizedState) : undefined,
    city: normalizedCity,
    isRemote,
    isUnitedStates: normalizedCountry === "United States",
    confidence: normalizedCountry || normalizedState || normalizedCity ? "low" : "none",
    evidence: fallbackEvidence,
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
            source: "description",
            value: hint,
            priority: 58,
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
  const structuredParts = inferStructuredUsParts(candidate.value, analysis.isRemote);
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
    const normalizedCountry = normalizeCountryValue(candidate.value);
    if (!normalizedCountry || normalizedCountry === "United States") {
      return undefined;
    }

    return {
      candidate,
      location: compactResolvedLocation({
        country: normalizedCountry,
        state: undefined,
        stateCode: undefined,
        city: undefined,
        isRemote: analysis.isRemote,
        isUnitedStates: false,
        confidence: candidate.source === "structured_fields" ? "medium" : "low",
        evidence: [{ source: candidate.source, value: candidate.value }],
      }),
      score: candidate.priority,
    };
  }

  const confidence = resolveConfidence(candidate.source, {
    city,
    state,
    isRemote: analysis.isRemote,
    explicitUsAlias: isUnitedStatesValue(candidate.value),
  });

  return {
    candidate,
    location: compactResolvedLocation({
      country: "United States",
      state,
      stateCode,
      city,
      isRemote: analysis.isRemote,
      isUnitedStates: true,
      confidence,
      evidence: [{ source: candidate.source, value: candidate.value }],
    }),
    score: candidate.priority + confidenceScore(confidence) + (city ? 12 : 0) + (state ? 14 : 0),
  };
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

function collectSupportingEvidence(
  resolvedCandidates: ResolvedCandidate[],
  predicate: (candidate: ResolvedCandidate) => boolean,
) {
  const evidence: ResolvedLocationEvidence[] = [];
  const seen = new Set<string>();

  resolvedCandidates.forEach((candidate) => {
    if (!predicate(candidate)) {
      return;
    }

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
    .filter((segment) => analyzeUsLocation(segment).isUnitedStates);
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

  return isUnitedStatesValue(trimmed) ? "United States" : trimmed;
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
    .split(/[,/|]| - /g)
    .map((part) => part.trim())
    .filter(Boolean);
}

function stripLeadingWorkplaceDescriptor(value: string) {
  return value
    .replace(/^(?:remote|hybrid|onsite|on site)\s+(?:in|within)\s+/i, "")
    .replace(/^(?:remote|hybrid|onsite|on site)\s+/i, "")
    .trim();
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

function compactResolvedLocation(location: ResolvedLocation): ResolvedLocation {
  return {
    ...(location.country ? { country: location.country } : {}),
    ...(location.state ? { state: location.state } : {}),
    ...(location.stateCode ? { stateCode: location.stateCode } : {}),
    ...(location.city ? { city: location.city } : {}),
    isRemote: location.isRemote,
    isUnitedStates: location.isUnitedStates,
    confidence: location.confidence,
    evidence: location.evidence,
  };
}
