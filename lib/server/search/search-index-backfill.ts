import "server-only";

import { isDeepStrictEqual } from "node:util";

import { normalizeLocationText, resolveUsState } from "@/lib/server/locations/us";
import { resolveJobLocation } from "@/lib/server/location-resolution/resolve";
import { buildJobSearchIndex } from "@/lib/server/search/job-search-index";
import { normalizeTitleText } from "@/lib/server/title-retrieval";
import type { JobListing, JobSearchIndex, ResolvedLocation } from "@/lib/types";
import { resolvedLocationSchema } from "@/lib/types";

export type SearchIndexBackfillJob = Partial<
  Pick<
    JobListing,
    | "_id"
    | "title"
    | "company"
    | "normalizedTitle"
    | "titleNormalized"
    | "country"
    | "state"
    | "city"
    | "locationText"
    | "locationRaw"
    | "normalizedLocation"
    | "locationNormalized"
    | "resolvedLocation"
    | "remoteType"
    | "experienceLevel"
    | "experienceClassification"
    | "sourcePlatform"
    | "linkStatus"
    | "rawSourceMetadata"
    | "isActive"
    | "postingDate"
    | "postedAt"
    | "lastSeenAt"
    | "crawledAt"
    | "discoveredAt"
    | "indexedAt"
    | "searchIndex"
  >
> & {
  _id?: unknown;
  rawSourceMetadata?: Record<string, unknown>;
  searchIndex?: JobSearchIndex;
};

export type SearchIndexBackfillRepair = {
  update: {
    normalizedTitle: string;
    titleNormalized: string;
    normalizedLocation: string;
    locationNormalized: string;
    resolvedLocation?: ResolvedLocation;
    searchIndex: JobSearchIndex;
    indexedAt: string;
  };
  changedFields: string[];
  diagnostics: {
    missingTitle: boolean;
    missingLocation: boolean;
    ambiguousLocation: boolean;
    generatedTitleKeyCount: number;
    generatedLocationKeyCount: number;
  };
};

export function buildSearchIndexBackfillRepair(
  job: SearchIndexBackfillJob,
  options: { now?: Date | string } = {},
): SearchIndexBackfillRepair {
  const title = normalizedSourceString(job.title);
  if (!title) {
    throw new Error("Cannot backfill search index for a job without a title.");
  }

  const locationRaw = buildIndexableLocationRaw(job);
  const normalizedTitle = normalizeTitleText(job.normalizedTitle || title) || normalizeTitleText(title);
  const titleNormalized = normalizeTitleText(job.titleNormalized || normalizedTitle || title);
  const normalizedLocation =
    normalizeLocationText(job.normalizedLocation || locationRaw) ||
    normalizeLocationText(locationRaw) ||
    "location unavailable";
  const locationNormalized =
    normalizeLocationText(job.locationNormalized || normalizedLocation) ||
    normalizedLocation;
  const resolvedLocation = resolveBackfillLocation(job, locationRaw);
  const indexedAt = normalizedSourceString(job.indexedAt) ?? resolveIndexedAt(job, options.now);
  const searchIndex = buildJobSearchIndex({
    title,
    normalizedTitle,
    country: normalizedSourceString(job.country),
    state: normalizedSourceString(job.state),
    city: normalizedSourceString(job.city),
    locationText: normalizedSourceString(job.locationText) ?? locationRaw,
    normalizedLocation,
    locationNormalized,
    resolvedLocation,
    experienceLevel: job.experienceLevel,
    experienceClassification: job.experienceClassification,
    sourcePlatform: job.sourcePlatform,
    linkStatus: job.linkStatus,
    isActive: job.isActive,
    postingDate: normalizedSourceString(job.postingDate),
    postedAt: normalizedSourceString(job.postedAt),
    lastSeenAt: normalizedSourceString(job.lastSeenAt),
    crawledAt: normalizedSourceString(job.crawledAt),
    discoveredAt: normalizedSourceString(job.discoveredAt),
    indexedAt,
  });

  const update = compactUndefinedFields({
    normalizedTitle,
    titleNormalized,
    normalizedLocation,
    locationNormalized,
    ...(resolvedLocation ? { resolvedLocation } : {}),
    searchIndex,
    indexedAt,
  }) as SearchIndexBackfillRepair["update"];

  return {
    update,
    changedFields: collectChangedFields(job, update),
    diagnostics: {
      missingTitle: false,
      missingLocation: !locationRaw,
      ambiguousLocation: isAmbiguousBackfillLocation(locationRaw, resolvedLocation),
      generatedTitleKeyCount: searchIndex.titleSearchKeys.length,
      generatedLocationKeyCount: searchIndex.locationSearchKeys.length,
    },
  };
}

export function isBackfillLocationMissing(job: SearchIndexBackfillJob) {
  return !buildIndexableLocationRaw(job);
}

export function isAmbiguousBackfillLocation(
  locationRaw: string,
  resolvedLocation?: ResolvedLocation,
) {
  if (!locationRaw) {
    return false;
  }

  return (
    !resolvedLocation ||
    resolvedLocation.confidence === "none" ||
    Boolean(resolvedLocation.conflicts?.length)
  );
}

function resolveBackfillLocation(
  job: SearchIndexBackfillJob,
  locationRaw: string,
) {
  const parsedExisting = resolvedLocationSchema.safeParse(job.resolvedLocation);
  const next = sanitizeBackfillResolvedLocation(
    resolveJobLocation({
      country: normalizedSourceString(job.country),
      state: normalizedSourceString(job.state),
      city: normalizedSourceString(job.city),
      locationText: locationRaw || normalizedSourceString(job.locationText),
      rawSourceMetadata: job.rawSourceMetadata,
    }),
    locationRaw,
  );

  if (next.confidence !== "none") {
    return next;
  }

  return parsedExisting.success ? parsedExisting.data : undefined;
}

function sanitizeBackfillResolvedLocation(
  location: ResolvedLocation,
  locationRaw: string,
): ResolvedLocation {
  if (isGlobalRemoteLocation(locationRaw)) {
    return {
      country: undefined,
      state: undefined,
      stateCode: undefined,
      city: undefined,
      isRemote: true,
      isUnitedStates: false,
      confidence: location.confidence === "none" ? "medium" : location.confidence,
      evidence: location.evidence,
      physicalLocations: [],
      eligibilityCountries: [],
      conflicts: [],
    };
  }

  const canonicalCountry = canonicalizeCountry(location.country);
  const countryNormalizedLocation: ResolvedLocation = canonicalCountry
    ? {
        ...location,
        country: canonicalCountry,
        physicalLocations: location.physicalLocations?.map((point) => ({
          ...point,
          country: canonicalizeCountry(point.country) ?? point.country,
        })),
        eligibilityCountries: location.eligibilityCountries?.map(
          (country) => canonicalizeCountry(country) ?? country,
        ),
      }
    : location;

  if (
    countryNormalizedLocation.city &&
    countryNormalizedLocation.state &&
    normalizeLocationText(countryNormalizedLocation.city) ===
      normalizeLocationText(countryNormalizedLocation.state) &&
    resolveUsState(locationRaw) === countryNormalizedLocation.state
  ) {
    const physicalLocations = countryNormalizedLocation.physicalLocations?.map((point) =>
      normalizeLocationText(point.city) ===
      normalizeLocationText(countryNormalizedLocation.state)
        ? { ...point, city: undefined }
        : point,
    );

    return {
      ...countryNormalizedLocation,
      city: undefined,
      ...(physicalLocations ? { physicalLocations } : {}),
    };
  }

  return countryNormalizedLocation;
}

function isGlobalRemoteLocation(value: string) {
  return /\b(worldwide|global|anywhere)\b/i.test(value);
}

function canonicalizeCountry(value?: string) {
  const trimmed = normalizedSourceString(value);
  if (!trimmed) {
    return undefined;
  }

  if (/^[A-Z]{2}$/.test(trimmed)) {
    const displayName = new Intl.DisplayNames(["en"], { type: "region" }).of(trimmed);
    return displayName && displayName !== trimmed ? displayName : trimmed;
  }

  return trimmed;
}

function buildIndexableLocationRaw(job: SearchIndexBackfillJob) {
  return (
    normalizedSourceString(job.locationText) ??
    normalizedSourceString(job.locationRaw) ??
    [job.city, job.state, job.country].map(normalizedSourceString).filter(Boolean).join(", ")
  );
}

function resolveIndexedAt(job: SearchIndexBackfillJob, now: Date | string | undefined) {
  return (
    normalizedSourceString(job.crawledAt) ??
    normalizedSourceString(job.discoveredAt) ??
    normalizeDate(now) ??
    new Date().toISOString()
  );
}

function collectChangedFields(
  job: SearchIndexBackfillJob,
  update: SearchIndexBackfillRepair["update"],
) {
  const changedFields: string[] = [];

  for (const [field, value] of Object.entries(update)) {
    if (!isDeepStrictEqual((job as Record<string, unknown>)[field], value)) {
      changedFields.push(field);
    }
  }

  return changedFields;
}

function compactUndefinedFields(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(compactUndefinedFields);
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, entryValue]) => entryValue !== undefined)
      .map(([key, entryValue]) => [key, compactUndefinedFields(entryValue)]),
  );
}

function normalizeDate(value: Date | string | undefined) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return normalizedSourceString(value);
}

function normalizedSourceString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}
