import type { JobListing, ProviderPlatform } from "@/lib/types";

type IdentityInput = Omit<
  Pick<
    JobListing,
    | "_id"
    | "sourcePlatform"
    | "sourceJobId"
    | "sourceUrl"
    | "applyUrl"
    | "resolvedUrl"
    | "canonicalUrl"
    | "sourceLookupKeys"
    | "company"
    | "title"
    | "locationRaw"
    | "locationText"
    | "normalizedCompany"
    | "normalizedTitle"
    | "normalizedLocation"
    | "dedupeFingerprint"
    | "companyNormalized"
    | "titleNormalized"
    | "locationNormalized"
    | "contentFingerprint"
  >,
  "_id"
> & { _id?: string };

export type CanonicalJobIdentity = {
  databaseId?: string;
  originalIdentifiers: {
    sourcePlatform: ProviderPlatform;
    sourceJobId: string;
    sourceUrl: string;
    applyUrl: string;
    resolvedUrl?: string;
    canonicalUrl?: string;
    sourceLookupKeys: string[];
  };
  normalizedIdentity: {
    company: string;
    title: string;
    location: string;
    platformJobKeys: string[];
    sourceUrl: string;
    applyUrl: string;
    resolvedUrl?: string;
    canonicalUrl?: string;
    fallbackFingerprint: string;
  };
  strongKeys: string[];
  weakKeys: string[];
  primaryKey: string;
  hasStrongIdentity: boolean;
};

export function normalizeComparableIdentityText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function buildCanonicalJobIdentity(job: IdentityInput): CanonicalJobIdentity {
  const normalizedCompany =
    job.normalizedCompany ??
    job.companyNormalized ??
    normalizeComparableIdentityText(job.company);
  const normalizedTitle =
    job.normalizedTitle ??
    job.titleNormalized ??
    normalizeComparableIdentityText(job.title);
  const normalizedLocation =
    job.normalizedLocation ??
    job.locationNormalized ??
    normalizeComparableIdentityText(job.locationRaw ?? job.locationText);
  const fallbackFingerprint =
    job.dedupeFingerprint ||
    job.contentFingerprint ||
    [normalizedCompany, normalizedTitle, normalizedLocation].filter(Boolean).join("|");
  const platformJobKeys = buildPlatformJobKeys(job);
  const strongKeys = collectUniqueStrings([
    job.canonicalUrl ? `canonical:${job.canonicalUrl}` : undefined,
    job.resolvedUrl ? `resolved:${job.resolvedUrl}` : undefined,
    job.applyUrl ? `apply:${job.applyUrl}` : undefined,
    job.sourceUrl ? `source:${job.sourceUrl}` : undefined,
    ...platformJobKeys.map((key) => `platform_job:${key}`),
  ]);
  const weakKeys = fallbackFingerprint ? [`fallback:${fallbackFingerprint}`] : [];
  const primaryKey = strongKeys[0] ?? weakKeys[0] ?? `database:${job._id ?? "unknown"}`;

  return {
    databaseId: job._id,
    originalIdentifiers: {
      sourcePlatform: job.sourcePlatform,
      sourceJobId: job.sourceJobId,
      sourceUrl: job.sourceUrl,
      applyUrl: job.applyUrl,
      resolvedUrl: job.resolvedUrl,
      canonicalUrl: job.canonicalUrl,
      sourceLookupKeys: collectUniqueStrings(job.sourceLookupKeys),
    },
    normalizedIdentity: {
      company: normalizedCompany,
      title: normalizedTitle,
      location: normalizedLocation,
      platformJobKeys,
      sourceUrl: normalizeComparableIdentityText(job.sourceUrl),
      applyUrl: normalizeComparableIdentityText(job.applyUrl),
      resolvedUrl: job.resolvedUrl
        ? normalizeComparableIdentityText(job.resolvedUrl)
        : undefined,
      canonicalUrl: job.canonicalUrl
        ? normalizeComparableIdentityText(job.canonicalUrl)
        : undefined,
      fallbackFingerprint,
    },
    strongKeys,
    weakKeys,
    primaryKey,
    hasStrongIdentity: strongKeys.length > 0,
  };
}

export function buildStableJobRenderIdentity(job: IdentityInput) {
  const identity = buildCanonicalJobIdentity(job);
  return identity.primaryKey;
}

function buildPlatformJobKeys(job: IdentityInput) {
  const lookupKeys = collectUniqueStrings(
    job.sourceLookupKeys.map((key) => normalizeComparableIdentityText(key)).filter(Boolean),
  );
  if (lookupKeys.length > 0) {
    return lookupKeys;
  }

  const normalizedSourceJobId = normalizeComparableIdentityText(job.sourceJobId);
  if (!normalizedSourceJobId) {
    return [];
  }

  return [`${job.sourcePlatform}:${normalizedSourceJobId}`];
}

function collectUniqueStrings(values: Array<string | undefined>) {
  const results: string[] = [];
  const seen = new Set<string>();

  values.forEach((value) => {
    if (!value || seen.has(value)) {
      return;
    }

    seen.add(value);
    results.push(value);
  });

  return results;
}
