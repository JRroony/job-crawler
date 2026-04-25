import type { JobListing, ProviderPlatform } from "@/lib/types";

type IdentityInput = Omit<
  Pick<
    JobListing,
    | "_id"
    | "sourcePlatform"
    | "sourceCompanySlug"
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
  canonicalJobKey: string;
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
    boardToken?: string;
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
  const boardToken = resolveBoardCompanyToken(job);
  const fallbackFingerprint = buildConservativeFallbackFingerprint(job, {
    normalizedCompany,
    normalizedTitle,
    normalizedLocation,
    boardToken,
  });
  const platformJobKeys = buildPlatformJobKeys(job, boardToken);
  const canonicalJobKey = buildCanonicalJobKey(job, boardToken, fallbackFingerprint);
  const strongKeys = collectUniqueStrings([
    canonicalJobKey,
    job.canonicalUrl ? `canonical:${job.canonicalUrl}` : undefined,
    job.resolvedUrl ? `resolved:${job.resolvedUrl}` : undefined,
    job.applyUrl ? `apply:${job.applyUrl}` : undefined,
    job.sourceUrl ? `source:${job.sourceUrl}` : undefined,
    ...platformJobKeys.map((key) => `platform_job:${key}`),
  ]);
  const weakKeys = fallbackFingerprint ? [`fallback:${fallbackFingerprint}`] : [];
  const primaryKey = canonicalJobKey || weakKeys[0] || `database:${job._id ?? "unknown"}`;

  return {
    databaseId: job._id,
    canonicalJobKey,
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
      boardToken,
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
    hasStrongIdentity: !canonicalJobKey.startsWith("fallback:"),
  };
}

export function buildStableJobRenderIdentity(job: IdentityInput) {
  return buildCanonicalJobIdentity(job).canonicalJobKey;
}

function buildCanonicalJobKey(
  job: IdentityInput,
  boardToken: string | undefined,
  fallbackFingerprint: string,
) {
  const normalizedSourceJobId = normalizeComparableIdentityText(job.sourceJobId);

  if (boardToken && normalizedSourceJobId) {
    return `platform:${job.sourcePlatform}:${boardToken}:${normalizedSourceJobId}`;
  }

  return `fallback:${fallbackFingerprint}`;
}

function buildPlatformJobKeys(job: IdentityInput, boardToken?: string) {
  const normalizedSourceJobId = normalizeComparableIdentityText(job.sourceJobId);
  const explicitPlatformKey =
    boardToken && normalizedSourceJobId
      ? `${job.sourcePlatform}:${boardToken}:${normalizedSourceJobId}`
      : undefined;
  const lookupKeys = collectUniqueStrings(
    job.sourceLookupKeys.map((key) => normalizeComparableIdentityText(key)).filter(Boolean),
  );

  if (explicitPlatformKey || lookupKeys.length > 0) {
    return collectUniqueStrings([explicitPlatformKey, ...lookupKeys]);
  }

  if (!normalizedSourceJobId) {
    return [];
  }

  return [`${job.sourcePlatform}:${normalizedSourceJobId}`];
}

function buildConservativeFallbackFingerprint(
  job: IdentityInput,
  normalized: {
    normalizedCompany: string;
    normalizedTitle: string;
    normalizedLocation: string;
    boardToken?: string;
  },
) {
  if (job.dedupeFingerprint) {
    return normalizeComparableIdentityText(job.dedupeFingerprint);
  }

  if (job.contentFingerprint) {
    return normalizeComparableIdentityText(job.contentFingerprint);
  }

  const urlHash = buildUrlHash(
    job.canonicalUrl ?? job.resolvedUrl ?? job.applyUrl ?? job.sourceUrl,
  );

  return [
    normalized.normalizedCompany,
    normalized.normalizedTitle,
    normalized.normalizedLocation,
    urlHash ? `url:${urlHash}` : undefined,
  ]
    .filter(Boolean)
    .join("|");
}

function buildUrlHash(value?: string) {
  const normalized = normalizeComparableIdentityText(value);
  if (!normalized) {
    return undefined;
  }

  let hash = 5381;
  for (let index = 0; index < normalized.length; index += 1) {
    hash = ((hash << 5) + hash + normalized.charCodeAt(index)) >>> 0;
  }

  return hash.toString(36);
}

function resolveBoardCompanyToken(job: IdentityInput) {
  const platform = normalizeComparableIdentityText(job.sourcePlatform);
  for (const lookupKey of job.sourceLookupKeys) {
    const segments = lookupKey
      .split(":")
      .map((segment) => normalizeComparableIdentityText(segment))
      .filter(Boolean);

    if (segments[0] !== platform) {
      continue;
    }

    if (segments.length >= 3) {
      return segments[1];
    }
  }

  const slugToken = normalizeComparableIdentityText(job.sourceCompanySlug);
  if (slugToken) {
    return slugToken;
  }

  return extractNonNumericLastPathToken(job.sourceUrl) ?? extractNonNumericLastPathToken(job.applyUrl);
}

function extractNonNumericLastPathToken(value?: string) {
  if (!value) {
    return undefined;
  }

  const segments = value
    .split(/[/?#]/)
    .map((segment) => normalizeComparableIdentityText(segment))
    .filter(Boolean);
  const candidate = segments[segments.length - 1];

  if (!candidate || /^\d+$/.test(candidate)) {
    return undefined;
  }

  return candidate;
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
