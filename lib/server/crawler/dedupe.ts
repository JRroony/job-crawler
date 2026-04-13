import "server-only";

import { normalizeComparableText } from "@/lib/server/crawler/helpers";
import { parseGreenhouseUrl } from "@/lib/server/discovery/greenhouse-url";
import type { JobListing } from "@/lib/types";

type ComparableJobRecord = Omit<JobListing, "_id" | "crawlRunIds"> &
  Partial<Pick<JobListing, "_id" | "crawlRunIds">>;

type PersistableDedupeCandidate = Omit<JobListing, "_id" | "crawlRunIds">;

type IdentityBucket<T extends ComparableJobRecord> = {
  job: T;
  keys: string[];
};

export function dedupeJobs<T extends PersistableDedupeCandidate>(jobs: T[]) {
  return dedupeComparableJobs(jobs);
}

export function dedupeStoredJobs(jobs: JobListing[]) {
  return dedupeComparableJobs(jobs);
}

function dedupeComparableJobs<T extends ComparableJobRecord>(jobs: T[]) {
  const buckets: Array<IdentityBucket<T> | undefined> = [];
  const keyToBucketIndex = new Map<string, number>();

  for (const job of jobs) {
    const jobKeys = buildIdentityKeys(job);
    const matchedBucketIndexes = collectMatchedBucketIndexes(jobKeys, keyToBucketIndex);

    if (matchedBucketIndexes.length === 0) {
      const nextIndex = buckets.length;
      const keys = Array.from(new Set(jobKeys));
      buckets.push({
        job,
        keys,
      });
      keys.forEach((key) => keyToBucketIndex.set(key, nextIndex));
      continue;
    }

    const primaryBucketIndex = matchedBucketIndexes[0];
    const primaryBucket = buckets[primaryBucketIndex];
    if (!primaryBucket) {
      continue;
    }

    let mergedJob = primaryBucket.job;
    const mergedKeys = new Set<string>([...primaryBucket.keys, ...jobKeys]);

    for (const bucketIndex of matchedBucketIndexes.slice(1)) {
      const candidateBucket = buckets[bucketIndex];
      if (!candidateBucket) {
        continue;
      }

      mergedJob = mergeCandidates(mergedJob, candidateBucket.job);
      candidateBucket.keys.forEach((key) => mergedKeys.add(key));
      buckets[bucketIndex] = undefined;
    }

    mergedJob = mergeCandidates(mergedJob, job);
    buildIdentityKeys(mergedJob).forEach((key) => mergedKeys.add(key));

    const mergedKeyList = Array.from(mergedKeys);
    buckets[primaryBucketIndex] = {
      job: mergedJob,
      keys: mergedKeyList,
    };
    mergedKeyList.forEach((key) => keyToBucketIndex.set(key, primaryBucketIndex));
  }

  return buckets
    .filter((bucket): bucket is IdentityBucket<T> => Boolean(bucket))
    .map((bucket) => bucket.job);
}

function collectMatchedBucketIndexes(
  keys: string[],
  keyToBucketIndex: Map<string, number>,
) {
  const matchedIndexes: number[] = [];
  const seen = new Set<number>();

  for (const key of keys) {
    const bucketIndex = keyToBucketIndex.get(key);
    if (typeof bucketIndex !== "number" || seen.has(bucketIndex)) {
      continue;
    }

    seen.add(bucketIndex);
    matchedIndexes.push(bucketIndex);
  }

  return matchedIndexes;
}

function buildIdentityKeys(job: ComparableJobRecord) {
  const keys: string[] = [];

  if (job._id) {
    keys.push(`id:${job._id}`);
  }

  if (job.canonicalUrl) {
    keys.push(`canonical:${job.canonicalUrl}`);
  }

  if (job.resolvedUrl) {
    keys.push(`resolved:${job.resolvedUrl}`);
  }

  if (job.applyUrl) {
    keys.push(`apply:${job.applyUrl}`);
  }

  job.sourceLookupKeys.forEach((lookupKey) => {
    if (lookupKey) {
      keys.push(`lookup:${lookupKey}`);
    }
  });

  const fallbackKey = buildFallbackIdentityKey(job);
  if (fallbackKey) {
    keys.push(`fallback:${buildScopedFallbackIdentityKey(job, fallbackKey)}`);
  }

  return keys;
}

function buildFallbackIdentityKey(job: ComparableJobRecord) {
  if (job.contentFingerprint) {
    return job.contentFingerprint;
  }

  const company = job.companyNormalized || normalizeComparableText(job.company);
  const title = job.titleNormalized || normalizeComparableText(job.title);
  const location =
    job.locationNormalized ||
    normalizeComparableText(
      `${job.city ?? ""} ${job.state ?? ""} ${job.country ?? ""} ${job.locationText ?? ""}`,
    );

  if (!company || !title || !location) {
    return undefined;
  }

  return `${company}|${title}|${location}`;
}

function buildScopedFallbackIdentityKey(
  job: ComparableJobRecord,
  fallbackKey: string,
) {
  const greenhouseBoardToken = resolveGreenhouseBoardToken(job);
  if (!greenhouseBoardToken) {
    return fallbackKey;
  }

  return `greenhouse:${greenhouseBoardToken}:${fallbackKey}`;
}

function resolveGreenhouseBoardToken(job: ComparableJobRecord) {
  const lookupBoardToken = resolveGreenhouseBoardTokenFromLookupKeys(job.sourceLookupKeys);
  if (lookupBoardToken) {
    return lookupBoardToken;
  }

  return (
    parseGreenhouseUrl(job.canonicalUrl ?? "")?.boardSlug ??
    parseGreenhouseUrl(job.sourceUrl)?.boardSlug ??
    parseGreenhouseUrl(job.applyUrl)?.boardSlug
  );
}

function resolveGreenhouseBoardTokenFromLookupKeys(sourceLookupKeys: string[]) {
  for (const lookupKey of sourceLookupKeys) {
    const parts = lookupKey.split(":");
    if (parts.length >= 3 && parts[0] === "greenhouse" && parts[1]) {
      return parts[1];
    }
  }

  return undefined;
}

function mergeCandidates<T extends ComparableJobRecord>(left: T, right: T): T {
  const primary = score(right) >= score(left) ? right : left;
  const mergedId = primary._id ?? left._id ?? right._id;

  return {
    ...primary,
    postedAt: latestDate(left.postedAt, right.postedAt),
    discoveredAt: left.discoveredAt < right.discoveredAt ? left.discoveredAt : right.discoveredAt,
    sourceLookupKeys: Array.from(new Set([...left.sourceLookupKeys, ...right.sourceLookupKeys])),
    sourceProvenance: dedupeProvenance([...left.sourceProvenance, ...right.sourceProvenance]),
    ...(mergedId
      ? {
          _id: mergedId,
        }
      : {}),
    ...(left.crawlRunIds || right.crawlRunIds
      ? {
          crawlRunIds: Array.from(
            new Set([...(left.crawlRunIds ?? []), ...(right.crawlRunIds ?? [])]),
          ),
        }
      : {}),
  } as T;
}

function dedupeProvenance(records: ComparableJobRecord["sourceProvenance"]) {
  const map = new Map<string, ComparableJobRecord["sourceProvenance"][number]>();
  for (const record of records) {
    map.set(`${record.sourcePlatform}:${record.sourceJobId}:${record.applyUrl}`, record);
  }
  return Array.from(map.values());
}

function score(candidate: ComparableJobRecord) {
  let score = 0;
  if (candidate.linkStatus === "valid") {
    score += 3;
  } else if (candidate.linkStatus === "unknown") {
    score += 2;
  } else if (candidate.linkStatus === "stale") {
    score += 1;
  }

  if (candidate.resolvedUrl) {
    score += 1;
  }

  if (candidate.canonicalUrl) {
    score += 1;
  }

  if (candidate.lastValidatedAt) {
    score += 1;
  }

  return score;
}

function latestDate(left?: string, right?: string) {
  if (!left) {
    return right;
  }

  if (!right) {
    return left;
  }

  return left > right ? left : right;
}
