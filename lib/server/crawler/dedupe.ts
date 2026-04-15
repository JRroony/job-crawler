import "server-only";

import { buildCanonicalJobIdentity } from "@/lib/job-identity";
import type { JobListing } from "@/lib/types";

type ComparableJobRecord = Omit<JobListing, "_id" | "crawlRunIds"> &
  Partial<Pick<JobListing, "_id" | "crawlRunIds">>;

type PersistableDedupeCandidate = Omit<JobListing, "_id" | "crawlRunIds">;

type IdentityBucket<T extends ComparableJobRecord> = {
  job: T;
  strongKeys: string[];
  weakKeys: string[];
  traceIds: string[];
  survivorTraceId: string;
};

export type DedupeDecisionTrace = {
  traceId: string;
  keptTraceId: string;
  matchedKeys: string[];
  dropReason: string;
};

export function dedupeJobs<T extends PersistableDedupeCandidate>(jobs: T[]) {
  return dedupeComparableJobs(jobs).jobs;
}

export function dedupeStoredJobs(jobs: JobListing[]) {
  return dedupeComparableJobs(jobs).jobs;
}

export function dedupeJobsWithDiagnostics<T extends PersistableDedupeCandidate>(
  jobs: T[],
  getTraceId: (job: T) => string,
) {
  return dedupeComparableJobs(jobs, getTraceId);
}

function dedupeComparableJobs<T extends ComparableJobRecord>(
  jobs: T[],
  getTraceId?: (job: T) => string,
) {
  const buckets: Array<IdentityBucket<T> | undefined> = [];
  const strongKeyToBucketIndex = new Map<string, number>();
  const weakKeyToBucketIndex = new Map<string, number>();
  const dropDecisions = new Map<string, DedupeDecisionTrace>();

  for (const job of jobs) {
    const identity = buildCanonicalJobIdentity(job);
    const activeKeys = identity.hasStrongIdentity ? identity.strongKeys : identity.weakKeys;
    const matchedBucketIndexes = collectMatchedBucketIndexes(
      activeKeys,
      identity.hasStrongIdentity ? strongKeyToBucketIndex : weakKeyToBucketIndex,
    );
    const traceId = getTraceId?.(job);

    if (matchedBucketIndexes.length === 0) {
      const nextIndex = buckets.length;
      buckets.push({
        job,
        strongKeys: identity.strongKeys,
        weakKeys: identity.weakKeys,
        traceIds: traceId ? [traceId] : [],
        survivorTraceId: traceId ?? "",
      });
      indexBucketKeys(identity.strongKeys, strongKeyToBucketIndex, nextIndex);
      if (!identity.hasStrongIdentity) {
        indexBucketKeys(identity.weakKeys, weakKeyToBucketIndex, nextIndex);
      }
      continue;
    }

    const primaryBucketIndex = matchedBucketIndexes[0];
    const primaryBucket = buckets[primaryBucketIndex];
    if (!primaryBucket) {
      continue;
    }

    let mergedJob = primaryBucket.job;
    const mergedStrongKeys = new Set<string>(primaryBucket.strongKeys);
    const mergedWeakKeys = new Set<string>(primaryBucket.weakKeys);
    const mergedTraceIds = new Set<string>(primaryBucket.traceIds);
    let survivorTraceId = primaryBucket.survivorTraceId;

    for (const bucketIndex of matchedBucketIndexes.slice(1)) {
      const candidateBucket = buckets[bucketIndex];
      if (!candidateBucket) {
        continue;
      }

      const mergedIntoCandidate = score(candidateBucket.job) >= score(mergedJob);
      const keptTraceId = mergedIntoCandidate
        ? candidateBucket.survivorTraceId
        : survivorTraceId;
      const droppedTraceIds = mergedIntoCandidate
        ? Array.from(mergedTraceIds)
        : candidateBucket.traceIds;
      const matchedKeys = intersectKeys(
        identity.hasStrongIdentity ? primaryBucket.strongKeys : primaryBucket.weakKeys,
        identity.hasStrongIdentity ? candidateBucket.strongKeys : candidateBucket.weakKeys,
      );
      droppedTraceIds.forEach((candidateTraceId) => {
        if (!candidateTraceId || candidateTraceId === keptTraceId) {
          return;
        }

        dropDecisions.set(candidateTraceId, {
          traceId: candidateTraceId,
          keptTraceId,
          matchedKeys,
          dropReason: classifyMatchedKeys(matchedKeys),
        });
      });

      mergedJob = mergeCandidates(mergedJob, candidateBucket.job);
      candidateBucket.strongKeys.forEach((key) => mergedStrongKeys.add(key));
      candidateBucket.weakKeys.forEach((key) => mergedWeakKeys.add(key));
      candidateBucket.traceIds.forEach((candidateTraceId) => mergedTraceIds.add(candidateTraceId));
      survivorTraceId = keptTraceId;
      buckets[bucketIndex] = undefined;
    }

    const newJobWins = score(job) >= score(mergedJob);
    const keptTraceId = newJobWins && traceId ? traceId : survivorTraceId;
    const droppedTraceIds = newJobWins ? Array.from(mergedTraceIds) : traceId ? [traceId] : [];
    const matchedKeys = intersectKeys(
      identity.hasStrongIdentity ? primaryBucket.strongKeys : primaryBucket.weakKeys,
      activeKeys,
    );
    droppedTraceIds.forEach((candidateTraceId) => {
      if (!candidateTraceId || candidateTraceId === keptTraceId) {
        return;
      }

      dropDecisions.set(candidateTraceId, {
        traceId: candidateTraceId,
        keptTraceId,
        matchedKeys,
        dropReason: classifyMatchedKeys(matchedKeys),
      });
    });

    mergedJob = mergeCandidates(mergedJob, job);
    const mergedIdentity = buildCanonicalJobIdentity(mergedJob);
    mergedIdentity.strongKeys.forEach((key) => mergedStrongKeys.add(key));
    mergedIdentity.weakKeys.forEach((key) => mergedWeakKeys.add(key));
    if (traceId) {
      mergedTraceIds.add(traceId);
    }

    buckets[primaryBucketIndex] = {
      job: mergedJob,
      strongKeys: Array.from(mergedStrongKeys),
      weakKeys: Array.from(mergedWeakKeys),
      traceIds: Array.from(mergedTraceIds),
      survivorTraceId: keptTraceId,
    };
    indexBucketKeys(
      buckets[primaryBucketIndex]?.strongKeys ?? [],
      strongKeyToBucketIndex,
      primaryBucketIndex,
    );
    if (!(buckets[primaryBucketIndex]?.strongKeys.length ?? 0)) {
      indexBucketKeys(
        buckets[primaryBucketIndex]?.weakKeys ?? [],
        weakKeyToBucketIndex,
        primaryBucketIndex,
      );
    }
  }

  const finalBuckets = buckets
    .filter((bucket): bucket is IdentityBucket<T> => Boolean(bucket))
    .map((bucket) => {
      bucket.traceIds.forEach((traceId) => {
        if (!traceId || traceId === bucket.survivorTraceId) {
          return;
        }

        const existing = dropDecisions.get(traceId);
        dropDecisions.set(traceId, {
          traceId,
          keptTraceId: bucket.survivorTraceId,
          matchedKeys: existing?.matchedKeys ?? [],
          dropReason: existing?.dropReason ?? "dedupe_matched",
        });
      });
      return bucket.job;
    });

  return {
    jobs: finalBuckets,
    dropped: Array.from(dropDecisions.values()),
  };
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

function indexBucketKeys(keys: string[], keyToBucketIndex: Map<string, number>, bucketIndex: number) {
  keys.forEach((key) => keyToBucketIndex.set(key, bucketIndex));
}

function intersectKeys(left: string[], right: string[]) {
  const rightSet = new Set(right);
  return Array.from(new Set(left.filter((key) => rightSet.has(key))));
}

function classifyMatchedKeys(keys: string[]) {
  if (keys.some((key) => key.startsWith("canonical:"))) {
    return "dedupe:canonical_url";
  }

  if (keys.some((key) => key.startsWith("resolved:"))) {
    return "dedupe:resolved_url";
  }

  if (keys.some((key) => key.startsWith("apply:"))) {
    return "dedupe:apply_url";
  }

  if (keys.some((key) => key.startsWith("source:"))) {
    return "dedupe:source_url";
  }

  if (keys.some((key) => key.startsWith("platform_job:"))) {
    return "dedupe:platform_job";
  }

  if (keys.some((key) => key.startsWith("fallback:"))) {
    return "dedupe:content_fingerprint";
  }

  return "dedupe:matched";
}

function mergeCandidates<T extends ComparableJobRecord>(left: T, right: T): T {
  const primary = score(right) >= score(left) ? right : left;
  const mergedId = primary._id ?? left._id ?? right._id;

  return {
    ...primary,
    rawSourceMetadata: {
      ...(left.rawSourceMetadata ?? {}),
      ...(right.rawSourceMetadata ?? {}),
    },
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
