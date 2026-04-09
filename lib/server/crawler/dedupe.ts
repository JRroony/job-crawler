import "server-only";

import type { JobListing } from "@/lib/types";

type ComparableJobRecord = Omit<JobListing, "_id" | "crawlRunIds"> &
  Partial<Pick<JobListing, "_id" | "crawlRunIds">>;

type PersistableDedupeCandidate = Omit<JobListing, "_id" | "crawlRunIds">;

export function dedupeJobs<T extends PersistableDedupeCandidate>(jobs: T[]) {
  return dedupeComparableJobs(jobs);
}

export function dedupeStoredJobs(jobs: JobListing[]) {
  return dedupeComparableJobs(jobs);
}

function dedupeComparableJobs<T extends ComparableJobRecord>(jobs: T[]) {
  const deduped: T[] = [];

  for (const job of jobs) {
    const existingIndex = deduped.findIndex((candidate) => isDuplicate(candidate, job));
    if (existingIndex === -1) {
      deduped.push(job);
      continue;
    }

    deduped[existingIndex] = mergeCandidates(deduped[existingIndex], job);
  }

  return deduped;
}

function isDuplicate(left: ComparableJobRecord, right: ComparableJobRecord) {
  if (left._id && right._id && left._id === right._id) {
    return true;
  }

  if (left.canonicalUrl && right.canonicalUrl && left.canonicalUrl === right.canonicalUrl) {
    return true;
  }

  if (left.resolvedUrl && right.resolvedUrl && left.resolvedUrl === right.resolvedUrl) {
    return true;
  }

  if (left.applyUrl === right.applyUrl) {
    return true;
  }

  return left.sourceLookupKeys.some((lookupKey) => right.sourceLookupKeys.includes(lookupKey));
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
