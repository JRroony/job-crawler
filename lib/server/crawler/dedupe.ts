import "server-only";

import type { JobListing } from "@/lib/types";

type DedupeCandidate = Omit<JobListing, "_id" | "crawlRunIds">;

export function dedupeJobs(jobs: DedupeCandidate[]) {
  const deduped: DedupeCandidate[] = [];

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

function isDuplicate(left: DedupeCandidate, right: DedupeCandidate) {
  if (left.canonicalUrl && right.canonicalUrl && left.canonicalUrl === right.canonicalUrl) {
    return true;
  }

  if (left.resolvedUrl && right.resolvedUrl && left.resolvedUrl === right.resolvedUrl) {
    return true;
  }

  if (left.applyUrl === right.applyUrl) {
    return true;
  }

  return left.contentFingerprint === right.contentFingerprint;
}

function mergeCandidates(left: DedupeCandidate, right: DedupeCandidate): DedupeCandidate {
  const primary = score(right) >= score(left) ? right : left;

  return {
    ...primary,
    postedAt: latestDate(left.postedAt, right.postedAt),
    discoveredAt: left.discoveredAt < right.discoveredAt ? left.discoveredAt : right.discoveredAt,
    sourceLookupKeys: Array.from(new Set([...left.sourceLookupKeys, ...right.sourceLookupKeys])),
    sourceProvenance: dedupeProvenance([...left.sourceProvenance, ...right.sourceProvenance]),
  };
}

function dedupeProvenance(records: DedupeCandidate["sourceProvenance"]) {
  const map = new Map<string, DedupeCandidate["sourceProvenance"][number]>();
  for (const record of records) {
    map.set(`${record.sourcePlatform}:${record.sourceJobId}:${record.applyUrl}`, record);
  }
  return Array.from(map.values());
}

function score(candidate: DedupeCandidate) {
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
