import "server-only";

import { getTitleMatchResult } from "@/lib/server/crawler/helpers";
import type { JobListing } from "@/lib/types";

type SortableJob = Pick<JobListing, "sourcePlatform" | "title"> &
  Partial<
    Pick<
      JobListing,
      | "postingDate"
      | "postedAt"
      | "discoveredAt"
      | "crawledAt"
      | "sourceJobId"
      | "company"
      | "canonicalUrl"
      | "applyUrl"
      | "sourceUrl"
      | "rawSourceMetadata"
    >
  >;

export type JobRankingDiagnostics = {
  relevanceScore: number;
  relevanceTier: string;
  relevanceExplanation: string;
  dateScore: number;
  dateSource: "postingDate" | "crawledAt" | "discoveredAt" | "none";
  dateExplanation: string;
  usedFallbackDate: boolean;
  effectiveDate: string | undefined;
  ageDays: number | undefined;
  finalScore: number;
  finalRankContributors: string[];
};

type RankedJob<T extends SortableJob> = {
  job: T;
  ranking: JobRankingDiagnostics;
  originalIndex: number;
};

const postingDateScoreBands = [
  { maxAgeDays: 3, score: 180 },
  { maxAgeDays: 7, score: 165 },
  { maxAgeDays: 14, score: 145 },
  { maxAgeDays: 30, score: 120 },
  { maxAgeDays: 60, score: 85 },
  { maxAgeDays: 120, score: 45 },
  { maxAgeDays: Number.POSITIVE_INFINITY, score: 15 },
] as const;

const fallbackDateScoreBands = [
  { maxAgeDays: 3, score: 55 },
  { maxAgeDays: 7, score: 45 },
  { maxAgeDays: 14, score: 35 },
  { maxAgeDays: 30, score: 25 },
  { maxAgeDays: 60, score: 15 },
  { maxAgeDays: Number.POSITIVE_INFINITY, score: 5 },
] as const;

export function sortJobs<T extends SortableJob>(
  jobs: T[],
  titleQuery?: string,
  now: Date = new Date(),
) {
  return rankJobs(jobs, titleQuery, now).map((entry) => entry.job);
}

export function sortJobsForPersistence<T extends SortableJob>(jobs: T[], titleQuery?: string) {
  return [...jobs].sort((left, right) => {
    const titleRelevanceComparison = compareTitleRelevance(left, right, titleQuery);
    if (titleRelevanceComparison !== 0) {
      return titleRelevanceComparison;
    }

    const leftPostingDate = left.postingDate ?? left.postedAt;
    const rightPostingDate = right.postingDate ?? right.postedAt;

    if (leftPostingDate && rightPostingDate && leftPostingDate !== rightPostingDate) {
      return leftPostingDate > rightPostingDate ? -1 : 1;
    }

    if (leftPostingDate && !rightPostingDate) {
      return -1;
    }

    if (!leftPostingDate && rightPostingDate) {
      return 1;
    }

    const titleComparison = left.title.localeCompare(right.title);
    if (titleComparison !== 0) {
      return titleComparison;
    }

    return left.sourcePlatform.localeCompare(right.sourcePlatform);
  });
}

export function rankJobs<T extends SortableJob>(
  jobs: T[],
  titleQuery?: string,
  now: Date = new Date(),
) {
  return jobs
    .map((job, index) => ({
      job,
      ranking: explainJobRanking(job, titleQuery, now),
      originalIndex: index,
    }))
    .sort(compareRankedJobs);
}

export function sortJobsWithDiagnostics<T extends SortableJob>(
  jobs: T[],
  titleQuery?: string,
  now: Date = new Date(),
) {
  return rankJobs(jobs, titleQuery, now).map(({ job, ranking }) => ({
    ...job,
    rawSourceMetadata: {
      ...(job.rawSourceMetadata ?? {}),
      crawlRanking: ranking,
    },
  }));
}

export function explainJobRanking<T extends SortableJob>(
  job: T,
  titleQuery?: string,
  now: Date = new Date(),
): JobRankingDiagnostics {
  const normalizedQuery = titleQuery?.trim();
  const titleMatch = normalizedQuery
    ? getTitleMatchResult(job.title, normalizedQuery)
    : undefined;
  const relevanceScore = titleMatch?.score ?? 0;
  const { source, effectiveDate, ageDays } = resolveEffectiveDate(job, now);
  const dateScore = computeDateScore(source, ageDays);
  const usedFallbackDate = source === "crawledAt" || source === "discoveredAt";
  const finalScore = relevanceScore + dateScore;

  return {
    relevanceScore,
    relevanceTier: titleMatch?.tier ?? "none",
    relevanceExplanation:
      titleMatch?.explanation ??
      "No title query was provided, so ranking relies on date signals and stable tie-breakers.",
    dateScore,
    dateSource: source,
    dateExplanation: buildDateExplanation(source, effectiveDate, ageDays),
    usedFallbackDate,
    effectiveDate,
    ageDays,
    finalScore,
    finalRankContributors: [
      `relevance=${relevanceScore}`,
      `date=${dateScore}`,
      `dateSource=${source}`,
      ...(usedFallbackDate ? ["fallbackDateUsed=true"] : []),
    ],
  };
}

function compareRankedJobs<T extends SortableJob>(
  left: RankedJob<T>,
  right: RankedJob<T>,
) {
  const finalScoreComparison = right.ranking.finalScore - left.ranking.finalScore;
  if (finalScoreComparison !== 0) {
    return finalScoreComparison;
  }

  const relevanceComparison = right.ranking.relevanceScore - left.ranking.relevanceScore;
  if (relevanceComparison !== 0) {
    return relevanceComparison;
  }

  const dateSourceComparison =
    getDateSourcePriority(right.ranking.dateSource) -
    getDateSourcePriority(left.ranking.dateSource);
  if (dateSourceComparison !== 0) {
    return dateSourceComparison;
  }

  const dateScoreComparison = right.ranking.dateScore - left.ranking.dateScore;
  if (dateScoreComparison !== 0) {
    return dateScoreComparison;
  }

  if (left.ranking.effectiveDate && right.ranking.effectiveDate) {
    if (left.ranking.effectiveDate !== right.ranking.effectiveDate) {
      return right.ranking.effectiveDate.localeCompare(left.ranking.effectiveDate);
    }
  } else if (left.ranking.effectiveDate || right.ranking.effectiveDate) {
    return left.ranking.effectiveDate ? -1 : 1;
  }

  const companyComparison = (left.job.company ?? "").localeCompare(right.job.company ?? "");
  if (companyComparison !== 0) {
    return companyComparison;
  }

  const titleComparison = left.job.title.localeCompare(right.job.title);
  if (titleComparison !== 0) {
    return titleComparison;
  }

  const platformComparison = left.job.sourcePlatform.localeCompare(right.job.sourcePlatform);
  if (platformComparison !== 0) {
    return platformComparison;
  }

  const sourceJobIdComparison = (left.job.sourceJobId ?? "").localeCompare(
    right.job.sourceJobId ?? "",
  );
  if (sourceJobIdComparison !== 0) {
    return sourceJobIdComparison;
  }

  const urlComparison = resolveStableUrl(left.job).localeCompare(resolveStableUrl(right.job));
  if (urlComparison !== 0) {
    return urlComparison;
  }

  return left.originalIndex - right.originalIndex;
}

function resolveEffectiveDate<T extends SortableJob>(
  job: T,
  now: Date,
) {
  const postingDate = parseIsoDate(job.postingDate ?? job.postedAt);
  if (postingDate) {
    return {
      source: "postingDate" as const,
      effectiveDate: postingDate,
      ageDays: computeAgeDays(postingDate, now),
    };
  }

  const crawledAt = parseIsoDate(job.crawledAt);
  if (crawledAt) {
    return {
      source: "crawledAt" as const,
      effectiveDate: crawledAt,
      ageDays: computeAgeDays(crawledAt, now),
    };
  }

  const discoveredAt = parseIsoDate(job.discoveredAt);
  if (discoveredAt) {
    return {
      source: "discoveredAt" as const,
      effectiveDate: discoveredAt,
      ageDays: computeAgeDays(discoveredAt, now),
    };
  }

  return {
    source: "none" as const,
    effectiveDate: undefined,
    ageDays: undefined,
  };
}

function computeDateScore(
  source: JobRankingDiagnostics["dateSource"],
  ageDays: number | undefined,
) {
  if (source === "none" || ageDays === undefined) {
    return 0;
  }

  const bands = source === "postingDate" ? postingDateScoreBands : fallbackDateScoreBands;
  return bands.find((band) => ageDays <= band.maxAgeDays)?.score ?? 0;
}

function buildDateExplanation(
  source: JobRankingDiagnostics["dateSource"],
  effectiveDate: string | undefined,
  ageDays: number | undefined,
) {
  if (source === "none" || !effectiveDate || ageDays === undefined) {
    return "No posting, crawl, or discovery timestamp was available, so date did not contribute.";
  }

  if (source === "postingDate") {
    return `Used the true posting date (${effectiveDate}) with age ${ageDays}d.`;
  }

  return `Posting date missing, so ranking fell back to ${source} (${effectiveDate}) with age ${ageDays}d.`;
}

function getDateSourcePriority(source: JobRankingDiagnostics["dateSource"]) {
  switch (source) {
    case "postingDate":
      return 3;
    case "crawledAt":
      return 2;
    case "discoveredAt":
      return 1;
    default:
      return 0;
  }
}

function parseIsoDate(value?: string) {
  if (!value) {
    return undefined;
  }

  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : new Date(parsed).toISOString();
}

function computeAgeDays(value: string, now: Date) {
  const ageMs = Math.max(0, now.getTime() - Date.parse(value));
  return Math.floor(ageMs / 86_400_000);
}

function resolveStableUrl<T extends SortableJob>(job: T) {
  return job.canonicalUrl ?? job.applyUrl ?? job.sourceUrl ?? "";
}

function compareTitleRelevance<T extends SortableJob>(
  left: T,
  right: T,
  titleQuery?: string,
) {
  const normalizedQuery = titleQuery?.trim();
  if (!normalizedQuery) {
    return 0;
  }

  const leftScore = getTitleMatchResult(left.title, normalizedQuery).score;
  const rightScore = getTitleMatchResult(right.title, normalizedQuery).score;
  return rightScore - leftScore;
}
