import "server-only";

import { applyResolvedExperienceLevel } from "@/lib/server/crawler/pipeline";
import {
  evaluateSearchFilters,
  type FilterEvaluation,
} from "@/lib/server/crawler/helpers";
import { dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { IndexedJobCandidateQuery } from "@/lib/server/search/job-search-index";
import { resolveOperationalCrawlerPlatforms, type ActiveCrawlerPlatform, type JobListing, type SearchFilters } from "@/lib/types";

type IndexedJobSearchMatch = {
  job: JobListing;
  evaluation: Extract<FilterEvaluation, { matches: true }>;
};

type IndexedJobDelta = {
  cursor: number;
  jobs: JobListing[];
};

export type IndexedJobSearchResult = {
  candidateCount: number;
  matchedCount: number;
  requestTimeEvaluationCount: number;
  requestTimeExcludedCount: number;
  excludedByTitleCount: number;
  excludedByLocationCount: number;
  excludedByExperienceCount: number;
  timingsMs: {
    candidateQuery: number;
    requestTimeRefinement: number;
    total: number;
  };
  candidateQuery: IndexedJobCandidateQuery["diagnostics"];
  matches: IndexedJobSearchMatch[];
};

export async function getIndexedJobsForSearch(
  repository: JobCrawlerRepository,
  filters: SearchFilters,
): Promise<IndexedJobSearchResult> {
  const startedAt = Date.now();
  const candidateResult = await repository.getIndexedJobCandidatesForSearch(filters);
  const candidateLoadedAt = Date.now();
  const matches = matchIndexedJobsForSearch(candidateResult.jobs, filters, {
    candidateCount: candidateResult.jobs.length,
    candidateQuery: candidateResult.query.diagnostics,
  });
  const finishedAt = Date.now();

  return {
    candidateCount: candidateResult.jobs.length,
    matchedCount: matches.length,
    candidateQuery: candidateResult.query.diagnostics,
    requestTimeEvaluationCount: candidateResult.jobs.length,
    requestTimeExcludedCount: Math.max(0, candidateResult.jobs.length - matches.length),
    ...countIndexedExclusions(candidateResult.jobs, filters),
    timingsMs: {
      candidateQuery: candidateLoadedAt - startedAt,
      requestTimeRefinement: finishedAt - candidateLoadedAt,
      total: finishedAt - startedAt,
    },
    matches,
  };
}

export async function getIndexedJobDeltasForSearch(
  repository: JobCrawlerRepository,
  filters: SearchFilters,
  afterSequence = 0,
): Promise<IndexedJobDelta> {
  const indexedDelta = await repository.getIndexedJobsAfterSequence(afterSequence);

  return {
    cursor: indexedDelta.cursor,
    jobs: matchIndexedJobsForSearch(indexedDelta.jobs, filters).map(({ job }) => job),
  };
}

export function mergeSearchResultJobs(
  indexedJobs: JobListing[],
  supplementalJobs: JobListing[],
) {
  return dedupeStoredJobs([...indexedJobs, ...supplementalJobs]);
}

function matchIndexedJobsForSearch(
  jobs: JobListing[],
  filters: SearchFilters,
  context?: {
    candidateCount: number;
    candidateQuery: IndexedJobCandidateQuery["diagnostics"];
  },
) {
  const allowedPlatforms = resolveAllowedPlatforms(filters);

  return jobs.flatMap((job) => {
    if (!matchesPlatformFilter(job, allowedPlatforms)) {
      return [];
    }

    const normalizedJob = applyResolvedExperienceLevel(job);
    const evaluation = evaluateSearchFilters(normalizedJob, filters, {
      includeExperience: true,
    });

    if (!evaluation.matches) {
      return [];
    }

    return [{
      job: attachIndexedSearchDiagnostics(normalizedJob, evaluation, context),
      evaluation,
    }];
  });
}

function countIndexedExclusions(jobs: JobListing[], filters: SearchFilters) {
  const counts = {
    excludedByTitleCount: 0,
    excludedByLocationCount: 0,
    excludedByExperienceCount: 0,
  };

  for (const job of jobs) {
    const normalizedJob = applyResolvedExperienceLevel(job);
    const evaluation = evaluateSearchFilters(normalizedJob, filters, {
      includeExperience: true,
    });
    if (evaluation.matches) {
      continue;
    }

    if (evaluation.reason === "title") {
      counts.excludedByTitleCount += 1;
    } else if (evaluation.reason === "location") {
      counts.excludedByLocationCount += 1;
    } else if (evaluation.reason === "experience") {
      counts.excludedByExperienceCount += 1;
    }
  }

  return counts;
}

function matchesPlatformFilter(
  job: JobListing,
  allowedPlatforms: ActiveCrawlerPlatform[] | undefined,
) {
  if (!allowedPlatforms?.length) {
    return true;
  }

  return allowedPlatforms.some((platform) => platform === job.sourcePlatform);
}

function resolveAllowedPlatforms(filters: SearchFilters) {
  if (!filters.platforms?.length) {
    return undefined;
  }

  return resolveOperationalCrawlerPlatforms(filters.platforms) as ActiveCrawlerPlatform[];
}

function attachIndexedSearchDiagnostics(
  job: JobListing,
  evaluation: Extract<FilterEvaluation, { matches: true }>,
  context?: {
    candidateCount: number;
    candidateQuery: IndexedJobCandidateQuery["diagnostics"];
  },
) {
  return {
    ...job,
    rawSourceMetadata: {
      ...(job.rawSourceMetadata ?? {}),
      indexedSearch: {
        source: "jobs_collection",
        candidateCount: context?.candidateCount,
        candidateQuery: context?.candidateQuery,
        titleMatch: {
          tier: evaluation.titleMatch.tier,
          score: evaluation.titleMatch.score,
          explanation: evaluation.titleMatch.explanation,
        },
        locationMatch: evaluation.locationMatch
          ? {
              matches: evaluation.locationMatch.matches,
              explanation: evaluation.locationMatch.explanation,
              matchedTerms: evaluation.locationMatch.matchedTerms,
            }
          : undefined,
        experienceMatch: evaluation.experienceMatch
          ? {
              matches: evaluation.experienceMatch.matches,
              matchedLevel: evaluation.experienceMatch.matchedLevel,
              explanation: evaluation.experienceMatch.explanation,
            }
          : undefined,
      },
    },
  } satisfies JobListing;
}
