import "server-only";

import { applyResolvedExperienceLevel } from "@/lib/server/crawler/pipeline";
import {
  evaluateSearchFilters,
  type FilterEvaluation,
} from "@/lib/server/crawler/helpers";
import { dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import {
  resolveOperationalCrawlerPlatforms,
  type ActiveCrawlerPlatform,
  type JobListing,
  type SearchFilters,
} from "@/lib/types";

type IndexedJobSearchMatch = {
  job: JobListing;
  evaluation: Extract<FilterEvaluation, { matches: true }>;
};

export async function getIndexedJobsForSearch(
  repository: JobCrawlerRepository,
  filters: SearchFilters,
) {
  const jobs = await repository.listJobs();
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
        job: attachIndexedSearchDiagnostics(normalizedJob, evaluation),
        evaluation,
      }];
    });
}

export function mergeSearchResultJobs(
  indexedJobs: JobListing[],
  supplementalJobs: JobListing[],
) {
  return dedupeStoredJobs([...indexedJobs, ...supplementalJobs]);
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
) {
  return {
    ...job,
    rawSourceMetadata: {
      ...(job.rawSourceMetadata ?? {}),
      indexedSearch: {
        source: "jobs_collection",
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
