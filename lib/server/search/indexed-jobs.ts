import "server-only";

import { applyResolvedExperienceLevel } from "@/lib/server/crawler/pipeline";
import {
  evaluateSearchFilters,
  type FilterEvaluation,
} from "@/lib/server/crawler/helpers";
import { dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { rankJobs } from "@/lib/server/crawler/sort";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import type {
  IndexedJobCandidateChannelBreakdown,
  IndexedJobCandidateQuery,
} from "@/lib/server/search/job-search-index";
import {
  emitSearchTraceStage,
  sampleCandidateIds,
  sampleCandidateLocations,
  sampleCandidateTitles,
  searchTraceSampleLimit,
  toJsonSafeRecord,
  type SearchTraceDiagnostics,
} from "@/lib/server/search/search-trace";
import { resolveOperationalCrawlerPlatforms, type ActiveCrawlerPlatform, type JobListing, type SearchFilters } from "@/lib/types";

type IndexedJobSearchMatch = {
  job: JobListing;
  evaluation: Extract<FilterEvaluation, { matches: true }>;
};

type IndexedJobDelta = {
  cursor: number;
  jobs: JobListing[];
};

type FinalFilterTrace = {
  evaluatedCount: number;
  matchedCount: number;
  excludedByActive: number;
  excludedByPlatform: number;
  excludedByTitle: number;
  excludedByLocation: number;
  excludedByExperience: number;
  sampleActiveExclusions: Array<Record<string, unknown>>;
  samplePlatformExclusions: Array<Record<string, unknown>>;
  sampleTitleExclusions: Array<Record<string, unknown>>;
  sampleLocationExclusions: Array<Record<string, unknown>>;
  sampleExperienceExclusions: Array<Record<string, unknown>>;
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
  candidateChannelBreakdown: IndexedJobCandidateChannelBreakdown;
  searchTrace?: SearchTraceDiagnostics;
  matches: IndexedJobSearchMatch[];
};

export async function getIndexedJobsForSearch(
  repository: JobCrawlerRepository,
  filters: SearchFilters,
  options: { traceId?: string } = {},
): Promise<IndexedJobSearchResult> {
  const startedAt = Date.now();
  const candidateResult = await repository.getIndexedJobCandidatesForSearch(filters, {
    traceId: options.traceId,
  });
  const candidateLoadedAt = Date.now();
  const candidateDbResultTrace = options.traceId
    ? emitSearchTraceStage("candidate-db-result", {
        traceId: options.traceId,
        candidateCountReturned: candidateResult.jobs.length,
        candidateLimit: candidateResult.query.limit,
        candidateChannelBreakdown: candidateResult.candidateChannelBreakdown,
        sampleCandidateIds: sampleCandidateIds(candidateResult.jobs),
        sampleCandidateTitles: sampleCandidateTitles(candidateResult.jobs),
        sampleCandidateLocations: sampleCandidateLocations(candidateResult.jobs),
      })
    : undefined;
  const candidateQueryDiagnostics = {
    ...candidateResult.query.diagnostics,
  };
  const matchResult = matchIndexedJobsForSearch(candidateResult.jobs, filters, {
    candidateCount: candidateResult.jobs.length,
    candidateQuery: candidateQueryDiagnostics,
  });
  const candidateChannelBreakdown = {
    ...candidateResult.candidateChannelBreakdown,
    finalMatchedCount: matchResult.matches.length,
    returnedCount: matchResult.matches.length,
  };
  candidateQueryDiagnostics.candidateChannelBreakdown = candidateChannelBreakdown;
  const finalFilterTrace = options.traceId
    ? emitSearchTraceStage("final-filter", {
        traceId: options.traceId,
        ...matchResult.finalFilter,
        candidateChannelBreakdown,
      })
    : undefined;
  const finishedAt = Date.now();
  const candidateQueryTrace = options.traceId
    ? toJsonSafeRecord({
        traceId: options.traceId,
        filter: candidateResult.query.filter,
        sort: candidateResult.query.sort,
        limit: candidateResult.query.limit,
        channels: candidateResult.query.channels.map((channel) => ({
          name: channel.name,
          filter: channel.filter,
          limit: channel.limit,
          diagnostics: channel.diagnostics,
        })),
        diagnostics: candidateQueryDiagnostics,
      })
    : undefined;

  return {
    candidateCount: candidateResult.jobs.length,
    matchedCount: matchResult.matches.length,
    candidateQuery: candidateQueryDiagnostics,
    candidateChannelBreakdown,
    requestTimeEvaluationCount: candidateResult.jobs.length,
    requestTimeExcludedCount: Math.max(0, candidateResult.jobs.length - matchResult.matches.length),
    excludedByTitleCount: matchResult.finalFilter.excludedByTitle,
    excludedByLocationCount: matchResult.finalFilter.excludedByLocation,
    excludedByExperienceCount: matchResult.finalFilter.excludedByExperience,
    timingsMs: {
      candidateQuery: candidateLoadedAt - startedAt,
      requestTimeRefinement: finishedAt - candidateLoadedAt,
      total: finishedAt - startedAt,
    },
    searchTrace: options.traceId
      ? {
          traceId: options.traceId,
          candidateQuery: candidateQueryTrace,
          candidateDbResult: candidateDbResultTrace,
          candidateChannelBreakdown,
          finalFilter: finalFilterTrace,
        }
      : undefined,
    matches: matchResult.matches,
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
    jobs: matchIndexedJobsForSearch(indexedDelta.jobs, filters).matches.map(({ job }) => job),
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
  const finalFilter: FinalFilterTrace = {
    evaluatedCount: 0,
    matchedCount: 0,
    excludedByActive: 0,
    excludedByPlatform: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    sampleActiveExclusions: [],
    samplePlatformExclusions: [],
    sampleTitleExclusions: [],
    sampleLocationExclusions: [],
    sampleExperienceExclusions: [],
  };
  const matches: IndexedJobSearchMatch[] = [];

  for (const job of jobs) {
    finalFilter.evaluatedCount += 1;

    if (job.isActive === false) {
      finalFilter.excludedByActive += 1;
      pushSample(finalFilter.sampleActiveExclusions, sampleExcludedJob(job));
      continue;
    }

    if (!matchesPlatformFilter(job, allowedPlatforms)) {
      finalFilter.excludedByPlatform += 1;
      pushSample(finalFilter.samplePlatformExclusions, {
        ...sampleExcludedJob(job),
        sourcePlatform: job.sourcePlatform,
        allowedPlatforms,
      });
      continue;
    }

    const normalizedJob = applyResolvedExperienceLevel(job);
    const evaluation = evaluateSearchFilters(normalizedJob, filters, {
      includeExperience: true,
    });

    if (!evaluation.matches) {
      recordFilterExclusionSample(finalFilter, normalizedJob, evaluation);
      continue;
    }

    finalFilter.matchedCount += 1;
    matches.push({
      job: attachIndexedSearchDiagnostics(normalizedJob, evaluation, context),
      evaluation,
    });
  }

  return { matches: rankIndexedSearchMatches(matches, filters.title), finalFilter };
}

function rankIndexedSearchMatches(
  matches: IndexedJobSearchMatch[],
  titleQuery: string,
) {
  const matchByJobId = new Map(matches.map((match) => [match.job._id, match] as const));

  return rankJobs(
    matches.map((match) => match.job),
    titleQuery,
  )
    .map(({ job }) => matchByJobId.get(job._id))
    .filter((match): match is IndexedJobSearchMatch => Boolean(match));
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

function recordFilterExclusionSample(
  finalFilter: FinalFilterTrace,
  job: JobListing,
  evaluation: Extract<FilterEvaluation, { matches: false }>,
) {
  if (evaluation.reason === "title") {
    finalFilter.excludedByTitle += 1;
    pushSample(finalFilter.sampleTitleExclusions, {
      ...sampleExcludedJob(job),
      titleMatch: evaluation.titleMatch
        ? {
            tier: evaluation.titleMatch.tier,
            score: evaluation.titleMatch.score,
            threshold: evaluation.titleMatch.threshold,
            explanation: evaluation.titleMatch.explanation,
          }
        : undefined,
    });
    return;
  }

  if (evaluation.reason === "location") {
    finalFilter.excludedByLocation += 1;
    pushSample(finalFilter.sampleLocationExclusions, {
      ...sampleExcludedJob(job),
      locationMatch: evaluation.locationMatch
        ? {
            matches: evaluation.locationMatch.matches,
            explanation: evaluation.locationMatch.explanation,
            matchedTerms: evaluation.locationMatch.matchedTerms,
            jobCountry: evaluation.locationMatch.jobDiagnostics.country,
            jobState: evaluation.locationMatch.jobDiagnostics.state,
            jobCity: evaluation.locationMatch.jobDiagnostics.city,
            isUnitedStates: evaluation.locationMatch.jobDiagnostics.isUnitedStates,
          }
        : undefined,
    });
    return;
  }

  finalFilter.excludedByExperience += 1;
  pushSample(finalFilter.sampleExperienceExclusions, {
    ...sampleExcludedJob(job),
    experienceMatch: evaluation.experienceMatch
      ? {
          matches: evaluation.experienceMatch.matches,
          matchedLevel: evaluation.experienceMatch.matchedLevel,
          selectedLevels: evaluation.experienceMatch.selectedLevels,
          mode: evaluation.experienceMatch.mode,
          includeUnspecified: evaluation.experienceMatch.includeUnspecified,
          explanation: evaluation.experienceMatch.explanation,
        }
      : undefined,
  });
}

function sampleExcludedJob(job: JobListing) {
  return {
    jobId: job._id,
    title: job.title,
    locationText: job.locationText,
    country: job.country,
    state: job.state,
    city: job.city,
  };
}

function pushSample(samples: Array<Record<string, unknown>>, sample: Record<string, unknown>) {
  if (samples.length < searchTraceSampleLimit) {
    samples.push(sample);
  }
}
