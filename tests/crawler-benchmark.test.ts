import { describe, expect, it } from "vitest";

import {
  buildBenchmarkDatabaseName,
  buildBenchmarkTotals,
  buildScenarioBenchmarkMetrics,
  collectInvalidUrlFields,
  createProviderBenchmarkRecorder,
  formatBenchmarkSummary,
  parseBenchmarkArgs,
  selectBenchmarkScenarios,
} from "@/scripts/benchmark-crawler";
import type { NormalizedJobSeed } from "@/lib/server/providers/types";
import type { CrawlResponse } from "@/lib/types";

function createSeed(overrides: Partial<NormalizedJobSeed> = {}): NormalizedJobSeed {
  return {
    title: "Software Engineer",
    company: "Acme",
    locationText: "Remote, United States",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: "acme",
    sourceJobId: "role-1",
    sourceUrl: "https://example.com/jobs/role-1",
    applyUrl: "https://example.com/jobs/role-1/apply",
    canonicalUrl: "https://example.com/jobs/role-1",
    discoveredAt: "2026-04-27T12:00:00.000Z",
    rawSourceMetadata: {},
    ...overrides,
  };
}

function createResponse(): CrawlResponse {
  return {
    search: {
      _id: "search-1",
      filters: {
        title: "Software Engineer",
        country: "United States",
        crawlMode: "fast",
      },
      createdAt: "2026-04-27T12:00:00.000Z",
      updatedAt: "2026-04-27T12:00:07.000Z",
      latestCrawlRunId: "run-1",
      latestSearchSessionId: "session-1",
      lastStatus: "completed",
    },
    searchSession: {
      _id: "session-1",
      searchId: "search-1",
      latestCrawlRunId: "run-1",
      status: "completed",
      createdAt: "2026-04-27T12:00:00.000Z",
      updatedAt: "2026-04-27T12:00:07.000Z",
      finishedAt: "2026-04-27T12:00:07.000Z",
      lastEventSequence: 3,
      lastEventAt: "2026-04-27T12:00:05.000Z",
    },
    crawlRun: {
      _id: "run-1",
      searchId: "search-1",
      searchSessionId: "session-1",
      startedAt: "2026-04-27T12:00:00.000Z",
      finishedAt: "2026-04-27T12:00:07.000Z",
      status: "completed",
      stage: "finalizing",
      discoveredSourcesCount: 1,
      crawledSourcesCount: 1,
      totalFetchedJobs: 5,
      totalMatchedJobs: 4,
      dedupedJobs: 3,
      validationMode: "deferred",
      providerSummary: [],
      diagnostics: {
        discoveredSources: 1,
        crawledSources: 1,
        providersEnqueued: 1,
        providerFailures: 0,
        directJobsHarvested: 1,
        jobsBeforeDedupe: 4,
        jobsAfterDedupe: 3,
        excludedByTitle: 1,
        excludedByLocation: 0,
        excludedByExperience: 0,
        dedupedOut: 1,
        validationDeferred: 3,
        performance: {
          stageTimingsMs: {
            discovery: 100,
            providerExecution: 1500,
            filtering: 50,
            dedupe: 25,
            persistence: 75,
            validation: 0,
            responseAssembly: 10,
            total: 1760,
          },
          providerTimingsMs: [
            {
              provider: "greenhouse",
              duration: 1500,
              sourceCount: 1,
              timedOut: false,
            },
          ],
          progressUpdateCount: 2,
          persistenceBatchCount: 1,
        },
        dropReasonCounts: {
          "dedupe:canonical_url": 1,
        },
        filterDecisionTraces: [],
        dedupeDecisionTraces: [],
      },
    },
    sourceResults: [
      {
        _id: "source-result-1",
        crawlRunId: "run-1",
        searchId: "search-1",
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 5,
        matchedCount: 4,
        savedCount: 3,
        warningCount: 0,
        startedAt: "2026-04-27T12:00:01.000Z",
        finishedAt: "2026-04-27T12:00:03.000Z",
      },
    ],
    jobs: [],
    diagnostics: {
      discoveredSources: 1,
      crawledSources: 1,
      providersEnqueued: 1,
      providerFailures: 0,
      directJobsHarvested: 1,
      jobsBeforeDedupe: 4,
      jobsAfterDedupe: 3,
      excludedByTitle: 1,
      excludedByLocation: 0,
      excludedByExperience: 0,
      dedupedOut: 1,
      validationDeferred: 3,
      performance: {
        stageTimingsMs: {
          discovery: 100,
          providerExecution: 1500,
          filtering: 50,
          dedupe: 25,
          persistence: 75,
          validation: 0,
          responseAssembly: 10,
          total: 1760,
        },
        providerTimingsMs: [
          {
            provider: "greenhouse",
            duration: 1500,
            sourceCount: 1,
            timedOut: false,
          },
        ],
        progressUpdateCount: 2,
        persistenceBatchCount: 1,
      },
      dropReasonCounts: {
        "dedupe:canonical_url": 1,
      },
      filterDecisionTraces: [],
      dedupeDecisionTraces: [],
    },
  };
}

describe("crawler benchmark harness metrics", () => {
  it("counts invalid URL fields on normalized seeds", () => {
    expect(
      collectInvalidUrlFields(
        createSeed({
          sourceUrl: "not a url",
          canonicalUrl: "ftp://example.com/jobs/role-1",
        }),
      ),
    ).toEqual(["sourceUrl", "canonicalUrl"]);
  });

  it("records normalized seed counts once across provider batches and final results", () => {
    const recorder = createProviderBenchmarkRecorder();
    const seed = createSeed();

    recorder.recordBatch({
      provider: "greenhouse",
      fetchedCount: 1,
      sourceCount: 1,
      jobs: [seed],
    });
    recorder.recordProviderResult(
      {
        provider: "greenhouse",
        status: "success",
        fetchedCount: 1,
        matchedCount: 1,
        sourceCount: 1,
        jobs: [seed],
      },
      25,
    );

    expect(recorder.snapshot()).toEqual([
      expect.objectContaining({
        provider: "greenhouse",
        normalizedJobCount: 1,
        measuredDurationMs: 25,
      }),
    ]);
  });

  it("builds scenario metrics from crawl, provider, Mongo, and DB-search counts", () => {
    const response = createResponse();
    const savedJobs = [
      {
        title: "Backend Engineer",
        experienceLevel: "senior",
        linkStatus: "valid",
        isActive: true,
        resolvedLocation: {
          isUnitedStates: true,
          isRemote: false,
          city: "Seattle",
          state: "Washington",
        },
        rawSourceMetadata: {
          crawlTitleMatch: {
            tier: "adjacent_concept",
          },
        },
      },
      {
        title: "Software Engineer",
        experienceLevel: "manager",
        linkStatus: "invalid",
        isActive: true,
        resolvedLocation: {
          isUnitedStates: true,
          isRemote: true,
        },
        rawSourceMetadata: {
          crawlTitleMatch: {
            tier: "exact",
          },
        },
      },
    ] as never;
    const scenarioMetric = buildScenarioBenchmarkMetrics({
      scenario: {
        id: "software-engineer-united-states",
        title: "Software Engineer",
        country: "United States",
      },
      crawlMode: "fast",
      response,
      providerInstrumentation: [
        {
          provider: "greenhouse",
          normalizedJobCount: 5,
          invalidUrlCount: 1,
          invalidUrlFieldCount: 2,
          measuredDurationMs: 1500,
          invocationCount: 1,
          resultFetchedCount: 5,
          resultSourceCount: 1,
          failureCount: 0,
          sampleInvalidUrls: [],
        },
      ],
      directJobsNormalized: 1,
      directInvalidUrlCount: 0,
      directInvalidUrlFieldCount: 0,
      jobsSavedToMongoDb: 3,
      savedJobs,
      searchResultCountFromDb: 8,
      searchResponseResultCount: 8,
      wallDurationMs: 2000,
    });

    expect(scenarioMetric.jobsFetchedPerProvider).toEqual({ greenhouse: 5 });
    expect(scenarioMetric.jobsNormalized).toBe(6);
    expect(scenarioMetric.jobsSavedToMongoDb).toBe(3);
    expect(scenarioMetric.searchResultCountFromDb).toBe(8);
    expect(scenarioMetric.duplicateRatio).toBe(0.25);
    expect(scenarioMetric.invalidUrlCount).toBe(1);
    expect(scenarioMetric.expiredJobCount).toBe(1);
    expect(scenarioMetric.titleRelevanceDistribution).toEqual({
      adjacent_concept: 1,
      exact: 1,
    });
    expect(scenarioMetric.seniorityDistribution).toEqual({
      manager: 1,
      senior: 1,
    });
    expect(scenarioMetric.locationMatchDistribution).toEqual({
      city_state_us: 1,
      remote_us: 1,
    });
    expect(scenarioMetric.providers[0]).toMatchObject({
      provider: "greenhouse",
      jobsFetched: 5,
      jobsNormalized: 5,
      jobsSavedToMongoDb: 3,
      crawlDurationMs: 1500,
    });
  });

  it("formats a human-readable summary and derives benchmark totals", () => {
    const response = createResponse();
    const scenarioMetric = buildScenarioBenchmarkMetrics({
      scenario: {
        id: "software-engineer-united-states",
        title: "Software Engineer",
        country: "United States",
      },
      crawlMode: "fast",
      response,
      providerInstrumentation: [],
      directJobsNormalized: 0,
      directInvalidUrlCount: 0,
      directInvalidUrlFieldCount: 0,
      jobsSavedToMongoDb: 3,
      savedJobs: [],
      searchResultCountFromDb: 8,
      searchResponseResultCount: 8,
      wallDurationMs: 2000,
    });
    const result = {
      benchmarkVersion: 1 as const,
      generatedAt: "2026-04-27T12:00:00.000Z",
      database: {
        uriHost: "127.0.0.1:27017",
        databaseName: "job_crawler_benchmark",
        resetBeforeRun: true,
      },
      config: {
        crawlMode: "fast" as const,
        scenarioCount: 1,
        artifactDir: "/tmp/artifacts",
      },
      totals: buildBenchmarkTotals([scenarioMetric]),
      scenarios: [scenarioMetric],
    };

    expect(formatBenchmarkSummary(result)).toContain("Software Engineer, United States");
    expect(result.totals.jobsFetched).toBe(5);
    expect(result.totals.jobsSavedToMongoDb).toBe(3);
  });

  it("uses an isolated benchmark database name by default", () => {
    expect(buildBenchmarkDatabaseName("mongodb://127.0.0.1:27017/job_crawler")).toBe(
      "job_crawler_benchmark",
    );
    expect(
      parseBenchmarkArgs([], {
        MONGODB_URI: "mongodb://127.0.0.1:27017/job_crawler",
      } as unknown as NodeJS.ProcessEnv).dbName,
    ).toBe("job_crawler_benchmark");
  });

  it("can select one benchmark scenario by id for focused repair runs", () => {
    expect(
      parseBenchmarkArgs(["--scenario-id", "data-analyst-united-states"]).scenarioIds,
    ).toEqual(["data-analyst-united-states"]);
    expect(
      parseBenchmarkArgs([], {
        BENCHMARK_CRAWLER_SCENARIO_IDS:
          "data-analyst-united-states,machine-learning-engineer-united-states",
      } as unknown as NodeJS.ProcessEnv).scenarioIds,
    ).toEqual(["data-analyst-united-states", "machine-learning-engineer-united-states"]);
    expect(
      selectBenchmarkScenarios({
        scenarioIds: ["data-analyst-united-states"],
      }).map((scenario) => scenario.id),
    ).toEqual(["data-analyst-united-states"]);
    expect(() =>
      selectBenchmarkScenarios({
        scenarioIds: ["unknown-scenario"],
      }),
    ).toThrow(/Unknown benchmark scenario id/);
  });
});
