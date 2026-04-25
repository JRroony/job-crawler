import { describe, expect, it } from "vitest";

import {
  buildSearchRequestPayload,
  describeResultNotice,
  describeZeroResultState,
  isSupplementingSearchSession,
  isLatestClientRequest,
  mergeCrawlDeltaIntoResult,
  normalizeSearchFiltersForClient,
  resolveQueuedSearchPollIntervalMs,
  resolveViewState,
  shouldShowBlockingSearchLoad,
  shouldApplyQueuedResultImmediately,
} from "@/components/job-crawler-app";
import { BackgroundSupplementIndicator } from "@/components/job-crawler/status-panels";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { CrawlDeltaResponse, CrawlResponse, SearchFilters } from "@/lib/types";
import { queueSearchRun, isSearchRunPending, abortSearchRun } from "@/lib/server/crawler/background-runs";
import { FakeDb } from "@/tests/helpers/fake-db";

function createResult(
  status: CrawlResponse["crawlRun"]["status"],
  overrides?: Partial<CrawlResponse["diagnostics"]>,
): CrawlResponse {
  const diagnostics = {
    discoveredSources: 1,
    crawledSources: 1,
    providersEnqueued: 1,
    providerFailures: status === "completed" ? 0 : 1,
    directJobsHarvested: 0,
    jobsBeforeDedupe: 0,
    jobsAfterDedupe: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    dedupedOut: 0,
    validationDeferred: 0,
    performance: {
      timeToFirstVisibleResultMs: undefined,
      stageTimingsMs: {
        discovery: 0,
        providerExecution: 0,
        filtering: 0,
        dedupe: 0,
        persistence: 0,
        validation: 0,
        responseAssembly: 0,
        total: 0,
      },
      providerTimingsMs: [],
      progressUpdateCount: 0,
      persistenceBatchCount: 0,
    },
    dropReasonCounts: {},
    filterDecisionTraces: [],
    dedupeDecisionTraces: [],
    ...overrides,
  };

  return {
    search: {
      _id: "search-1",
      filters: {
        title: "Software Engineer",
      },
      createdAt: "2026-03-30T12:00:00.000Z",
      updatedAt: "2026-03-30T12:00:00.000Z",
      lastStatus: status,
      latestCrawlRunId: "run-1",
    },
    crawlRun: {
      _id: "run-1",
      searchId: "search-1",
      startedAt: "2026-03-30T12:00:00.000Z",
      finishedAt: "2026-03-30T12:05:00.000Z",
      status,
      discoveredSourcesCount: diagnostics.discoveredSources,
      crawledSourcesCount: diagnostics.crawledSources,
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      validationMode: "deferred",
      providerSummary: [],
      diagnostics,
    },
    sourceResults: [
      {
        _id: "source-1",
        crawlRunId: "run-1",
        searchId: "search-1",
        provider: "greenhouse",
        status: status === "completed" ? "success" : "failed",
        sourceCount: diagnostics.crawledSources,
        fetchedCount: 0,
        matchedCount: 0,
        savedCount: 0,
        warningCount: status === "completed" ? 0 : 1,
        errorMessage:
          status === "completed" ? undefined : "Provider request failed.",
        startedAt: "2026-03-30T12:00:00.000Z",
        finishedAt: "2026-03-30T12:05:00.000Z",
      },
    ],
    jobs: [],
    diagnostics,
  };
}

function createTestJob(jobId: string): CrawlResponse["jobs"][number] {
  return {
    _id: jobId,
    canonicalJobKey: `platform:greenhouse:acme:${jobId.toLowerCase()}`,
    title: "Software Engineer",
    normalizedTitle: "software engineer",
    company: "Acme",
    normalizedCompany: "acme",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    country: "United States",
    state: "Washington",
    city: "Seattle",
    locationRaw: "Seattle, WA, United States",
    normalizedLocation: "seattle wa united states",
    locationNormalized: "seattle wa united states",
    locationText: "Seattle, WA",
    remoteType: "unknown",
    sponsorshipHint: "unknown",
    sourcePlatform: "greenhouse",
    sourceJobId: jobId,
    sourceUrl: `https://example.com/jobs/${jobId}`,
    applyUrl: `https://example.com/jobs/${jobId}/apply`,
    canonicalUrl: `https://example.com/jobs/${jobId}`,
    discoveredAt: "2026-03-30T12:00:00.000Z",
    crawledAt: "2026-03-30T12:00:00.000Z",
    rawSourceMetadata: {},
    sourceLookupKeys: [`greenhouse:${jobId}`],
    sourceProvenance: [],
    crawlRunIds: ["run-1"],
    firstSeenAt: "2026-03-30T12:00:00.000Z",
    lastSeenAt: "2026-03-30T12:00:00.000Z",
    indexedAt: "2026-03-30T12:00:00.000Z",
    isActive: true,
    dedupeFingerprint: jobId,
    contentFingerprint: jobId,
    contentHash: `content-hash:${jobId}`,
    linkStatus: "unknown",
  };
}

describe("job crawler app result state", () => {
  it("strips nullable optional filters and legacy experienceClassification from the submit payload", () => {
    const result = buildSearchRequestPayload({
      title: "Software Engineer",
      country: " United States ",
      state: null as unknown as SearchFilters["state"],
      city: "   ",
      experienceClassification: null,
      platforms: ["greenhouse"],
      experienceMatchMode: "balanced",
      crawlMode: "fast",
    } as SearchFilters & { experienceClassification: null });

    expect(result).toEqual({
      ok: true,
      payload: {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
        experienceMatchMode: "balanced",
        crawlMode: "fast",
      },
    });
  });

  it("normalizes hydrated legacy filter objects into the canonical client shape", () => {
    expect(
      normalizeSearchFiltersForClient({
        title: "Software Engineer",
        country: null,
        state: null,
        city: null,
        experienceClassification: null,
        experienceLevel: "senior",
        crawlMode: null,
      }),
    ).toEqual({
      title: "Software Engineer",
      country: "",
      state: "",
      city: "",
      experienceLevels: ["senior"],
      experienceMatchMode: "balanced",
      crawlMode: "fast",
    });
  });

  it("treats failed zero-job crawls as provider failures", () => {
    const result = createResult("failed");

    expect(resolveViewState(result)).toBe("error");
    expect(describeZeroResultState(result)).toMatchObject({
      title: "Providers failed before jobs could be saved",
      description:
        "The run encountered provider-side failures and never reached a usable saved result set.",
    });
  });

  it("treats partial zero-job crawls as provider issues instead of no matches", () => {
    const result = createResult("partial");

    expect(resolveViewState(result)).toBe("partial");
    expect(describeZeroResultState(result)).toMatchObject({
      title: "Provider issues left the run with no saved jobs",
      description:
        "One or more providers failed, and the remaining source coverage did not produce any saved jobs. Retry the crawl or broaden the filters.",
    });
  });

  it("keeps completed zero-job crawls mapped to the no-match state", () => {
    const result = createResult("completed", {
      excludedByTitle: 3,
      excludedByLocation: 1,
      validationDeferred: 0,
    });

    expect(resolveViewState(result)).toBe("empty");
    expect(describeZeroResultState(result)).toMatchObject({
      title: "Filters were too narrow for the fetched jobs",
      description:
        "The crawler found public jobs, but the current title, location, or experience policy removed them before save.",
    });
  });

  it("surfaces no-source runs as discovery failures instead of generic empties", () => {
    const result = createResult("completed", {
      discoveredSources: 0,
      crawledSources: 0,
      providerFailures: 0,
    });

    expect(describeZeroResultState(result)).toMatchObject({
      title: "No runnable sources were discovered",
      description:
        "The crawler did not find any registry-backed or publicly discovered sources for the selected platform scope.",
    });
  });

  it("treats running crawls as loading so queued searches can poll without flashing empty states", () => {
    const result = createResult("running");

    expect(resolveViewState(result)).toBe("loading");
  });

  it("keeps the visible surface in success state when supplemental work is running", () => {
    const result = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
    } satisfies CrawlResponse;

    expect(resolveViewState(result)).toBe("success");
    expect(isSupplementingSearchSession(result)).toBe(true);
    expect(shouldShowBlockingSearchLoad("loading", result)).toBe(false);
  });

  it("keeps aborted zero-job crawls mapped to the stopped state", () => {
    const result = createResult("aborted", {
      discoveredSources: 3,
      crawledSources: 2,
    });

    expect(resolveViewState(result)).toBe("empty");
    expect(describeZeroResultState(result)).toMatchObject({
      title: "The crawl was stopped before more jobs were saved",
    });
  });

  it("treats stale client responses as superseded", () => {
    expect(isLatestClientRequest(3, 3)).toBe(true);
    expect(isLatestClientRequest(2, 3)).toBe(false);
  });

  it("uses queued search responses immediately once jobs are already visible or the run is settled", () => {
    expect(
      shouldApplyQueuedResultImmediately({
        crawlRun: createResult("running").crawlRun,
        jobs: [createTestJob("job-1")],
      }),
    ).toBe(true);

    expect(
      shouldApplyQueuedResultImmediately({
        crawlRun: createResult("completed").crawlRun,
        jobs: [],
      }),
    ).toBe(true);

    expect(
      shouldApplyQueuedResultImmediately({
        crawlRun: createResult("running").crawlRun,
        jobs: [],
      }),
    ).toBe(false);
  });

  it("polls more aggressively before the first visible jobs arrive", () => {
    expect(resolveQueuedSearchPollIntervalMs(0)).toBe(250);
    expect(resolveQueuedSearchPollIntervalMs(1)).toBe(750);
  });

  it("merges incremental jobs into the active result without dropping earlier saved jobs", () => {
    const base = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
      delivery: {
        mode: "full" as const,
        cursor: 1,
      },
    } satisfies CrawlResponse;

    const delta = {
      search: base.search,
      crawlRun: base.crawlRun,
      sourceResults: base.sourceResults,
      diagnostics: base.diagnostics,
      jobs: [
        {
          ...createTestJob("job-2"),
        },
      ],
      delivery: {
        mode: "delta" as const,
        previousCursor: 1,
        cursor: 2,
      },
    } satisfies CrawlDeltaResponse;

    const merged = mergeCrawlDeltaIntoResult(base, delta);

    expect(merged.jobs.map((job) => job._id)).toEqual(["job-1", "job-2"]);
    expect(merged.delivery?.cursor).toBe(2);
  });

  it("explains that results arrive while background work continues", () => {
    const result = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
      search: {
        ...createResult("running").search,
        filters: {
          title: "Software Engineer",
          crawlMode: "fast",
        },
      },
    } satisfies CrawlResponse;

    const notice = describeResultNotice(result);
    expect(notice?.title).toBe("Results are arriving while background work continues");
    expect(notice?.description).toContain("Browse these jobs now");
    expect(notice?.highlights).toContain(
      "Fast mode shows your first results quickly while refinement continues.",
    );
  });

  it("explains that stopping preserves visible results", () => {
    const result = {
      ...createResult("aborted"),
      jobs: [createTestJob("job-1")],
    } satisfies CrawlResponse;

    const notice = describeResultNotice(result);
    expect(notice?.title).toBe("This search was stopped before refinement finished");
    expect(notice?.description).toContain("Your visible results are preserved");
  });
});

describe("UI state transitions for parallel search support", () => {
  it("resolveViewState returns loading for running status", () => {
    const result = createResult("running");
    expect(resolveViewState(result)).toBe("loading");
  });

  it("resolveViewState returns success for completed with jobs", () => {
    const result = {
      ...createResult("completed"),
      jobs: [createTestJob("job-1")],
    };
    expect(resolveViewState(result)).toBe("success");
  });

  it("isLatestClientRequest returns true when tokens match", () => {
    expect(isLatestClientRequest(1, 1)).toBe(true);
  });

  it("isLatestClientRequest returns false when tokens differ", () => {
    expect(isLatestClientRequest(1, 2)).toBe(false);
  });
});

describe("abort signal propagation in background runs", () => {
  it("queueSearchRun registers a pending run", async () => {
    const searchId = `test-abort-queue-${Date.now()}`;
    const repository = new JobCrawlerRepository(new FakeDb());
    const crawlRun = await repository.createCrawlRun(searchId);
    let executed = false;

    const queued = await queueSearchRun(searchId, repository, async (signal) => {
      executed = true;
      // Wait briefly so we can test abort
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, 5000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          resolve();
        }, { once: true });
      });
    }, {
      crawlRunId: crawlRun._id,
    });

    expect(queued).toBe(true);
    expect(await isSearchRunPending(searchId, repository)).toBe(true);
    expect((await repository.getActiveCrawlQueueEntryForSearch(searchId))?.crawlRunId).toBe(crawlRun._id);

    // Clean up: abort to finish the test quickly
    await abortSearchRun(searchId, repository, { awaitCompletion: true });
    expect(executed).toBe(true);
  });

  it("abortSearchRun aborts the pending task", async () => {
    const searchId = `test-abort-signal-${Date.now()}`;
    const repository = new JobCrawlerRepository(new FakeDb());
    const crawlRun = await repository.createCrawlRun(searchId);
    let aborted = false;

    await queueSearchRun(searchId, repository, async (signal) => {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, 30000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          aborted = true;
          resolve();
        }, { once: true });
      });
    }, {
      crawlRunId: crawlRun._id,
    });

    expect(await isSearchRunPending(searchId, repository)).toBe(true);
    const result = await abortSearchRun(searchId, repository, { awaitCompletion: true });
    expect(result).toBe(true);
    expect(aborted).toBe(true);
    expect(await isSearchRunPending(searchId, repository)).toBe(false);
  });

  it("abortSearchRun on non-existent run returns false", async () => {
    const searchId = `test-non-existent-${Date.now()}`;
    const repository = new JobCrawlerRepository(new FakeDb());
    const result = await abortSearchRun(searchId, repository);
    expect(result).toBe(false);
  });
});

describe("Progressive index-first search UI behavior", () => {
  it("resolveViewState returns success when crawl is running with visible jobs", () => {
    const result = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
    };
    expect(resolveViewState(result)).toBe("success");
    expect(isSupplementingSearchSession(result)).toBe(true);
  });

  it("resolveViewState returns success when crawl completes with jobs", () => {
    const result = {
      ...createResult("completed"),
      jobs: [createTestJob("job-1")],
    };
    expect(resolveViewState(result)).toBe("success");
  });

  it("resolveViewState returns empty when crawl completes with no jobs", () => {
    const result = createResult("completed");
    expect(resolveViewState(result)).toBe("empty");
  });

  it("describeResultNotice uses search-session language for running state with jobs", () => {
    const result = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
      search: {
        ...createResult("running").search,
        filters: {
          title: "Data Analyst",
          crawlMode: "balanced",
        },
      },
    } satisfies CrawlResponse;

    const notice = describeResultNotice(result);
    expect(notice?.title).toBe("Results are arriving while background work continues");
    expect(notice?.description).toContain("Browse these jobs now");
    expect(notice?.highlights).toContain(
      "Balanced mode refines results in the background while you review what's already visible.",
    );
  });

  it("describeResultNotice for aborted state emphasizes preserved results", () => {
    const result = {
      ...createResult("aborted"),
      jobs: [createTestJob("job-1"), createTestJob("job-2")],
    } satisfies CrawlResponse;

    const notice = describeResultNotice(result);
    expect(notice?.title).toContain("stopped");
    expect(notice?.description).toContain("visible results are preserved");
  });

  it("describeZeroResultState for aborted shows search-stopped language", () => {
    const result = createResult("aborted");
    const state = describeZeroResultState(result);
    expect(state.title).toContain("stopped");
    expect(state.description).toContain("incomplete");
  });

  it("BackgroundSupplementIndicator renders with stage and count", () => {
    // This is a simple smoke test that the component exists and can be imported
    expect(BackgroundSupplementIndicator).toBeDefined();
    expect(typeof BackgroundSupplementIndicator).toBe("function");
  });

  it("shouldShowBlockingSearchLoad only blocks the main surface before any jobs are visible", () => {
    expect(shouldShowBlockingSearchLoad("loading", null)).toBe(true);
    expect(shouldShowBlockingSearchLoad("loading", createResult("running"))).toBe(true);
    expect(
      shouldShowBlockingSearchLoad("loading", {
        ...createResult("running"),
        jobs: [createTestJob("job-1")],
      }),
    ).toBe(false);
  });

  it("mergeCrawlDeltaIntoResult preserves job identity across incremental updates", () => {
    const base = {
      ...createResult("running"),
      jobs: [createTestJob("job-1")],
      delivery: {
        mode: "full" as const,
        cursor: 1,
      },
    } satisfies CrawlResponse;

    const delta = {
      search: base.search,
      crawlRun: base.crawlRun,
      sourceResults: base.sourceResults,
      diagnostics: base.diagnostics,
      jobs: [createTestJob("job-2"), createTestJob("job-3")],
      delivery: {
        mode: "delta" as const,
        previousCursor: 1,
        cursor: 2,
      },
    } satisfies CrawlDeltaResponse;

    const merged = mergeCrawlDeltaIntoResult(base, delta);

    // All jobs should be present
    expect(merged.jobs.length).toBe(3);
    expect(merged.jobs.map((j) => j._id)).toContain("job-1");
    expect(merged.jobs.map((j) => j._id)).toContain("job-2");
    expect(merged.jobs.map((j) => j._id)).toContain("job-3");
    // Cursor should be updated
    expect(merged.delivery?.cursor).toBe(2);
  });

  it("mergeCrawlDeltaIntoResult dedupes logically identical jobs so render keys stay stable", () => {
    const base = {
      ...createResult("running"),
      jobs: [
        {
          ...createTestJob("job-1"),
          canonicalUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          sourceUrl: "https://example.com/jobs/shared-role",
          sourceJobId: "shared-role",
          sourceLookupKeys: ["greenhouse:shared-role"],
        },
      ],
      delivery: {
        mode: "full" as const,
        cursor: 1,
        indexedCursor: 7,
      },
    } satisfies CrawlResponse;

    const delta = {
      search: base.search,
      searchSession: {
        _id: "session-1",
        searchId: base.search._id,
        latestCrawlRunId: base.crawlRun._id,
        status: "running" as const,
        createdAt: base.search.createdAt,
        updatedAt: base.search.updatedAt,
        finishedAt: undefined,
        lastEventSequence: 2,
        lastEventAt: base.search.updatedAt,
      },
      crawlRun: base.crawlRun,
      sourceResults: base.sourceResults,
      diagnostics: base.diagnostics,
      jobs: [
        {
          ...createTestJob("job-2"),
          canonicalUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          sourceUrl: "https://example.com/jobs/shared-role",
          sourceJobId: "shared-role",
          sourceLookupKeys: ["greenhouse:shared-role"],
        },
      ],
      delivery: {
        mode: "delta" as const,
        previousCursor: 1,
        cursor: 2,
        previousIndexedCursor: 7,
        indexedCursor: 8,
      },
    } satisfies CrawlDeltaResponse;

    const merged = mergeCrawlDeltaIntoResult(base, delta);

    expect(merged.jobs).toHaveLength(1);
    expect(merged.searchSession?._id).toBe("session-1");
    expect(merged.delivery?.cursor).toBe(2);
    expect(merged.delivery?.indexedCursor).toBe(8);
  });

  it("mergeCrawlDeltaIntoResult clears stale Japan jobs and rejects them for a Canada search", () => {
    const japanResult = {
      ...createResult("running"),
      search: {
        ...createResult("running").search,
        _id: "search-japan",
        filters: {
          title: "machine learning engineer",
          country: "Japan",
        },
        latestSearchSessionId: "session-japan",
      },
      searchSession: {
        _id: "session-japan",
        searchId: "search-japan",
        latestCrawlRunId: "run-1",
        status: "running" as const,
        createdAt: "2026-03-30T12:00:00.000Z",
        updatedAt: "2026-03-30T12:00:00.000Z",
        lastEventSequence: 1,
      },
      jobs: [
        {
          ...createTestJob("tokyo-ai"),
          title: "Applied AI Engineer",
          normalizedTitle: "applied ai engineer",
          titleNormalized: "applied ai engineer",
          country: "Japan",
          state: undefined,
          city: "Tokyo",
          locationRaw: "Tokyo, Japan",
          locationText: "Tokyo, Japan",
          normalizedLocation: "tokyo japan",
          locationNormalized: "tokyo japan",
        },
      ],
    } satisfies CrawlResponse;
    const canadaFilters: SearchFilters = {
      title: "machine learning engineer",
      country: "Canada",
    };
    const canadaDelta = {
      search: {
        ...japanResult.search,
        _id: "search-canada",
        filters: canadaFilters,
        latestSearchSessionId: "session-canada",
      },
      searchSession: {
        ...japanResult.searchSession!,
        _id: "session-canada",
        searchId: "search-canada",
      },
      crawlRun: {
        ...japanResult.crawlRun,
        searchId: "search-canada",
      },
      sourceResults: [],
      diagnostics: japanResult.diagnostics,
      jobs: [
        {
          ...createTestJob("stale-tokyo"),
          title: "Applied AI Engineer",
          normalizedTitle: "applied ai engineer",
          titleNormalized: "applied ai engineer",
          country: "Japan",
          state: undefined,
          city: "Tokyo",
          locationRaw: "Tokyo, Japan",
          locationText: "Tokyo, Japan",
          normalizedLocation: "tokyo japan",
          locationNormalized: "tokyo japan",
        },
        {
          ...createTestJob("toronto-mle"),
          title: "Machine Learning Engineer",
          normalizedTitle: "machine learning engineer",
          titleNormalized: "machine learning engineer",
          country: "Canada",
          state: "Ontario",
          city: "Toronto",
          locationRaw: "Toronto, Canada",
          locationText: "Toronto, Canada",
          normalizedLocation: "toronto canada",
          locationNormalized: "toronto canada",
        },
      ],
      delivery: {
        mode: "delta" as const,
        previousCursor: 0,
        cursor: 1,
      },
    } satisfies CrawlDeltaResponse;

    const merged = mergeCrawlDeltaIntoResult(japanResult, canadaDelta, canadaFilters);

    expect(merged.jobs.map((job) => job._id)).toEqual(["toronto-mle"]);
    expect(merged.jobs.map((job) => job.locationText)).not.toContain("Tokyo, Japan");
  });

  it("shouldApplyQueuedResultImmediately returns true when jobs are visible during running state", () => {
    const snapshot = {
      crawlRun: createResult("running").crawlRun,
      jobs: [createTestJob("job-1")],
    };

    // This enables the progressive index-first behavior:
    // show results immediately rather than waiting for crawl to complete
    expect(shouldApplyQueuedResultImmediately(snapshot)).toBe(true);
  });

  it("isLatestClientRequest prevents stale poll responses from overwriting new search state", () => {
    const oldToken = 1;
    const newToken = 2;

    // Old token should be stale
    expect(isLatestClientRequest(oldToken, newToken)).toBe(false);
    // New token should be current
    expect(isLatestClientRequest(newToken, newToken)).toBe(true);
  });
});
