import { describe, expect, it } from "vitest";

import {
  buildSearchRequestPayload,
  describeResultNotice,
  describeZeroResultState,
  isLatestClientRequest,
  mergeCrawlDeltaIntoResult,
  normalizeSearchFiltersForClient,
  resolveViewState,
} from "@/components/job-crawler-app";
import type { CrawlDeltaResponse, CrawlResponse, SearchFilters } from "@/lib/types";
import { queueSearchRun, isSearchRunPending, abortSearchRun } from "@/lib/server/crawler/background-runs";

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
    dedupeFingerprint: jobId,
    contentFingerprint: jobId,
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

  it("explains that visible jobs can arrive before supplemental recall finishes", () => {
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

    expect(describeResultNotice(result)).toMatchObject({
      title: "Initial jobs are visible while supplemental recall keeps running",
      highlights: expect.arrayContaining([
        "Fast mode keeps heavier recall work behind the first visible batch.",
      ]),
    });
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
    let executed = false;

    const queued = queueSearchRun(searchId, async (signal) => {
      executed = true;
      // Wait briefly so we can test abort
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, 5000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          resolve();
        }, { once: true });
      });
    });

    expect(queued).toBe(true);
    expect(isSearchRunPending(searchId)).toBe(true);

    // Clean up: abort to finish the test quickly
    await abortSearchRun(searchId, { awaitCompletion: true });
    expect(executed).toBe(true);
  });

  it("abortSearchRun aborts the pending task", async () => {
    const searchId = `test-abort-signal-${Date.now()}`;
    let aborted = false;

    queueSearchRun(searchId, async (signal) => {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, 30000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          aborted = true;
          resolve();
        }, { once: true });
      });
    });

    expect(isSearchRunPending(searchId)).toBe(true);
    const result = await abortSearchRun(searchId, { awaitCompletion: true });
    expect(result).toBe(true);
    expect(aborted).toBe(true);
    expect(isSearchRunPending(searchId)).toBe(false);
  });

  it("abortSearchRun on non-existent run returns false", async () => {
    const searchId = `test-non-existent-${Date.now()}`;
    const result = await abortSearchRun(searchId);
    expect(result).toBe(false);
  });
});
