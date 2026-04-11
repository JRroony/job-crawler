import { describe, expect, it } from "vitest";

import {
  buildSearchRequestPayload,
  describeZeroResultState,
  normalizeSearchFiltersForClient,
  resolveViewState,
} from "@/components/job-crawler-app";
import type { CrawlResponse, SearchFilters } from "@/lib/types";

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
});
