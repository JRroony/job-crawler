import { describe, expect, it } from "vitest";

import {
  describeZeroResultState,
  resolveViewState,
} from "@/components/job-crawler-app";
import type { CrawlResponse } from "@/lib/types";

function createResult(status: CrawlResponse["crawlRun"]["status"]): CrawlResponse {
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
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
    },
    sourceResults: [
      {
        _id: "source-1",
        crawlRunId: "run-1",
        searchId: "search-1",
        provider: "greenhouse",
        status: status === "completed" ? "success" : "failed",
        fetchedCount: 0,
        matchedCount: 0,
        savedCount: 0,
        errorMessage:
          status === "completed" ? undefined : "Provider request failed.",
        startedAt: "2026-03-30T12:00:00.000Z",
        finishedAt: "2026-03-30T12:05:00.000Z",
      },
    ],
    jobs: [],
  };
}

describe("job crawler app result state", () => {
  it("treats failed zero-job crawls as provider failures", () => {
    const result = createResult("failed");

    expect(resolveViewState(result)).toBe("error");
    expect(describeZeroResultState(result)).toEqual({
      title: "Providers failed before results could be returned",
      description:
        "The crawl did not complete successfully. Retry the crawl after checking provider connectivity or endpoint configuration.",
    });
  });

  it("treats partial zero-job crawls as provider issues instead of no matches", () => {
    const result = createResult("partial");

    expect(resolveViewState(result)).toBe("partial");
    expect(describeZeroResultState(result)).toEqual({
      title: "Providers had issues during the crawl",
      description:
        "Some providers failed and the remaining sources did not produce any saved matches. Retry the crawl or broaden the filters.",
    });
  });

  it("keeps completed zero-job crawls mapped to the no-match state", () => {
    const result = createResult("completed");

    expect(resolveViewState(result)).toBe("empty");
    expect(describeZeroResultState(result)).toEqual({
      title: "No matching jobs yet",
      description:
        "The crawl finished, but none of the public-source results matched the current filters. Adjust the title or location and rerun.",
    });
  });
});
