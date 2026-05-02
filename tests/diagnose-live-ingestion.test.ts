import { describe, expect, it } from "vitest";

import {
  collectLiveIngestionFailures,
  parseDiagnoseLiveIngestionArgs,
} from "@/scripts/diagnose-live-ingestion";

describe("live ingestion diagnostic script helpers", () => {
  it("parses optional CLI limits and Greenhouse board token", () => {
    expect(
      parseDiagnoseLiveIngestionArgs([
        "--timeout-ms",
        "2500",
        "--board-token",
        "Greenhouse!",
      ]),
    ).toEqual({
      timeoutMs: 2500,
      boardToken: "greenhouse",
    });
  });

  it("passes when live Greenhouse background ingestion persisted jobs and finalized providers", () => {
    expect(
      collectLiveIngestionFailures({
        triggerStatus: "started",
        runStatus: "completed",
        jobsBefore: 10,
        jobsAfter: 11,
        persistenceCounts: {
          insertedCount: 1,
          updatedCount: 0,
          linkedToRunCount: 1,
          indexedEventCount: 1,
        },
        providerTimeoutMs: 120_000,
        runningProviderCountAfterFinalize: 0,
        providerResults: [
          {
            provider: "greenhouse",
            status: "completed",
            sourceCount: 1,
            fetchedCount: 3,
            matchedCount: 3,
            savedCount: 3,
          },
        ],
      }),
    ).toEqual([]);
  });

  it("fails closed on request-time budget, empty provider counts, empty persistence, and running providers", () => {
    expect(
      collectLiveIngestionFailures({
        triggerStatus: "started",
        runStatus: "completed",
        jobsBefore: 10,
        jobsAfter: 10,
        persistenceCounts: {
          insertedCount: 0,
          updatedCount: 0,
          linkedToRunCount: 0,
          indexedEventCount: 0,
        },
        providerTimeoutMs: 9_000,
        runningProviderCountAfterFinalize: 1,
        providerResults: [
          {
            provider: "greenhouse",
            status: "running",
            sourceCount: 1,
            fetchedCount: 0,
            matchedCount: 0,
            savedCount: 0,
          },
        ],
      }),
    ).toEqual([
      "Live background ingestion used the request-time 9000ms provider timeout.",
      "Greenhouse crawlSourceResult remained running after crawlRun finalization.",
      "Greenhouse fetchedCount is 0 for the controlled live board.",
      "Greenhouse matchedCount is 0 for the controlled live board.",
      "Greenhouse savedCount is 0 for the controlled live board.",
      "No jobs were inserted or updated by the controlled live ingestion cycle.",
      "MongoDB jobs did not increase and no existing jobs were updated; jobsBefore=10, jobsAfter=10.",
      "1 crawlSourceResult document(s) remained running after crawlRun finalization.",
    ]);
  });
});
