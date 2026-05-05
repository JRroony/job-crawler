import { describe, expect, it } from "vitest";

import {
  collectIngestionRootCauseFailures,
  databaseNameFromMongoUri,
  parseDiagnoseIngestionRootCauseArgs,
  uriHostFromMongoUri,
} from "@/scripts/diagnose-ingestion-root-cause";

describe("ingestion root-cause diagnostic helpers", () => {
  it("parses optional CLI limits", () => {
    expect(
      parseDiagnoseIngestionRootCauseArgs([
        "--timeout-ms",
        "2500",
        "--latest-limit",
        "3",
      ]),
    ).toEqual({
      timeoutMs: 2500,
      latestLimit: 3,
    });
  });

  it("uses the same MongoDB database-name default as the app", () => {
    expect(databaseNameFromMongoUri("mongodb://127.0.0.1:27017/job_crawler")).toBe(
      "job_crawler",
    );
    expect(databaseNameFromMongoUri("mongodb://127.0.0.1:27017")).toBe(
      "job_crawler",
    );
    expect(uriHostFromMongoUri("mongodb://127.0.0.1:27017/job_crawler")).toBe(
      "127.0.0.1:27017",
    );
  });

  it("fails closed on persistence, sequence, event, queue, and source-result blockers", () => {
    expect(
      collectIngestionRootCauseFailures({
        storageMode: "mongodb",
        bootstrapStatus: { status: "succeeded" },
        activeCrawlQueueCount: 0,
        terminalRunsWithRunningSourceResultsCount: 0,
        triggerStatus: "started",
        crawlRunStatus: "completed",
        jobsBefore: 10,
        jobsAfter: 11,
        persistenceCounts: {
          insertedCount: 1,
          updatedCount: 0,
          linkedToRunCount: 1,
          indexedEventCount: 1,
        },
        crawlRunJobEventsCount: 1,
        indexedEventLatestSequenceBefore: 4,
        indexedEventLatestSequenceAfter: 5,
        duplicateSequenceErrors: [],
        backgroundTaskErrors: [],
      }),
    ).toEqual([]);

    expect(
      collectIngestionRootCauseFailures({
        storageMode: "memory",
        bootstrapStatus: {
          status: "failed",
          errorMessage: "index initialization failed",
        },
        activeCrawlQueueCount: 2,
        terminalRunsWithRunningSourceResultsCount: 1,
        triggerStatus: "started",
        crawlRunStatus: "failed",
        jobsBefore: 10,
        jobsAfter: 10,
        persistenceCounts: {
          insertedCount: 0,
          updatedCount: 0,
          linkedToRunCount: 0,
          indexedEventCount: 0,
        },
        crawlRunJobEventsCount: 0,
        indexedEventLatestSequenceBefore: 4,
        indexedEventLatestSequenceAfter: 4,
        duplicateSequenceErrors: ["E11000 duplicate key error sequence 1"],
        backgroundTaskErrors: [
          {
            crawlRunId: "run-1",
            message: "task failed",
            crawlRunStatus: "completed",
          },
        ],
      }),
    ).toEqual([
      "A. storage resolved to memory fallback instead of MongoDB.",
      "A. MongoDB bootstrap failed; status=failed message=index initialization failed.",
      "G. controlled crawlRun did not finalize successfully; status=failed.",
      "D. fake-provider job reached no successful insert or update.",
      "D. jobs count did not increase and no existing job was updated; jobsBefore=10 jobsAfter=10.",
      "F. persistJobsWithStats did not link the fake job to the crawlRun.",
      "F. crawlRunJobEvents were not written for the controlled crawlRun.",
      "F. indexedJobEvents were not written for the controlled crawlRun.",
      "E. indexedJobEvents latest sequence did not advance; before=4 after=4.",
      "E. duplicate indexed/crawl event sequence error occurred: E11000 duplicate key error sequence 1",
      "G. background task error was not reflected in crawlRun failure diagnostics; crawlRunId=run-1 message=task failed.",
      "G. 1 terminal crawlRun(s) still have running crawlSourceResults.",
    ]);
  });
});
