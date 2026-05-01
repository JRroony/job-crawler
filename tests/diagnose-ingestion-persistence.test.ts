import { describe, expect, it } from "vitest";

import {
  collectIngestionPersistenceFailures,
  createDiagnosticJobSeed,
  databaseNameFromMongoUri,
  extractPersistenceCounts,
  parseDiagnoseIngestionPersistenceArgs,
  uriHostFromMongoUri,
} from "@/scripts/diagnose-ingestion-persistence";

describe("ingestion persistence diagnostic script helpers", () => {
  it("parses optional CLI limits", () => {
    expect(
      parseDiagnoseIngestionPersistenceArgs([
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

  it("creates a diagnostic seed that can be indexed and found by normal title/location search", () => {
    const seed = createDiagnosticJobSeed({
      filters: {
        title: "software engineer",
        country: "United States",
      },
      diagnosticRunId: "diag-test",
      sourceToken: "diagtest",
      sourceIndex: 1,
      now: new Date("2026-05-01T12:00:00.000Z"),
    });

    expect(seed).toMatchObject({
      title: "software engineer",
      company: "Ingestion Persistence Diagnostics",
      locationText: "Remote - United States",
      remoteType: "remote",
      sourcePlatform: "greenhouse",
      sourceCompanySlug: "diagtest",
      sourceJobId: "diag-test-1",
      rawSourceMetadata: {
        source: "diagnose-ingestion-persistence",
        diagnosticRunId: "diag-test",
        greenhouseBoardToken: "diagtest",
      },
    });
    expect(seed.resolvedLocation).toMatchObject({
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
      confidence: "high",
    });
  });

  it("extracts the persistence counters emitted by background ingestion", () => {
    expect(
      extractPersistenceCounts({
        diagnostics: {
          backgroundPersistence: {
            jobsInserted: 2,
            jobsUpdated: 1,
            jobsLinkedToRun: 3,
            indexedEventsEmitted: 2,
          },
        },
      } as never),
    ).toEqual({
      insertedCount: 2,
      updatedCount: 1,
      linkedToRunCount: 3,
      indexedEventCount: 2,
    });
  });

  it("fails closed on memory fallback, empty persistence, and DB-search misses", () => {
    expect(
      collectIngestionPersistenceFailures({
        triggerStatus: "started",
        runStatus: "completed",
        fellBackToMemory: false,
        persistenceCounts: {
          insertedCount: 1,
          updatedCount: 0,
          linkedToRunCount: 1,
          indexedEventCount: 1,
        },
        matchingDiagnosticSearchJobs: 1,
        latestDiagnosticJobs: 1,
      }),
    ).toEqual([]);

    expect(
      collectIngestionPersistenceFailures({
        triggerStatus: "skipped-no-mongo",
        runStatus: "failed",
        fellBackToMemory: true,
        persistenceCounts: {
          insertedCount: 0,
          updatedCount: 0,
          linkedToRunCount: 0,
          indexedEventCount: 0,
        },
        matchingDiagnosticSearchJobs: 0,
        latestDiagnosticJobs: 0,
      }),
    ).toEqual([
      "Storage fell back to the in-memory database.",
      "Background ingestion did not start; status=skipped-no-mongo.",
      "Background ingestion did not finish successfully; status=failed.",
      "No jobs were inserted or updated by the controlled ingestion cycle.",
      "No persisted jobs were linked to the diagnostic crawl run.",
      "No indexed job events were emitted for the diagnostic crawl run.",
      "The latest jobs query did not include the diagnostic job by lastSeenAt.",
      "Normal search did not return the persisted diagnostic job from MongoDB.",
    ]);
  });
});
