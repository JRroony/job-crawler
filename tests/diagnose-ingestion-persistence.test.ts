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

  it("fails closed on memory fallback, empty persistence, running providers, contamination, and request-time budgets", () => {
    expect(
      collectIngestionPersistenceFailures({
        triggerStatus: "started",
        runStatus: "completed",
        fellBackToMemory: false,
        jobsBefore: 10,
        jobsAfter: 11,
        persistenceCounts: {
          insertedCount: 1,
          updatedCount: 0,
          linkedToRunCount: 1,
          indexedEventCount: 1,
        },
        runningProviderCountAfterFinalize: 0,
        sourceInventoryContaminationCount: 0,
        backgroundProviderTimeoutMs: 120_000,
      }),
    ).toEqual([]);

    expect(
      collectIngestionPersistenceFailures({
        triggerStatus: "skipped-no-mongo",
        runStatus: "failed",
        fellBackToMemory: true,
        jobsBefore: 10,
        jobsAfter: 10,
        persistenceCounts: {
          insertedCount: 0,
          updatedCount: 0,
          linkedToRunCount: 0,
          indexedEventCount: 0,
        },
        runningProviderCountAfterFinalize: 1,
        sourceInventoryContaminationCount: 2,
        backgroundProviderTimeoutMs: 9_000,
      }),
    ).toEqual([
      "Storage fell back to the in-memory database.",
      "Background ingestion did not start; status=skipped-no-mongo.",
      "Background ingestion did not finish successfully; status=failed.",
      "No jobs were inserted or updated by the controlled ingestion cycle.",
      "MongoDB jobs did not increase and no existing jobs were updated; jobsBefore=10, jobsAfter=10.",
      "1 crawlSourceResult document(s) remained running after crawlRun finalization.",
      "2 sourceInventory record(s) still have cross-source contaminated lastFailureReason values.",
      "Background ingestion used the request-time 9000ms provider timeout.",
    ]);
  });
});
