import { pathToFileURL } from "node:url";

import { MongoClient } from "mongodb";

type CrawlRunDiagnosticRecord = {
  _id: string;
  status?: unknown;
  finishedAt?: unknown;
};

type CrawlSourceResultDiagnosticRecord = {
  _id: string;
  crawlRunId?: unknown;
  searchId?: unknown;
  provider?: unknown;
  status?: unknown;
  sourceCount?: unknown;
  fetchedCount?: unknown;
  matchedCount?: unknown;
  savedCount?: unknown;
  warningCount?: unknown;
  errorMessage?: unknown;
  finishedAt?: unknown;
};

type ProviderLifecycleViolation = {
  crawlRunId: string;
  crawlRunStatus: string;
  crawlRunFinishedAt?: string;
  sourceResultId: string;
  provider?: string;
  sourceCount: number;
  fetchedCount: number;
  matchedCount: number;
  savedCount: number;
};

const prefix = "[provider-lifecycle:diagnose]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const crawlRunsCollectionName = "crawlRuns";
const crawlSourceResultsCollectionName = "crawlSourceResults";
const terminalRunStatuses = new Set(["completed", "partial", "failed", "aborted"]);
const maxViolationSamples = 25;

async function main() {
  const shouldFinalizeStaleRunningProviders = process.argv.includes(
    "--finalize-stale-running-providers",
  );
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const serverSelectionTimeoutMS = Number(
    process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "1500",
  );
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS });

  try {
    await client.connect();
    const dbName = databaseNameFromMongoUri(mongoUri);
    const db = client.db(dbName);
    const crawlRuns = db.collection<CrawlRunDiagnosticRecord>(crawlRunsCollectionName);
    const crawlSourceResults = db.collection<CrawlSourceResultDiagnosticRecord>(
      crawlSourceResultsCollectionName,
    );

    const terminalRuns = await crawlRuns
      .find(
        { status: { $in: Array.from(terminalRunStatuses) } },
        {
          projection: {
            _id: 1,
            status: 1,
            finishedAt: 1,
          },
        },
      )
      .toArray();
    const terminalRunById = new Map(
      terminalRuns.map((run) => [readString(run._id), run] as const),
    );
    const runningSourceResults = await crawlSourceResults
      .find(
        { status: "running" },
        {
          projection: {
            _id: 1,
            crawlRunId: 1,
            searchId: 1,
            provider: 1,
            status: 1,
            sourceCount: 1,
            fetchedCount: 1,
            matchedCount: 1,
            savedCount: 1,
            warningCount: 1,
            errorMessage: 1,
            finishedAt: 1,
          },
        },
      )
      .toArray();
    const violations = runningSourceResults.flatMap((sourceResult) => {
      const crawlRunId = readString(sourceResult.crawlRunId);
      if (!crawlRunId) {
        return [];
      }

      const crawlRun = terminalRunById.get(crawlRunId);
      const crawlRunStatus = readString(crawlRun?.status);
      if (!crawlRun || !crawlRunStatus || !terminalRunStatuses.has(crawlRunStatus)) {
        return [];
      }

      return [
        {
          crawlRunId,
          crawlRunStatus,
          crawlRunFinishedAt: readString(crawlRun.finishedAt),
          sourceResultId: readString(sourceResult._id) ?? "",
          provider: readString(sourceResult.provider),
          sourceCount: readNumber(sourceResult.sourceCount),
          fetchedCount: readNumber(sourceResult.fetchedCount),
          matchedCount: readNumber(sourceResult.matchedCount),
          savedCount: readNumber(sourceResult.savedCount),
        },
      ];
    });

    if (violations.length > 0) {
      if (shouldFinalizeStaleRunningProviders) {
        const finishedAt = new Date().toISOString();
        let modifiedCount = 0;

        for (const violation of violations) {
          const sourceResult = runningSourceResults.find(
            (candidate) => readString(candidate._id) === violation.sourceResultId,
          );
          if (!sourceResult) {
            continue;
          }

          const terminalStatus = resolveRepairStatus(violation);
          const update = {
            status: terminalStatus,
            warningCount: readNumber(sourceResult.warningCount) + 1,
            errorMessage: resolveRepairErrorMessage(sourceResult, terminalStatus),
            finishedAt,
          };
          const result = await crawlSourceResults.updateOne(
            { _id: violation.sourceResultId, status: "running" },
            { $set: update },
          );
          modifiedCount += result.modifiedCount;
        }

        console.log(
          `${prefix} ${JSON.stringify(
            {
              status: "repaired",
              databaseName: dbName,
              crawlRunsCollectionName,
              crawlSourceResultsCollectionName,
              terminalRuns: terminalRuns.length,
              staleRunningProviderCount: violations.length,
              modifiedCount,
              sampleViolations: violations.slice(0, maxViolationSamples),
            },
            null,
            2,
          )}`,
        );
        return;
      }

      console.error(
        `${prefix} ${JSON.stringify(
          {
            status: "failed",
            databaseName: dbName,
            crawlRunsCollectionName,
            crawlSourceResultsCollectionName,
            terminalRuns: terminalRuns.length,
            runningProviderResults: runningSourceResults.length,
            violationCount: violations.length,
            sampleViolations: violations.slice(0, maxViolationSamples),
          },
          null,
          2,
        )}`,
      );
      process.exitCode = 1;
      return;
    }

    console.log(
      `${prefix} ${JSON.stringify(
        {
          status: "passed",
          databaseName: dbName,
          crawlRunsCollectionName,
          crawlSourceResultsCollectionName,
          terminalRuns: terminalRuns.length,
          runningProviderResults: runningSourceResults.length,
          violationCount: 0,
        },
        null,
        2,
      )}`,
    );
  } finally {
    await client.close();
  }
}

function resolveRepairStatus(
  violation: ProviderLifecycleViolation,
): "partial" | "failed" | "aborted" | "unsupported" {
  if (violation.sourceCount === 0 && violation.savedCount === 0) {
    return "unsupported";
  }

  if (violation.savedCount > 0 && violation.crawlRunStatus !== "aborted") {
    return "partial";
  }

  return violation.crawlRunStatus === "aborted" ? "aborted" : "failed";
}

function resolveRepairErrorMessage(
  sourceResult: CrawlSourceResultDiagnosticRecord,
  status: "partial" | "failed" | "aborted" | "unsupported",
) {
  if (status === "unsupported") {
    return "no_sources_for_provider";
  }

  return readString(sourceResult.errorMessage) ?? "provider_lifecycle_stale_finalized";
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function readNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function databaseNameFromMongoUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const dbName = parsed.pathname.replace(/^\//, "");
    return dbName || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          status: "failed",
          error: error instanceof Error ? error.message : "Unknown diagnostic failure",
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
