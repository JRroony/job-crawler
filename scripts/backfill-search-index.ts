import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import {
  type AnyBulkWriteOperation,
  type Collection,
  MongoClient,
  type Document,
  type Filter,
} from "mongodb";

import type {
  SearchIndexBackfillJob,
  SearchIndexBackfillRepair,
} from "@/lib/server/search/search-index-backfill";

type CliOptions = {
  batchSize: number;
  dryRun: boolean;
  limit?: number;
};

type BackfillSummary = {
  scannedCount: number;
  updatedCount: number;
  unchangedCount: number;
  failedCount: number;
  missingTitleCount: number;
  missingLocationCount: number;
  ambiguousLocationCount: number;
  generatedTitleKeyCount: number;
  generatedLocationKeyCount: number;
  sampleUpdatedJobs: Array<Record<string, unknown>>;
  sampleFailedJobs: Array<Record<string, unknown>>;
};

const prefix = "[search-index:backfill]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const jobsCollectionName = "jobs";
const defaultBatchSize = 500;
const sampleLimit = 10;

async function main() {
  installServerOnlyShim();

  const { buildSearchIndexBackfillRepair, isBackfillLocationMissing } =
    await import("@/lib/server/search/search-index-backfill");
  const options = parseArgs(process.argv.slice(2));
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const serverSelectionTimeoutMS = Number(
    process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "1500",
  );
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS });
  const summary = createEmptySummary();

  try {
    await client.connect();
    const dbName = databaseNameFromMongoUri(mongoUri);
    const jobs = client.db(dbName).collection(jobsCollectionName);
    const cursor = jobs
      .find({}, { projection: buildProjection() })
      .sort({ _id: 1 })
      .limit(options.limit ?? 0);
    let operations: AnyBulkWriteOperation<Document>[] = [];

    for await (const document of cursor) {
      summary.scannedCount += 1;

      if (isBackfillLocationMissing(document as unknown as SearchIndexBackfillJob)) {
        summary.missingLocationCount += 1;
      }

      try {
        const repair = buildSearchIndexBackfillRepair(
          document as unknown as SearchIndexBackfillJob,
        );
        summary.generatedTitleKeyCount += repair.diagnostics.generatedTitleKeyCount;
        summary.generatedLocationKeyCount += repair.diagnostics.generatedLocationKeyCount;

        if (repair.diagnostics.ambiguousLocation) {
          summary.ambiguousLocationCount += 1;
        }

        if (repair.changedFields.length === 0) {
          summary.unchangedCount += 1;
          continue;
        }

        summary.updatedCount += 1;
        pushSample(summary.sampleUpdatedJobs, formatUpdatedSample(document, repair));

        if (!options.dryRun) {
          operations.push({
            updateOne: {
              filter: { _id: document._id } satisfies Filter<Document>,
              update: { $set: repair.update },
            },
          });
        }

        if (operations.length >= options.batchSize) {
          await flushBulkWrite(jobs, operations);
          operations = [];
        }
      } catch (error) {
        summary.failedCount += 1;

        if (isMissingTitleError(error)) {
          summary.missingTitleCount += 1;
        }

        pushSample(summary.sampleFailedJobs, {
          id: String(document._id ?? ""),
          title: document.title,
          company: document.company,
          locationText: document.locationText,
          locationRaw: document.locationRaw,
          error: error instanceof Error ? error.message : "Unknown backfill error",
        });
      }
    }

    if (operations.length > 0 && !options.dryRun) {
      await flushBulkWrite(jobs, operations);
    }

    console.log(
      `${prefix} ${JSON.stringify(
        {
          databaseName: dbName,
          collectionName: jobsCollectionName,
          dryRun: options.dryRun,
          batchSize: options.batchSize,
          ...summary,
        },
        null,
        2,
      )}`,
    );
  } finally {
    await client.close();
  }
}

function installServerOnlyShim() {
  const require = createRequire(import.meta.url);
  const id = require.resolve("server-only");
  require.cache[id] = {
    id,
    filename: id,
    loaded: true,
    exports: {},
  } as NodeJS.Module;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    batchSize: defaultBatchSize,
    dryRun: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    const next = argv[index + 1];

    if (value === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (value === "--batch-size" && next) {
      options.batchSize = positiveInteger(next, defaultBatchSize);
      index += 1;
      continue;
    }

    if (value === "--limit" && next) {
      options.limit = positiveInteger(next, 0) || undefined;
      index += 1;
    }
  }

  return options;
}

function positiveInteger(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function databaseNameFromMongoUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

function buildProjection() {
  return {
    _id: 1,
    title: 1,
    company: 1,
    normalizedTitle: 1,
    titleNormalized: 1,
    country: 1,
    state: 1,
    city: 1,
    locationText: 1,
    locationRaw: 1,
    normalizedLocation: 1,
    locationNormalized: 1,
    resolvedLocation: 1,
    remoteType: 1,
    experienceLevel: 1,
    experienceClassification: 1,
    sourcePlatform: 1,
    linkStatus: 1,
    rawSourceMetadata: 1,
    isActive: 1,
    postingDate: 1,
    postedAt: 1,
    lastSeenAt: 1,
    crawledAt: 1,
    discoveredAt: 1,
    indexedAt: 1,
    searchIndex: 1,
  };
}

async function flushBulkWrite(
  jobs: Collection<Document>,
  operations: AnyBulkWriteOperation<Document>[],
) {
  if (operations.length === 0) {
    return;
  }

  await jobs.bulkWrite(operations, { ordered: false });
}

function createEmptySummary(): BackfillSummary {
  return {
    scannedCount: 0,
    updatedCount: 0,
    unchangedCount: 0,
    failedCount: 0,
    missingTitleCount: 0,
    missingLocationCount: 0,
    ambiguousLocationCount: 0,
    generatedTitleKeyCount: 0,
    generatedLocationKeyCount: 0,
    sampleUpdatedJobs: [],
    sampleFailedJobs: [],
  };
}

function formatUpdatedSample(
  document: Document,
  repair: SearchIndexBackfillRepair,
) {
  return {
    id: String(document._id ?? ""),
    title: document.title,
    company: document.company,
    locationText: document.locationText,
    locationRaw: document.locationRaw,
    changedFields: repair.changedFields,
    titleFamily: repair.update.searchIndex.titleFamily,
    titleConceptIds: repair.update.searchIndex.titleConceptIds,
    titleSearchKeyCount: repair.update.searchIndex.titleSearchKeys.length,
    locationSearchKeyCount: repair.update.searchIndex.locationSearchKeys.length,
    resolvedLocation: repair.update.resolvedLocation
      ? {
          country: repair.update.resolvedLocation.country,
          state: repair.update.resolvedLocation.state,
          stateCode: repair.update.resolvedLocation.stateCode,
          city: repair.update.resolvedLocation.city,
          isRemote: repair.update.resolvedLocation.isRemote,
          isUnitedStates: repair.update.resolvedLocation.isUnitedStates,
          confidence: repair.update.resolvedLocation.confidence,
        }
      : undefined,
  };
}

function pushSample(
  target: Array<Record<string, unknown>>,
  sample: Record<string, unknown>,
) {
  if (target.length < sampleLimit) {
    target.push(sample);
  }
}

function isMissingTitleError(error: unknown) {
  return error instanceof Error && /without a title/i.test(error.message);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          error: error instanceof Error ? error.message : "Unknown backfill failure",
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
