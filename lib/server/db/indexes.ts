import "server-only";

import { collectionNames } from "@/lib/server/db/collections";
import {
  JobsCanonicalKeyMigrationError,
  migrateLegacyJobsForCanonicalKey,
} from "@/lib/server/db/job-migration";

type IndexSpec = {
  key: Record<string, 1 | -1>;
  name: string;
  unique?: boolean;
  sparse?: boolean;
  partialFilterExpression?: Record<string, unknown>;
};

export type CollectionLike = {
  createIndexes(indexes: IndexSpec[]): Promise<unknown>;
  listIndexes?(): { toArray(): Promise<IndexSpec[]> };
  dropIndex?(name: string): Promise<unknown>;
};

export type DatabaseLike = {
  collection(name: string): CollectionLike;
};

export { collectionNames } from "@/lib/server/db/collections";

let indexesEnsured = false;

export async function ensureDatabaseIndexes(db: DatabaseLike) {
  if (indexesEnsured) {
    return;
  }

  await db.collection(collectionNames.searches).createIndexes([
    {
      key: { createdAt: -1 },
      name: "searches_createdAt_desc",
    },
    {
      key: { latestCrawlRunId: 1 },
      name: "searches_latestCrawlRunId",
      sparse: true,
    },
    {
      key: { latestSearchSessionId: 1 },
      name: "searches_latestSearchSessionId",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.searchSessions).createIndexes([
    {
      key: { searchId: 1, createdAt: -1 },
      name: "searchSessions_searchId_createdAt_desc",
    },
    {
      key: { latestCrawlRunId: 1 },
      name: "searchSessions_latestCrawlRunId",
      sparse: true,
    },
    {
      key: { status: 1, updatedAt: -1 },
      name: "searchSessions_status_updatedAt_desc",
    },
  ]);

  try {
    const migration = await migrateLegacyJobsForCanonicalKey(db as never);
    if (
      migration.rewrittenCount > 0 ||
      migration.deletedCount > 0 ||
      migration.canonicalBackfillCount > 0 ||
      migration.lifecycleBackfillCount > 0 ||
      migration.duplicateCanonicalKeyCount > 0
    ) {
      console.info("[db:jobs-migration]", migration);
    }
  } catch (error) {
    if (error instanceof JobsCanonicalKeyMigrationError) {
      throw new Error(
        `${error.message} scanned=${error.diagnostics.scannedCount} rewritten=${error.diagnostics.rewrittenCount} merged=${error.diagnostics.mergedDocumentCount} deleted=${error.diagnostics.deletedCount}`,
        { cause: error },
      );
    }

    throw new Error(
      "Failed to migrate legacy jobs before creating jobs_canonical_job_key.",
      { cause: error },
    );
  }

  try {
    await ensureCanonicalJobKeyIndex(db.collection(collectionNames.jobs));
    await db.collection(collectionNames.jobs).createIndexes([
      {
        key: { crawlRunIds: 1, postedAt: -1, sourcePlatform: 1, title: 1 },
        name: "jobs_listing_by_run_and_sort",
      },
      {
        key: { sourcePlatform: 1, postedAt: -1, companyNormalized: 1, titleNormalized: 1 },
        name: "jobs_export_by_platform_and_postedAt",
      },
      {
        key: { sourceLookupKeys: 1 },
        name: "jobs_source_lookup_keys",
      },
      {
        key: { canonicalUrl: 1 },
        name: "jobs_canonical_url",
        sparse: true,
      },
      {
        key: { resolvedUrl: 1 },
        name: "jobs_resolved_url",
        sparse: true,
      },
      {
        key: { applyUrl: 1 },
        name: "jobs_apply_url",
      },
      {
        key: { sourceUrl: 1 },
        name: "jobs_source_url",
      },
      {
        key: { contentFingerprint: 1 },
        name: "jobs_content_fingerprint",
      },
      {
        key: { isActive: 1, lastSeenAt: -1, postedAt: -1 },
        name: "jobs_lifecycle_activity_lastSeenAt_desc",
      },
      {
        key: {
          isActive: 1,
          sourcePlatform: 1,
          "searchIndex.titleFamily": 1,
          postingDate: -1,
          lastSeenAt: -1,
        },
        name: "jobs_search_active_platform_family_recent",
      },
      {
        key: {
          "searchIndex.titleConceptIds": 1,
          isActive: 1,
          sourcePlatform: 1,
          postingDate: -1,
        },
        name: "jobs_search_title_concepts_active_recent",
      },
      {
        key: {
          "searchIndex.titleSearchKeys": 1,
          isActive: 1,
          sourcePlatform: 1,
          postingDate: -1,
        },
        name: "jobs_search_ready_title_active_recent",
      },
      {
        key: {
          "searchIndex.locationSearchKeys": 1,
          isActive: 1,
          postingDate: -1,
        },
        name: "jobs_search_ready_location_active_recent",
      },
      {
        key: {
          "searchIndex.experienceSearchKeys": 1,
          isActive: 1,
          postingDate: -1,
        },
        name: "jobs_search_ready_experience_active_recent",
      },
      {
        key: {
          "resolvedLocation.isUnitedStates": 1,
          "resolvedLocation.state": 1,
          city: 1,
          isActive: 1,
          postingDate: -1,
        },
        name: "jobs_search_location_activity_recent",
      },
      {
        key: {
          experienceLevel: 1,
          "experienceClassification.inferredLevel": 1,
          isActive: 1,
          postingDate: -1,
        },
        name: "jobs_search_experience_activity_recent",
      },
      {
        key: { linkStatus: 1, lastValidatedAt: -1 },
        name: "jobs_linkStatus_lastValidatedAt_desc",
      },
    ]);
  } catch (error) {
    console.error("[db:jobs-index-error]", {
      phase: "jobs_index_creation",
      ...formatMongoErrorDiagnostics(error),
    });
    throw new Error(
      `MongoDB jobs index creation failed after legacy canonicalJobKey migration. ${formatMongoErrorMessage(error)}`,
      { cause: error },
    );
  }

  await db.collection(collectionNames.crawlRuns).createIndexes([
    {
      key: { searchId: 1, startedAt: -1 },
      name: "crawlRuns_searchId_startedAt_desc",
    },
    {
      key: { searchSessionId: 1, startedAt: -1 },
      name: "crawlRuns_searchSessionId_startedAt_desc",
      sparse: true,
    },
    {
      key: { status: 1, startedAt: -1 },
      name: "crawlRuns_status_startedAt_desc",
    },
    {
      key: { validationMode: 1, startedAt: -1 },
      name: "crawlRuns_validationMode_startedAt_desc",
    },
  ]);

  await db.collection(collectionNames.crawlControls).createIndexes([
    {
      key: { crawlRunId: 1 },
      name: "crawlControls_crawlRunId",
      unique: true,
    },
    {
      key: { searchId: 1, status: 1, updatedAt: -1 },
      name: "crawlControls_searchId_status_updatedAt_desc",
    },
    {
      key: { ownerKey: 1, status: 1, updatedAt: -1 },
      name: "crawlControls_ownerKey_status_updatedAt_desc",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.crawlQueue).createIndexes([
    {
      key: { crawlRunId: 1 },
      name: "crawlQueue_crawlRunId",
      unique: true,
    },
    {
      key: { searchId: 1, status: 1, updatedAt: -1 },
      name: "crawlQueue_searchId_status_updatedAt_desc",
    },
    {
      key: { ownerKey: 1, status: 1, updatedAt: -1 },
      name: "crawlQueue_ownerKey_status_updatedAt_desc",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.crawlSourceResults).createIndexes([
    {
      key: { crawlRunId: 1, provider: 1 },
      name: "crawlSourceResults_run_provider",
    },
    {
      key: { searchId: 1, finishedAt: -1 },
      name: "crawlSourceResults_searchId_finishedAt_desc",
    },
  ]);

  await db.collection(collectionNames.crawlRunJobEvents).createIndexes([
    {
      key: { crawlRunId: 1, sequence: 1 },
      name: "crawlRunJobEvents_run_sequence",
      unique: true,
    },
    {
      key: { crawlRunId: 1, jobId: 1 },
      name: "crawlRunJobEvents_run_job",
    },
  ]);

  await db.collection(collectionNames.searchSessionJobEvents).createIndexes([
    {
      key: { searchSessionId: 1, sequence: 1 },
      name: "searchSessionJobEvents_session_sequence",
      unique: true,
    },
    {
      key: { searchSessionId: 1, jobId: 1 },
      name: "searchSessionJobEvents_session_job",
    },
    {
      key: { searchSessionId: 1, crawlRunId: 1, sequence: 1 },
      name: "searchSessionJobEvents_session_run_sequence",
    },
  ]);

  await db.collection(collectionNames.indexedJobEvents).createIndexes([
    {
      key: { sequence: 1 },
      name: "indexedJobEvents_sequence",
      unique: true,
    },
    {
      key: { jobId: 1, sequence: 1 },
      name: "indexedJobEvents_job_sequence",
    },
    {
      key: { crawlRunId: 1, sequence: 1 },
      name: "indexedJobEvents_run_sequence",
    },
  ]);

  await db.collection(collectionNames.linkValidations).createIndexes([
    {
      key: { jobId: 1, checkedAt: -1 },
      name: "linkValidations_job_checkedAt_desc",
    },
    {
      key: { applyUrl: 1, checkedAt: -1 },
      name: "linkValidations_applyUrl_checkedAt_desc",
    },
  ]);

  await db.collection(collectionNames.sourceInventory).createIndexes([
    {
      key: { platform: 1, status: 1, crawlPriority: 1, companyHint: 1 },
      name: "sourceInventory_platform_status_priority_companyHint",
    },
    {
      key: { token: 1, platform: 1 },
      name: "sourceInventory_token_platform",
      sparse: true,
    },
    {
      key: { lastRefreshedAt: -1, platform: 1 },
      name: "sourceInventory_lastRefreshedAt_desc_platform",
    },
    {
      key: { lastCrawledAt: -1, platform: 1, health: 1 },
      name: "sourceInventory_lastCrawledAt_desc_platform_health",
      sparse: true,
    },
  ]);

  indexesEnsured = true;
}

const canonicalJobKeyIndex: IndexSpec = {
  key: { canonicalJobKey: 1 },
  name: "jobs_canonical_job_key",
  unique: true,
  partialFilterExpression: {
    canonicalJobKey: {
      $type: "string",
      $gt: "",
    },
  },
};

async function ensureCanonicalJobKeyIndex(collection: CollectionLike) {
  const indexes = collection.listIndexes ? await collection.listIndexes().toArray() : [];
  const incompatibleIndexes = indexes.filter(isIncompatibleCanonicalJobKeyIndex);

  for (const index of incompatibleIndexes) {
    if (!collection.dropIndex) {
      console.warn("[db:jobs-index-repair-skipped]", {
        indexName: index.name,
        reason: "collection_adapter_missing_dropIndex",
      });
      continue;
    }

    console.warn("[db:jobs-index-repair]", {
      action: "drop_incompatible_canonical_job_key_index",
      indexName: index.name,
      key: index.key,
      unique: index.unique,
      sparse: index.sparse,
      partialFilterExpression: index.partialFilterExpression,
    });
    await collection.dropIndex(index.name);
  }

  await collection.createIndexes([canonicalJobKeyIndex]);
}

function isIncompatibleCanonicalJobKeyIndex(index: IndexSpec) {
  if (!isCanonicalJobKeyIndex(index)) {
    return false;
  }

  return !indexMatchesCanonicalJobKeyIndex(index);
}

function isCanonicalJobKeyIndex(index: IndexSpec) {
  return index.name === canonicalJobKeyIndex.name || sameIndexKey(index.key, canonicalJobKeyIndex.key);
}

function indexMatchesCanonicalJobKeyIndex(index: IndexSpec) {
  return (
    index.name === canonicalJobKeyIndex.name &&
    index.unique === true &&
    sameIndexKey(index.key, canonicalJobKeyIndex.key) &&
    JSON.stringify(index.partialFilterExpression ?? {}) ===
      JSON.stringify(canonicalJobKeyIndex.partialFilterExpression)
  );
}

function sameIndexKey(left: Record<string, 1 | -1>, right: Record<string, 1 | -1>) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function formatMongoErrorDiagnostics(error: unknown) {
  const record = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const cause = record.cause && typeof record.cause === "object"
    ? record.cause as Record<string, unknown>
    : undefined;

  return {
    code: record.code ?? cause?.code,
    codeName: record.codeName ?? cause?.codeName,
    message: error instanceof Error ? error.message : String(error),
    keyValue: record.keyValue ?? cause?.keyValue,
    keyPattern: record.keyPattern ?? cause?.keyPattern,
  };
}

function formatMongoErrorMessage(error: unknown) {
  const diagnostics = formatMongoErrorDiagnostics(error);
  return [
    diagnostics.code ? `code=${String(diagnostics.code)}` : undefined,
    diagnostics.codeName ? `codeName=${String(diagnostics.codeName)}` : undefined,
    diagnostics.message ? `message=${diagnostics.message}` : undefined,
    diagnostics.keyValue ? `keyValue=${JSON.stringify(diagnostics.keyValue)}` : undefined,
  ]
    .filter(Boolean)
    .join(" ");
}

export function resetDatabaseIndexesForTests() {
  indexesEnsured = false;
}
