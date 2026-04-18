import "server-only";

import { collectionNames } from "@/lib/server/db/collections";
import type { CollectionAdapter, DatabaseAdapter } from "@/lib/server/db/repository";
import { mergeStoredJobs, parseStoredJob } from "@/lib/server/db/repository";
import type { JobListing } from "@/lib/types";

type JobMigrationCollection = CollectionAdapter<Record<string, unknown>>;

export type JobsCanonicalKeyMigrationResult = {
  scannedCount: number;
  canonicalBackfillCount: number;
  lifecycleBackfillCount: number;
  rewrittenCount: number;
  mergedDocumentCount: number;
  duplicateGroupCount: number;
  deletedCount: number;
};

export class JobsCanonicalKeyMigrationError extends Error {
  constructor(
    message: string,
    readonly diagnostics: JobsCanonicalKeyMigrationResult,
    options?: { cause?: unknown },
  ) {
    super(message, options);
    this.name = "JobsCanonicalKeyMigrationError";
  }
}

export async function migrateLegacyJobsForCanonicalKey(
  db: DatabaseAdapter,
): Promise<JobsCanonicalKeyMigrationResult> {
  const collection = db.collection<Record<string, unknown>>(collectionNames.jobs);
  const storedDocuments = await collection.find({}).toArray();
  const diagnostics: JobsCanonicalKeyMigrationResult = {
    scannedCount: storedDocuments.length,
    canonicalBackfillCount: 0,
    lifecycleBackfillCount: 0,
    rewrittenCount: 0,
    mergedDocumentCount: 0,
    duplicateGroupCount: 0,
    deletedCount: 0,
  };

  if (storedDocuments.length === 0) {
    return diagnostics;
  }

  try {
    const groups = new Map<
      string,
      Array<{ raw: Record<string, unknown>; normalized: JobListing }>
    >();

    for (const raw of storedDocuments) {
      const normalized = parseStoredJob(raw);
      if (!normalizeString(raw.canonicalJobKey)) {
        diagnostics.canonicalBackfillCount += 1;
      }

      if (isLifecycleBackfillNeeded(raw, normalized)) {
        diagnostics.lifecycleBackfillCount += 1;
      }

      const group = groups.get(normalized.canonicalJobKey);
      if (group) {
        group.push({ raw, normalized });
      } else {
        groups.set(normalized.canonicalJobKey, [{ raw, normalized }]);
      }
    }

    const operations: Array<
      | { deleteOne: { filter: Record<string, unknown> } }
      | {
          updateOne: {
            filter: Record<string, unknown>;
            update: Record<string, unknown>;
            options?: Record<string, unknown>;
          };
        }
    > = [];

    for (const group of groups.values()) {
      const survivor = group.reduce((current, candidate) => ({
        raw: current.raw,
        normalized: mergeStoredJobs(current.normalized, candidate.normalized),
      }));
      const duplicateDocuments = group.filter(
        (candidate) => candidate.normalized._id !== survivor.normalized._id,
      );

      if (duplicateDocuments.length > 0) {
        diagnostics.duplicateGroupCount += 1;
        diagnostics.mergedDocumentCount += duplicateDocuments.length;
        diagnostics.deletedCount += duplicateDocuments.length;
      }

      const rawSurvivor =
        group.find((candidate) => candidate.normalized._id === survivor.normalized._id)?.raw ??
        group[0]?.raw;

      if (!rawSurvivor) {
        continue;
      }

      if (!documentMatchesNormalizedShape(rawSurvivor, survivor.normalized)) {
        diagnostics.rewrittenCount += 1;
        operations.push({
          updateOne: {
            filter: { _id: survivor.normalized._id },
            update: { $set: serializeNormalizedDocumentForUpdate(survivor.normalized) },
          },
        });
      }

      for (const duplicate of duplicateDocuments) {
        operations.push({
          deleteOne: {
            filter: { _id: duplicate.normalized._id },
          },
        });
      }
    }

    await applyMigrationOperations(collection, operations);
    return diagnostics;
  } catch (error) {
    throw new JobsCanonicalKeyMigrationError(
      "Failed to migrate legacy jobs before enforcing jobs.canonicalJobKey uniqueness.",
      diagnostics,
      { cause: error },
    );
  }
}

async function applyMigrationOperations(
  collection: JobMigrationCollection,
  operations: Array<
    | { deleteOne: { filter: Record<string, unknown> } }
    | {
        updateOne: {
          filter: Record<string, unknown>;
          update: Record<string, unknown>;
          options?: Record<string, unknown>;
        };
      }
  >,
) {
  if (operations.length === 0) {
    return;
  }

  if (collection.bulkWrite) {
    await collection.bulkWrite(operations);
    return;
  }

  throw new Error("The configured database adapter does not support bulkWrite for job migration.");
}

function serializeNormalizedDocumentForUpdate(document: JobListing) {
  const { _id: _ignoredId, ...updateFields } = document;
  return JSON.parse(JSON.stringify(updateFields)) as Record<string, unknown>;
}

function documentMatchesNormalizedShape(actual: unknown, expected: unknown): boolean {
  if (expected === undefined) {
    return actual === undefined || actual === null;
  }

  if (expected === null || typeof expected !== "object") {
    return actual === expected;
  }

  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) {
      return false;
    }

    return expected.every((entry, index) =>
      documentMatchesNormalizedShape((actual as unknown[])[index], entry),
    );
  }

  if (!actual || typeof actual !== "object" || Array.isArray(actual)) {
    return false;
  }

  return Object.entries(expected as Record<string, unknown>).every(([key, value]) =>
    documentMatchesNormalizedShape((actual as Record<string, unknown>)[key], value),
  );
}

function isLifecycleBackfillNeeded(raw: Record<string, unknown>, normalized: JobListing) {
  return (
    normalizeString(raw.firstSeenAt) !== normalized.firstSeenAt ||
    normalizeString(raw.lastSeenAt) !== normalized.lastSeenAt ||
    normalizeString(raw.indexedAt) !== normalized.indexedAt ||
    normalizeString(raw.contentHash) !== normalized.contentHash ||
    normalizeBoolean(raw.isActive) !== normalized.isActive ||
    normalizeString(raw.closedAt) !== normalized.closedAt
  );
}

function normalizeString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function normalizeBoolean(value: unknown) {
  return typeof value === "boolean" ? value : undefined;
}
