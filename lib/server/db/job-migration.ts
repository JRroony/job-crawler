import "server-only";

import { collectionNames } from "@/lib/server/db/collections";
import { buildCanonicalJobIdentity } from "@/lib/job-identity";
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
  duplicateCanonicalKeyCount: number;
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

  const emptyResult: JobsCanonicalKeyMigrationResult = {
    scannedCount: 0,
    canonicalBackfillCount: 0,
    lifecycleBackfillCount: 0,
    rewrittenCount: 0,
    mergedDocumentCount: 0,
    duplicateGroupCount: 0,
    duplicateCanonicalKeyCount: 0,
    deletedCount: 0,
  };

  const migrationNeeded = await isLegacyCanonicalKeyMigrationNeeded(db);
  if (!migrationNeeded) {
    return emptyResult;
  }

  const storedDocuments = await collection.find({}).toArray();
  const diagnostics: JobsCanonicalKeyMigrationResult = {
    scannedCount: storedDocuments.length,
    canonicalBackfillCount: 0,
    lifecycleBackfillCount: 0,
    rewrittenCount: 0,
    mergedDocumentCount: 0,
    duplicateGroupCount: 0,
    duplicateCanonicalKeyCount: 0,
    deletedCount: 0,
  };

  if (storedDocuments.length === 0) {
    return diagnostics;
  }

  try {
    const rawCanonicalCounts = countCanonicalKeys(storedDocuments);
    diagnostics.duplicateCanonicalKeyCount = Array.from(rawCanonicalCounts.values()).filter(
      (count) => count > 1,
    ).length;
    const groups = new Map<string, Array<{ raw: Record<string, unknown>; normalized: JobListing }>>();

    for (const raw of storedDocuments) {
      const rawCanonicalJobKey = normalizeString(raw.canonicalJobKey);
      const normalized = parseStoredJob(raw);
      const repairedCanonicalJobKey = buildRepairCanonicalJobKey(normalized);
      const shouldRepairCanonicalJobKey =
        !rawCanonicalJobKey || (rawCanonicalCounts.get(rawCanonicalJobKey) ?? 0) > 1;

      if (!rawCanonicalJobKey) {
        diagnostics.canonicalBackfillCount += 1;
      }

      if (isLifecycleBackfillNeeded(raw, normalized)) {
        diagnostics.lifecycleBackfillCount += 1;
      }

      const normalizedForMigration = shouldRepairCanonicalJobKey
        ? { ...normalized, canonicalJobKey: repairedCanonicalJobKey }
        : normalized;

      const group = groups.get(normalizedForMigration.canonicalJobKey);
      if (group) {
        group.push({ raw, normalized: normalizedForMigration });
      } else {
        groups.set(normalizedForMigration.canonicalJobKey, [
          { raw, normalized: normalizedForMigration },
        ]);
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

async function isLegacyCanonicalKeyMigrationNeeded(
  db: DatabaseAdapter,
): Promise<boolean> {
  const collection = db.collection<Record<string, unknown>>(collectionNames.jobs);

  if (collection.listIndexes) {
    const indexes = await collection.listIndexes().toArray();
    const hasCanonicalKeyIndex = indexes.some(
      (index) => index.name === "jobs_canonical_job_key" && index.unique === true,
    );
    if (hasCanonicalKeyIndex) {
      return false;
    }
  }

  const totalSample = await collection.find({}).toArray();
  return totalSample.length > 0;
}

function countCanonicalKeys(documents: Record<string, unknown>[]) {
  const counts = new Map<string, number>();

  for (const document of documents) {
    const canonicalJobKey = normalizeString(document.canonicalJobKey);
    if (!canonicalJobKey) {
      continue;
    }

    counts.set(canonicalJobKey, (counts.get(canonicalJobKey) ?? 0) + 1);
  }

  return counts;
}

function buildRepairCanonicalJobKey(job: JobListing) {
  return buildCanonicalJobIdentity({
    _id: job._id,
    sourcePlatform: job.sourcePlatform,
    sourceCompanySlug: job.sourceCompanySlug,
    sourceJobId: job.sourceJobId,
    sourceUrl: job.sourceUrl,
    applyUrl: job.applyUrl,
    resolvedUrl: job.resolvedUrl,
    canonicalUrl: job.canonicalUrl,
    sourceLookupKeys: job.sourceLookupKeys,
    company: job.company,
    title: job.title,
    locationRaw: job.locationRaw,
    locationText: job.locationText,
    normalizedCompany: job.normalizedCompany,
    normalizedTitle: job.normalizedTitle,
    normalizedLocation: job.normalizedLocation,
    companyNormalized: job.companyNormalized,
    titleNormalized: job.titleNormalized,
    locationNormalized: job.locationNormalized,
    dedupeFingerprint: job.dedupeFingerprint,
    contentFingerprint: job.contentFingerprint,
  }).canonicalJobKey;
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
