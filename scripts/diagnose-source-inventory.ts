import { pathToFileURL } from "node:url";

import { MongoClient } from "mongodb";

type SourceInventoryDiagnosticRecord = {
  _id: string;
  token?: unknown;
  sourceKey?: unknown;
  companyHint?: unknown;
  lastFailureReason?: unknown;
};

type KnownSourceTerm = {
  ownerId: string;
  term: string;
  field: "id" | "token" | "sourceKey" | "companyHint";
};

const prefix = "[source-inventory:diagnose]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const sourceInventoryCollectionName = "sourceInventory";
const maxViolationSamples = 25;

const ignoredTerms = new Set([
  "career",
  "careers",
  "company",
  "corp",
  "corporation",
  "group",
  "inc",
  "jobs",
  "labs",
  "limited",
  "llc",
  "ltd",
  "software",
  "systems",
  "tech",
  "technologies",
  "technology",
  "the",
]);

async function main() {
  const shouldClearContaminatedFailureReasons = process.argv.includes(
    "--clear-contaminated-failure-reasons",
  );
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const serverSelectionTimeoutMS = Number(
    process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "1500",
  );
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS });

  try {
    await client.connect();
    const dbName = databaseNameFromMongoUri(mongoUri);
    const sourceInventory = client
      .db(dbName)
      .collection<SourceInventoryDiagnosticRecord>(sourceInventoryCollectionName);
    const records = await sourceInventory
      .find(
        {},
        {
          projection: {
            _id: 1,
            token: 1,
            sourceKey: 1,
            companyHint: 1,
            lastFailureReason: 1,
          },
        },
      )
      .toArray();
    const knownTerms = dedupeKnownSourceTerms(records.flatMap(collectKnownSourceTerms));
    const violations = findFailureReasonContamination(records, knownTerms);

    if (violations.length > 0) {
      if (shouldClearContaminatedFailureReasons) {
        const contaminatedSourceIds = Array.from(
          new Set(violations.map((violation) => violation.sourceId)),
        ).sort();
        const repairResult = await sourceInventory.updateMany(
          { _id: { $in: contaminatedSourceIds } },
          { $set: { lastFailureReason: null } },
        );
        console.log(
          `${prefix} ${JSON.stringify(
            {
              status: "repaired",
              databaseName: dbName,
              collectionName: sourceInventoryCollectionName,
              contaminatedSourceCount: contaminatedSourceIds.length,
              matchedCount: repairResult.matchedCount,
              modifiedCount: repairResult.modifiedCount,
              sampleSourceIds: contaminatedSourceIds.slice(0, maxViolationSamples),
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
            collectionName: sourceInventoryCollectionName,
            inventoryRecords: records.length,
            recordsWithFailureReason: records.filter(hasFailureReason).length,
            knownSourceTerms: knownTerms.length,
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
          collectionName: sourceInventoryCollectionName,
          inventoryRecords: records.length,
          recordsWithFailureReason: records.filter(hasFailureReason).length,
          knownSourceTerms: knownTerms.length,
        },
        null,
        2,
      )}`,
    );
  } finally {
    await client.close();
  }
}

function findFailureReasonContamination(
  records: SourceInventoryDiagnosticRecord[],
  knownTerms: KnownSourceTerm[],
) {
  return records.flatMap((record) => {
    const recordId = readString(record._id);
    const failureReason = readString(record.lastFailureReason);
    if (!recordId || !failureReason) {
      return [];
    }

    return knownTerms
      .filter((term) => term.ownerId !== recordId)
      .filter((term) => termPattern(term.term).test(failureReason))
      .map((term) => ({
        sourceId: recordId,
        lastFailureReason: failureReason,
        mentionedOtherSourceId: term.ownerId,
        matchedTerm: term.term,
        matchedField: term.field,
      }));
  });
}

function collectKnownSourceTerms(record: SourceInventoryDiagnosticRecord): KnownSourceTerm[] {
  const ownerId = readString(record._id);
  if (!ownerId) {
    return [];
  }

  const terms: KnownSourceTerm[] = [];
  pushTerm(terms, ownerId, "id", sourceIdToken(ownerId));
  pushTerm(terms, ownerId, "token", readString(record.token));
  pushTerm(terms, ownerId, "sourceKey", readString(record.sourceKey));

  const companyHint = readString(record.companyHint);
  if (companyHint) {
    pushTerm(terms, ownerId, "companyHint", companyHint);
    for (const part of companyHint.split(/[^a-z0-9]+/i)) {
      pushTerm(terms, ownerId, "companyHint", part);
    }
  }

  return terms;
}

function pushTerm(
  terms: KnownSourceTerm[],
  ownerId: string,
  field: KnownSourceTerm["field"],
  rawTerm: string | undefined,
) {
  const term = normalizeTerm(rawTerm);
  if (!term || term.length < 4 || ignoredTerms.has(term)) {
    return;
  }

  terms.push({ ownerId, field, term });
}

function dedupeKnownSourceTerms(terms: KnownSourceTerm[]) {
  const deduped = new Map<string, KnownSourceTerm>();
  for (const term of terms) {
    deduped.set(`${term.ownerId}:${term.field}:${term.term}`, term);
  }

  return Array.from(deduped.values());
}

function sourceIdToken(sourceId: string) {
  return sourceId.includes(":") ? sourceId.split(":").pop() : sourceId;
}

function termPattern(term: string) {
  const escaped = escapeRegExp(term).replace(/\s+/g, "\\s+");
  return new RegExp(`(^|[^a-z0-9])${escaped}($|[^a-z0-9])`, "i");
}

function normalizeTerm(value: string | undefined) {
  return value?.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function hasFailureReason(record: SourceInventoryDiagnosticRecord) {
  return Boolean(readString(record.lastFailureReason));
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
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

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
