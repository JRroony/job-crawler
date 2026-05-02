import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { MongoClient, type Db, type Document } from "mongodb";

import type { DatabaseAdapter } from "@/lib/server/db/repository";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import {
  createDeterministicFakeProvider,
  createDeterministicFakeSource,
  deterministicTestCompanySlug,
  deterministicTestJobId,
} from "@/scripts/support/deterministic-persistence-provider";

type PersistenceGateSummary = {
  storageMode: "mongodb";
  jobsBefore: number;
  jobsAfter: number;
  insertedCount: number;
  updatedCount: number;
  linkedToRunCount: number;
  indexedEventCount: number;
  crawlRunJobEventCount: number;
  runningProviderCount: number;
  pass: boolean;
  crawlRunId?: string;
  canonicalJobKey?: string;
  jobFoundBySourceJobId?: boolean;
  jobFoundByCanonicalJobKey?: boolean;
  failures?: string[];
  error?: string;
};

const prefix = "[persistence-gate:diagnose]";
const jobsCollectionName = "jobs";
const crawlRunJobEventsCollectionName = "crawlRunJobEvents";
const indexedJobEventsCollectionName = "indexedJobEvents";

async function main() {
  installServerOnlyShim();
  const [{ executeCrawlPipeline }, { ensureDatabaseIndexes }, { JobCrawlerRepository }, { getEnv }] =
    await Promise.all([
      import("@/lib/server/crawler/pipeline"),
      import("@/lib/server/db/indexes"),
      import("@/lib/server/db/repository"),
      import("@/lib/server/env"),
    ]);
  const env = getEnv();
  const client = new MongoClient(env.MONGODB_URI, {
    serverSelectionTimeoutMS: env.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
  });

  let summary: PersistenceGateSummary = {
    storageMode: "mongodb",
    jobsBefore: 0,
    jobsAfter: 0,
    insertedCount: 0,
    updatedCount: 0,
    linkedToRunCount: 0,
    indexedEventCount: 0,
    crawlRunJobEventCount: 0,
    runningProviderCount: 0,
    pass: false,
  };

  try {
    await client.connect();
    const db = client.db(databaseNameFromMongoUri(env.MONGODB_URI));
    await ensureDatabaseIndexes(db);

    const repository = new JobCrawlerRepository(db as unknown as DatabaseAdapter);
    const jobs = db.collection(jobsCollectionName);
    const now = new Date();
    const nowIso = now.toISOString();
    const fakeSource = createDeterministicFakeSource();
    const jobBefore = await findDeterministicJob(db);
    const jobsBefore = await jobs.countDocuments();
    const search = await repository.createSearch(
      {
        title: "Software Engineer",
        crawlMode: "balanced",
      },
      nowIso,
    );
    const searchSession = await repository.createSearchSession(search._id, nowIso, {
      status: "running",
    });
    const crawlRun = await repository.createCrawlRun(search._id, nowIso, {
      searchSessionId: searchSession._id,
      stage: "queued",
      validationMode: "deferred",
    });
    await repository.updateSearchLatestSession(search._id, searchSession._id, "running", nowIso);
    await repository.updateSearchLatestRun(search._id, crawlRun._id, "running", nowIso);
    await repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: nowIso,
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [fakeSource];
      },
    };

    await executeCrawlPipeline({
      search,
      searchSession,
      crawlRun,
      repository,
      discovery,
      providers: [createDeterministicFakeProvider()],
      fetchImpl: createFailingFetch(),
      now,
      linkValidationMode: "deferred",
      providerTimeoutMs: 5_000,
      sourceTimeoutMs: 5_000,
      progressUpdateIntervalMs: 1,
    });

    const jobsAfter = await jobs.countDocuments();
    const jobAfter = await findDeterministicJob(db);
    const canonicalJobKey = readString(jobAfter?.canonicalJobKey);
    const jobByCanonicalJobKey = canonicalJobKey
      ? await jobs.findOne({ canonicalJobKey })
      : null;
    const [crawlRunJobEventCount, indexedEventCount, sourceResults] =
      await Promise.all([
        db.collection(crawlRunJobEventsCollectionName).countDocuments({
          crawlRunId: crawlRun._id,
        }),
        db.collection(indexedJobEventsCollectionName).countDocuments({
          crawlRunId: crawlRun._id,
        }),
        repository.getCrawlSourceResults(crawlRun._id),
      ]);
    const runningProviderCount = sourceResults.filter(
      (sourceResult) => sourceResult.status === "running",
    ).length;
    const insertedCount = jobBefore ? 0 : jobAfter ? 1 : 0;
    const updatedCount = jobBefore && jobAfter ? 1 : 0;
    const linkedToRunCount = crawlRunJobEventCount;
    const failures = validatePersistenceGate({
      jobsBefore,
      jobsAfter,
      insertedCount,
      updatedCount,
      crawlRunJobEventCount,
      runningProviderCount,
      jobAfter,
      canonicalJobKey,
      jobByCanonicalJobKey,
    });

    summary = {
      storageMode: "mongodb",
      jobsBefore,
      jobsAfter,
      insertedCount,
      updatedCount,
      linkedToRunCount,
      indexedEventCount,
      crawlRunJobEventCount,
      runningProviderCount,
      pass: failures.length === 0,
      crawlRunId: crawlRun._id,
      canonicalJobKey,
      jobFoundBySourceJobId: Boolean(jobAfter),
      jobFoundByCanonicalJobKey: Boolean(jobByCanonicalJobKey),
      failures,
    };
  } catch (error) {
    summary = {
      ...summary,
      pass: false,
      error:
        error instanceof Error
          ? error.message
          : "Deterministic persistence gate failed unexpectedly.",
      failures: [
        error instanceof Error
          ? error.message
          : "Deterministic persistence gate failed unexpectedly.",
      ],
    };
  } finally {
    await client.close().catch(() => undefined);
  }

  console.log(`${prefix} ${JSON.stringify(summary, null, 2)}`);
  process.exitCode = summary.pass ? 0 : 1;
}

function validatePersistenceGate(input: {
  jobsBefore: number;
  jobsAfter: number;
  insertedCount: number;
  updatedCount: number;
  crawlRunJobEventCount: number;
  runningProviderCount: number;
  jobAfter: Document | null;
  canonicalJobKey?: string;
  jobByCanonicalJobKey: Document | null;
}) {
  const failures: string[] = [];

  if (!input.jobAfter) {
    failures.push(`No job was found with sourceJobId=${deterministicTestJobId}.`);
  }

  if (!input.canonicalJobKey) {
    failures.push("The deterministic job was persisted without canonicalJobKey.");
  }

  if (input.canonicalJobKey && !input.jobByCanonicalJobKey) {
    failures.push("The deterministic job could not be queried by canonicalJobKey.");
  }

  if (input.jobsAfter <= input.jobsBefore && input.updatedCount === 0) {
    failures.push("jobsAfter did not increase and updatedCount is 0.");
  }

  if (input.insertedCount === 0 && input.updatedCount === 0) {
    failures.push("The fake provider job was neither inserted nor updated.");
  }

  if (input.crawlRunJobEventCount === 0) {
    failures.push("No crawlRunJobEvents were written for the controlled crawlRun.");
  }

  if (input.runningProviderCount > 0) {
    failures.push(`${input.runningProviderCount} crawlSourceResult record(s) remain running.`);
  }

  return failures;
}

async function findDeterministicJob(db: Db) {
  return db.collection(jobsCollectionName).findOne({
    sourcePlatform: "greenhouse",
    sourceCompanySlug: deterministicTestCompanySlug,
    sourceJobId: deterministicTestJobId,
  });
}

function createFailingFetch(): typeof fetch {
  return (async () => {
    throw new Error("The deterministic persistence gate must not call fetch.");
  }) as typeof fetch;
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

function databaseNameFromMongoUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          storageMode: "mongodb",
          pass: false,
          failures: [
            error instanceof Error
              ? error.message
              : "Deterministic persistence gate failed unexpectedly.",
          ],
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
