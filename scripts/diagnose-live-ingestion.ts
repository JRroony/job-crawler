import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { MongoClient, type Db } from "mongodb";

import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { SourceInventoryRecord } from "@/lib/server/discovery/inventory";
import type { CrawlRun } from "@/lib/types";
import {
  databaseNameFromMongoUri,
  extractPersistenceCounts,
  type PersistenceCounts,
} from "@/scripts/diagnose-ingestion-persistence";

type CliOptions = {
  timeoutMs: number;
  boardToken: string;
};

type LiveIngestionProviderResult = {
  provider: string;
  status: string;
  sourceCount: number;
  fetchedCount: number;
  matchedCount: number;
  savedCount: number;
  errorMessage?: string | null;
};

type LiveIngestionSummary = {
  storageMode: "mongodb";
  failures: string[];
  databaseName: string;
  mongoUriHost: string;
  sourceSet: Array<{
    provider: "greenhouse";
    boardToken: string;
    sourceId: string;
  }>;
  controlledFilters?: {
    title?: string;
    country?: string;
    state?: string;
    city?: string;
    crawlMode?: string;
  };
  timeoutPolicy: {
    envBackgroundProviderTimeoutMs: number;
    envBackgroundSourceTimeoutMs: number;
    envBackgroundRunTimeoutMs: number;
    providerTimeoutMs?: number;
    sourceTimeoutMs?: number;
    runTimeoutMs: number;
    usesBackgroundProviderTimeout: boolean;
    usesBackgroundSourceTimeout: boolean;
    usesBackgroundRunTimeout: boolean;
  };
  searchId?: string;
  crawlRunId?: string;
  backgroundTriggerStatus?: string;
  crawlRunStatus?: string;
  jobsBefore: number;
  jobsAfter: number;
  insertedCount: number;
  updatedCount: number;
  linkedToRunCount: number;
  indexedEventCount: number;
  dbEventCounts: {
    linkedToRunCount: number;
    indexedEventCount: number;
  };
  providerResults: LiveIngestionProviderResult[];
  runningProviderCountAfterFinalize: number;
  pass: boolean;
  error?: string;
};

const prefix = "[live-ingestion:diagnose]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const defaultTimeoutMs = 180_000;
const defaultKnownGoodGreenhouseBoardToken = "greenhouse";
const jobsCollectionName = "jobs";
const crawlRunJobEventsCollectionName = "crawlRunJobEvents";
const indexedJobEventsCollectionName = "indexedJobEvents";
const deterministicSchedulingIntervalMs = 9_000_000_000_000;

export function parseDiagnoseLiveIngestionArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    timeoutMs: defaultTimeoutMs,
    boardToken: defaultKnownGoodGreenhouseBoardToken,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    const next = argv[index + 1];

    if (value === "--timeout-ms" && next) {
      options.timeoutMs = positiveInteger(next, defaultTimeoutMs);
      index += 1;
      continue;
    }

    if (value === "--board-token" && next) {
      options.boardToken = normalizeBoardToken(next) ?? defaultKnownGoodGreenhouseBoardToken;
      index += 1;
    }
  }

  return options;
}

export function collectLiveIngestionFailures(input: {
  triggerStatus?: string;
  runStatus?: string;
  jobsBefore: number;
  jobsAfter: number;
  persistenceCounts: PersistenceCounts;
  providerResults: LiveIngestionProviderResult[];
  runningProviderCountAfterFinalize: number;
  providerTimeoutMs?: number;
}) {
  const failures: string[] = [];
  const terminalStatuses = new Set(["completed", "partial"]);

  if (input.triggerStatus !== "started") {
    failures.push(`Background ingestion did not start; status=${input.triggerStatus ?? "unknown"}.`);
  }

  if (!input.runStatus || !terminalStatuses.has(input.runStatus)) {
    failures.push(`Background ingestion did not finish successfully; status=${input.runStatus ?? "unknown"}.`);
  }

  if (input.providerTimeoutMs === 9_000) {
    failures.push("Live background ingestion used the request-time 9000ms provider timeout.");
  }

  const greenhouseResult = input.providerResults.find(
    (result) => result.provider === "greenhouse",
  );
  if (!greenhouseResult) {
    failures.push("No Greenhouse crawlSourceResult was recorded.");
  } else {
    if (greenhouseResult.status === "running") {
      failures.push("Greenhouse crawlSourceResult remained running after crawlRun finalization.");
    }

    if (greenhouseResult.fetchedCount <= 0) {
      failures.push("Greenhouse fetchedCount is 0 for the controlled live board.");
    }

    if (greenhouseResult.matchedCount <= 0) {
      failures.push("Greenhouse matchedCount is 0 for the controlled live board.");
    }

    if (greenhouseResult.savedCount <= 0) {
      failures.push("Greenhouse savedCount is 0 for the controlled live board.");
    }
  }

  if (input.persistenceCounts.insertedCount + input.persistenceCounts.updatedCount <= 0) {
    failures.push("No jobs were inserted or updated by the controlled live ingestion cycle.");
  }

  if (input.jobsAfter <= input.jobsBefore && input.persistenceCounts.updatedCount <= 0) {
    failures.push(
      `MongoDB jobs did not increase and no existing jobs were updated; jobsBefore=${input.jobsBefore}, jobsAfter=${input.jobsAfter}.`,
    );
  }

  if (input.runningProviderCountAfterFinalize > 0) {
    failures.push(
      `${input.runningProviderCountAfterFinalize} crawlSourceResult document(s) remained running after crawlRun finalization.`,
    );
  }

  return failures;
}

export async function runLiveIngestionDiagnostic(
  options: CliOptions = parseDiagnoseLiveIngestionArgs([]),
): Promise<LiveIngestionSummary> {
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const databaseName = databaseNameFromMongoUri(mongoUri);
  const runTimeoutMs = options.timeoutMs;
  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: Number(
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "30000",
    ),
  });

  let jobsBefore = 0;
  let jobsAfter = 0;
  let searchId: string | undefined;
  let crawlRunId: string | undefined;
  let backgroundTriggerStatus: string | undefined;
  let crawlRunStatus: string | undefined;
  let persistenceCounts: PersistenceCounts = {
    insertedCount: 0,
    updatedCount: 0,
    linkedToRunCount: 0,
    indexedEventCount: 0,
  };
  let dbEventCounts = { linkedToRunCount: 0, indexedEventCount: 0 };
  let providerResults: LiveIngestionProviderResult[] = [];
  let runningProviderCountAfterFinalize = 0;
  let controlledFilters: LiveIngestionSummary["controlledFilters"];
  let timeoutPolicy: LiveIngestionSummary["timeoutPolicy"] = {
    envBackgroundProviderTimeoutMs: 0,
    envBackgroundSourceTimeoutMs: 0,
    envBackgroundRunTimeoutMs: 0,
    runTimeoutMs,
    usesBackgroundProviderTimeout: false,
    usesBackgroundSourceTimeout: false,
    usesBackgroundRunTimeout: false,
  };
  let sourceRecord: SourceInventoryRecord | undefined;

  try {
    await client.connect();
    const db = client.db(databaseName);
    await db.command({ ping: 1 });

    const [
      { ensureDatabaseIndexes },
      { JobCrawlerRepository },
      { triggerRecurringBackgroundIngestion },
      { classifySourceCandidate },
      { sourceInventoryRecordSchema, toSourceInventoryRecord },
      { createGreenhouseProvider },
      { getEnv },
    ] = await Promise.all([
      import("@/lib/server/db/indexes"),
      import("@/lib/server/db/repository"),
      import("@/lib/server/background/recurring-ingestion"),
      import("@/lib/server/discovery/classify-source"),
      import("@/lib/server/discovery/inventory"),
      import("@/lib/server/providers/greenhouse"),
      import("@/lib/server/env"),
    ]);

    const env = getEnv();
    timeoutPolicy = {
      envBackgroundProviderTimeoutMs: env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS,
      envBackgroundSourceTimeoutMs: env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS,
      envBackgroundRunTimeoutMs: env.BACKGROUND_INGESTION_RUN_TIMEOUT_MS,
      providerTimeoutMs: env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS,
      sourceTimeoutMs: env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS,
      runTimeoutMs,
      usesBackgroundProviderTimeout:
        env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS !== env.CRAWL_PROVIDER_TIMEOUT_MS,
      usesBackgroundSourceTimeout:
        env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS !== env.CRAWL_SOURCE_TIMEOUT_MS,
      usesBackgroundRunTimeout: runTimeoutMs === env.BACKGROUND_INGESTION_RUN_TIMEOUT_MS,
    };

    await ensureDatabaseIndexes(db as never);
    const repository = new JobCrawlerRepository(db as never) as JobCrawlerRepository;
    const now = new Date();
    const boardToken = normalizeBoardToken(options.boardToken) ?? defaultKnownGoodGreenhouseBoardToken;
    sourceRecord = sourceInventoryRecordSchema.parse(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://boards.greenhouse.io/${boardToken}`,
          token: boardToken,
          companyHint: boardToken === "greenhouse" ? "Greenhouse" : boardToken,
          confidence: "high",
          discoveryMethod: "manual_config",
        }),
        {
          now: now.toISOString(),
          inventoryOrigin: "manual_config",
          inventoryRank: 0,
        },
      ),
    ) as SourceInventoryRecord;

    await repository.upsertSourceInventory([sourceRecord]);
    jobsBefore = await db.collection(jobsCollectionName).countDocuments();

    const triggerResult = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [createGreenhouseProvider()],
      now,
      maxProfiles: 1,
      maxSources: 1,
      maxSourcesPerProvider: 1,
      providerConcurrency: 1,
      schedulingIntervalMs: deterministicSchedulingIntervalMs,
      isolationKey: `live-greenhouse-${Date.now().toString(36)}`,
      runTimeoutMs,
      providerTimeoutMs: env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS,
      sourceTimeoutMs: env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS,
      refreshInventory: async () => [sourceRecord!],
    });

    backgroundTriggerStatus = triggerResult.status;
    if (triggerResult.status === "started") {
      searchId = triggerResult.searchId;
      crawlRunId = triggerResult.crawlRunId;
      const crawlRun = await waitForCrawlRunCompletion(
        repository,
        triggerResult.crawlRunId,
        runTimeoutMs + 5_000,
      );
      crawlRunStatus = crawlRun?.status;
      controlledFilters = crawlRun?.diagnostics?.systemProfile?.filters;
      persistenceCounts = extractPersistenceCounts(crawlRun);
      dbEventCounts = await countRunEvents(db, triggerResult.crawlRunId);
      timeoutPolicy = {
        ...timeoutPolicy,
        providerTimeoutMs:
          typeof crawlRun?.diagnostics?.backgroundCycle?.providerTimeoutMs === "number"
            ? crawlRun.diagnostics.backgroundCycle.providerTimeoutMs
            : timeoutPolicy.providerTimeoutMs,
        sourceTimeoutMs:
          typeof crawlRun?.diagnostics?.backgroundCycle?.sourceTimeoutMs === "number"
            ? crawlRun.diagnostics.backgroundCycle.sourceTimeoutMs
            : timeoutPolicy.sourceTimeoutMs,
        runTimeoutMs:
          typeof crawlRun?.diagnostics?.backgroundCycle?.runTimeoutMs === "number"
            ? crawlRun.diagnostics.backgroundCycle.runTimeoutMs
            : timeoutPolicy.runTimeoutMs,
      };
      timeoutPolicy = {
        ...timeoutPolicy,
        usesBackgroundProviderTimeout:
          timeoutPolicy.providerTimeoutMs === timeoutPolicy.envBackgroundProviderTimeoutMs &&
          timeoutPolicy.providerTimeoutMs !== 9_000,
        usesBackgroundSourceTimeout:
          timeoutPolicy.sourceTimeoutMs === timeoutPolicy.envBackgroundSourceTimeoutMs,
        usesBackgroundRunTimeout:
          timeoutPolicy.runTimeoutMs === timeoutPolicy.envBackgroundRunTimeoutMs,
      };
      providerResults = (await repository.getCrawlSourceResults(triggerResult.crawlRunId)).map(
        (result) => ({
          provider: result.provider,
          status: result.status,
          sourceCount: result.sourceCount,
          fetchedCount: result.fetchedCount,
          matchedCount: result.matchedCount,
          savedCount: result.savedCount,
          errorMessage: result.errorMessage,
        }),
      );
      runningProviderCountAfterFinalize = providerResults.filter(
        (result) => result.status === "running",
      ).length;
    }

    jobsAfter = await db.collection(jobsCollectionName).countDocuments();
    const failures = collectLiveIngestionFailures({
      triggerStatus: backgroundTriggerStatus,
      runStatus: crawlRunStatus,
      jobsBefore,
      jobsAfter,
      persistenceCounts,
      providerResults,
      runningProviderCountAfterFinalize,
      providerTimeoutMs: timeoutPolicy.providerTimeoutMs,
    });

    return {
      storageMode: "mongodb",
      failures,
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      sourceSet: [
        {
          provider: "greenhouse",
          boardToken: sourceRecord.token ?? options.boardToken,
          sourceId: sourceRecord._id,
        },
      ],
      controlledFilters,
      timeoutPolicy,
      searchId,
      crawlRunId,
      backgroundTriggerStatus,
      crawlRunStatus,
      jobsBefore,
      jobsAfter,
      ...persistenceCounts,
      dbEventCounts,
      providerResults,
      runningProviderCountAfterFinalize,
      pass: failures.length === 0,
    };
  } catch (error) {
    const failures = [
      error instanceof Error ? error.message : "Live ingestion diagnostic failed.",
    ];

    return {
      storageMode: "mongodb",
      failures,
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      sourceSet: sourceRecord
        ? [
            {
              provider: "greenhouse",
              boardToken: sourceRecord.token ?? options.boardToken,
              sourceId: sourceRecord._id,
            },
          ]
        : [
            {
              provider: "greenhouse",
              boardToken: options.boardToken,
              sourceId: `greenhouse:${options.boardToken}`,
            },
          ],
      controlledFilters,
      timeoutPolicy,
      searchId,
      crawlRunId,
      backgroundTriggerStatus,
      crawlRunStatus,
      jobsBefore,
      jobsAfter,
      ...persistenceCounts,
      dbEventCounts,
      providerResults,
      runningProviderCountAfterFinalize,
      pass: false,
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    };
  } finally {
    await client.close().catch(() => undefined);
  }
}

async function waitForCrawlRunCompletion(
  repository: JobCrawlerRepository,
  crawlRunId: string,
  timeoutMs: number,
) {
  const deadline = Date.now() + timeoutMs;
  let latestRun: CrawlRun | null = await repository.getCrawlRun(crawlRunId);

  while (Date.now() < deadline) {
    latestRun = await repository.getCrawlRun(crawlRunId);
    if (latestRun?.finishedAt && latestRun.status !== "running") {
      return latestRun;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error(
    `Timed out waiting for crawlRun ${crawlRunId} to reach a terminal state after ${timeoutMs}ms; latestStatus=${latestRun?.status ?? "missing"}.`,
  );
}

async function countRunEvents(db: Db, crawlRunId: string) {
  const [linkedToRunCount, indexedEventCount] = await Promise.all([
    db.collection(crawlRunJobEventsCollectionName).countDocuments({ crawlRunId }),
    db.collection(indexedJobEventsCollectionName).countDocuments({ crawlRunId }),
  ]);

  return {
    linkedToRunCount,
    indexedEventCount,
  };
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

function uriHostFromMongoUri(uri: string) {
  try {
    return new URL(uri).host;
  } catch {
    return "unknown";
  }
}

function normalizeBoardToken(value?: string) {
  const trimmed = value?.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "");
  return trimmed ? trimmed : undefined;
}

function positiveInteger(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

async function main() {
  installServerOnlyShim();
  const options = parseDiagnoseLiveIngestionArgs(process.argv.slice(2));
  const summary = await runLiveIngestionDiagnostic(options);
  const output = `${prefix} ${JSON.stringify(summary, null, 2)}`;

  if (summary.pass) {
    console.log(output);
    return;
  }

  console.error(output);
  process.exitCode = 1;
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
              : "Live ingestion diagnostic failed unexpectedly.",
          ],
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
