import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { MongoClient, type Db, type Document } from "mongodb";

import type { JobCrawlerRepository, PersistJobsWithStatsResult } from "@/lib/server/db/repository";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import type { CrawlRun, SearchFilters } from "@/lib/types";

type CliOptions = {
  timeoutMs: number;
  latestLimit: number;
};

type BootstrapStatus = {
  status: "pending" | "running" | "succeeded" | "failed";
  startedAt?: string;
  finishedAt?: string;
  errorMessage?: string;
  errorName?: string;
};

type PersistenceCounts = Pick<
  PersistJobsWithStatsResult,
  "insertedCount" | "updatedCount" | "linkedToRunCount" | "indexedEventCount"
>;

type IngestionRootCauseDiagnosticSummary = {
  storageMode: "mongodb" | "memory";
  databaseName: string;
  mongoUriHost: string;
  diagnosticRunId: string;
  bootstrapStatus: BootstrapStatus;
  activeCrawlQueueCount: number;
  activeCrawlQueueEntries: DiagnosticQueueEntry[];
  runningCrawlRunCount: number;
  runningCrawlRuns: DiagnosticRunEntry[];
  terminalRunsWithRunningSourceResultsCount: number;
  terminalRunsWithRunningSourceResults: DiagnosticTerminalSourceResult[];
  jobsBefore: number;
  jobsAfter: number;
  insertedCount: number;
  updatedCount: number;
  linkedToRunCount: number;
  indexedEventCount: number;
  crawlRunJobEventsCount: number;
  indexedEventLatestSequenceBefore: number;
  indexedEventLatestSequenceAfter: number;
  backgroundTriggerStatus?: string;
  searchId?: string;
  crawlRunId?: string;
  crawlRunStatus?: string;
  sourceResults: DiagnosticSourceResult[];
  duplicateSequenceErrors: string[];
  backgroundTaskErrors: DiagnosticBackgroundTaskError[];
  failures: string[];
  pass: boolean;
  error?: string;
};

type RootCauseFailureInput = {
  storageMode: "mongodb" | "memory";
  bootstrapStatus: BootstrapStatus;
  activeCrawlQueueCount: number;
  terminalRunsWithRunningSourceResultsCount: number;
  triggerStatus?: string;
  crawlRunStatus?: string;
  jobsBefore: number;
  jobsAfter: number;
  persistenceCounts: PersistenceCounts;
  crawlRunJobEventsCount: number;
  indexedEventLatestSequenceBefore: number;
  indexedEventLatestSequenceAfter: number;
  duplicateSequenceErrors: string[];
  backgroundTaskErrors: DiagnosticBackgroundTaskError[];
};

type DiagnosticQueueEntry = {
  _id?: string;
  crawlRunId?: string;
  searchId?: string;
  searchSessionId?: string;
  ownerKey?: string;
  status?: string;
  queuedAt?: string;
  startedAt?: string;
  updatedAt?: string;
  lastHeartbeatAt?: string;
  workerId?: string;
};

type DiagnosticRunEntry = {
  _id?: string;
  searchId?: string;
  searchSessionId?: string;
  status?: string;
  stage?: string;
  startedAt?: string;
  finishedAt?: string;
  lastHeartbeatAt?: string;
  errorMessage?: string;
};

type DiagnosticTerminalSourceResult = DiagnosticRunEntry & {
  runningSourceResultIds: string[];
  runningProviders: string[];
};

type DiagnosticSourceResult = {
  provider?: string;
  status?: string;
  sourceCount?: number;
  fetchedCount?: number;
  matchedCount?: number;
  savedCount?: number;
  warningCount?: number;
  errorMessage?: string;
};

type DiagnosticBackgroundTaskError = {
  crawlRunId?: string;
  searchId?: string;
  message: string;
  crawlRunStatus?: string;
  crawlRunFinishedAt?: string;
  crawlRunErrorMessage?: string;
};

const prefix = "[ingestion-root-cause:diagnose]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const defaultTimeoutMs = 15_000;
const defaultLatestLimit = 10;
const terminalRunStatuses = ["completed", "partial", "failed", "aborted"];
const emptyPersistenceCounts: PersistenceCounts = {
  insertedCount: 0,
  updatedCount: 0,
  linkedToRunCount: 0,
  indexedEventCount: 0,
};

export function parseDiagnoseIngestionRootCauseArgs(argv: string[]): CliOptions {
  const options = {
    timeoutMs: defaultTimeoutMs,
    latestLimit: defaultLatestLimit,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    const next = argv[index + 1];

    if (value === "--timeout-ms" && next) {
      options.timeoutMs = positiveInteger(next, defaultTimeoutMs);
      index += 1;
      continue;
    }

    if (value === "--latest-limit" && next) {
      options.latestLimit = positiveInteger(next, defaultLatestLimit);
      index += 1;
    }
  }

  return options;
}

export function databaseNameFromMongoUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

export function uriHostFromMongoUri(uri: string) {
  try {
    return new URL(uri).host;
  } catch {
    return "unknown";
  }
}

export function createDiagnosticRunId(now = new Date()) {
  const timestamp = now.toISOString().replace(/[^0-9]/g, "").slice(0, 14);
  const suffix = Math.random().toString(36).slice(2, 8);
  return `root-cause-${timestamp}-${suffix}`;
}

export function collectIngestionRootCauseFailures(input: RootCauseFailureInput) {
  const failures: string[] = [];

  if (input.storageMode === "memory") {
    failures.push("A. storage resolved to memory fallback instead of MongoDB.");
  }

  if (input.bootstrapStatus.status !== "succeeded") {
    failures.push(
      `A. MongoDB bootstrap failed; status=${input.bootstrapStatus.status} message=${input.bootstrapStatus.errorMessage ?? "none"}.`,
    );
  }

  if (input.triggerStatus && input.triggerStatus !== "started") {
    failures.push(`B. controlled background ingestion did not start; status=${input.triggerStatus}.`);
  }

  if (input.triggerStatus === "started" && input.crawlRunStatus !== "completed" && input.crawlRunStatus !== "partial") {
    failures.push(`G. controlled crawlRun did not finalize successfully; status=${input.crawlRunStatus ?? "unknown"}.`);
  }

  if (
    input.triggerStatus === "started" &&
    input.persistenceCounts.insertedCount + input.persistenceCounts.updatedCount <= 0
  ) {
    failures.push("D. fake-provider job reached no successful insert or update.");
  }

  if (
    input.triggerStatus === "started" &&
    input.jobsAfter <= input.jobsBefore &&
    input.persistenceCounts.updatedCount === 0
  ) {
    failures.push(
      `D. jobs count did not increase and no existing job was updated; jobsBefore=${input.jobsBefore} jobsAfter=${input.jobsAfter}.`,
    );
  }

  if (input.triggerStatus === "started" && input.persistenceCounts.linkedToRunCount <= 0) {
    failures.push("F. persistJobsWithStats did not link the fake job to the crawlRun.");
  }

  if (input.triggerStatus === "started" && input.crawlRunJobEventsCount <= 0) {
    failures.push("F. crawlRunJobEvents were not written for the controlled crawlRun.");
  }

  if (input.triggerStatus === "started" && input.persistenceCounts.indexedEventCount <= 0) {
    failures.push("F. indexedJobEvents were not written for the controlled crawlRun.");
  }

  if (
    input.triggerStatus === "started" &&
    input.indexedEventLatestSequenceAfter <= input.indexedEventLatestSequenceBefore
  ) {
    failures.push(
      `E. indexedJobEvents latest sequence did not advance; before=${input.indexedEventLatestSequenceBefore} after=${input.indexedEventLatestSequenceAfter}.`,
    );
  }

  if (input.duplicateSequenceErrors.length > 0) {
    failures.push(
      `E. duplicate indexed/crawl event sequence error occurred: ${input.duplicateSequenceErrors[0]}`,
    );
  }

  const swallowedTaskError = input.backgroundTaskErrors.find((error) =>
    !error.crawlRunFinishedAt ||
    error.crawlRunStatus === "completed" ||
    !error.crawlRunErrorMessage,
  );
  if (swallowedTaskError) {
    failures.push(
      `G. background task error was not reflected in crawlRun failure diagnostics; crawlRunId=${swallowedTaskError.crawlRunId ?? "unknown"} message=${swallowedTaskError.message}.`,
    );
  }

  if (input.terminalRunsWithRunningSourceResultsCount > 0) {
    failures.push(
      `G. ${input.terminalRunsWithRunningSourceResultsCount} terminal crawlRun(s) still have running crawlSourceResults.`,
    );
  }

  return failures;
}

export async function runIngestionRootCauseDiagnostic(
  options: CliOptions = parseDiagnoseIngestionRootCauseArgs([]),
): Promise<IngestionRootCauseDiagnosticSummary> {
  installServerOnlyShim();
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const databaseName = databaseNameFromMongoUri(mongoUri);
  const diagnosticRunId = createDiagnosticRunId();
  const consoleCapture = captureDiagnosticConsole();
  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: Number(
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "30000",
    ),
  });

  let bootstrapStatus: BootstrapStatus = { status: "pending" };
  let jobsBefore = 0;
  let jobsAfter = 0;
  let indexedEventLatestSequenceBefore = 0;
  let indexedEventLatestSequenceAfter = 0;
  let persistenceCounts = { ...emptyPersistenceCounts };
  let crawlRunJobEventsCount = 0;
  let backgroundTriggerStatus: string | undefined;
  let searchId: string | undefined;
  let crawlRunId: string | undefined;
  let crawlRunStatus: string | undefined;
  let sourceResults: DiagnosticSourceResult[] = [];
  let activeCrawlQueueEntries: DiagnosticQueueEntry[] = [];
  let activeCrawlQueueCount = 0;
  let runningCrawlRuns: DiagnosticRunEntry[] = [];
  let runningCrawlRunCount = 0;
  let terminalRunsWithRunningSourceResults: DiagnosticTerminalSourceResult[] = [];
  let terminalRunsWithRunningSourceResultsCount = 0;

  try {
    await client.connect();
    const db = client.db(databaseName);
    await db.command({ ping: 1 });

    const [
      { ensureDatabaseIndexes },
      { JobCrawlerRepository },
      { triggerRecurringBackgroundIngestion },
      { classifySourceCandidate },
      { toSourceInventoryRecord },
    ] = await Promise.all([
      import("@/lib/server/db/indexes"),
      import("@/lib/server/db/repository"),
      import("@/lib/server/background/recurring-ingestion"),
      import("@/lib/server/discovery/classify-source"),
      import("@/lib/server/discovery/inventory"),
    ]);

    bootstrapStatus = {
      status: "running",
      startedAt: new Date().toISOString(),
    };
    try {
      await ensureDatabaseIndexes(db as never);
      bootstrapStatus = {
        ...bootstrapStatus,
        status: "succeeded",
        finishedAt: new Date().toISOString(),
      };
    } catch (error) {
      bootstrapStatus = {
        ...bootstrapStatus,
        status: "failed",
        finishedAt: new Date().toISOString(),
        errorName: error instanceof Error ? error.name : "Error",
        errorMessage: error instanceof Error ? error.message : String(error),
      };
      throw error;
    }

    const repository = new JobCrawlerRepository(db as never) as JobCrawlerRepository;
    const snapshotsBefore = await readPreflightSnapshots(db, options.latestLimit);
    activeCrawlQueueEntries = snapshotsBefore.activeCrawlQueueEntries;
    activeCrawlQueueCount = snapshotsBefore.activeCrawlQueueCount;
    runningCrawlRuns = snapshotsBefore.runningCrawlRuns;
    runningCrawlRunCount = snapshotsBefore.runningCrawlRunCount;
    terminalRunsWithRunningSourceResults =
      snapshotsBefore.terminalRunsWithRunningSourceResults;
    terminalRunsWithRunningSourceResultsCount =
      snapshotsBefore.terminalRunsWithRunningSourceResultsCount;

    jobsBefore = await db.collection("jobs").countDocuments();
    indexedEventLatestSequenceBefore = await latestIndexedEventSequence(db);

    const now = new Date();
    const sourceToken = diagnosticSourceToken(diagnosticRunId);
    const sourceRecord = toSourceInventoryRecord(
      classifySourceCandidate({
        url: `https://boards.greenhouse.io/${sourceToken}`,
        token: sourceToken,
        companyHint: "Ingestion Root Cause Diagnostics",
        confidence: "high",
        discoveryMethod: "manual_config",
      }),
      {
        now: now.toISOString(),
        inventoryOrigin: "manual_config",
        inventoryRank: 0,
      },
    );
    await repository.upsertSourceInventory([sourceRecord]);

    const provider = createDiagnosticProvider({
      diagnosticRunId,
      sourceToken,
    });
    const triggerResult = await triggerRecurringBackgroundIngestion({
      repository,
      providers: [provider],
      now,
      maxProfiles: 1,
      maxSources: 1,
      maxSourcesPerProvider: 1,
      providerConcurrency: 1,
      schedulingIntervalMs: 1,
      isolationKey: diagnosticRunId,
      runTimeoutMs: options.timeoutMs,
      providerTimeoutMs: Math.max(1_000, Math.floor(options.timeoutMs / 2)),
      sourceTimeoutMs: Math.max(1_000, Math.floor(options.timeoutMs / 2)),
      refreshInventory: async () => [sourceRecord],
      fetchImpl: (async () => {
        throw new Error("diagnose:ingestion-root-cause must not call external fetch.");
      }) as typeof fetch,
    });

    backgroundTriggerStatus = triggerResult.status;
    if (triggerResult.status === "started") {
      searchId = triggerResult.searchId;
      crawlRunId = triggerResult.crawlRunId;
      const crawlRun = await waitForCrawlRunCompletion(
        repository,
        triggerResult.crawlRunId,
        options.timeoutMs,
      );
      crawlRunStatus = crawlRun.status;
      persistenceCounts = extractPersistenceCounts(crawlRun);
      sourceResults = (await repository.getCrawlSourceResults(triggerResult.crawlRunId)).map(
        toDiagnosticSourceResult,
      );
      crawlRunJobEventsCount = await db
        .collection("crawlRunJobEvents")
        .countDocuments({ crawlRunId: triggerResult.crawlRunId });
    }

    jobsAfter = await db.collection("jobs").countDocuments();
    indexedEventLatestSequenceAfter = await latestIndexedEventSequence(db);
    const snapshotsAfter = await readPreflightSnapshots(db, options.latestLimit);
    terminalRunsWithRunningSourceResults =
      snapshotsAfter.terminalRunsWithRunningSourceResults;
    terminalRunsWithRunningSourceResultsCount =
      snapshotsAfter.terminalRunsWithRunningSourceResultsCount;

    const backgroundTaskErrors = await hydrateBackgroundTaskErrors(
      db,
      consoleCapture.backgroundTaskErrors,
    );
    const storageMode = consoleCapture.memoryFallbackWarnings.length > 0 ? "memory" : "mongodb";
    const failures = collectIngestionRootCauseFailures({
      storageMode,
      bootstrapStatus,
      activeCrawlQueueCount,
      terminalRunsWithRunningSourceResultsCount,
      triggerStatus: backgroundTriggerStatus,
      crawlRunStatus,
      jobsBefore,
      jobsAfter,
      persistenceCounts,
      crawlRunJobEventsCount,
      indexedEventLatestSequenceBefore,
      indexedEventLatestSequenceAfter,
      duplicateSequenceErrors: consoleCapture.duplicateSequenceErrors,
      backgroundTaskErrors,
    });

    return {
      storageMode,
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      diagnosticRunId,
      bootstrapStatus,
      activeCrawlQueueCount,
      activeCrawlQueueEntries,
      runningCrawlRunCount,
      runningCrawlRuns,
      terminalRunsWithRunningSourceResultsCount,
      terminalRunsWithRunningSourceResults,
      jobsBefore,
      jobsAfter,
      ...persistenceCounts,
      crawlRunJobEventsCount,
      indexedEventLatestSequenceBefore,
      indexedEventLatestSequenceAfter,
      backgroundTriggerStatus,
      searchId,
      crawlRunId,
      crawlRunStatus,
      sourceResults,
      duplicateSequenceErrors: consoleCapture.duplicateSequenceErrors,
      backgroundTaskErrors,
      failures,
      pass: failures.length === 0,
    };
  } catch (error) {
    const backgroundTaskErrors = await hydrateBackgroundTaskErrors(
      client.db(databaseName),
      consoleCapture.backgroundTaskErrors,
    ).catch(() => []);
    const failures = [
      error instanceof Error ? error.message : "Ingestion root-cause diagnostic failed.",
      ...consoleCapture.duplicateSequenceErrors.map(
        (message) => `E. duplicate indexed/crawl event sequence error occurred: ${message}`,
      ),
    ];

    return {
      storageMode: consoleCapture.memoryFallbackWarnings.length > 0 ? "memory" : "mongodb",
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      diagnosticRunId,
      bootstrapStatus,
      activeCrawlQueueCount,
      activeCrawlQueueEntries,
      runningCrawlRunCount,
      runningCrawlRuns,
      terminalRunsWithRunningSourceResultsCount,
      terminalRunsWithRunningSourceResults,
      jobsBefore,
      jobsAfter,
      ...persistenceCounts,
      crawlRunJobEventsCount,
      indexedEventLatestSequenceBefore,
      indexedEventLatestSequenceAfter,
      backgroundTriggerStatus,
      searchId,
      crawlRunId,
      crawlRunStatus,
      sourceResults,
      duplicateSequenceErrors: consoleCapture.duplicateSequenceErrors,
      backgroundTaskErrors,
      failures,
      pass: false,
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    };
  } finally {
    consoleCapture.restore();
    await client.close().catch(() => undefined);
  }
}

async function main() {
  const options = parseDiagnoseIngestionRootCauseArgs(process.argv.slice(2));
  const summary = await runIngestionRootCauseDiagnostic(options);
  const output = `${prefix} ${JSON.stringify(summary, null, 2)}`;

  if (summary.pass) {
    console.log(output);
    return;
  }

  console.error(output);
  process.exitCode = 1;
}

function createDiagnosticProvider(input: {
  diagnosticRunId: string;
  sourceToken: string;
}): CrawlProvider {
  return {
    provider: "greenhouse",
    supportsSource(source: DiscoveredSource): source is DiscoveredSource {
      return source.platform === "greenhouse" && source.token === input.sourceToken;
    },
    async crawlSources(context, sources) {
      await context.throwIfCanceled?.();
      const sourceCount = sources.length;
      const jobs = sources.slice(0, 1).map((_source, index) =>
        createDiagnosticJobSeed({
          filters: context.filters,
          diagnosticRunId: input.diagnosticRunId,
          sourceToken: input.sourceToken,
          sourceIndex: index + 1,
          now: context.now,
        }),
      );

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount,
        fetchedCount: jobs.length,
        matchedCount: jobs.length,
        warningCount: 0,
        jobs,
        diagnostics: {
          provider: "greenhouse",
          discoveryCount: sourceCount,
          sourceCount,
          sourceSucceededCount: sourceCount,
          sourceTimedOutCount: 0,
          sourceFailedCount: 0,
          sourceSkippedCount: 0,
          fetchCount: 0,
          fetchedCount: jobs.length,
          parseSuccessCount: jobs.length,
          parseFailureCount: 0,
          rawFetchedCount: jobs.length,
          parsedSeedCount: jobs.length,
          validSeedCount: jobs.length,
          invalidSeedCount: 0,
          jobsEmittedViaOnBatch: 0,
          sourceObservations: sources.map((source) => ({
            sourceId: source.id,
            succeeded: true,
            errorType: "none" as const,
          })),
          dropReasonCounts: {},
          sampleDropReasons: [],
          sampleInvalidSeeds: [],
        },
      };
    },
  };
}

function createDiagnosticJobSeed(input: {
  filters: Pick<SearchFilters, "title" | "country" | "state" | "city">;
  diagnosticRunId: string;
  sourceToken: string;
  sourceIndex: number;
  now: Date;
}): NormalizedJobSeed {
  const country = input.filters.country ?? "United States";
  const state = input.filters.state;
  const city = input.filters.city;
  const sourceJobId = `${input.diagnosticRunId}-${input.sourceIndex}`;
  const sourceUrl = `https://example.com/diagnostics/ingestion-root-cause/${sourceJobId}`;
  const locationText = city
    ? [city, state, country].filter(Boolean).join(", ")
    : state
      ? [state, country].filter(Boolean).join(", ")
      : `Remote - ${country}`;

  return {
    title: input.filters.title,
    company: "Ingestion Root Cause Diagnostics",
    country,
    state,
    city,
    locationText,
    resolvedLocation: {
      country,
      state,
      city,
      isRemote: !state && !city,
      isUnitedStates: isUnitedStatesCountry(country),
      confidence: "high",
      evidence: [
        {
          source: "structured_fields",
          value: locationText,
        },
      ],
    },
    remoteType: !state && !city ? "remote" : "onsite",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: input.sourceToken,
    sourceJobId,
    sourceUrl,
    applyUrl: `${sourceUrl}/apply`,
    canonicalUrl: sourceUrl,
    discoveredAt: input.now.toISOString(),
    rawSourceMetadata: {
      source: "diagnose-ingestion-root-cause",
      diagnosticRunId: input.diagnosticRunId,
      greenhouseBoardToken: input.sourceToken,
    },
  };
}

async function readPreflightSnapshots(db: Db, latestLimit: number) {
  const [
    activeCrawlQueueCount,
    activeCrawlQueueEntries,
    runningCrawlRunCount,
    runningCrawlRuns,
    terminalRunsWithRunningSourceResults,
  ] = await Promise.all([
    db.collection("crawlQueue").countDocuments({ status: { $in: ["queued", "running"] } }),
    db.collection("crawlQueue")
      .find(
        { status: { $in: ["queued", "running"] } },
        { sort: { updatedAt: -1 }, limit: latestLimit },
      )
      .toArray(),
    db.collection("crawlRuns").countDocuments({ status: "running" }),
    db.collection("crawlRuns")
      .find({ status: "running" }, { sort: { startedAt: -1 }, limit: latestLimit })
      .toArray(),
    findTerminalRunsWithRunningSourceResults(db, latestLimit),
  ]);

  return {
    activeCrawlQueueCount,
    activeCrawlQueueEntries: activeCrawlQueueEntries.map(toDiagnosticQueueEntry),
    runningCrawlRunCount,
    runningCrawlRuns: runningCrawlRuns.map(toDiagnosticRunEntry),
    terminalRunsWithRunningSourceResultsCount:
      terminalRunsWithRunningSourceResults.totalCount,
    terminalRunsWithRunningSourceResults: terminalRunsWithRunningSourceResults.entries,
  };
}

async function findTerminalRunsWithRunningSourceResults(db: Db, latestLimit: number) {
  const pipeline = [
    { $match: { status: "running" } },
    {
      $lookup: {
        from: "crawlRuns",
        localField: "crawlRunId",
        foreignField: "_id",
        as: "crawlRun",
      },
    },
    { $unwind: "$crawlRun" },
    { $match: { "crawlRun.status": { $in: terminalRunStatuses } } },
    {
      $group: {
        _id: "$crawlRunId",
        run: { $first: "$crawlRun" },
        runningSourceResultIds: { $push: "$_id" },
        runningProviders: { $push: "$provider" },
      },
    },
    { $sort: { "run.finishedAt": -1 } },
    {
      $facet: {
        entries: [{ $limit: latestLimit }],
        total: [{ $count: "count" }],
      },
    },
  ];
  const [result] = await db
    .collection("crawlSourceResults")
    .aggregate<Document>(pipeline)
    .toArray();
  const entries = Array.isArray(result?.entries) ? result.entries : [];
  const total = Array.isArray(result?.total) ? result.total : [];
  const totalCount = Number(total[0]?.count ?? entries.length);

  return {
    totalCount,
    entries: entries.map((entry) => ({
      ...toDiagnosticRunEntry(readRecord(entry.run)),
      runningSourceResultIds: asStringArray(entry.runningSourceResultIds),
      runningProviders: asStringArray(entry.runningProviders),
    })),
  };
}

async function waitForCrawlRunCompletion(
  repository: JobCrawlerRepository,
  crawlRunId: string,
  timeoutMs: number,
) {
  const deadline = Date.now() + timeoutMs;
  let latestRun = await repository.getCrawlRun(crawlRunId);

  while (Date.now() < deadline) {
    latestRun = await repository.getCrawlRun(crawlRunId);
    if (latestRun?.finishedAt && latestRun.status !== "running") {
      return latestRun;
    }

    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  throw new Error(
    `Timed out waiting for crawlRun ${crawlRunId} to reach a terminal state after ${timeoutMs}ms; latestStatus=${latestRun?.status ?? "missing"}.`,
  );
}

function extractPersistenceCounts(crawlRun: Pick<CrawlRun, "diagnostics"> | null | undefined): PersistenceCounts {
  const persistence = crawlRun?.diagnostics?.backgroundPersistence;
  if (!persistence) {
    return { ...emptyPersistenceCounts };
  }

  return {
    insertedCount: nonnegativeNumber(persistence.jobsInserted),
    updatedCount: nonnegativeNumber(persistence.jobsUpdated),
    linkedToRunCount: nonnegativeNumber(persistence.jobsLinkedToRun),
    indexedEventCount: nonnegativeNumber(persistence.indexedEventsEmitted),
  };
}

async function latestIndexedEventSequence(db: Db) {
  const latest = await db
    .collection("indexedJobEvents")
    .findOne({}, { sort: { sequence: -1 }, projection: { sequence: 1 } });

  return nonnegativeNumber(latest?.sequence);
}

async function hydrateBackgroundTaskErrors(
  db: Db,
  errors: Array<{ crawlRunId?: string; searchId?: string; message: string }>,
): Promise<DiagnosticBackgroundTaskError[]> {
  if (errors.length === 0) {
    return [];
  }

  const crawlRunIds = errors
    .map((error) => error.crawlRunId)
    .filter((value): value is string => Boolean(value));
  const runs = crawlRunIds.length > 0
    ? await db
        .collection<Document & { _id: string }>("crawlRuns")
        .find({ _id: { $in: crawlRunIds } })
        .toArray()
    : [];
  const runsById = new Map(runs.map((run) => [String(run._id), run]));

  return errors.map((error) => {
    const crawlRun = error.crawlRunId ? runsById.get(error.crawlRunId) : undefined;
    return {
      ...error,
      crawlRunStatus: readString(crawlRun?.status),
      crawlRunFinishedAt: readString(crawlRun?.finishedAt),
      crawlRunErrorMessage: readString(crawlRun?.errorMessage),
    };
  });
}

function toDiagnosticQueueEntry(document: Document): DiagnosticQueueEntry {
  return {
    _id: readString(document._id),
    crawlRunId: readString(document.crawlRunId),
    searchId: readString(document.searchId),
    searchSessionId: readString(document.searchSessionId),
    ownerKey: readString(document.ownerKey),
    status: readString(document.status),
    queuedAt: readString(document.queuedAt),
    startedAt: readString(document.startedAt),
    updatedAt: readString(document.updatedAt),
    lastHeartbeatAt: readString(document.lastHeartbeatAt),
    workerId: readString(document.workerId),
  };
}

function toDiagnosticRunEntry(document: Document): DiagnosticRunEntry {
  return {
    _id: readString(document._id),
    searchId: readString(document.searchId),
    searchSessionId: readString(document.searchSessionId),
    status: readString(document.status),
    stage: readString(document.stage),
    startedAt: readString(document.startedAt),
    finishedAt: readString(document.finishedAt),
    lastHeartbeatAt: readString(document.lastHeartbeatAt),
    errorMessage: readString(document.errorMessage),
  };
}

function toDiagnosticSourceResult(document: Document): DiagnosticSourceResult {
  return {
    provider: readString(document.provider),
    status: readString(document.status),
    sourceCount: optionalNumber(document.sourceCount),
    fetchedCount: optionalNumber(document.fetchedCount),
    matchedCount: optionalNumber(document.matchedCount),
    savedCount: optionalNumber(document.savedCount),
    warningCount: optionalNumber(document.warningCount),
    errorMessage: readString(document.errorMessage),
  };
}

function captureDiagnosticConsole() {
  const originalWarn = console.warn;
  const originalError = console.error;
  const memoryFallbackWarnings: string[] = [];
  const duplicateSequenceErrors: string[] = [];
  const backgroundTaskErrors: Array<{ crawlRunId?: string; searchId?: string; message: string }> = [];

  const inspect = (args: unknown[]) => {
    const message = args.map(stringifyLogPart).join(" ");
    if (message.includes("[db:fallback]")) {
      memoryFallbackWarnings.push(message);
    }
    if (/E11000 duplicate key error/i.test(message) && /sequence/i.test(message)) {
      duplicateSequenceErrors.push(message);
    }
    if (message.includes("[crawl:background-run]")) {
      const payload = args.find((arg) => arg && typeof arg === "object") as
        | Record<string, unknown>
        | undefined;
      backgroundTaskErrors.push({
        crawlRunId: readString(payload?.crawlRunId),
        searchId: readString(payload?.searchId),
        message: readString(payload?.message) ?? message,
      });
    }
  };

  console.warn = (...args: unknown[]) => {
    inspect(args);
    originalWarn(...args);
  };
  console.error = (...args: unknown[]) => {
    inspect(args);
    originalError(...args);
  };

  return {
    memoryFallbackWarnings,
    duplicateSequenceErrors,
    backgroundTaskErrors,
    restore() {
      console.warn = originalWarn;
      console.error = originalError;
    },
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

function diagnosticSourceToken(diagnosticRunId: string) {
  return diagnosticRunId.replace(/[^a-z0-9]+/gi, "").toLowerCase().slice(0, 48);
}

function isUnitedStatesCountry(country: string) {
  return /^(united states|usa|us|u\.s\.|u\.s\.a\.)$/i.test(country.trim());
}

function readRecord(value: unknown): Document {
  return value && typeof value === "object" ? value as Document : {};
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function asStringArray(value: unknown) {
  return Array.isArray(value)
    ? value.map((entry) => readString(entry)).filter((entry): entry is string => Boolean(entry))
    : [];
}

function optionalNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function nonnegativeNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : 0;
}

function positiveInteger(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function stringifyLogPart(value: unknown) {
  if (typeof value === "string") {
    return value;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          status: "failed",
          failures: [
            error instanceof Error
              ? error.message
              : "Ingestion root-cause diagnostic failed unexpectedly.",
          ],
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
