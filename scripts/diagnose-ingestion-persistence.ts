import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { MongoClient, type Db, type Document } from "mongodb";

import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { SourceInventoryRecord } from "@/lib/server/discovery/inventory";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import type { CrawlRun, SearchFilters } from "@/lib/types";

type CliOptions = {
  timeoutMs: number;
  latestLimit: number;
};

export type PersistenceCounts = {
  insertedCount: number;
  updatedCount: number;
  linkedToRunCount: number;
  indexedEventCount: number;
};

export type IngestionPersistenceValidationInput = {
  triggerStatus?: string;
  runStatus?: string;
  fellBackToMemory: boolean;
  jobsBefore: number;
  jobsAfter: number;
  persistenceCounts: PersistenceCounts;
  runningProviderCountAfterFinalize: number;
  sourceInventoryContaminationCount: number;
  backgroundProviderTimeoutMs?: number;
};

type IngestionPersistenceDiagnosticSummary = {
  storageMode: "mongodb" | "memory";
  failures: string[];
  databaseName: string;
  mongoUriHost: string;
  diagnosticRunId: string;
  sourceId?: string;
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
  runningProviderCountAfterFinalize: number;
  providerResults: Array<{
    provider: string;
    status: string;
    sourceCount: number;
    fetchedCount: number;
    matchedCount: number;
    savedCount: number;
    errorMessage?: string | null;
  }>;
  sourceInventoryContaminationCount: number;
  repairedSourceInventoryContaminationCount: number;
  backgroundProviderTimeoutMs?: number;
  dbEventCounts: {
    linkedToRunCount: number;
    indexedEventCount: number;
  };
  usedEligibilityTimeShift: boolean;
  pass: boolean;
  error?: string;
};

type DiagnosticJobRecord = Document & {
  _id?: unknown;
  title?: unknown;
  company?: unknown;
  locationText?: unknown;
  sourcePlatform?: unknown;
  sourceJobId?: unknown;
  firstSeenAt?: unknown;
  lastSeenAt?: unknown;
  rawSourceMetadata?: {
    diagnosticRunId?: unknown;
  };
};

const prefix = "[ingestion-persistence:diagnose]";
const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const jobsCollectionName = "jobs";
const crawlRunJobEventsCollectionName = "crawlRunJobEvents";
const indexedJobEventsCollectionName = "indexedJobEvents";
const defaultTimeoutMs = 15_000;
const defaultLatestLimit = 5;
const eligibilityRetryOffsetMs = 8 * 24 * 60 * 60 * 1000;

const emptyPersistenceCounts: PersistenceCounts = {
  insertedCount: 0,
  updatedCount: 0,
  linkedToRunCount: 0,
  indexedEventCount: 0,
};

export function parseDiagnoseIngestionPersistenceArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
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
  return `diag-${timestamp}-${suffix}`;
}

export function createDiagnosticJobSeed(input: {
  filters: Pick<SearchFilters, "title" | "country" | "state" | "city">;
  diagnosticRunId: string;
  sourceToken: string;
  sourceIndex: number;
  now: Date;
}): NormalizedJobSeed {
  const location = resolveDiagnosticLocation(input.filters);
  const sourceJobId = `${input.diagnosticRunId}-${input.sourceIndex}`;
  const sourceUrl = `https://example.com/diagnostics/ingestion-persistence/${sourceJobId}`;

  return {
    title: input.filters.title,
    company: "Ingestion Persistence Diagnostics",
    country: location.country,
    state: location.state,
    city: location.city,
    locationText: location.locationText,
    resolvedLocation: location.resolvedLocation,
    remoteType: location.isRemote ? "remote" : "onsite",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: input.sourceToken,
    sourceJobId,
    sourceUrl,
    applyUrl: `${sourceUrl}/apply`,
    canonicalUrl: sourceUrl,
    discoveredAt: input.now.toISOString(),
    rawSourceMetadata: {
      source: "diagnose-ingestion-persistence",
      diagnosticRunId: input.diagnosticRunId,
      greenhouseBoardToken: input.sourceToken,
      selectedTitle: input.filters.title,
      selectedCountry: input.filters.country ?? null,
      selectedState: input.filters.state ?? null,
      selectedCity: input.filters.city ?? null,
    },
  };
}

export function extractPersistenceCounts(crawlRun: Pick<CrawlRun, "diagnostics"> | null | undefined): PersistenceCounts {
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

export function collectIngestionPersistenceFailures(
  input: IngestionPersistenceValidationInput,
) {
  const failures: string[] = [];
  const terminalStatuses = new Set(["completed", "partial"]);

  if (input.fellBackToMemory) {
    failures.push("Storage fell back to the in-memory database.");
  }

  if (input.triggerStatus !== "started") {
    failures.push(`Background ingestion did not start; status=${input.triggerStatus ?? "unknown"}.`);
  }

  if (!input.runStatus || !terminalStatuses.has(input.runStatus)) {
    failures.push(`Background ingestion did not finish successfully; status=${input.runStatus ?? "unknown"}.`);
  }

  if (input.persistenceCounts.insertedCount + input.persistenceCounts.updatedCount <= 0) {
    failures.push("No jobs were inserted or updated by the controlled ingestion cycle.");
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

  if (input.sourceInventoryContaminationCount > 0) {
    failures.push(
      `${input.sourceInventoryContaminationCount} sourceInventory record(s) still have cross-source contaminated lastFailureReason values.`,
    );
  }

  if (input.backgroundProviderTimeoutMs === 9000) {
    failures.push("Background ingestion used the request-time 9000ms provider timeout.");
  }

  return failures;
}

export async function runIngestionPersistenceDiagnostic(
  options: CliOptions = parseDiagnoseIngestionPersistenceArgs([]),
): Promise<IngestionPersistenceDiagnosticSummary> {
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const databaseName = databaseNameFromMongoUri(mongoUri);
  const diagnosticRunId = createDiagnosticRunId();
  const fallbackDetector = captureMemoryFallbackWarnings();
  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: Number(
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "30000",
    ),
  });

  let jobCountBefore = 0;
  let jobCountAfter = 0;
  let sourceId: string | undefined;
  let searchId: string | undefined;
  let crawlRunId: string | undefined;
  let backgroundTriggerStatus: string | undefined;
  let crawlRunStatus: string | undefined;
  let persistenceCounts = { ...emptyPersistenceCounts };
  let dbEventCounts = { linkedToRunCount: 0, indexedEventCount: 0 };
  let providerResults: IngestionPersistenceDiagnosticSummary["providerResults"] = [];
  let runningProviderCountAfterFinalize = 0;
  let sourceInventoryContaminationCount = 0;
  let repairedSourceInventoryContaminationCount = 0;
  let backgroundProviderTimeoutMs: number | undefined;
  let usedEligibilityTimeShift = false;

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
    ] = await Promise.all([
      import("@/lib/server/db/indexes"),
      import("@/lib/server/db/repository"),
      import("@/lib/server/background/recurring-ingestion"),
      import("@/lib/server/discovery/classify-source"),
      import("@/lib/server/discovery/inventory"),
    ]);

    await ensureDatabaseIndexes(db as never);

    const repository = new JobCrawlerRepository(db as never) as JobCrawlerRepository;
    const jobs = db.collection<DiagnosticJobRecord>(jobsCollectionName);
    jobCountBefore = await jobs.countDocuments();

    const now = new Date();
    const sourceToken = diagnosticSourceToken(diagnosticRunId);
    const sourceRecord = sourceInventoryRecordSchema.parse(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://boards.greenhouse.io/${sourceToken}`,
          token: sourceToken,
          companyHint: "Ingestion Persistence Diagnostics",
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
    sourceId = sourceRecord._id;

    await repository.upsertSourceInventory([sourceRecord]);

    const provider = createDiagnosticProvider({
      diagnosticRunId,
      sourceToken,
    });

    let triggerResult = await triggerRecurringBackgroundIngestion({
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
    });

    if (triggerResult.status === "skipped-disabled") {
      usedEligibilityTimeShift = true;
      triggerResult = await triggerRecurringBackgroundIngestion({
        repository,
        providers: [provider],
        now: new Date(now.getTime() + eligibilityRetryOffsetMs),
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
      });
    }

    backgroundTriggerStatus = triggerResult.status;
    if (triggerResult.status === "started") {
      searchId = triggerResult.searchId;
      crawlRunId = triggerResult.crawlRunId;
      const crawlRun = await waitForCrawlRunCompletion(
        repository,
        triggerResult.crawlRunId,
        options.timeoutMs,
      );
      crawlRunStatus = crawlRun?.status;
      persistenceCounts = extractPersistenceCounts(crawlRun);
      dbEventCounts = await countRunEvents(db, triggerResult.crawlRunId);
      backgroundProviderTimeoutMs =
        typeof crawlRun?.diagnostics?.backgroundCycle?.providerTimeoutMs === "number"
          ? crawlRun.diagnostics.backgroundCycle.providerTimeoutMs
          : undefined;
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

    jobCountAfter = await jobs.countDocuments();
    repairedSourceInventoryContaminationCount =
      await repairSourceInventoryContamination(db);
    sourceInventoryContaminationCount =
      (await findSourceInventoryContamination(db)).length;

    const failures = collectIngestionPersistenceFailures({
      triggerStatus: backgroundTriggerStatus,
      runStatus: crawlRunStatus,
      fellBackToMemory: fallbackDetector.warnings.length > 0,
      jobsBefore: jobCountBefore,
      jobsAfter: jobCountAfter,
      persistenceCounts,
      runningProviderCountAfterFinalize,
      sourceInventoryContaminationCount,
      backgroundProviderTimeoutMs,
    });

    return {
      storageMode: fallbackDetector.warnings.length > 0 ? "memory" : "mongodb",
      failures,
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      diagnosticRunId,
      sourceId,
      searchId,
      crawlRunId,
      backgroundTriggerStatus,
      crawlRunStatus,
      jobsBefore: jobCountBefore,
      jobsAfter: jobCountAfter,
      ...persistenceCounts,
      runningProviderCountAfterFinalize,
      providerResults,
      sourceInventoryContaminationCount,
      repairedSourceInventoryContaminationCount,
      backgroundProviderTimeoutMs,
      dbEventCounts,
      usedEligibilityTimeShift,
      pass: failures.length === 0,
    };
  } catch (error) {
    const failures = [
      error instanceof Error ? error.message : "Ingestion persistence diagnostic failed.",
    ];

    return {
      storageMode: fallbackDetector.warnings.length > 0 ? "memory" : "mongodb",
      failures,
      databaseName,
      mongoUriHost: uriHostFromMongoUri(mongoUri),
      diagnosticRunId,
      sourceId,
      searchId,
      crawlRunId,
      backgroundTriggerStatus,
      crawlRunStatus,
      jobsBefore: jobCountBefore,
      jobsAfter: jobCountAfter,
      ...persistenceCounts,
      runningProviderCountAfterFinalize,
      providerResults,
      sourceInventoryContaminationCount,
      repairedSourceInventoryContaminationCount,
      backgroundProviderTimeoutMs,
      dbEventCounts,
      usedEligibilityTimeShift,
      pass: false,
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    };
  } finally {
    fallbackDetector.restore();
    await client.close().catch(() => undefined);
  }
}

async function main() {
  installServerOnlyShim();
  const options = parseDiagnoseIngestionPersistenceArgs(process.argv.slice(2));
  const summary = await runIngestionPersistenceDiagnostic(options);
  const output = `${prefix} ${JSON.stringify(summary, null, 2)}`;

  if (summary.pass) {
    console.log(output);
    return;
  }

  console.error(output);
  process.exitCode = 1;
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
      const jobs = sources.map((_source, index) =>
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
        sourceCount: sources.length,
        fetchedCount: jobs.length,
        matchedCount: jobs.length,
        warningCount: 0,
        jobs,
      };
    },
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

type SourceInventoryDiagnosticRecord = Document & {
  _id?: unknown;
  platform?: unknown;
  token?: unknown;
  companyHint?: unknown;
  lastFailureReason?: unknown;
};

async function findSourceInventoryContamination(db: Db) {
  const records = await db
    .collection<SourceInventoryDiagnosticRecord>("sourceInventory")
    .find(
      { lastFailureReason: { $type: "string" } },
      {
        projection: {
          _id: 1,
          platform: 1,
          token: 1,
          companyHint: 1,
          lastFailureReason: 1,
        },
      },
    )
    .toArray();

  return records.filter((record) =>
    isContaminatedSourceInventoryRecord(record, records),
  );
}

async function repairSourceInventoryContamination(db: Db) {
  const contaminated = await findSourceInventoryContamination(db);
  for (const record of contaminated) {
    if (!record._id) {
      continue;
    }

    await db.collection("sourceInventory").updateOne(
      { _id: record._id },
      {
        $unset: {
          lastFailureReason: "",
        },
      },
    );
  }

  return contaminated.length;
}

function isContaminatedSourceInventoryRecord(
  record: SourceInventoryDiagnosticRecord,
  allRecords: SourceInventoryDiagnosticRecord[],
) {
  const reason = readString(record.lastFailureReason)?.toLowerCase();
  if (!reason) {
    return false;
  }

  const ownTokens = sourceIdentityTokens(record);
  const foreignTokens = allRecords
    .filter((candidate) => candidate._id !== record._id)
    .flatMap(sourceIdentityTokens)
    .filter((token) => !ownTokens.includes(token));

  return foreignTokens.some((token) => reason.includes(token));
}

function sourceIdentityTokens(record: SourceInventoryDiagnosticRecord) {
  return [record.token, record.companyHint, record._id]
    .map((value) => readString(value)?.toLowerCase())
    .filter((value): value is string => Boolean(value))
    .flatMap((value) =>
      value
        .split(/[^a-z0-9]+/i)
        .map((part) => part.trim())
        .filter((part) => part.length >= 3),
    );
}

function resolveDiagnosticLocation(filters: Pick<SearchFilters, "country" | "state" | "city">) {
  const country = filters.country ?? "United States";
  const state = filters.state;
  const city = filters.city;
  const isRemote = !state && !city;
  const locationText = city
    ? [city, state, country].filter(Boolean).join(", ")
    : state
      ? [state, country].filter(Boolean).join(", ")
      : `Remote - ${country}`;

  return {
    country,
    state,
    city,
    locationText,
    isRemote,
    resolvedLocation: {
      country,
      state,
      city,
      isRemote,
      isUnitedStates: isUnitedStatesCountry(country),
      confidence: "high" as const,
      evidence: [
        {
          source: "structured_fields" as const,
          value: locationText,
        },
      ],
    },
  };
}

function diagnosticSourceToken(diagnosticRunId: string) {
  return diagnosticRunId.replace(/[^a-z0-9]+/gi, "").toLowerCase().slice(0, 48);
}

function isUnitedStatesCountry(country: string) {
  return /^(united states|usa|us|u\.s\.|u\.s\.a\.)$/i.test(country.trim());
}

function positiveInteger(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function nonnegativeNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : 0;
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function captureMemoryFallbackWarnings() {
  const originalWarn = console.warn;
  const warnings: string[] = [];

  console.warn = (...args: unknown[]) => {
    const message = args.map((arg) =>
      typeof arg === "string" ? arg : JSON.stringify(arg),
    ).join(" ");
    if (message.includes("[db:fallback]")) {
      warnings.push(message);
    }

    originalWarn(...args);
  };

  return {
    warnings,
    restore() {
      console.warn = originalWarn;
    },
  };
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
              : "Ingestion persistence diagnostic failed unexpectedly.",
          ],
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
