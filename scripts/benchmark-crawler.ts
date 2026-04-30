import { createRequire } from "node:module";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";
import { mkdir, writeFile } from "node:fs/promises";

import { MongoClient, type Db } from "mongodb";

import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import type {
  DiscoveryExecution,
  DiscoveryExecutionStage,
  DiscoveryService,
} from "@/lib/server/discovery/types";
import type {
  CrawlProvider,
  NormalizedJobSeed,
  ProviderBatchProgress,
  ProviderResult,
} from "@/lib/server/providers/types";
import type {
  CrawlMode,
  CrawlResponse,
  CrawlSourceResult,
  JobListing,
  ProviderPlatform,
  SearchFilters,
} from "@/lib/types";

type BenchmarkScenario = {
  id: string;
  title: string;
  country: string;
};

type CliOptions = {
  artifactDir: string;
  crawlMode: CrawlMode;
  dbName: string;
  resetDb: boolean;
  allowSharedDbReset: boolean;
  scenarioLimit?: number;
  scenarioIds?: string[];
  providerTimeoutMs?: number;
  progressUpdateIntervalMs?: number;
  verbose: boolean;
};

type ProviderInstrumentationMetric = {
  provider: ProviderPlatform;
  normalizedJobCount: number;
  invalidUrlCount: number;
  invalidUrlFieldCount: number;
  measuredDurationMs: number;
  invocationCount: number;
  resultFetchedCount: number;
  resultSourceCount: number;
  failureCount: number;
  sampleInvalidUrls: InvalidUrlSample[];
};

type InvalidUrlSample = {
  provider: ProviderPlatform | "discovery_harvest";
  sourceJobId: string;
  title: string;
  field: "sourceUrl" | "applyUrl" | "canonicalUrl";
  value: string;
};

type ProviderBenchmarkMetric = {
  provider: ProviderPlatform;
  status: CrawlSourceResult["status"];
  sourceCount: number;
  jobsFetched: number;
  jobsNormalized: number;
  jobsMatched: number;
  jobsSavedToMongoDb: number;
  invalidUrlCount: number;
  invalidUrlFieldCount: number;
  crawlDurationMs: number;
  timedOut: boolean;
  warningCount: number;
  errorMessage?: string;
};

type BenchmarkDistribution = Record<string, number>;

type ScenarioBenchmarkMetric = {
  id: string;
  title: string;
  country: string;
  crawlMode: CrawlMode;
  status: CrawlResponse["crawlRun"]["status"];
  searchId: string;
  searchSessionId?: string;
  crawlRunId: string;
  jobsFetched: number;
  jobsFetchedPerProvider: Record<string, number>;
  jobsNormalized: number;
  jobsNormalizedPerProvider: Record<string, number>;
  directJobsNormalized: number;
  jobsMatched: number;
  jobsSavedToMongoDb: number;
  jobsReturnedByCrawl: number;
  searchResultCountFromDb: number;
  searchResponseResultCount: number;
  duplicateCount: number;
  dedupeInputCount: number;
  duplicateRatio: number;
  invalidUrlCount: number;
  invalidUrlFieldCount: number;
  expiredJobCount: number;
  crawlDurationMs: number;
  wallDurationMs: number;
  titleRelevanceDistribution: BenchmarkDistribution;
  seniorityDistribution: BenchmarkDistribution;
  locationMatchDistribution: BenchmarkDistribution;
  providers: ProviderBenchmarkMetric[];
  diagnostics: {
    discoveredSources: number;
    crawledSources: number;
    providersEnqueued: number;
    providerFailures: number;
    excludedByTitle: number;
    excludedByLocation: number;
    excludedByExperience: number;
    validationDeferred: number;
    stoppedReason?: string;
    dropReasonCounts: Record<string, number>;
  };
};

type BenchmarkResult = {
  benchmarkVersion: 1;
  generatedAt: string;
  database: {
    uriHost: string;
    databaseName: string;
    resetBeforeRun: boolean;
  };
  config: {
    crawlMode: CrawlMode;
    scenarioCount: number;
    artifactDir: string;
    providerTimeoutMs?: number;
    progressUpdateIntervalMs?: number;
  };
  totals: {
    jobsFetched: number;
    jobsNormalized: number;
    jobsSavedToMongoDb: number;
    searchResultCountFromDb: number;
    duplicateCount: number;
    duplicateRatio: number;
    invalidUrlCount: number;
    invalidUrlFieldCount: number;
    expiredJobCount: number;
    crawlDurationMs: number;
    wallDurationMs: number;
  };
  scenarios: ScenarioBenchmarkMetric[];
  artifacts?: {
    json: string;
    summary: string;
  };
};

type ScenarioMetricInput = {
  scenario: BenchmarkScenario;
  crawlMode: CrawlMode;
  response: CrawlResponse;
  providerInstrumentation: ProviderInstrumentationMetric[];
  directJobsNormalized: number;
  directInvalidUrlCount: number;
  directInvalidUrlFieldCount: number;
  jobsSavedToMongoDb: number;
  savedJobs: readonly JobListing[];
  searchResultCountFromDb: number;
  searchResponseResultCount: number;
  wallDurationMs: number;
};

const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const defaultArtifactDir = "artifacts/benchmark-results";
const benchmarkVersion = 1 as const;
const benchmarkCollectionNames = [
  "searches",
  "searchSessions",
  "jobs",
  "crawlRuns",
  "crawlControls",
  "crawlQueue",
  "crawlSourceResults",
  "crawlRunJobEvents",
  "searchSessionJobEvents",
  "indexedJobEvents",
  "linkValidations",
  "sourceInventory",
] as const;

export const benchmarkScenarios: BenchmarkScenario[] = [
  {
    id: "software-engineer-united-states",
    title: "Software Engineer",
    country: "United States",
  },
  {
    id: "data-analyst-united-states",
    title: "Data Analyst",
    country: "United States",
  },
  {
    id: "product-manager-united-states",
    title: "Product Manager",
    country: "United States",
  },
  {
    id: "machine-learning-engineer-united-states",
    title: "Machine Learning Engineer",
    country: "United States",
  },
];

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

export function buildBenchmarkDatabaseName(mongoUri: string, explicitName?: string) {
  const trimmed = explicitName?.trim();
  if (trimmed) {
    return trimmed;
  }

  return `${databaseNameFromMongoUri(mongoUri)}_benchmark`;
}

export function parseBenchmarkArgs(
  argv: string[],
  env: NodeJS.ProcessEnv = process.env,
): CliOptions {
  const mongoUri = env.MONGODB_URI ?? defaultMongoUri;
  const options: CliOptions = {
    artifactDir: env.BENCHMARK_CRAWLER_ARTIFACT_DIR ?? defaultArtifactDir,
    crawlMode: parseCrawlMode(env.BENCHMARK_CRAWLER_MODE, "fast"),
    dbName: buildBenchmarkDatabaseName(mongoUri, env.BENCHMARK_CRAWLER_DB_NAME),
    resetDb: env.BENCHMARK_CRAWLER_RESET_DB === "false" ? false : true,
    allowSharedDbReset: env.BENCHMARK_CRAWLER_ALLOW_SHARED_DB_RESET === "true",
    providerTimeoutMs: positiveIntegerOrUndefined(
      env.BENCHMARK_CRAWLER_PROVIDER_TIMEOUT_MS,
    ),
    progressUpdateIntervalMs: positiveIntegerOrUndefined(
      env.BENCHMARK_CRAWLER_PROGRESS_UPDATE_INTERVAL_MS,
    ),
    scenarioLimit: positiveIntegerOrUndefined(env.BENCHMARK_CRAWLER_SCENARIO_LIMIT),
    scenarioIds: parseScenarioIds(env.BENCHMARK_CRAWLER_SCENARIO_IDS),
    verbose: env.BENCHMARK_CRAWLER_VERBOSE === "true",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    const next = argv[index + 1];

    if (value === "--help") {
      throw new Error(buildUsage());
    }

    if (value === "--crawl-mode" && next) {
      options.crawlMode = parseCrawlMode(next, options.crawlMode);
      index += 1;
      continue;
    }

    if (value === "--db-name" && next) {
      options.dbName = next;
      index += 1;
      continue;
    }

    if (value === "--artifacts-dir" && next) {
      options.artifactDir = next;
      index += 1;
      continue;
    }

    if (value === "--scenario-limit" && next) {
      options.scenarioLimit = positiveInteger(next, benchmarkScenarios.length);
      index += 1;
      continue;
    }

    if (value === "--scenario-id" && next) {
      options.scenarioIds = [...(options.scenarioIds ?? []), next];
      index += 1;
      continue;
    }

    if (value === "--provider-timeout-ms" && next) {
      options.providerTimeoutMs = positiveInteger(next, 0) || undefined;
      index += 1;
      continue;
    }

    if (value === "--progress-update-interval-ms" && next) {
      options.progressUpdateIntervalMs = positiveInteger(next, 0) || undefined;
      index += 1;
      continue;
    }

    if (value === "--no-reset-db") {
      options.resetDb = false;
      continue;
    }

    if (value === "--allow-shared-db-reset") {
      options.allowSharedDbReset = true;
      continue;
    }

    if (value === "--verbose") {
      options.verbose = true;
      continue;
    }

    throw new Error(`Unknown benchmark option: ${value}\n${buildUsage()}`);
  }

  return options;
}

export function selectBenchmarkScenarios(options: Pick<CliOptions, "scenarioIds" | "scenarioLimit">) {
  const requestedIds = options.scenarioIds?.map((id) => id.trim()).filter(Boolean) ?? [];
  const selected =
    requestedIds.length > 0
      ? benchmarkScenarios.filter((scenario) => requestedIds.includes(scenario.id))
      : benchmarkScenarios;
  const missingIds = requestedIds.filter(
    (id) => !benchmarkScenarios.some((scenario) => scenario.id === id),
  );

  if (missingIds.length > 0) {
    throw new Error(
      `Unknown benchmark scenario id(s): ${missingIds.join(", ")}. Known ids: ${benchmarkScenarios.map((scenario) => scenario.id).join(", ")}`,
    );
  }

  return selected.slice(0, options.scenarioLimit);
}

function parseScenarioIds(value?: string) {
  const ids = value
    ?.split(",")
    .map((id) => id.trim())
    .filter(Boolean);

  return ids && ids.length > 0 ? ids : undefined;
}

export function createProviderBenchmarkRecorder() {
  const providerMetrics = new Map<ProviderPlatform, ProviderMetricDraft>();
  const discoverySeedKeys = new Set<string>();
  const discoveryInvalidSeedKeys = new Set<string>();
  let discoveryInvalidUrlFieldCount = 0;

  const ensureProvider = (provider: ProviderPlatform) => {
    const existing = providerMetrics.get(provider);
    if (existing) {
      return existing;
    }

    const metric: ProviderMetricDraft = {
      provider,
      normalizedSeedKeys: new Set(),
      invalidSeedKeys: new Set(),
      invalidUrlFieldCount: 0,
      sampleInvalidUrls: [],
      measuredDurationMs: 0,
      invocationCount: 0,
      resultFetchedCount: 0,
      resultSourceCount: 0,
      failureCount: 0,
    };
    providerMetrics.set(provider, metric);
    return metric;
  };

  const recordSeeds = (
    provider: ProviderPlatform | "discovery_harvest",
    seeds: readonly NormalizedJobSeed[],
  ) => {
    for (const seed of seeds) {
      const seedKey = buildSeedKey(seed);
      const invalidUrlFields = collectInvalidUrlFields(seed);

      if (provider === "discovery_harvest") {
        discoverySeedKeys.add(seedKey);
        if (invalidUrlFields.length > 0) {
          discoveryInvalidSeedKeys.add(seedKey);
          discoveryInvalidUrlFieldCount += invalidUrlFields.length;
        }
        continue;
      }

      const metric = ensureProvider(provider);
      metric.normalizedSeedKeys.add(seedKey);
      if (invalidUrlFields.length > 0) {
        metric.invalidSeedKeys.add(seedKey);
        metric.invalidUrlFieldCount += invalidUrlFields.length;
        for (const field of invalidUrlFields) {
          pushInvalidUrlSample(metric.sampleInvalidUrls, {
            provider,
            sourceJobId: seed.sourceJobId,
            title: seed.title,
            field,
            value: seed[field] ?? "",
          });
        }
      }
    }
  };

  return {
    recordBatch(batch: ProviderBatchProgress) {
      recordSeeds(batch.provider, batch.jobs);
    },
    recordProviderResult(result: ProviderResult, durationMs: number) {
      const metric = ensureProvider(result.provider);
      metric.measuredDurationMs += durationMs;
      metric.invocationCount += 1;
      metric.resultFetchedCount += result.fetchedCount;
      metric.resultSourceCount += result.sourceCount ?? 0;
      recordSeeds(result.provider, result.jobs);
    },
    recordProviderFailure(provider: ProviderPlatform, durationMs: number) {
      const metric = ensureProvider(provider);
      metric.measuredDurationMs += durationMs;
      metric.invocationCount += 1;
      metric.failureCount += 1;
    },
    recordDiscoveryStage(stage: Pick<DiscoveryExecutionStage, "jobs">) {
      recordSeeds("discovery_harvest", stage.jobs ?? []);
    },
    snapshot() {
      return Array.from(providerMetrics.values())
        .sort((left, right) => left.provider.localeCompare(right.provider))
        .map((metric) => ({
          provider: metric.provider,
          normalizedJobCount: metric.normalizedSeedKeys.size,
          invalidUrlCount: metric.invalidSeedKeys.size,
          invalidUrlFieldCount: metric.invalidUrlFieldCount,
          measuredDurationMs: metric.measuredDurationMs,
          invocationCount: metric.invocationCount,
          resultFetchedCount: metric.resultFetchedCount,
          resultSourceCount: metric.resultSourceCount,
          failureCount: metric.failureCount,
          sampleInvalidUrls: metric.sampleInvalidUrls,
        }));
    },
    discoverySnapshot() {
      return {
        normalizedJobCount: discoverySeedKeys.size,
        invalidUrlCount: discoveryInvalidSeedKeys.size,
        invalidUrlFieldCount: discoveryInvalidUrlFieldCount,
      };
    },
  };
}

type ProviderMetricDraft = {
  provider: ProviderPlatform;
  normalizedSeedKeys: Set<string>;
  invalidSeedKeys: Set<string>;
  invalidUrlFieldCount: number;
  sampleInvalidUrls: InvalidUrlSample[];
  measuredDurationMs: number;
  invocationCount: number;
  resultFetchedCount: number;
  resultSourceCount: number;
  failureCount: number;
};

export function instrumentProviders(
  providers: CrawlProvider[],
  recorder: ReturnType<typeof createProviderBenchmarkRecorder>,
): CrawlProvider[] {
  return providers.map((provider) => ({
    provider: provider.provider,
    supportsSource(source): source is never {
      return provider.supportsSource(source);
    },
    async crawlSources(context, sources) {
      const startedMs = Date.now();
      try {
        const result = await provider.crawlSources(
          {
            ...context,
            onBatch: async (batch) => {
              recorder.recordBatch(batch);
              await context.onBatch?.(batch);
            },
          },
          sources,
        );
        recorder.recordProviderResult(result, Date.now() - startedMs);
        return result;
      } catch (error) {
        recorder.recordProviderFailure(provider.provider, Date.now() - startedMs);
        throw error;
      }
    },
  }));
}

export function instrumentDiscovery(
  discovery: DiscoveryService,
  recorder: ReturnType<typeof createProviderBenchmarkRecorder>,
): DiscoveryService {
  const instrumented: DiscoveryService = {
    async discover(input) {
      return discovery.discover(input);
    },
  };

  if (discovery.discoverWithDiagnostics) {
    const discoverWithDiagnostics = discovery.discoverWithDiagnostics.bind(discovery);
    instrumented.discoverWithDiagnostics = async (input) => {
      const result: DiscoveryExecution = await discoverWithDiagnostics(input);
      recorder.recordDiscoveryStage({ jobs: result.jobs ?? [] });
      return result;
    };
  }

  if (discovery.discoverInStages) {
    const discoverInStages = discovery.discoverInStages.bind(discovery);
    instrumented.discoverInStages = async (input) => {
      const stages = await discoverInStages(input);
      for (const stage of stages) {
        recorder.recordDiscoveryStage(stage);
      }
      return stages;
    };
  }

  if (discovery.discoverBaseline) {
    const discoverBaseline = discovery.discoverBaseline.bind(discovery);
    instrumented.discoverBaseline = async (input) => {
      const stage = await discoverBaseline(input);
      recorder.recordDiscoveryStage(stage);
      return stage;
    };
  }

  if (discovery.discoverSupplemental) {
    const discoverSupplemental = discovery.discoverSupplemental.bind(discovery);
    instrumented.discoverSupplemental = async (input, options) => {
      const stage = await discoverSupplemental(input, options);
      recorder.recordDiscoveryStage(stage);
      return stage;
    };
  }

  return instrumented;
}

export function collectInvalidUrlFields(seed: NormalizedJobSeed) {
  const fields: Array<"sourceUrl" | "applyUrl" | "canonicalUrl"> = [];
  if (!isHttpUrl(seed.sourceUrl)) {
    fields.push("sourceUrl");
  }

  if (!isHttpUrl(seed.applyUrl)) {
    fields.push("applyUrl");
  }

  if (seed.canonicalUrl && !isHttpUrl(seed.canonicalUrl)) {
    fields.push("canonicalUrl");
  }

  return fields;
}

export function buildScenarioBenchmarkMetrics(
  input: ScenarioMetricInput,
): ScenarioBenchmarkMetric {
  const diagnostics = input.response.diagnostics;
  const providerInstrumentationByName = new Map(
    input.providerInstrumentation.map((metric) => [metric.provider, metric]),
  );
  const providerTimingsByName = new Map(
    (diagnostics.performance?.providerTimingsMs ?? []).map((timing) => [
      timing.provider,
      timing,
    ]),
  );
  const providers = input.response.sourceResults
    .map((sourceResult) => {
      const instrumentation = providerInstrumentationByName.get(sourceResult.provider);
      const timing = providerTimingsByName.get(sourceResult.provider);
      return {
        provider: sourceResult.provider,
        status: sourceResult.status,
        sourceCount: sourceResult.sourceCount,
        jobsFetched: sourceResult.fetchedCount,
        jobsNormalized: instrumentation?.normalizedJobCount ?? 0,
        jobsMatched: sourceResult.matchedCount,
        jobsSavedToMongoDb: sourceResult.savedCount,
        invalidUrlCount: instrumentation?.invalidUrlCount ?? 0,
        invalidUrlFieldCount: instrumentation?.invalidUrlFieldCount ?? 0,
        crawlDurationMs:
          timing?.duration ?? instrumentation?.measuredDurationMs ?? 0,
        timedOut: timing?.timedOut ?? sourceResult.status === "timed_out",
        warningCount: sourceResult.warningCount,
        errorMessage: sourceResult.errorMessage,
      } satisfies ProviderBenchmarkMetric;
    })
    .sort((left, right) => left.provider.localeCompare(right.provider));
  const jobsFetchedPerProvider = Object.fromEntries(
    providers.map((provider) => [provider.provider, provider.jobsFetched]),
  );
  const jobsNormalizedPerProvider = Object.fromEntries(
    providers.map((provider) => [provider.provider, provider.jobsNormalized]),
  );
  const providerNormalizedJobs = providers.reduce(
    (total, provider) => total + provider.jobsNormalized,
    0,
  );
  const providerInvalidUrlCount = providers.reduce(
    (total, provider) => total + provider.invalidUrlCount,
    0,
  );
  const providerInvalidUrlFieldCount = providers.reduce(
    (total, provider) => total + provider.invalidUrlFieldCount,
    0,
  );
  const dedupeInputCount = diagnostics.jobsBeforeDedupe;
  const duplicateCount = diagnostics.dedupedOut;
  const crawlDurationMs =
    diagnostics.performance?.stageTimingsMs?.total ??
    durationBetweenIso(input.response.crawlRun.startedAt, input.response.crawlRun.finishedAt) ??
    input.wallDurationMs;
  const expiredJobCount = input.savedJobs.filter(isExpiredOrInactiveJob).length;

  return {
    id: input.scenario.id,
    title: input.scenario.title,
    country: input.scenario.country,
    crawlMode: input.crawlMode,
    status: input.response.crawlRun.status,
    searchId: input.response.search._id,
    searchSessionId: input.response.searchSession?._id,
    crawlRunId: input.response.crawlRun._id,
    jobsFetched: input.response.crawlRun.totalFetchedJobs,
    jobsFetchedPerProvider,
    jobsNormalized: providerNormalizedJobs + input.directJobsNormalized,
    jobsNormalizedPerProvider,
    directJobsNormalized: input.directJobsNormalized,
    jobsMatched: input.response.crawlRun.totalMatchedJobs,
    jobsSavedToMongoDb: input.jobsSavedToMongoDb,
    jobsReturnedByCrawl: input.response.jobs.length,
    searchResultCountFromDb: input.searchResultCountFromDb,
    searchResponseResultCount: input.searchResponseResultCount,
    duplicateCount,
    dedupeInputCount,
    duplicateRatio: dedupeInputCount > 0 ? duplicateCount / dedupeInputCount : 0,
    invalidUrlCount: providerInvalidUrlCount + input.directInvalidUrlCount,
    invalidUrlFieldCount:
      providerInvalidUrlFieldCount + input.directInvalidUrlFieldCount,
    expiredJobCount,
    crawlDurationMs,
    wallDurationMs: input.wallDurationMs,
    titleRelevanceDistribution: buildTitleRelevanceDistribution(input.savedJobs),
    seniorityDistribution: buildSeniorityDistribution(input.savedJobs),
    locationMatchDistribution: buildLocationMatchDistribution(input.savedJobs),
    providers,
    diagnostics: {
      discoveredSources: diagnostics.discoveredSources,
      crawledSources: diagnostics.crawledSources,
      providersEnqueued: diagnostics.providersEnqueued,
      providerFailures: diagnostics.providerFailures,
      excludedByTitle: diagnostics.excludedByTitle,
      excludedByLocation: diagnostics.excludedByLocation,
      excludedByExperience: diagnostics.excludedByExperience,
      validationDeferred: diagnostics.validationDeferred,
      stoppedReason: diagnostics.stoppedReason,
      dropReasonCounts: diagnostics.dropReasonCounts,
    },
  };
}

export function buildBenchmarkTotals(
  scenarios: readonly ScenarioBenchmarkMetric[],
): BenchmarkResult["totals"] {
  const jobsNormalized = sum(scenarios, (scenario) => scenario.jobsNormalized);
  const duplicateCount = sum(scenarios, (scenario) => scenario.duplicateCount);
  const dedupeInputCount = sum(scenarios, (scenario) => scenario.dedupeInputCount);

  return {
    jobsFetched: sum(scenarios, (scenario) => scenario.jobsFetched),
    jobsNormalized,
    jobsSavedToMongoDb: sum(scenarios, (scenario) => scenario.jobsSavedToMongoDb),
    searchResultCountFromDb: sum(
      scenarios,
      (scenario) => scenario.searchResultCountFromDb,
    ),
    duplicateCount,
    duplicateRatio: dedupeInputCount > 0 ? duplicateCount / dedupeInputCount : 0,
    invalidUrlCount: sum(scenarios, (scenario) => scenario.invalidUrlCount),
    invalidUrlFieldCount: sum(
      scenarios,
      (scenario) => scenario.invalidUrlFieldCount,
    ),
    expiredJobCount: sum(scenarios, (scenario) => scenario.expiredJobCount),
    crawlDurationMs: sum(scenarios, (scenario) => scenario.crawlDurationMs),
    wallDurationMs: sum(scenarios, (scenario) => scenario.wallDurationMs),
  };
}

export function formatBenchmarkSummary(result: BenchmarkResult) {
  const lines = [
    `Crawler benchmark ${result.generatedAt}`,
    `Database: ${result.database.databaseName} (${result.database.uriHost})`,
    `Mode: ${result.config.crawlMode}`,
    `Scenarios: ${result.scenarios.length}`,
    "",
    "Totals",
    `  Jobs fetched: ${result.totals.jobsFetched}`,
    `  Jobs normalized: ${result.totals.jobsNormalized}`,
    `  Jobs saved to MongoDB: ${result.totals.jobsSavedToMongoDb}`,
    `  Search results from DB: ${result.totals.searchResultCountFromDb}`,
    `  Duplicate ratio: ${formatRatio(result.totals.duplicateRatio)} (${result.totals.duplicateCount} duplicates)`,
    `  Invalid URL count: ${result.totals.invalidUrlCount}`,
    `  Expired job count: ${result.totals.expiredJobCount}`,
    `  Total crawl duration: ${formatDuration(result.totals.crawlDurationMs)}`,
    "",
    "Scenario Summary",
    ...result.scenarios.map((scenario) =>
      [
        `  ${scenario.title}, ${scenario.country}`,
        `status=${scenario.status}`,
        `fetched=${scenario.jobsFetched}`,
        `normalized=${scenario.jobsNormalized}`,
        `saved=${scenario.jobsSavedToMongoDb}`,
        `dbSearch=${scenario.searchResultCountFromDb}`,
        `dupRatio=${formatRatio(scenario.duplicateRatio)}`,
        `invalidUrls=${scenario.invalidUrlCount}`,
        `expired=${scenario.expiredJobCount}`,
        `duration=${formatDuration(scenario.crawlDurationMs)}`,
      ].join(" | "),
    ),
    "",
    "Scenario Distributions",
    ...result.scenarios.flatMap((scenario) => [
      `  ${scenario.title}, ${scenario.country}`,
      `    titleRelevance=${formatDistribution(scenario.titleRelevanceDistribution)}`,
      `    seniority=${formatDistribution(scenario.seniorityDistribution)}`,
      `    location=${formatDistribution(scenario.locationMatchDistribution)}`,
    ]),
    "",
    "Provider Summary",
  ];

  for (const scenario of result.scenarios) {
    lines.push(`  ${scenario.title}, ${scenario.country}`);
    for (const provider of scenario.providers) {
      lines.push(
        [
          `    ${provider.provider}`,
          `status=${provider.status}`,
          `sources=${provider.sourceCount}`,
          `fetched=${provider.jobsFetched}`,
          `normalized=${provider.jobsNormalized}`,
          `matched=${provider.jobsMatched}`,
          `saved=${provider.jobsSavedToMongoDb}`,
          `invalidUrls=${provider.invalidUrlCount}`,
          `duration=${formatDuration(provider.crawlDurationMs)}`,
        ].join(" | "),
      );
    }
  }

  if (result.artifacts) {
    lines.push(
      "",
      "Artifacts",
      `  JSON: ${result.artifacts.json}`,
      `  Summary: ${result.artifacts.summary}`,
    );
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  const options = parseBenchmarkArgs(process.argv.slice(2));
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const baseDbName = databaseNameFromMongoUri(mongoUri);
  assertSafeResetTarget({
    baseDbName,
    benchmarkDbName: options.dbName,
    resetDb: options.resetDb,
    allowSharedDbReset: options.allowSharedDbReset,
  });

  installServerOnlyShim();

  const [
    { ensureDatabaseIndexes, resetDatabaseIndexesForTests },
    { JobCrawlerRepository },
    { createDefaultProviders },
    { createDiscoveryService },
    { runSearchIngestionFromFilters, runSearchFromFilters },
    { getIndexedJobsForSearch },
  ] = await Promise.all([
    import("@/lib/server/db/indexes"),
    import("@/lib/server/db/repository"),
    import("@/lib/server/providers"),
    import("@/lib/server/discovery/service"),
    import("@/lib/server/search/service"),
    import("@/lib/server/search/indexed-jobs"),
  ]);

  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: Number(
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "30000",
    ),
  });
  const generatedAt = new Date().toISOString();
  const artifactDir = path.resolve(process.cwd(), options.artifactDir);
  const scenarios = selectBenchmarkScenarios(options);
  const metrics: ScenarioBenchmarkMetric[] = [];

  await client.connect();
  try {
    const db = client.db(options.dbName);
    if (options.resetDb) {
      await db.dropDatabase();
    }
    await ensureBenchmarkCollections(db);
    resetDatabaseIndexesForTests();
    await ensureDatabaseIndexes(db as never);

    const repository = new JobCrawlerRepository(db as never) as JobCrawlerRepository;
    const restoreConsole = options.verbose ? undefined : muteCrawlerInfoLogs();

    try {
      for (const scenario of scenarios) {
        const scenarioStartedMs = Date.now();
        const recorder = createProviderBenchmarkRecorder();
        const providers = instrumentProviders(createDefaultProviders(), recorder);
        const discovery = instrumentDiscovery(
          createDiscoveryService({ repository }),
          recorder,
        );
        const filters: SearchFilters = {
          title: scenario.title,
          country: scenario.country,
          crawlMode: options.crawlMode,
        };
        const response = await runSearchIngestionFromFilters(filters, {
          repository,
          discovery,
          providers,
          fetchImpl: fetch,
          now: new Date(),
          requestOwnerKey: `benchmark:crawler:${scenario.id}`,
          linkValidationMode: "deferred",
          providerTimeoutMs: options.providerTimeoutMs,
          progressUpdateIntervalMs: options.progressUpdateIntervalMs,
        });
        const directDiscovery = recorder.discoverySnapshot();
        const [savedJobsForRun, indexedSearch, searchResponse] = await Promise.all([
          repository.getJobsByCrawlRun(response.crawlRun._id),
          getIndexedJobsForSearch(repository, filters),
          runSearchFromFilters(filters, {
            repository,
            providers: [],
            discovery: createEmptyDiscoveryService(),
            fetchImpl: fetch,
            now: new Date(),
            requestOwnerKey: `benchmark:crawler:db-search:${scenario.id}`,
          }),
        ]);

        metrics.push(
          buildScenarioBenchmarkMetrics({
            scenario,
            crawlMode: options.crawlMode,
            response,
            providerInstrumentation: recorder.snapshot(),
            directJobsNormalized: directDiscovery.normalizedJobCount,
            directInvalidUrlCount: directDiscovery.invalidUrlCount,
            directInvalidUrlFieldCount: directDiscovery.invalidUrlFieldCount,
            jobsSavedToMongoDb: savedJobsForRun.length,
            savedJobs: savedJobsForRun,
            searchResultCountFromDb: indexedSearch.matchedCount,
            searchResponseResultCount:
              searchResponse.totalMatchedCount ?? searchResponse.jobs.length,
            wallDurationMs: Date.now() - scenarioStartedMs,
          }),
        );
      }
    } finally {
      restoreConsole?.();
    }
  } finally {
    await client.close();
  }

  const result: BenchmarkResult = {
    benchmarkVersion,
    generatedAt,
    database: {
      uriHost: uriHostFromMongoUri(mongoUri),
      databaseName: options.dbName,
      resetBeforeRun: options.resetDb,
    },
    config: {
      crawlMode: options.crawlMode,
      scenarioCount: metrics.length,
      artifactDir,
      providerTimeoutMs: options.providerTimeoutMs,
      progressUpdateIntervalMs: options.progressUpdateIntervalMs,
    },
    totals: buildBenchmarkTotals(metrics),
    scenarios: metrics,
  };

  await mkdir(artifactDir, { recursive: true });
  const timestamp = generatedAt.replace(/[:.]/g, "-");
  const jsonPath = path.join(artifactDir, `crawler-benchmark-${timestamp}.json`);
  const summaryPath = path.join(artifactDir, `crawler-benchmark-${timestamp}.txt`);
  result.artifacts = {
    json: jsonPath,
    summary: summaryPath,
  };
  const summary = formatBenchmarkSummary(result);
  await Promise.all([
    writeFile(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8"),
    writeFile(summaryPath, summary, "utf8"),
    writeFile(
      path.join(artifactDir, "latest-crawler-benchmark.json"),
      `${JSON.stringify(result, null, 2)}\n`,
      "utf8",
    ),
    writeFile(path.join(artifactDir, "latest-crawler-benchmark.txt"), summary, "utf8"),
  ]);

  console.log(summary);
  console.log(JSON.stringify(result, null, 2));
}

function createEmptyDiscoveryService(): DiscoveryService {
  return {
    async discover() {
      return [];
    },
  };
}

async function ensureBenchmarkCollections(db: Db) {
  await Promise.all(
    benchmarkCollectionNames.map(async (collectionName) => {
      try {
        await db.createCollection(collectionName);
      } catch (error) {
        if (isMongoNamespaceExistsError(error)) {
          return;
        }

        throw error;
      }
    }),
  );
}

function isMongoNamespaceExistsError(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === 48
  );
}

function assertSafeResetTarget(input: {
  baseDbName: string;
  benchmarkDbName: string;
  resetDb: boolean;
  allowSharedDbReset: boolean;
}) {
  if (
    input.resetDb &&
    input.baseDbName === input.benchmarkDbName &&
    !input.allowSharedDbReset
  ) {
    throw new Error(
      `Refusing to reset MongoDB database "${input.benchmarkDbName}" because it matches MONGODB_URI. Use --db-name with a benchmark database or pass --allow-shared-db-reset.`,
    );
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

function muteCrawlerInfoLogs() {
  const originalInfo = console.info;
  console.info = () => undefined;

  return () => {
    console.info = originalInfo;
  };
}

function parseCrawlMode(value: string | undefined, fallback: CrawlMode): CrawlMode {
  if (value === "fast" || value === "balanced" || value === "deep") {
    return value;
  }

  return fallback;
}

function positiveInteger(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function positiveIntegerOrUndefined(value: string | undefined) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

function buildUsage() {
  return [
    "Usage: npm run benchmark:crawler -- [options]",
    "",
    "Options:",
    "  --crawl-mode fast|balanced|deep",
    "  --db-name <name>",
    "  --artifacts-dir <path>",
    "  --scenario-limit <count>",
    "  --scenario-id <id>",
    "  --provider-timeout-ms <ms>",
    "  --progress-update-interval-ms <ms>",
    "  --no-reset-db",
    "  --allow-shared-db-reset",
    "  --verbose",
  ].join("\n");
}

function buildSeedKey(seed: NormalizedJobSeed) {
  return [
    seed.sourcePlatform,
    seed.sourceCompanySlug ?? "",
    seed.sourceJobId,
    seed.canonicalUrl ?? seed.applyUrl ?? seed.sourceUrl,
    seed.title,
    seed.company,
  ].join("|");
}

function isHttpUrl(value: string | undefined) {
  if (!value) {
    return false;
  }

  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function pushInvalidUrlSample(target: InvalidUrlSample[], sample: InvalidUrlSample) {
  if (target.length < 10) {
    target.push(sample);
  }
}

function buildTitleRelevanceDistribution(jobs: readonly JobListing[]) {
  return buildDistribution(
    jobs,
    (job) => readStringMetadata(job.rawSourceMetadata, ["crawlTitleMatch", "tier"]) ?? "unknown",
  );
}

function buildSeniorityDistribution(jobs: readonly JobListing[]) {
  return buildDistribution(
    jobs,
    (job) =>
      job.experienceLevel ??
      job.experienceClassification?.explicitLevel ??
      job.experienceClassification?.inferredLevel ??
      "unknown",
  );
}

function buildLocationMatchDistribution(jobs: readonly JobListing[]) {
  return buildDistribution(jobs, categorizeLocationMatch);
}

function categorizeLocationMatch(job: JobListing) {
  const location = job.resolvedLocation;
  if (location?.isUnitedStates) {
    if (location.isRemote) {
      return "remote_us";
    }

    if (location.city && location.state) {
      return "city_state_us";
    }

    if (location.state) {
      return "state_us";
    }

    return "united_states";
  }

  if (location?.country) {
    return `country:${normalizeDistributionKey(location.country)}`;
  }

  if (job.country) {
    return `country:${normalizeDistributionKey(job.country)}`;
  }

  return "unknown";
}

function isExpiredOrInactiveJob(job: JobListing) {
  return job.isActive === false || Boolean(job.closedAt) || job.linkStatus === "invalid";
}

function readStringMetadata(
  metadata: Record<string, unknown> | undefined,
  pathParts: string[],
) {
  let current: unknown = metadata;
  for (const part of pathParts) {
    if (!current || typeof current !== "object" || !(part in current)) {
      return undefined;
    }

    current = (current as Record<string, unknown>)[part];
  }

  return typeof current === "string" && current.trim() ? current.trim() : undefined;
}

function buildDistribution<T>(
  values: readonly T[],
  classify: (value: T) => string | undefined,
) {
  const distribution: BenchmarkDistribution = {};
  for (const value of values) {
    const key = normalizeDistributionKey(classify(value) ?? "unknown");
    distribution[key] = (distribution[key] ?? 0) + 1;
  }

  return sortRecordByKey(distribution);
}

function normalizeDistributionKey(value: string) {
  return value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "unknown";
}

function sortRecordByKey(record: BenchmarkDistribution) {
  return Object.fromEntries(Object.entries(record).sort(([left], [right]) => left.localeCompare(right)));
}

function durationBetweenIso(startedAt?: string, finishedAt?: string | null) {
  if (!startedAt || !finishedAt) {
    return undefined;
  }

  const startedMs = Date.parse(startedAt);
  const finishedMs = Date.parse(finishedAt);
  if (!Number.isFinite(startedMs) || !Number.isFinite(finishedMs)) {
    return undefined;
  }

  return Math.max(0, finishedMs - startedMs);
}

function sum<T>(values: readonly T[], select: (value: T) => number) {
  return values.reduce((total, value) => total + select(value), 0);
}

function formatDuration(value: number) {
  if (value < 1000) {
    return `${Math.round(value)}ms`;
  }

  return `${(value / 1000).toFixed(1)}s`;
}

function formatRatio(value: number) {
  return `${(value * 100).toFixed(1)}%`;
}

function formatDistribution(distribution: BenchmarkDistribution) {
  const entries = Object.entries(distribution);
  if (entries.length === 0) {
    return "none";
  }

  return entries.map(([key, count]) => `${key}:${count}`).join(", ");
}

function isMainModule() {
  return process.argv[1]
    ? import.meta.url === pathToFileURL(process.argv[1]).href
    : false;
}

if (isMainModule()) {
  main().catch((error) => {
    console.error(
      "[benchmark:crawler] failed",
      error instanceof Error
        ? {
            name: error.name,
            message: error.message,
            stack: error.stack,
          }
        : error,
    );
    process.exitCode = 1;
  });
}
