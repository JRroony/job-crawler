import "server-only";

import {
  backgroundIngestionOwnerKey,
  selectBackgroundSystemSearchProfiles,
  type SystemSearchProfile,
} from "@/lib/server/background/constants";
import { queueSearchRun } from "@/lib/server/crawler/background-runs";
import { CrawlAbortedError, seedToPersistableJob } from "@/lib/server/crawler/pipeline";
import { createId, runWithConcurrency } from "@/lib/server/crawler/helpers";
import {
  toDiscoveredSourceFromInventory,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { getEnv } from "@/lib/server/env";
import {
  planPersistentInventoryRecurringCrawl,
  runPersistentInventoryExpansion,
  refreshPersistentSourceInventory,
  type InventoryExpansionResult,
} from "@/lib/server/inventory/service";
import { resolveObservedSourceNextEligibleAt } from "@/lib/server/inventory/selection";
import { createDefaultProviders } from "@/lib/server/providers";
import {
  buildProviderSeedDiagnosticContext,
  validateNormalizedJobSeedForHydration,
  type ProviderSeedValidationDrop,
} from "@/lib/server/providers/shared";
import { selectProvidersForTieredCrawl } from "@/lib/server/providers/tiers";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import type {
  JobCrawlerRepository,
  PersistableJob,
  PersistJobsWithStatsResult,
} from "@/lib/server/db/repository";
import type {
  CrawlDiagnostics,
  CrawlRunStatus,
  CrawlSourceResult,
  SearchDocument,
  SearchSessionDocument,
} from "@/lib/types";
import { persistableJobSchema } from "@/lib/types";

type BackgroundIngestionRuntime = {
  repository?: JobCrawlerRepository;
  providers?: CrawlProvider[];
  fetchImpl?: typeof fetch;
  now?: Date;
  intervalMs?: number;
  staleAfterMs?: number;
  runTimeoutMs?: number;
  maxSources?: number;
  maxSourcesPerProvider?: number;
  maxProfiles?: number;
  initialDelayMs?: number;
  providerTimeoutMs?: number;
  sourceTimeoutMs?: number;
  providerConcurrency?: number;
  schedulingIntervalMs?: number;
  refreshInventory?: (runtime: {
    repository: JobCrawlerRepository;
    now: Date;
  }) => Promise<SourceInventoryRecord[]>;
  expandInventory?: (runtime: {
    repository: JobCrawlerRepository;
    now: Date;
    fetchImpl?: typeof fetch;
    intervalMs: number;
    maxSources: number;
    expansionFilters: SearchDocument["filters"][];
    systemProfile: SystemSearchProfile;
  }) => Promise<InventoryExpansionResult>;
  resolveRepository?: () => Promise<JobCrawlerRepository>;
};

type BackgroundIngestionTriggerResult =
  | {
      status: "started";
      searchId: string;
      crawlRunId: string;
      systemProfileId: string;
      selectedProfileIds: string[];
      startedRuns: BackgroundIngestionStartedRun[];
      skippedActiveRuns: BackgroundIngestionSkippedRun[];
      sourceBudgetPerCycle: number;
      sourceBudgetPerProfile: number;
    }
  | {
      status:
        | "skipped-disabled"
        | "skipped-active"
        | "skipped-no-mongo"
        | "skipped-mongo-transient"
        | "skipped-bootstrap-running"
        | "skipped-bootstrap-failed";
      searchId?: string;
      systemProfileId?: string;
      reason?: "mongo_unavailable" | "mongo_transient" | "bootstrap_running" | "bootstrap_failed";
      phase?: "repository_resolution" | "index_initialization";
      message?: string;
    };

type BackgroundIngestionStartedRun = {
  searchId: string;
  crawlRunId: string;
  systemProfileId: string;
};

type BackgroundIngestionSkippedRun = {
  searchId?: string;
  systemProfileId?: string;
};

type BackgroundSchedulerState = {
  startedAt: string;
  intervalMs: number;
  initialDelayMs: number;
  timer: ReturnType<typeof setTimeout> | null;
};

type BackgroundPersistenceCounts = Pick<
  PersistJobsWithStatsResult,
  "insertedCount" | "updatedCount" | "linkedToRunCount" | "indexedEventCount"
>;

type BackgroundProviderThroughputCounts = BackgroundPersistenceCounts & {
  sourceCount: number;
  fetchedCount: number;
  matchedCount: number;
  seedCount: number;
  warningCount: number;
  failedBatches: number;
};

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerBackgroundSchedulerState: BackgroundSchedulerState | undefined;
}

export function startRecurringBackgroundIngestionScheduler(
  runtime: BackgroundIngestionRuntime = {},
) {
  const env = getEnv();
  if (!env.BACKGROUND_INGESTION_ENABLED) {
    const result = {
      started: false,
      reason: "disabled" as const,
    };
    logBackgroundSchedulerStart(result);
    return result;
  }

  const existing = globalThis.__jobCrawlerBackgroundSchedulerState;
  if (existing) {
    const result = {
      started: true,
      reason: "already-started" as const,
      intervalMs: existing.intervalMs,
      initialDelayMs: existing.initialDelayMs,
    };
    logBackgroundSchedulerStart(result);
    return result;
  }

  const intervalMs = Math.max(
    1,
    Math.floor(runtime.intervalMs ?? env.BACKGROUND_INGESTION_INTERVAL_MS),
  );
  const initialDelayMs = Math.max(0, Math.floor(runtime.initialDelayMs ?? 0));
  const state: BackgroundSchedulerState = {
    startedAt: new Date().toISOString(),
    intervalMs,
    initialDelayMs,
    timer: null,
  };

  const scheduleNext = (delayMs: number) => {
    state.timer = setTimeout(() => {
      scheduleNext(intervalMs);
      void triggerRecurringBackgroundIngestion({
        ...runtime,
        maxProfiles: runtime.maxProfiles ?? env.BACKGROUND_INGESTION_PROFILES_PER_CYCLE,
        maxSources: runtime.maxSources ?? env.BACKGROUND_INGESTION_MAX_SOURCES_PER_CYCLE,
        maxSourcesPerProvider:
          runtime.maxSourcesPerProvider ?? env.BACKGROUND_INGESTION_MAX_SOURCES_PER_PROVIDER,
        providerTimeoutMs:
          runtime.providerTimeoutMs ?? env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS,
        sourceTimeoutMs:
          runtime.sourceTimeoutMs ?? env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS,
        providerConcurrency:
          runtime.providerConcurrency ?? env.BACKGROUND_INGESTION_PROVIDER_CONCURRENCY,
        schedulingIntervalMs: runtime.schedulingIntervalMs ?? intervalMs,
      }).catch((error) => {
        console.error("[background-ingestion:scheduler]", {
          message:
            error instanceof Error
              ? error.message
              : "Recurring background ingestion failed unexpectedly.",
        });
      });
    }, delayMs);
    state.timer.unref?.();
  };

  globalThis.__jobCrawlerBackgroundSchedulerState = state;
  scheduleNext(initialDelayMs);

  const result = {
    started: true,
    reason: "started" as const,
    intervalMs,
    initialDelayMs,
  };
  logBackgroundSchedulerStart(result);
  return result;
}

export function stopRecurringBackgroundIngestionScheduler() {
  const state = globalThis.__jobCrawlerBackgroundSchedulerState;
  if (!state) {
    return;
  }

  if (state.timer) {
    clearTimeout(state.timer);
  }

  globalThis.__jobCrawlerBackgroundSchedulerState = undefined;
}

export async function triggerRecurringBackgroundIngestion(
  runtime: BackgroundIngestionRuntime = {},
): Promise<BackgroundIngestionTriggerResult> {
  const env = getEnv();
  if (!env.BACKGROUND_INGESTION_ENABLED) {
    const result = { status: "skipped-disabled" as const };
    logBackgroundIngestionTrigger(result);
    return result;
  }

  const repositoryResolution = await resolveDurableBackgroundRepository(runtime);
  if (!repositoryResolution.repository) {
    const result = {
      status:
        repositoryResolution.reason === "bootstrap_running"
          ? "skipped-bootstrap-running"
          : repositoryResolution.reason === "mongo_transient"
            ? "skipped-mongo-transient"
            : repositoryResolution.reason === "bootstrap_failed"
              ? "skipped-bootstrap-failed"
              : "skipped-no-mongo",
      reason: repositoryResolution.reason,
      phase: repositoryResolution.phase,
      message: repositoryResolution.message,
    } as const;
    logBackgroundIngestionTrigger(result);
    return result;
  }
  const repository = repositoryResolution.repository;

  const now = runtime.now ?? new Date();
  const schedulingIntervalMs = Math.max(
    1,
    Math.floor(runtime.schedulingIntervalMs ?? env.BACKGROUND_INGESTION_INTERVAL_MS),
  );
  const maxProfiles = Math.max(1, Math.floor(runtime.maxProfiles ?? 1));
  const systemProfiles = selectBackgroundSystemSearchProfiles({
    now,
    intervalMs: schedulingIntervalMs,
    maxProfiles,
    profileRunStates: await repository.listSystemSearchProfileRunStates(),
  });
  if (systemProfiles.length === 0) {
    const result = {
      status: "skipped-disabled" as const,
      message:
        "No enabled system search profiles are currently eligible for recurring ingestion.",
    };
    logBackgroundIngestionTrigger(result);
    return result;
  }

  const sourceBudgetPerCycle = Math.max(
    1,
    Math.floor(runtime.maxSources ?? env.BACKGROUND_INGESTION_MAX_SOURCES_PER_CYCLE),
  );
  const sourceBudgetPerProfile = Math.max(
    1,
    Math.ceil(sourceBudgetPerCycle / systemProfiles.length),
  );
  const providerTimeoutMs = Math.max(
    1,
    Math.floor(runtime.providerTimeoutMs ?? env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS),
  );
  const sourceTimeoutMs = Math.max(
    1,
    Math.floor(runtime.sourceTimeoutMs ?? env.BACKGROUND_INGESTION_SOURCE_TIMEOUT_MS),
  );
  const maxSourcesPerProvider = Math.max(
    1,
    Math.floor(
      runtime.maxSourcesPerProvider ?? env.BACKGROUND_INGESTION_MAX_SOURCES_PER_PROVIDER,
    ),
  );
  const providerConcurrency = Math.max(
    1,
    Math.floor(runtime.providerConcurrency ?? env.BACKGROUND_INGESTION_PROVIDER_CONCURRENCY),
  );
  const runTimeoutMs = Math.max(
    1,
    Math.floor(runtime.runTimeoutMs ?? env.BACKGROUND_INGESTION_RUN_TIMEOUT_MS),
  );

  console.info("[background-ingestion:profiles-selected]", {
    selectedProfiles: systemProfiles.length,
    selectedProfileIds: systemProfiles.map((profile) => profile.id),
    selectedProfileLabels: systemProfiles.map((profile) => profile.label),
    sourceBudgetPerCycle,
    sourceBudgetPerProfile,
    schedulingIntervalMs,
    providerTimeoutMs,
    sourceTimeoutMs,
    maxSourcesPerProvider,
    providerConcurrency,
    runTimeoutMs,
  });

  const startedRuns: BackgroundIngestionStartedRun[] = [];
  const skippedActiveRuns: BackgroundIngestionSkippedRun[] = [];

  for (const systemProfile of systemProfiles) {
    const result = await startRecurringBackgroundProfileIngestion({
      repository,
      runtime,
      now,
      systemProfile,
      schedulingIntervalMs,
      staleAfterMs: Math.max(
        1,
        Math.floor(runtime.staleAfterMs ?? env.BACKGROUND_INGESTION_STALE_AFTER_MS),
      ),
      runTimeoutMs,
      maxSources: sourceBudgetPerProfile,
      maxSourcesPerProvider,
      providerTimeoutMs,
      sourceTimeoutMs,
      providerConcurrency,
      cycleDiagnostics: {
        selectedProfiles: systemProfiles.length,
        selectedProfileIds: systemProfiles.map((profile) => profile.id),
        selectedProfileLabels: systemProfiles.map((profile) => profile.label),
        startedRuns: 0,
        skippedActiveRuns: 0,
        sourceBudgetPerCycle,
        sourceBudgetPerProfile,
        schedulingIntervalMs,
        providerTimeoutMs,
        sourceTimeoutMs,
        maxSourcesPerProvider,
        providerConcurrency,
        runTimeoutMs,
      },
    });

    if (result.status === "started") {
      startedRuns.push({
        searchId: result.searchId,
        crawlRunId: result.crawlRunId,
        systemProfileId: result.systemProfileId,
      });
      continue;
    }

    if (result.status === "skipped-active") {
      skippedActiveRuns.push({
        searchId: result.searchId,
        systemProfileId: result.systemProfileId,
      });
    }
  }

  if (startedRuns.length > 0) {
    const firstRun = startedRuns[0]!;
    const result = {
      status: "started",
      searchId: firstRun.searchId,
      crawlRunId: firstRun.crawlRunId,
      systemProfileId: firstRun.systemProfileId,
      selectedProfileIds: systemProfiles.map((profile) => profile.id),
      startedRuns,
      skippedActiveRuns,
      sourceBudgetPerCycle,
      sourceBudgetPerProfile,
    } as const;
    logBackgroundIngestionTrigger(result);
    return result;
  }

  const activeRun = skippedActiveRuns[0];
  if (activeRun) {
    const result = {
      status: "skipped-active",
      searchId: activeRun.searchId,
      systemProfileId: activeRun.systemProfileId,
    } as const;
    logBackgroundIngestionTrigger(result);
    return result;
  }

  const result = {
    status: "skipped-disabled" as const,
    message: "No recurring background ingestion profile run could be queued.",
  };
  logBackgroundIngestionTrigger(result);
  return result;
}

async function startRecurringBackgroundProfileIngestion(input: {
  repository: JobCrawlerRepository;
  runtime: BackgroundIngestionRuntime;
  now: Date;
  systemProfile: SystemSearchProfile;
  schedulingIntervalMs: number;
  staleAfterMs: number;
  runTimeoutMs: number;
  maxSources: number;
  maxSourcesPerProvider: number;
  providerTimeoutMs: number;
  sourceTimeoutMs: number;
  providerConcurrency: number;
  cycleDiagnostics: NonNullable<CrawlDiagnostics["backgroundCycle"]>;
}): Promise<
  | { status: "started"; searchId: string; crawlRunId: string; systemProfileId: string }
  | { status: "skipped-active"; searchId?: string; systemProfileId?: string }
> {
  const {
    repository,
    runtime,
    now,
    systemProfile,
    schedulingIntervalMs,
    staleAfterMs,
    runTimeoutMs,
    maxSources,
    maxSourcesPerProvider,
    providerTimeoutMs,
    sourceTimeoutMs,
    providerConcurrency,
    cycleDiagnostics,
  } = input;
  const search = await ensureBackgroundSearch(repository, now, systemProfile);

  await recoverStaleBackgroundRunIfNeeded(search._id, repository, now, staleAfterMs);

  const ownerKey = resolveBackgroundRunOwnerKey(systemProfile.id, cycleDiagnostics.selectedProfiles);
  const activeBackgroundQueueEntry =
    await repository.getActiveCrawlQueueEntryForOwner(ownerKey);
  if (activeBackgroundQueueEntry && activeBackgroundQueueEntry.searchId !== search._id) {
    const activeSearch = await repository.getSearch(activeBackgroundQueueEntry.searchId);
    return {
      status: "skipped-active" as const,
      searchId: activeBackgroundQueueEntry.searchId,
      systemProfileId: activeSearch?.systemProfileId,
    };
  }

  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForSearch(search._id);
  if (activeQueueEntry) {
    return {
      status: "skipped-active" as const,
      searchId: search._id,
      systemProfileId: systemProfile.id,
    };
  }

  const searchSession = await repository.createSearchSession(search._id, now.toISOString(), {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, now.toISOString(), {
    stage: "queued",
    validationMode: "deferred",
    searchSessionId: searchSession._id,
  });

  await Promise.all([
    repository.updateSearchLatestSession(search._id, searchSession._id, "running", now.toISOString()),
    repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now.toISOString()),
    repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: now.toISOString(),
    }),
  ]);

  const queued = await queueSearchRun(
    search._id,
    repository,
    async (signal) => {
      await executeRecurringInventoryIngestion(
        {
          search,
          searchSession,
          crawlRunId: crawlRun._id,
          systemProfile,
        },
        {
          repository,
          providers: runtime.providers,
          fetchImpl: runtime.fetchImpl,
          now,
          signal,
          runTimeoutMs,
          maxSources,
          maxSourcesPerProvider,
          providerTimeoutMs,
          sourceTimeoutMs,
          providerConcurrency,
          schedulingIntervalMs,
          cycleDiagnostics: {
            ...cycleDiagnostics,
            startedRuns: 1,
            skippedActiveRuns: 0,
          },
          refreshInventory: runtime.refreshInventory,
          expandInventory: runtime.expandInventory,
        },
      );
    },
    {
      ownerKey,
      crawlRunId: crawlRun._id,
      searchSessionId: searchSession._id,
      queuedAt: now.toISOString(),
    },
  );

  if (!queued) {
    return {
      status: "skipped-active" as const,
      searchId: search._id,
      systemProfileId: systemProfile.id,
    };
  }

  return {
    status: "started",
    searchId: search._id,
    crawlRunId: crawlRun._id,
    systemProfileId: systemProfile.id,
  } as const;
}

function logBackgroundSchedulerStart(result: {
  started: boolean;
  reason: "disabled" | "already-started" | "started";
  intervalMs?: number;
  initialDelayMs?: number;
}) {
  const payload = {
    started: result.started,
    reason: result.reason,
    intervalMs: result.intervalMs,
    initialDelayMs: result.initialDelayMs,
  };

  if (result.started) {
    console.info("[background-ingestion:scheduler-start]", payload);
    return;
  }

  console.warn("[background-ingestion:scheduler-start]", payload);
}

function logBackgroundIngestionTrigger(result: BackgroundIngestionTriggerResult) {
  const payload = {
    status: result.status,
    searchId: "searchId" in result ? result.searchId : undefined,
    crawlRunId: "crawlRunId" in result ? result.crawlRunId : undefined,
    systemProfileId: "systemProfileId" in result ? result.systemProfileId : undefined,
    selectedProfileIds: "selectedProfileIds" in result ? result.selectedProfileIds : undefined,
    startedRuns: "startedRuns" in result ? result.startedRuns.length : undefined,
    skippedActiveRuns:
      "skippedActiveRuns" in result ? result.skippedActiveRuns.length : undefined,
    sourceBudgetPerCycle:
      "sourceBudgetPerCycle" in result ? result.sourceBudgetPerCycle : undefined,
    sourceBudgetPerProfile:
      "sourceBudgetPerProfile" in result ? result.sourceBudgetPerProfile : undefined,
    reason: "reason" in result ? result.reason : undefined,
    phase: "phase" in result ? result.phase : undefined,
    message: "message" in result ? result.message : undefined,
  };

  if (result.status === "started" || result.status === "skipped-active") {
    console.info("[background-ingestion:trigger]", payload);
    return;
  }

  console.warn("[background-ingestion:trigger]", payload);
}

async function executeRecurringInventoryIngestion(
  target: {
    search: SearchDocument;
    searchSession: SearchSessionDocument;
    crawlRunId: string;
    systemProfile: SystemSearchProfile;
  },
  runtime: {
    repository: JobCrawlerRepository;
    providers?: CrawlProvider[];
    fetchImpl?: typeof fetch;
    now: Date;
    signal?: AbortSignal;
    runTimeoutMs: number;
    maxSources: number;
    maxSourcesPerProvider: number;
    providerTimeoutMs: number;
    sourceTimeoutMs: number;
    providerConcurrency: number;
    schedulingIntervalMs: number;
    cycleDiagnostics?: NonNullable<CrawlDiagnostics["backgroundCycle"]>;
    refreshInventory?: (runtime: {
      repository: JobCrawlerRepository;
      now: Date;
    }) => Promise<SourceInventoryRecord[]>;
    expandInventory?: (runtime: {
      repository: JobCrawlerRepository;
      now: Date;
      fetchImpl?: typeof fetch;
      intervalMs: number;
      maxSources: number;
      expansionFilters: SearchDocument["filters"][];
      systemProfile: SystemSearchProfile;
    }) => Promise<InventoryExpansionResult>;
  },
) {
  const repository = runtime.repository;
  const providerSelection = selectProvidersForTieredCrawl({
    providers: runtime.providers ?? createDefaultProviders(),
    selectedPlatforms: target.systemProfile.filters.platforms,
    crawlMode: "deep",
    includeSlowProviders: true,
  });
  const providers = providerSelection.selectedProviders;
  const diagnostics = createEmptyDiagnostics();
  const sourceResults: CrawlSourceResult[] = [];
  const providerTimings: CrawlDiagnostics["performance"]["providerTimingsMs"] = [];
  const startMs = Date.now();
  const startedAtMs = runtime.now.getTime();
  const timeoutController = new AbortController();
  const timeoutHandle = setTimeout(() => {
    timeoutController.abort(
      Object.assign(
        new Error(`Background ingestion exceeded ${runtime.runTimeoutMs}ms.`),
        { name: "AbortError" },
      ),
    );
  }, runtime.runTimeoutMs);
  timeoutHandle.unref?.();

  const runController = await createBackgroundRunController({
    crawlRunId: target.crawlRunId,
    repository,
    signal: mergeSignals(runtime.signal, timeoutController.signal),
  });

  let totalFetchedJobs = 0;
  let totalMatchedJobs = 0;
  let totalSavedJobs = 0;
  const totalPersistenceStats = createEmptyPersistenceStats();
  const providerThroughputTotals = new Map<
    CrawlSourceResult["provider"],
    BackgroundProviderThroughputCounts
  >();
  const backgroundPersistenceFailures: string[] = [];
  const runTimestamp = () => new Date(startedAtMs + Math.max(0, Date.now() - startMs)).toISOString();
  const persistBackgroundProviderBatch = async (input: {
    provider: CrawlSourceResult["provider"];
    seeds: NormalizedJobSeed[];
    fetchedCount: number;
    sourceCount: number;
    status: "success" | "partial" | "failed" | "unsupported" | "running";
    warningCount?: number;
    errorMessage?: string;
    batchLabel: string;
  }) => {
    totalFetchedJobs += input.fetchedCount;
    diagnostics.performance.persistenceBatchCount += 1;
    console.info("[crawl:persistence-batch-start]", {
      systemProfileId: target.systemProfile.id,
      searchId: target.search._id,
      crawlRunId: target.crawlRunId,
      provider: input.provider,
      batch: input.batchLabel,
      sourceCount: input.sourceCount,
      fetchedCount: input.fetchedCount,
      seedCount: input.seeds.length,
    });
    console.info("[background-ingestion:persistence-batch-start]", {
      systemProfileId: target.systemProfile.id,
      searchId: target.search._id,
      crawlRunId: target.crawlRunId,
      provider: input.provider,
      sourceCount: input.sourceCount,
      fetchedCount: input.fetchedCount,
      seedCount: input.seeds.length,
    });

    let persistence: PersistJobsWithStatsResult;
    let validMatchedCount = 0;
    let hydrationDroppedCount = 0;
    try {
      const hydration = hydrateBackgroundJobs(input.seeds, runtime.now);
      hydrationDroppedCount = hydration.dropped.length;
      for (const drop of hydration.dropped) {
        diagnostics.dropReasonCounts[drop.reason] =
          (diagnostics.dropReasonCounts[drop.reason] ?? 0) + 1;
        console.warn("[background-ingestion:seed-normalization-drop]", {
          searchId: target.search._id,
          crawlRunId: target.crawlRunId,
          provider: input.provider,
          reason: drop.reason,
          errorMessage: drop.message,
          ...drop.context,
        });
      }
      const persistableJobs = hydration.jobs;
      validMatchedCount = persistableJobs.length;
      totalMatchedJobs += validMatchedCount;
      diagnostics.jobsBeforeDedupe += persistableJobs.length;
      persistence = await repository.persistJobsWithStats(target.crawlRunId, persistableJobs, {
        searchSessionId: target.searchSession._id,
      });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Background job persistence failed unexpectedly.";
      backgroundPersistenceFailures.push(`${input.provider}: ${message}`);
      diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
        totalPersistenceStats,
        Array.from(providerThroughputTotals.entries()),
        diagnostics.backgroundPersistence?.skippedReason,
        backgroundPersistenceFailures,
      );
      console.error("[background-ingestion:persistence-batch-failed]", {
        systemProfileId: target.systemProfile.id,
        searchId: target.search._id,
        crawlRunId: target.crawlRunId,
        provider: input.provider,
        sourceCount: input.sourceCount,
        fetchedCount: input.fetchedCount,
        seedCount: input.seeds.length,
        errorMessage: message,
      });
      throw error;
    }

    const effectiveWarningCount = (input.warningCount ?? 0) + hydrationDroppedCount;
    const effectiveProviderStatus =
      hydrationDroppedCount > 0 && input.status === "success" ? "partial" : input.status;
    const savedJobs = persistence.jobs;
    totalSavedJobs += persistence.linkedToRunCount;
    accumulatePersistenceStats(totalPersistenceStats, persistence);
    accumulateProviderThroughputStats(providerThroughputTotals, input.provider, {
      sourceCount: input.sourceCount,
      fetchedCount: input.fetchedCount,
      matchedCount: validMatchedCount,
      seedCount: input.seeds.length,
      warningCount: effectiveWarningCount,
      failedBatches: 0,
      ...persistence,
    });
    diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
      totalPersistenceStats,
      Array.from(providerThroughputTotals.entries()),
      diagnostics.backgroundPersistence?.skippedReason,
      backgroundPersistenceFailures,
    );
    diagnostics.jobsAfterDedupe = totalSavedJobs;
    diagnostics.dedupedOut = Math.max(0, diagnostics.jobsBeforeDedupe - diagnostics.jobsAfterDedupe);

    console.info("[ingestion:db-write-result]", {
      searchId: target.search._id,
      searchSessionId: target.searchSession._id,
      crawlRunId: target.crawlRunId,
      provider: input.provider,
      batch: input.batchLabel,
      insertedCount: persistence.insertedCount,
      updatedCount: persistence.updatedCount,
      linkedToRunCount: persistence.linkedToRunCount,
      indexedEventCount: persistence.indexedEventCount,
      savedCount: savedJobs.length,
    });
    console.info("[crawl:persistence-confirmed]", {
      searchId: target.search._id,
      searchSessionId: target.searchSession._id,
      crawlRunId: target.crawlRunId,
      provider: input.provider,
      insertedCount: persistence.insertedCount,
      updatedCount: persistence.updatedCount,
      linkedToRunCount: persistence.linkedToRunCount,
      indexedEventCount: persistence.indexedEventCount,
    });

    return {
      persistence,
      savedJobs,
      validMatchedCount,
      effectiveWarningCount,
      effectiveProviderStatus,
    };
  };
  console.info("[provider-tiering:selection]", {
    searchId: target.search._id,
    crawlRunId: target.crawlRunId,
    crawlMode: "deep",
    selectedFastProviders: providerSelection.selectedFastProviders,
    selectedSlowProviders: providerSelection.selectedSlowProviders,
    skippedSlowProviders: providerSelection.skippedSlowProviders,
    reason: providerSelection.reason,
  });
  console.info("[crawl:provider-tiering]", {
    searchId: target.search._id,
    crawlRunId: target.crawlRunId,
    crawlMode: "deep",
    selectedFastProviders: providerSelection.selectedFastProviders,
    selectedSlowProviders: providerSelection.selectedSlowProviders,
    skippedSlowProviders: providerSelection.skippedSlowProviders,
    reason: providerSelection.reason,
  });
  console.info("[crawl:timeout-policy]", {
    searchId: target.search._id,
    crawlRunId: target.crawlRunId,
    crawlMode: "deep",
    globalTimeoutMs: runtime.runTimeoutMs,
    providerTimeoutMs: runtime.providerTimeoutMs,
    sourceTimeoutMs: runtime.sourceTimeoutMs,
    maxSourcesPerProvider: runtime.maxSourcesPerProvider,
    providerConcurrency: runtime.providerConcurrency,
    isBackgroundRun: true,
    isRequestTimeRun: false,
  });

  try {
    diagnostics.backgroundCycle = runtime.cycleDiagnostics;
    diagnostics.systemProfile = {
      id: target.systemProfile.id,
      label: target.systemProfile.label,
      canonicalJobFamily: target.systemProfile.canonicalJobFamily,
      queryTitleVariant: target.systemProfile.queryTitleVariant,
      titleVariantTier: target.systemProfile.titleVariantTier,
      geography: {
        ...target.systemProfile.geography,
        variantTiers: [...target.systemProfile.geography.variantTiers],
      },
      platformPreference: target.systemProfile.platformPreference
        ? {
            mode: target.systemProfile.platformPreference.mode,
            platforms: [...target.systemProfile.platformPreference.platforms],
          }
        : undefined,
      priority: target.systemProfile.priority,
      enabled: target.systemProfile.enabled,
      cadenceMs: target.systemProfile.cadenceMs,
      cooldownMs: target.systemProfile.cooldownMs,
      lastRunAt: target.systemProfile.lastRunAt,
      nextEligibleAt: target.systemProfile.nextEligibleAt,
      successCount: target.systemProfile.successCount,
      failureCount: target.systemProfile.failureCount,
      consecutiveFailureCount: target.systemProfile.consecutiveFailureCount,
      filters: target.systemProfile.filters,
    };
    console.info("[background-ingestion:profile]", {
      searchId: target.search._id,
      crawlRunId: target.crawlRunId,
      systemProfileId: target.systemProfile.id,
      systemProfileLabel: target.systemProfile.label,
      filters: target.systemProfile.filters,
    });

    await repository.updateCrawlRunProgress(target.crawlRunId, {
      status: "running",
      stage: "discovering",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      diagnostics,
      validationMode: "deferred",
      providerSummary: [],
    });

    const discoveryStartedMs = Date.now();
    await runController.throwIfCanceled();
    const refreshedInventory = await (runtime.refreshInventory ?? refreshPersistentSourceInventory)({
      repository,
      now: runtime.now,
    });
    const inventoryExpansion = await resolveInventoryExpansion({
      repository,
      now: runtime.now,
      fetchImpl: runtime.fetchImpl,
      intervalMs: runtime.schedulingIntervalMs,
      maxSources: runtime.maxSources,
      refreshedInventory,
      refreshInventoryProvided: Boolean(runtime.refreshInventory),
      expandInventory: runtime.expandInventory,
      systemProfile: target.systemProfile,
    });
    const inventory = inventoryExpansion.inventory;
    diagnostics.discoveredSources = inventory.length;
    diagnostics.inventoryExpansion = inventoryExpansion.diagnostics;
    diagnostics.performance.stageTimingsMs.discovery = Date.now() - discoveryStartedMs;
    const selectionPlan = planPersistentInventoryRecurringCrawl({
      inventory,
      providers,
      now: runtime.now,
      maxSources: runtime.maxSources,
      intervalMs: runtime.schedulingIntervalMs,
      prioritySourceIds: inventoryExpansion.diagnostics.newSourceIds,
    });
    diagnostics.inventoryScheduling = selectionPlan.diagnostics;
    const selectionSkippedReason = resolveBackgroundSelectionSkippedReason(
      selectionPlan.diagnostics,
      inventoryExpansion.diagnostics.skippedReason,
    );
    if (selectionSkippedReason) {
      diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
        totalPersistenceStats,
        Array.from(providerThroughputTotals.entries()),
        selectionSkippedReason,
        backgroundPersistenceFailures,
      );
    }
    const selectedRecordById = new Map(
      selectionPlan.selectedRecords.map((record) => [record._id, record] as const),
    );
    const inventorySources = selectionPlan.selectedRecords.map(toDiscoveredSourceFromInventory);
    const selectedByProvider = providers.reduce<Record<string, number>>((counts, provider) => {
      counts[provider.provider] = inventorySources.filter((source) =>
        provider.supportsSource(source),
      ).length;
      return counts;
    }, {});
    diagnostics.inventoryScheduling = {
      ...selectionPlan.diagnostics,
      selectedByProvider,
    };

    console.info("[background-ingestion:inventory-expansion]", {
      systemProfileId: target.systemProfile.id,
      beforeCount: inventoryExpansion.diagnostics.beforeCount,
      afterRefreshCount: inventoryExpansion.diagnostics.afterRefreshCount,
      afterExpansionCount: inventoryExpansion.diagnostics.afterExpansionCount,
      selectedSearches: inventoryExpansion.diagnostics.selectedSearches,
      selectedSearchTitles: inventoryExpansion.diagnostics.selectedSearchTitles,
      selectedSearchFilters: inventoryExpansion.diagnostics.selectedSearchFilters,
      candidateSources: inventoryExpansion.diagnostics.candidateSources,
      newSourcesAdded: inventoryExpansion.diagnostics.newSourcesAdded,
      newSourceIds: inventoryExpansion.diagnostics.newSourceIds,
      skippedReason: inventoryExpansion.diagnostics.skippedReason,
      searchDiagnostics: inventoryExpansion.diagnostics.searchDiagnostics.map((diagnostic) => ({
        title: diagnostic.title,
        country: diagnostic.country,
        state: diagnostic.state,
        city: diagnostic.city,
        discoveredSources: diagnostic.discoveredSources,
        publicSources: diagnostic.publicSources,
        publicJobs: diagnostic.publicJobs,
        publicSearchStopReason: diagnostic.publicSearch?.stopReason,
      })),
    });

    console.info("[background-ingestion:source-selection]", {
      systemProfileId: target.systemProfile.id,
      inventorySources: selectionPlan.diagnostics.inventorySources,
      crawlableSources: selectionPlan.diagnostics.crawlableSources,
      eligibleSources: selectionPlan.diagnostics.eligibleSources,
      selectedSources: selectionPlan.diagnostics.selectedSources,
      inventoryByPlatform: selectionPlan.diagnostics.inventoryByPlatform,
      eligibleByPlatform: selectionPlan.diagnostics.eligibleByPlatform,
      skippedByReason: selectionPlan.diagnostics.skippedByReason,
      skippedByPlatformReason: selectionPlan.diagnostics.skippedByPlatformReason,
      freshnessBuckets: selectionPlan.diagnostics.freshnessBuckets,
      selectedByPlatform: selectionPlan.diagnostics.selectedByPlatform,
      platformSelectionBudgets: selectionPlan.diagnostics.platformSelectionBudgets,
      selectedByProvider,
      selectedByHealth: selectionPlan.diagnostics.selectedByHealth,
      selectedSourceIds: selectionPlan.diagnostics.selectedSourceIds,
      skippedSourceSamples: selectionPlan.diagnostics.skippedSourceSamples,
      skippedReason: selectionSkippedReason,
    });

    await repository.updateCrawlRunProgress(target.crawlRunId, {
      status: "running",
      stage: "crawling",
      totalFetchedJobs,
      totalMatchedJobs,
      dedupedJobs: totalSavedJobs,
      diagnostics,
      validationMode: "deferred",
      providerSummary: [],
    });
    await repository.updateSearchLatestRun(
      target.search._id,
      target.crawlRunId,
      "running",
      runTimestamp(),
    );

    await runWithConcurrency(
      providers,
      async (provider) => {
      await runController.throwIfCanceled();

      const providerSources = capBackgroundProviderSources(
        provider.provider,
        inventorySources.filter((source) => provider.supportsSource(source)),
        {
          searchId: target.search._id,
          crawlRunId: target.crawlRunId,
          maxSourcesPerProvider: runtime.maxSourcesPerProvider,
          diagnostics,
        },
      );
      if (providerSources.length === 0) {
        return;
      }

      diagnostics.providersEnqueued += 1;
      diagnostics.crawledSources += providerSources.length;
      const providerStartedAt = runTimestamp();
      const providerStartedMs = Date.now();
      let emittedLiveBatch = false;

      try {
        const providerContext = (providerSignal: AbortSignal): Parameters<CrawlProvider["crawlSources"]>[0] => ({
            fetchImpl: runtime.fetchImpl ?? fetch,
            now: runtime.now,
            filters: target.systemProfile.filters,
            signal: providerSignal,
            sourceTimeoutMs: runtime.sourceTimeoutMs,
            providerTimeoutMs: runtime.providerTimeoutMs,
            isBackgroundRun: true,
            throwIfCanceled: () => runController.throwIfCanceled(),
            onBatch: async (batch) => {
              emittedLiveBatch = true;
              await persistBackgroundProviderBatch({
                provider: batch.provider,
                seeds: batch.jobs,
                fetchedCount: batch.fetchedCount,
                sourceCount: batch.sourceCount ?? 1,
                status: "running",
                batchLabel: `${batch.provider}:batch`,
              });
            },
          });

        const result = provider.sourceTimeoutIsolation
          ? await provider.crawlSources(providerContext(runController.signal), providerSources)
          : await crawlProviderSourcesWithTimeout(
              provider,
              providerContext(runController.signal),
              providerSources,
              runtime.providerTimeoutMs,
            );
        const sourceStatus = mapProviderResultToSourceStatus(result);

        mergeBackgroundProviderDropReasonCounts(
          diagnostics,
          result.diagnostics?.dropReasonCounts,
        );

        const batchPersistence = emittedLiveBatch
          ? undefined
          : await persistBackgroundProviderBatch({
              provider: provider.provider,
              seeds: result.jobs,
              fetchedCount: result.fetchedCount,
              sourceCount: providerSources.length,
              status: result.status,
              warningCount: result.warningCount,
              errorMessage: result.errorMessage,
              batchLabel: result.provider,
            });
        const providerTotals = providerThroughputTotals.get(provider.provider);
        const validMatchedCount =
          batchPersistence?.validMatchedCount ?? providerTotals?.matchedCount ?? 0;
        const savedJobs = batchPersistence?.savedJobs ?? [];
        const effectiveWarningCount =
          (providerTotals?.warningCount ?? 0) + (emittedLiveBatch ? result.warningCount ?? 0 : 0);
        const effectiveProviderStatus =
          emittedLiveBatch
            ? sourceStatus
            : mapProviderStatus(batchPersistence?.effectiveProviderStatus ?? result.status, result);

        const finishedAt = runTimestamp();
        const sourceResult = createSourceResult({
          crawlRunId: target.crawlRunId,
          searchId: target.search._id,
          provider: provider.provider,
          status: effectiveProviderStatus,
          sourceCount: providerSources.length,
          fetchedCount: providerTotals?.fetchedCount ?? result.fetchedCount,
          matchedCount: validMatchedCount,
          savedCount: providerTotals?.linkedToRunCount ?? savedJobs.length,
          warningCount: effectiveWarningCount,
          errorMessage: result.errorMessage,
          startedAt: providerStartedAt,
          finishedAt,
        });
        sourceResults.push(sourceResult);
        await repository.updateCrawlSourceResult(sourceResult);

        console.info("[background-ingestion:provider-summary]", {
          systemProfileId: target.systemProfile.id,
          searchId: target.search._id,
          crawlRunId: target.crawlRunId,
          provider: provider.provider,
          sourceCount: providerSources.length,
          fetchedCount: providerTotals?.fetchedCount ?? result.fetchedCount,
          matchedCount: validMatchedCount,
          seedCount: result.jobs.length,
          savedCount: providerTotals?.linkedToRunCount ?? savedJobs.length,
          insertedCount: providerTotals?.insertedCount ?? batchPersistence?.persistence.insertedCount ?? 0,
          updatedCount: providerTotals?.updatedCount ?? batchPersistence?.persistence.updatedCount ?? 0,
          linkedToRunCount: providerTotals?.linkedToRunCount ?? batchPersistence?.persistence.linkedToRunCount ?? 0,
          indexedEventCount: providerTotals?.indexedEventCount ?? batchPersistence?.persistence.indexedEventCount ?? 0,
          status: effectiveProviderStatus,
          warningCount: effectiveWarningCount,
          errorMessage: result.errorMessage,
        });
        await repository.recordSourceInventoryObservations(
          providerSources.map((source) => ({
            sourceId: source.id,
            observedAt: finishedAt,
            succeeded: effectiveProviderStatus === "success" || effectiveProviderStatus === "partial",
            health:
              effectiveProviderStatus === "failed" || effectiveProviderStatus === "timed_out"
                ? "failing"
                : effectiveProviderStatus === "partial"
                  ? "degraded"
                  : "healthy",
            lastFailureReason:
              effectiveProviderStatus === "failed" || effectiveProviderStatus === "timed_out"
                ? result.errorMessage
                : undefined,
            nextEligibleAt: resolveObservedSourceNextEligibleAt({
              record: selectedRecordById.get(source.id) ?? inventory.find((record) => record._id === source.id)!,
              observedAt: finishedAt,
              intervalMs: runtime.schedulingIntervalMs,
              health:
                effectiveProviderStatus === "failed" || effectiveProviderStatus === "timed_out"
                  ? "failing"
                  : effectiveProviderStatus === "partial"
                    ? "degraded"
                    : "healthy",
              consecutiveFailures:
                effectiveProviderStatus === "failed" || effectiveProviderStatus === "timed_out"
                  ? (selectedRecordById.get(source.id)?.consecutiveFailures ?? 0) + 1
                  : 0,
              succeeded: effectiveProviderStatus === "success" || effectiveProviderStatus === "partial",
            }),
          })),
        );

        providerTimings.push({
          provider: provider.provider,
          duration: Date.now() - providerStartedMs,
          sourceCount: providerSources.length,
          timedOut: effectiveProviderStatus === "timed_out",
        });
      } catch (error) {
        const providerTimedOut = isBackgroundProviderTimeout(error);
        const abortLike =
          error instanceof Error && error.name === "AbortError" && !providerTimedOut;

        const finishedAt = runTimestamp();
        const providerTotals = providerThroughputTotals.get(provider.provider);
        const errorMessage =
          error instanceof Error ? error.message : "Provider crawl failed unexpectedly.";
        const hasPreservedProviderWork =
          (providerTotals?.fetchedCount ?? 0) > 0 ||
          (providerTotals?.matchedCount ?? 0) > 0 ||
          (providerTotals?.linkedToRunCount ?? 0) > 0;
        const recoveredStatus: CrawlSourceResult["status"] =
          providerTimedOut && hasPreservedProviderWork
            ? "partial"
            : providerTimedOut
              ? "timed_out"
              : abortLike
                ? "aborted"
                : "failed";
        if (!abortLike && recoveredStatus !== "partial") {
          diagnostics.providerFailures += 1;
        }
        if (providerTimedOut) {
          console.warn("[crawl:provider-timeout]", {
            searchId: target.search._id,
            crawlRunId: target.crawlRunId,
            provider: provider.provider,
            sourceCount: providerSources.length,
            timeoutMs: runtime.providerTimeoutMs,
            fetchedCountBeforeTimeout: providerTotals?.fetchedCount ?? 0,
            jobsPersistedBeforeTimeout: providerTotals?.linkedToRunCount ?? 0,
          });
        }
        const sourceResult = createSourceResult({
          crawlRunId: target.crawlRunId,
          searchId: target.search._id,
          provider: provider.provider,
          status: recoveredStatus,
          sourceCount: providerSources.length,
          fetchedCount: providerTotals?.fetchedCount ?? 0,
          matchedCount: providerTotals?.matchedCount ?? 0,
          savedCount: providerTotals?.linkedToRunCount ?? 0,
          warningCount: 1,
          errorMessage:
            recoveredStatus === "partial"
              ? `Partial timeout: ${errorMessage}`
              : errorMessage,
          startedAt: providerStartedAt,
          finishedAt,
        });
        sourceResults.push(sourceResult);
        await repository.updateCrawlSourceResult(sourceResult);
        await repository.recordSourceInventoryObservations(
          providerSources.map((source) => ({
            sourceId: source.id,
            observedAt: finishedAt,
            succeeded: recoveredStatus === "partial" ? undefined : false,
            health: recoveredStatus === "partial" || providerTimedOut ? "degraded" : "failing",
            lastFailureReason: sourceResult.errorMessage,
            nextEligibleAt: resolveObservedSourceNextEligibleAt({
              record: selectedRecordById.get(source.id) ?? inventory.find((record) => record._id === source.id)!,
              observedAt: finishedAt,
              intervalMs: runtime.schedulingIntervalMs,
              health: recoveredStatus === "partial" || providerTimedOut ? "degraded" : "failing",
              consecutiveFailures:
                recoveredStatus === "partial"
                  ? selectedRecordById.get(source.id)?.consecutiveFailures ?? 0
                  : (selectedRecordById.get(source.id)?.consecutiveFailures ?? 0) + 1,
              succeeded: recoveredStatus === "partial" ? undefined : false,
            }),
          })),
        );

        providerTimings.push({
          provider: provider.provider,
          duration: Date.now() - providerStartedMs,
          sourceCount: providerSources.length,
          timedOut: providerTimedOut && recoveredStatus !== "partial",
        });
        if (!hasPreservedProviderWork) {
          accumulateProviderThroughputStats(providerThroughputTotals, provider.provider, {
            ...createEmptyPersistenceStats(),
            sourceCount: providerSources.length,
            fetchedCount: 0,
            matchedCount: 0,
            seedCount: 0,
            warningCount: 0,
            failedBatches: 1,
          });
        }

        if (abortLike) {
          throw error;
        }
      }
      },
      runtime.providerConcurrency,
    );

    diagnostics.performance.stageTimingsMs.providerExecution = Math.max(
      0,
      Date.now() - startMs - diagnostics.performance.stageTimingsMs.discovery,
    );
    diagnostics.performance.stageTimingsMs.total = Date.now() - startMs;
    diagnostics.performance.progressUpdateCount = 2;
    diagnostics.performance.providerTimingsMs = providerTimings;
    diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
      totalPersistenceStats,
      Array.from(providerThroughputTotals.entries()),
      diagnostics.backgroundPersistence?.skippedReason,
      backgroundPersistenceFailures,
    );
    const finishedAt = runTimestamp();
    const status = resolveFinalStatus(sourceResults, totalSavedJobs);

    console.info("[background-ingestion:persistence-summary]", {
      systemProfileId: target.systemProfile.id,
      searchId: target.search._id,
      crawlRunId: target.crawlRunId,
      status,
      jobsInserted: totalPersistenceStats.insertedCount,
      jobsUpdated: totalPersistenceStats.updatedCount,
      jobsLinkedToRun: totalPersistenceStats.linkedToRunCount,
      indexedEventsEmitted: totalPersistenceStats.indexedEventCount,
      failedBatches: diagnostics.backgroundPersistence.failedBatches,
      failureSamples: diagnostics.backgroundPersistence.failureSamples,
      providerStats: diagnostics.backgroundPersistence.providerStats,
      skippedReason: diagnostics.backgroundPersistence.skippedReason,
    });

    await finalizeBackgroundRun({
      repository,
      search: target.search,
      searchSessionId: target.searchSession._id,
      crawlRunId: target.crawlRunId,
      status,
      finishedAt,
      totalFetchedJobs,
      totalMatchedJobs,
      totalSavedJobs,
      diagnostics,
      sourceResults,
    });
  } catch (error) {
    diagnostics.performance.stageTimingsMs.total = Date.now() - startMs;
    diagnostics.performance.providerTimingsMs = providerTimings;
    diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
      totalPersistenceStats,
      Array.from(providerThroughputTotals.entries()),
      diagnostics.backgroundPersistence?.skippedReason,
      backgroundPersistenceFailures,
    );
    const finishedAt = runTimestamp();
    const aborted = error instanceof Error && error.name === "AbortError";

    await finalizeBackgroundRun({
      repository,
      search: target.search,
      searchSessionId: target.searchSession._id,
      crawlRunId: target.crawlRunId,
      status: aborted ? "aborted" : "failed",
      finishedAt,
      totalFetchedJobs,
      totalMatchedJobs,
      totalSavedJobs,
      diagnostics,
      sourceResults,
      errorMessage:
        error instanceof Error ? error.message : "Recurring background ingestion failed unexpectedly.",
    });

    if (!aborted) {
      throw error;
    }
  } finally {
    clearTimeout(timeoutHandle);
    runController.cleanup();
  }
}

async function finalizeBackgroundRun(input: {
  repository: JobCrawlerRepository;
  search: SearchDocument;
  searchSessionId: string;
  crawlRunId: string;
  status: CrawlRunStatus;
  finishedAt: string;
  totalFetchedJobs: number;
  totalMatchedJobs: number;
  totalSavedJobs: number;
  diagnostics: CrawlDiagnostics;
  sourceResults: CrawlSourceResult[];
  errorMessage?: string;
}) {
  const providerSummary = input.sourceResults.map((sourceResult) => ({
    provider: sourceResult.provider,
    status: sourceResult.status,
    sourceCount: sourceResult.sourceCount,
    fetchedCount: sourceResult.fetchedCount,
    matchedCount: sourceResult.matchedCount,
    savedCount: sourceResult.savedCount,
    warningCount: sourceResult.warningCount,
    errorMessage: sourceResult.errorMessage,
  }));

  await Promise.all([
    input.repository.finalizeCrawlRun(input.crawlRunId, {
      status: input.status,
      stage: "finalizing",
      totalFetchedJobs: input.totalFetchedJobs,
      totalMatchedJobs: input.totalMatchedJobs,
      dedupedJobs: input.totalSavedJobs,
      diagnostics: input.diagnostics,
      validationMode: "deferred",
      providerSummary,
      errorMessage: input.errorMessage,
      finishedAt: input.finishedAt,
    }),
    input.repository.updateSearchLatestRun(
      input.search._id,
      input.crawlRunId,
      input.status,
      input.finishedAt,
    ),
    input.repository.updateSearchLatestSession(
      input.search._id,
      input.searchSessionId,
      input.status,
      input.finishedAt,
    ),
    input.repository.updateSearchSession(input.searchSessionId, {
      latestCrawlRunId: input.crawlRunId,
      status: input.status,
      finishedAt: input.finishedAt,
      updatedAt: input.finishedAt,
    }),
  ]);
}

async function ensureBackgroundSearch(
  repository: JobCrawlerRepository,
  now: Date,
  systemProfile: SystemSearchProfile,
) {
  const existing = await repository.findMostRecentSearchByFilters(systemProfile.filters, {
    systemProfileId: systemProfile.id,
  });
  if (existing) {
    return existing;
  }

  return repository.createSearch(systemProfile.filters, now.toISOString(), {
    systemProfileId: systemProfile.id,
    systemProfileLabel: systemProfile.label,
  });
}

async function resolveInventoryExpansion(input: {
  repository: JobCrawlerRepository;
  now: Date;
  fetchImpl?: typeof fetch;
  intervalMs: number;
  maxSources: number;
  refreshedInventory: SourceInventoryRecord[];
  refreshInventoryProvided: boolean;
  expandInventory?: (runtime: {
    repository: JobCrawlerRepository;
    now: Date;
    fetchImpl?: typeof fetch;
    intervalMs: number;
    maxSources: number;
    expansionFilters: SearchDocument["filters"][];
    systemProfile: SystemSearchProfile;
  }) => Promise<InventoryExpansionResult>;
  systemProfile: SystemSearchProfile;
}): Promise<InventoryExpansionResult> {
  if (input.expandInventory) {
    return input.expandInventory({
      repository: input.repository,
      now: input.now,
      fetchImpl: input.fetchImpl,
      intervalMs: input.intervalMs,
      maxSources: input.maxSources,
      expansionFilters: [input.systemProfile.filters],
      systemProfile: input.systemProfile,
    });
  }

  if (input.refreshInventoryProvided) {
    return {
      inventory: input.refreshedInventory,
      diagnostics: {
        beforeCount: input.refreshedInventory.length,
        afterRefreshCount: input.refreshedInventory.length,
        afterExpansionCount: input.refreshedInventory.length,
        selectedSearches: 0,
        candidateSources: 0,
        newSourcesAdded: 0,
        selectedSearchTitles: [],
        selectedSearchFilters: [],
        selectedSourceIds: [],
        newSourceIds: [],
        platformCountsBefore: summarizeInventoryPlatforms(input.refreshedInventory),
        platformCountsAfter: summarizeInventoryPlatforms(input.refreshedInventory),
        skippedReason: "legacy_refresh_inventory_override",
        searchDiagnostics: [],
      },
    };
  }

  return runPersistentInventoryExpansion({
    repository: input.repository,
    now: input.now,
    fetchImpl: input.fetchImpl,
    intervalMs: input.intervalMs,
    maxSources: input.maxSources,
    refreshedInventory: input.refreshedInventory,
    expansionFilters: [input.systemProfile.filters],
  });
}

function summarizeInventoryPlatforms(records: SourceInventoryRecord[]) {
  return records.reduce<Record<string, number>>((counts, record) => {
    counts[record.platform] = (counts[record.platform] ?? 0) + 1;
    return counts;
  }, {});
}

function createEmptyPersistenceStats(): BackgroundPersistenceCounts {
  return {
    insertedCount: 0,
    updatedCount: 0,
    linkedToRunCount: 0,
    indexedEventCount: 0,
  };
}

function accumulatePersistenceStats(
  total: BackgroundPersistenceCounts,
  next: BackgroundPersistenceCounts,
) {
  total.insertedCount += next.insertedCount;
  total.updatedCount += next.updatedCount;
  total.linkedToRunCount += next.linkedToRunCount;
  total.indexedEventCount += next.indexedEventCount;
}

function createEmptyProviderThroughputStats(): BackgroundProviderThroughputCounts {
  return {
    ...createEmptyPersistenceStats(),
    sourceCount: 0,
    fetchedCount: 0,
    matchedCount: 0,
    seedCount: 0,
    warningCount: 0,
    failedBatches: 0,
  };
}

function accumulateProviderThroughputStats(
  totals: Map<CrawlSourceResult["provider"], BackgroundProviderThroughputCounts>,
  provider: CrawlSourceResult["provider"],
  next: BackgroundProviderThroughputCounts,
) {
  const current = totals.get(provider) ?? createEmptyProviderThroughputStats();
  current.sourceCount += next.sourceCount;
  current.fetchedCount += next.fetchedCount;
  current.matchedCount += next.matchedCount;
  current.seedCount += next.seedCount;
  current.warningCount += next.warningCount;
  current.failedBatches += next.failedBatches;
  accumulatePersistenceStats(current, next);
  totals.set(provider, current);
}

function mergeBackgroundProviderDropReasonCounts(
  diagnostics: CrawlDiagnostics,
  dropReasonCounts?: Record<string, number>,
) {
  if (!dropReasonCounts) {
    return;
  }

  Object.entries(dropReasonCounts).forEach(([reason, count]) => {
    diagnostics.dropReasonCounts[reason] =
      (diagnostics.dropReasonCounts[reason] ?? 0) + count;
  });
}

function hydrateBackgroundJobs(seeds: NormalizedJobSeed[], now: Date) {
  const jobs: PersistableJob[] = [];
  const dropped: ProviderSeedValidationDrop[] = [];

  for (const seed of seeds) {
    const validation = validateNormalizedJobSeedForHydration(seed);
    if (!validation.ok) {
      dropped.push(validation.drop);
      continue;
    }

    try {
      jobs.push(persistableJobSchema.parse(seedToPersistableJob(validation.seed, now)));
    } catch (error) {
      dropped.push({
        seed: validation.seed,
        reason: classifyBackgroundSeedHydrationError(error),
        message: error instanceof Error ? error.message : "Seed could not be hydrated.",
        context: buildProviderSeedDiagnosticContext(validation.seed),
      });
    }
  }

  return {
    jobs,
    dropped,
  };
}

function classifyBackgroundSeedHydrationError(error: unknown) {
  const message = error instanceof Error ? error.message : "";

  if (
    message.includes("searchIndex.titleNormalized") ||
    message.includes("searchIndex.titleStrippedNormalized")
  ) {
    return "hydrate_invalid_search_index";
  }

  return "persistable_schema_validation_failed";
}

function buildBackgroundPersistenceDiagnostics(
  total: BackgroundPersistenceCounts,
  providerStats: Array<[CrawlSourceResult["provider"], BackgroundProviderThroughputCounts]>,
  skippedReason?: string,
  failureSamples: string[] = [],
): NonNullable<CrawlDiagnostics["backgroundPersistence"]> {
  return {
    jobsInserted: total.insertedCount,
    jobsUpdated: total.updatedCount,
    jobsLinkedToRun: total.linkedToRunCount,
    indexedEventsEmitted: total.indexedEventCount,
    failedBatches: failureSamples.length,
    failureSamples: failureSamples.slice(0, 8),
    skippedReason,
    providerStats: providerStats.map(([provider, stats]) => ({
      provider,
      sourceCount: stats.sourceCount,
      fetchedCount: stats.fetchedCount,
      matchedCount: stats.matchedCount,
      seedCount: stats.seedCount,
      savedCount: stats.linkedToRunCount,
      insertedCount: stats.insertedCount,
      updatedCount: stats.updatedCount,
      linkedToRunCount: stats.linkedToRunCount,
      indexedEventCount: stats.indexedEventCount,
      warningCount: stats.warningCount,
      failedBatches: stats.failedBatches,
    })),
  };
}

function resolveBackgroundSelectionSkippedReason(
  diagnostics: CrawlDiagnostics["inventoryScheduling"],
  expansionSkippedReason?: string,
) {
  if (!diagnostics) {
    return expansionSkippedReason;
  }

  if (diagnostics.selectedSources > 0) {
    return undefined;
  }

  if (diagnostics.inventorySources === 0) {
    return expansionSkippedReason ?? "no_inventory_sources";
  }

  if (diagnostics.crawlableSources === 0) {
    return "unsupported_provider";
  }

  if (diagnostics.eligibleSources === 0) {
    const skipReasons = diagnostics.skippedByReason;
    if ((skipReasons.health_backoff ?? 0) > 0) {
      return "health_backoff";
    }
    if ((skipReasons.freshness_cooldown ?? 0) > 0) {
      return "freshness_cooldown";
    }
    return "no_eligible_sources";
  }

  if ((diagnostics.skippedByReason.capacity_deprioritized ?? 0) > 0) {
    return "capacity_deprioritized";
  }

  return expansionSkippedReason ?? "no_eligible_sources";
}

async function resolveDurableBackgroundRepository(runtime: BackgroundIngestionRuntime): Promise<
  | { repository: JobCrawlerRepository; reason?: never; phase?: never; message?: never }
  | {
      repository: null;
      reason: "mongo_unavailable" | "mongo_transient" | "bootstrap_running" | "bootstrap_failed";
      phase: "repository_resolution" | "index_initialization";
      message: string;
    }
> {
  if (runtime.repository) {
    return { repository: runtime.repository };
  }

  try {
    if (runtime.resolveRepository) {
      return { repository: await runtime.resolveRepository() };
    }

    const [{ JobCrawlerRepository }, mongodb] = await Promise.all([
      import("@/lib/server/db/repository"),
      import("@/lib/server/mongodb"),
    ]);

    const bootstrapState = mongodb.getMongoBootstrapState();
    if (bootstrapState.status === "running") {
      return {
        repository: null,
        reason: "bootstrap_running",
        phase: "index_initialization",
        message: "MongoDB bootstrap/index initialization is already running.",
      };
    }

    return {
      repository: new JobCrawlerRepository(
        (await mongodb.getMongoDb({ ensureIndexes: true, requireIndexes: true })) as never,
      ),
    };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "MongoDB is unavailable for recurring background ingestion.";
    const failure = classifyBackgroundRepositoryFailure(error);
    const logLabel =
      failure.reason === "bootstrap_failed"
        ? "[background-ingestion:bootstrap-failed]"
        : failure.reason === "mongo_transient"
          ? "[background-ingestion:mongo-transient]"
          : "[background-ingestion:mongo-unavailable]";
    console.warn(logLabel, {
      status:
        failure.reason === "mongo_transient"
          ? "skipped-mongo-transient"
          : failure.reason === "bootstrap_failed"
            ? "skipped-bootstrap-failed"
            : "skipped-no-mongo",
      message,
      reason: failure.reason,
      phase: failure.phase,
      bootstrapFailure: failure.reason === "bootstrap_failed",
    });
    return failure;
  }
}

export function classifyBackgroundRepositoryFailure(error: unknown): {
  repository: null;
  reason: "mongo_unavailable" | "mongo_transient" | "bootstrap_running" | "bootstrap_failed";
  phase: "repository_resolution" | "index_initialization";
  message: string;
} {
  const message =
    error instanceof Error
      ? error.message
      : "MongoDB is unavailable for recurring background ingestion.";
  const bootstrapError = parseMongoBootstrapErrorLike(error);
  const reason =
    bootstrapError?.reason ??
    (/bootstrap already running|bootstrap\/index initialization is already running/i.test(message)
      ? "bootstrap_running"
      : /connection pool|server selection|timed out|pool cleared|cooldown after a recent failure/i.test(message)
        ? "mongo_transient"
        : /bootstrap failed|migration|index initialization|index init|jobs_canonical_job_key/i.test(
              message,
            )
          ? "bootstrap_failed"
          : "mongo_unavailable");
  const phase =
    reason === "bootstrap_failed" || reason === "bootstrap_running" || reason === "mongo_transient"
      ? "index_initialization"
      : "repository_resolution";

  return {
    repository: null,
    reason,
    phase,
    message,
  };
}

function parseMongoBootstrapErrorLike(error: unknown):
  | { reason: "mongo_transient" | "bootstrap_running" | "bootstrap_failed" }
  | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }

  const record = error as Record<string, unknown>;
  if (record.name !== "MongoBootstrapError") {
    return undefined;
  }

  if (
    record.reason === "mongo_transient" ||
    record.reason === "bootstrap_running" ||
    record.reason === "bootstrap_failed"
  ) {
    return { reason: record.reason };
  }

  return undefined;
}

async function recoverStaleBackgroundRunIfNeeded(
  searchId: string,
  repository: JobCrawlerRepository,
  now: Date,
  staleAfterMs: number,
) {
  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForSearch(searchId);
  if (!activeQueueEntry) {
    return;
  }

  const heartbeatAt =
    activeQueueEntry.lastHeartbeatAt ??
    activeQueueEntry.updatedAt ??
    activeQueueEntry.startedAt ??
    activeQueueEntry.queuedAt;
  if (now.getTime() - new Date(heartbeatAt).getTime() <= staleAfterMs) {
    return;
  }

  const crawlRun = await repository.getCrawlRun(activeQueueEntry.crawlRunId);
  if (!crawlRun) {
    await repository.finalizeCrawlQueueEntry(activeQueueEntry.crawlRunId, {
      status: "aborted",
      finishedAt: now.toISOString(),
    });
    return;
  }

  const search = await repository.getSearch(searchId);
  if (!search) {
    return;
  }

  const searchSessionId = crawlRun.searchSessionId ?? activeQueueEntry.searchSessionId;
  if (!searchSessionId) {
    await repository.finalizeCrawlQueueEntry(activeQueueEntry.crawlRunId, {
      status: "aborted",
      finishedAt: now.toISOString(),
    });
    return;
  }

  await finalizeBackgroundRun({
    repository,
    search,
    searchSessionId,
    crawlRunId: crawlRun._id,
    status: "aborted",
    finishedAt: now.toISOString(),
    totalFetchedJobs: crawlRun.totalFetchedJobs,
    totalMatchedJobs: crawlRun.totalMatchedJobs,
    totalSavedJobs: crawlRun.dedupedJobs,
    diagnostics: crawlRun.diagnostics,
    sourceResults: await repository.getCrawlSourceResults(crawlRun._id),
    errorMessage: "Recovered stale recurring background ingestion run after missed heartbeats.",
  });
  await repository.finalizeCrawlQueueEntry(activeQueueEntry.crawlRunId, {
    status: "aborted",
    finishedAt: now.toISOString(),
  });
}

async function createBackgroundRunController(input: {
  crawlRunId: string;
  repository: JobCrawlerRepository;
  signal?: AbortSignal;
}) {
  const controller = new AbortController();
  let lastHeartbeatAtMs = 0;

  const abort = (reason: unknown) => {
    if (controller.signal.aborted) {
      return;
    }

    controller.abort(
      reason instanceof Error
        ? reason
        : Object.assign(new Error(typeof reason === "string" ? reason : "The crawl was aborted."), {
            name: "AbortError",
          }),
    );
  };

  if (input.signal?.aborted) {
    abort(input.signal.reason);
  } else {
    input.signal?.addEventListener("abort", () => abort(input.signal?.reason), { once: true });
  }

  const interval = setInterval(() => {
    void input.repository
      .getCrawlRunControlState(input.crawlRunId)
      .then((state) => {
        if (state?.cancelRequestedAt) {
          abort(state.cancelReason ?? "The crawl was canceled.");
          return;
        }

        if (state?.status === "running" && Date.now() - lastHeartbeatAtMs >= 1000) {
          lastHeartbeatAtMs = Date.now();
          void input.repository.heartbeatCrawlRun(input.crawlRunId, new Date().toISOString());
        }
      })
      .catch(() => undefined);
  }, 250);
  interval.unref?.();

  return {
    signal: controller.signal,
    async throwIfCanceled() {
      if (controller.signal.aborted) {
        const error =
          controller.signal.reason instanceof Error
            ? controller.signal.reason
            : Object.assign(new Error("The crawl was aborted."), { name: "AbortError" });
        error.name = "AbortError";
        throw error;
      }

      const state = await input.repository.getCrawlRunControlState(input.crawlRunId);
      if (!state) {
        throw new CrawlAbortedError("The crawl run could not be found.");
      }

      if (state.cancelRequestedAt) {
        throw new CrawlAbortedError(state.cancelReason ?? "The crawl was canceled.");
      }

      if (Date.now() - lastHeartbeatAtMs >= 1000) {
        await input.repository.heartbeatCrawlRun(input.crawlRunId, new Date().toISOString());
        lastHeartbeatAtMs = Date.now();
      }
    },
    cleanup() {
      clearInterval(interval);
    },
  };
}

async function crawlProviderSourcesWithTimeout(
  provider: CrawlProvider,
  context: Parameters<CrawlProvider["crawlSources"]>[0],
  sources: Parameters<CrawlProvider["crawlSources"]>[1],
  timeoutMs: number,
): Promise<Awaited<ReturnType<CrawlProvider["crawlSources"]>>> {
  const timeoutController = new AbortController();
  const timeoutError = Object.assign(
    new Error(
      `Background provider ${provider.provider} exceeded the ${timeoutMs}ms crawl budget and was failed fast.`,
    ),
    { name: "ProviderTimeoutError" },
  );
  const timeoutHandle = setTimeout(() => {
    timeoutController.abort(timeoutError);
  }, timeoutMs);
  timeoutHandle.unref?.();

  try {
    const timedContext = {
      ...context,
      signal: mergeSignals(context.signal, timeoutController.signal),
    };
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutController.signal.addEventListener(
        "abort",
        () => reject(timeoutController.signal.reason ?? timeoutError),
        { once: true },
      );
    });

    return await Promise.race([
      provider.crawlSources(timedContext, sources),
      timeoutPromise,
    ]);
  } catch (error) {
    if (timeoutController.signal.aborted && !context.signal?.aborted) {
      throw timeoutController.signal.reason ?? timeoutError;
    }

    throw error;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

function isBackgroundProviderTimeout(error: unknown) {
  return (
    error instanceof Error &&
    (error.name === "ProviderTimeoutError" || error.message.includes("crawl budget"))
  );
}

function createSourceResult(input: Omit<CrawlSourceResult, "_id">): CrawlSourceResult {
  return {
    _id: `${input.crawlRunId}:${input.provider}`,
    ...input,
  };
}

function resolveBackgroundRunOwnerKey(systemProfileId: string, selectedProfiles: number) {
  return selectedProfiles > 1
    ? `${backgroundIngestionOwnerKey}:${systemProfileId}`
    : backgroundIngestionOwnerKey;
}

function capBackgroundProviderSources(
  provider: CrawlSourceResult["provider"],
  sources: DiscoveredSource[],
  input: {
    searchId: string;
    crawlRunId: string;
    maxSourcesPerProvider: number;
    diagnostics: CrawlDiagnostics;
  },
) {
  const maxSourcesPerProvider = Math.max(1, Math.floor(input.maxSourcesPerProvider));
  if (sources.length <= maxSourcesPerProvider) {
    return sources;
  }

  const selected = sources.slice(0, maxSourcesPerProvider);
  const truncatedCount = sources.length - selected.length;
  input.diagnostics.budgetExhausted = true;
  input.diagnostics.dropReasonCounts.provider_source_budget =
    (input.diagnostics.dropReasonCounts.provider_source_budget ?? 0) + truncatedCount;

  console.info("[crawl:provider-source-budget]", {
    searchId: input.searchId,
    crawlRunId: input.crawlRunId,
    provider,
    discoveredSourceCount: sources.length,
    maxSourcesPerProvider,
    selectedSourceCount: selected.length,
    truncatedCount,
  });

  return selected;
}

function createEmptyDiagnostics(): CrawlDiagnostics {
  return {
    discoveredSources: 0,
    crawledSources: 0,
    providersEnqueued: 0,
    providerFailures: 0,
    directJobsHarvested: 0,
    jobsBeforeDedupe: 0,
    jobsAfterDedupe: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    excludedByExperience: 0,
    dedupedOut: 0,
    validationDeferred: 0,
    inventoryScheduling: {
      inventorySources: 0,
      crawlableSources: 0,
      eligibleSources: 0,
      selectedSources: 0,
      inventoryByPlatform: {},
      eligibleByPlatform: {},
      skippedByReason: {},
      skippedByPlatformReason: {},
      freshnessBuckets: {},
      selectedByPlatform: {},
      platformSelectionBudgets: {},
      selectedByProvider: {},
      selectedByHealth: {},
      selectedSourceIds: [],
      skippedSourceSamples: [],
    },
    inventoryExpansion: {
      beforeCount: 0,
      afterRefreshCount: 0,
      afterExpansionCount: 0,
      selectedSearches: 0,
      candidateSources: 0,
      newSourcesAdded: 0,
      selectedSearchTitles: [],
      selectedSearchFilters: [],
      selectedSourceIds: [],
      newSourceIds: [],
      platformCountsBefore: {},
      platformCountsAfter: {},
      searchDiagnostics: [],
    },
    backgroundPersistence: {
      jobsInserted: 0,
      jobsUpdated: 0,
      jobsLinkedToRun: 0,
      indexedEventsEmitted: 0,
      failedBatches: 0,
      failureSamples: [],
      providerStats: [],
    },
    performance: {
      stageTimingsMs: {
        discovery: 0,
        providerExecution: 0,
        filtering: 0,
        dedupe: 0,
        persistence: 0,
        validation: 0,
        responseAssembly: 0,
        total: 0,
      },
      providerTimingsMs: [],
      progressUpdateCount: 0,
      persistenceBatchCount: 0,
    },
    dropReasonCounts: {},
    filterDecisionTraces: [],
    dedupeDecisionTraces: [],
  };
}

function resolveFinalStatus(sourceResults: CrawlSourceResult[], totalSavedJobs: number): CrawlRunStatus {
  const failedCount = sourceResults.filter((result) =>
    result.status === "failed" || result.status === "timed_out",
  ).length;
  const abortedCount = sourceResults.filter((result) => result.status === "aborted").length;

  if (abortedCount > 0) {
    return "aborted";
  }

  if (failedCount === 0) {
    return "completed";
  }

  return totalSavedJobs > 0 ? "partial" : "failed";
}

function mapProviderStatus(
  status: "success" | "partial" | "failed" | "unsupported" | "running",
  result?: Awaited<ReturnType<CrawlProvider["crawlSources"]>>,
): CrawlSourceResult["status"] {
  if (result && mapProviderResultToSourceStatus(result) === "timed_out") {
    return "timed_out";
  }

  return status;
}

function mapProviderResultToSourceStatus(
  result: Awaited<ReturnType<CrawlProvider["crawlSources"]>>,
): CrawlSourceResult["status"] {
  const diagnostics = result.diagnostics;
  const sourceCount = result.sourceCount ?? diagnostics?.sourceCount ?? 0;
  const sourceTimedOutCount = diagnostics?.sourceTimedOutCount ?? 0;
  const emittedJobs = result.jobs.length + (diagnostics?.jobsEmittedViaOnBatch ?? 0);

  if (
    result.status === "failed" &&
    sourceCount > 0 &&
    sourceTimedOutCount >= sourceCount &&
    result.fetchedCount === 0 &&
    emittedJobs === 0
  ) {
    return "timed_out";
  }

  return result.status;
}

function mergeSignals(...signals: Array<AbortSignal | undefined>) {
  const controller = new AbortController();

  for (const signal of signals) {
    if (!signal) {
      continue;
    }

    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }

    signal.addEventListener(
      "abort",
      () => {
        controller.abort(signal.reason);
      },
      { once: true },
    );
  }

  return controller.signal;
}

export function resetBackgroundIngestionSchedulerForTests() {
  stopRecurringBackgroundIngestionScheduler();
}
