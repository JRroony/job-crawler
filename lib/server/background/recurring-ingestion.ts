import "server-only";

import {
  backgroundIngestionOwnerKey,
  backgroundIngestionSearchFilters,
} from "@/lib/server/background/constants";
import { queueSearchRun } from "@/lib/server/crawler/background-runs";
import { CrawlAbortedError, seedToPersistableJob } from "@/lib/server/crawler/pipeline";
import { createId } from "@/lib/server/crawler/helpers";
import {
  toDiscoveredSourceFromInventory,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import { getEnv } from "@/lib/server/env";
import {
  planPersistentInventoryRecurringCrawl,
  runPersistentInventoryExpansion,
  refreshPersistentSourceInventory,
  type InventoryExpansionResult,
} from "@/lib/server/inventory/service";
import { resolveObservedSourceNextEligibleAt } from "@/lib/server/inventory/selection";
import { createDefaultProviders } from "@/lib/server/providers";
import type { CrawlProvider } from "@/lib/server/providers/types";
import type {
  JobCrawlerRepository,
  PersistJobsWithStatsResult,
} from "@/lib/server/db/repository";
import type {
  CrawlDiagnostics,
  CrawlRunStatus,
  CrawlSourceResult,
  SearchDocument,
  SearchSessionDocument,
} from "@/lib/types";

type BackgroundIngestionRuntime = {
  repository?: JobCrawlerRepository;
  providers?: CrawlProvider[];
  fetchImpl?: typeof fetch;
  now?: Date;
  intervalMs?: number;
  staleAfterMs?: number;
  runTimeoutMs?: number;
  maxSources?: number;
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
  }) => Promise<InventoryExpansionResult>;
  resolveRepository?: () => Promise<JobCrawlerRepository>;
};

type BackgroundIngestionTriggerResult =
  | { status: "started"; searchId: string; crawlRunId: string }
  | {
      status:
        | "skipped-disabled"
        | "skipped-active"
        | "skipped-no-mongo"
        | "skipped-bootstrap-failed";
      searchId?: string;
      reason?: "mongo_unavailable" | "bootstrap_failed";
      phase?: "repository_resolution" | "index_initialization";
      message?: string;
    };

type BackgroundSchedulerState = {
  startedAt: string;
  intervalMs: number;
  timer: ReturnType<typeof setTimeout> | null;
};

type BackgroundPersistenceCounts = Pick<
  PersistJobsWithStatsResult,
  "insertedCount" | "updatedCount" | "linkedToRunCount" | "indexedEventCount"
>;

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerBackgroundSchedulerState: BackgroundSchedulerState | undefined;
}

export function startRecurringBackgroundIngestionScheduler(
  runtime: BackgroundIngestionRuntime = {},
) {
  const env = getEnv();
  if (!env.BACKGROUND_INGESTION_ENABLED) {
    return {
      started: false,
      reason: "disabled" as const,
    };
  }

  const existing = globalThis.__jobCrawlerBackgroundSchedulerState;
  if (existing) {
    return {
      started: true,
      reason: "already-started" as const,
      intervalMs: existing.intervalMs,
    };
  }

  const intervalMs = Math.max(
    1,
    Math.floor(runtime.intervalMs ?? env.BACKGROUND_INGESTION_INTERVAL_MS),
  );
  const state: BackgroundSchedulerState = {
    startedAt: new Date().toISOString(),
    intervalMs,
    timer: null,
  };

  const scheduleNext = (delayMs: number) => {
    state.timer = setTimeout(() => {
      scheduleNext(intervalMs);
      void triggerRecurringBackgroundIngestion(runtime).catch((error) => {
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
  scheduleNext(0);

  return {
    started: true,
    reason: "started" as const,
    intervalMs,
  };
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
    return { status: "skipped-disabled" };
  }

  const repositoryResolution = await resolveDurableBackgroundRepository(runtime);
  if (!repositoryResolution.repository) {
    return {
      status:
        repositoryResolution.reason === "bootstrap_failed"
          ? "skipped-bootstrap-failed"
          : "skipped-no-mongo",
      reason: repositoryResolution.reason,
      phase: repositoryResolution.phase,
      message: repositoryResolution.message,
    };
  }
  const repository = repositoryResolution.repository;

  const now = runtime.now ?? new Date();
  const search = await ensureBackgroundSearch(repository, now);
  const staleAfterMs = Math.max(
    1,
    Math.floor(runtime.staleAfterMs ?? env.BACKGROUND_INGESTION_STALE_AFTER_MS),
  );

  await recoverStaleBackgroundRunIfNeeded(search._id, repository, now, staleAfterMs);

  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForSearch(search._id);
  if (activeQueueEntry) {
    return { status: "skipped-active", searchId: search._id };
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
        },
        {
          repository,
          providers: runtime.providers,
          fetchImpl: runtime.fetchImpl,
          now,
          signal,
          runTimeoutMs: runtime.runTimeoutMs ?? env.BACKGROUND_INGESTION_RUN_TIMEOUT_MS,
          maxSources: Math.max(1, Math.floor(runtime.maxSources ?? env.CRAWL_MAX_SOURCES)),
          schedulingIntervalMs: Math.max(
            1,
            Math.floor(runtime.schedulingIntervalMs ?? env.BACKGROUND_INGESTION_INTERVAL_MS),
          ),
          refreshInventory: runtime.refreshInventory,
          expandInventory: runtime.expandInventory,
        },
      );
    },
    {
      ownerKey: backgroundIngestionOwnerKey,
      crawlRunId: crawlRun._id,
      searchSessionId: searchSession._id,
      queuedAt: now.toISOString(),
    },
  );

  if (!queued) {
    return { status: "skipped-active", searchId: search._id };
  }

  return {
    status: "started",
    searchId: search._id,
    crawlRunId: crawlRun._id,
  };
}

async function executeRecurringInventoryIngestion(
  target: {
    search: SearchDocument;
    searchSession: SearchSessionDocument;
    crawlRunId: string;
  },
  runtime: {
    repository: JobCrawlerRepository;
    providers?: CrawlProvider[];
    fetchImpl?: typeof fetch;
    now: Date;
    signal?: AbortSignal;
    runTimeoutMs: number;
    maxSources: number;
    schedulingIntervalMs: number;
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
    }) => Promise<InventoryExpansionResult>;
  },
) {
  const repository = runtime.repository;
  const providers = runtime.providers ?? createDefaultProviders();
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
  const providerPersistenceTotals = new Map<
    CrawlSourceResult["provider"],
    BackgroundPersistenceCounts
  >();
  const runTimestamp = () => new Date(startedAtMs + Math.max(0, Date.now() - startMs)).toISOString();

  try {
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
        Array.from(providerPersistenceTotals.entries()),
        selectionSkippedReason,
      );
    }
    const selectedRecordById = new Map(
      selectionPlan.selectedRecords.map((record) => [record._id, record] as const),
    );
    const inventorySources = selectionPlan.selectedRecords.map(toDiscoveredSourceFromInventory);

    console.info("[background-ingestion:inventory-expansion]", {
      beforeCount: inventoryExpansion.diagnostics.beforeCount,
      afterRefreshCount: inventoryExpansion.diagnostics.afterRefreshCount,
      afterExpansionCount: inventoryExpansion.diagnostics.afterExpansionCount,
      selectedSearches: inventoryExpansion.diagnostics.selectedSearches,
      candidateSources: inventoryExpansion.diagnostics.candidateSources,
      newSourcesAdded: inventoryExpansion.diagnostics.newSourcesAdded,
      newSourceIds: inventoryExpansion.diagnostics.newSourceIds,
      skippedReason: inventoryExpansion.diagnostics.skippedReason,
    });

    console.info("[background-ingestion:source-selection]", {
      inventorySources: selectionPlan.diagnostics.inventorySources,
      crawlableSources: selectionPlan.diagnostics.crawlableSources,
      eligibleSources: selectionPlan.diagnostics.eligibleSources,
      selectedSources: selectionPlan.diagnostics.selectedSources,
      skippedByReason: selectionPlan.diagnostics.skippedByReason,
      freshnessBuckets: selectionPlan.diagnostics.freshnessBuckets,
      selectedByPlatform: selectionPlan.diagnostics.selectedByPlatform,
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

    for (const provider of providers) {
      await runController.throwIfCanceled();

      const providerSources = inventorySources.filter((source) => provider.supportsSource(source));
      if (providerSources.length === 0) {
        continue;
      }

      diagnostics.providersEnqueued += 1;
      diagnostics.crawledSources += providerSources.length;
      const providerStartedAt = runTimestamp();
      const providerStartedMs = Date.now();

      try {
        const result = await provider.crawlSources(
          {
            fetchImpl: runtime.fetchImpl ?? fetch,
            now: runtime.now,
            filters: backgroundIngestionSearchFilters,
            signal: runController.signal,
            throwIfCanceled: () => runController.throwIfCanceled(),
          },
          providerSources,
        );

        totalFetchedJobs += result.fetchedCount;
        totalMatchedJobs += result.matchedCount;
        diagnostics.jobsBeforeDedupe += result.jobs.length;

        const persistableJobs = result.jobs.map((job) => seedToPersistableJob(job, runtime.now));
        const persistence = await repository.persistJobsWithStats(target.crawlRunId, persistableJobs, {
          searchSessionId: target.searchSession._id,
        });
        const savedJobs = persistence.jobs;
        totalSavedJobs += persistence.linkedToRunCount;
        accumulatePersistenceStats(totalPersistenceStats, persistence);
        accumulateProviderPersistenceStats(providerPersistenceTotals, provider.provider, persistence);
        diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
          totalPersistenceStats,
          Array.from(providerPersistenceTotals.entries()),
          diagnostics.backgroundPersistence?.skippedReason,
        );
        diagnostics.jobsAfterDedupe = totalSavedJobs;
        diagnostics.dedupedOut = Math.max(0, diagnostics.jobsBeforeDedupe - diagnostics.jobsAfterDedupe);
        diagnostics.performance.persistenceBatchCount += 1;

        const finishedAt = runTimestamp();
        const sourceResult = createSourceResult({
          crawlRunId: target.crawlRunId,
          searchId: target.search._id,
          provider: provider.provider,
          status: mapProviderStatus(result.status),
          sourceCount: providerSources.length,
          fetchedCount: result.fetchedCount,
          matchedCount: result.matchedCount,
          savedCount: savedJobs.length,
          warningCount: result.warningCount ?? 0,
          errorMessage: result.errorMessage,
          startedAt: providerStartedAt,
          finishedAt,
        });
        sourceResults.push(sourceResult);
        await repository.updateCrawlSourceResult(sourceResult);

        console.info("[background-ingestion:provider-summary]", {
          searchId: target.search._id,
          crawlRunId: target.crawlRunId,
          provider: provider.provider,
          sourceCount: providerSources.length,
          fetchedCount: result.fetchedCount,
          matchedCount: result.matchedCount,
          savedCount: savedJobs.length,
          insertedCount: persistence.insertedCount,
          updatedCount: persistence.updatedCount,
          linkedToRunCount: persistence.linkedToRunCount,
          indexedEventCount: persistence.indexedEventCount,
          status: result.status,
          warningCount: result.warningCount ?? 0,
          errorMessage: result.errorMessage,
        });
        await repository.recordSourceInventoryObservations(
          providerSources.map((source) => ({
            sourceId: source.id,
            observedAt: finishedAt,
            succeeded: result.status === "success" || result.status === "partial",
            health:
              result.status === "failed"
                ? "failing"
                : result.status === "partial"
                  ? "degraded"
                  : "healthy",
            lastFailureReason: result.status === "failed" ? result.errorMessage : undefined,
            nextEligibleAt: resolveObservedSourceNextEligibleAt({
              record: selectedRecordById.get(source.id) ?? inventory.find((record) => record._id === source.id)!,
              observedAt: finishedAt,
              intervalMs: runtime.schedulingIntervalMs,
              health:
                result.status === "failed"
                  ? "failing"
                  : result.status === "partial"
                    ? "degraded"
                    : "healthy",
              consecutiveFailures:
                result.status === "failed"
                  ? (selectedRecordById.get(source.id)?.consecutiveFailures ?? 0) + 1
                  : 0,
              succeeded: result.status === "success" || result.status === "partial",
            }),
          })),
        );

        providerTimings.push({
          provider: provider.provider,
          duration: Date.now() - providerStartedMs,
          sourceCount: providerSources.length,
          timedOut: false,
        });
      } catch (error) {
        const abortLike = error instanceof Error && error.name === "AbortError";
        if (!abortLike) {
          diagnostics.providerFailures += 1;
        }

        const finishedAt = runTimestamp();
        const sourceResult = createSourceResult({
          crawlRunId: target.crawlRunId,
          searchId: target.search._id,
          provider: provider.provider,
          status: abortLike ? "aborted" : "failed",
          sourceCount: providerSources.length,
          fetchedCount: 0,
          matchedCount: 0,
          savedCount: 0,
          warningCount: 0,
          errorMessage: error instanceof Error ? error.message : "Provider crawl failed unexpectedly.",
          startedAt: providerStartedAt,
          finishedAt,
        });
        sourceResults.push(sourceResult);
        await repository.updateCrawlSourceResult(sourceResult);
        await repository.recordSourceInventoryObservations(
          providerSources.map((source) => ({
            sourceId: source.id,
            observedAt: finishedAt,
            succeeded: false,
            health: "failing",
            lastFailureReason:
              error instanceof Error ? error.message : "Provider crawl failed unexpectedly.",
            nextEligibleAt: resolveObservedSourceNextEligibleAt({
              record: selectedRecordById.get(source.id) ?? inventory.find((record) => record._id === source.id)!,
              observedAt: finishedAt,
              intervalMs: runtime.schedulingIntervalMs,
              health: "failing",
              consecutiveFailures: (selectedRecordById.get(source.id)?.consecutiveFailures ?? 0) + 1,
              succeeded: false,
            }),
          })),
        );

        providerTimings.push({
          provider: provider.provider,
          duration: Date.now() - providerStartedMs,
          sourceCount: providerSources.length,
          timedOut: abortLike,
        });

        if (abortLike) {
          throw error;
        }
      }
    }

    diagnostics.performance.stageTimingsMs.providerExecution = Math.max(
      0,
      Date.now() - startMs - diagnostics.performance.stageTimingsMs.discovery,
    );
    diagnostics.performance.stageTimingsMs.total = Date.now() - startMs;
    diagnostics.performance.progressUpdateCount = 2;
    diagnostics.performance.providerTimingsMs = providerTimings;
    diagnostics.backgroundPersistence = buildBackgroundPersistenceDiagnostics(
      totalPersistenceStats,
      Array.from(providerPersistenceTotals.entries()),
      diagnostics.backgroundPersistence?.skippedReason,
    );
    const finishedAt = runTimestamp();
    const status = resolveFinalStatus(sourceResults, totalSavedJobs);

    console.info("[background-ingestion:persistence-summary]", {
      searchId: target.search._id,
      crawlRunId: target.crawlRunId,
      status,
      jobsInserted: totalPersistenceStats.insertedCount,
      jobsUpdated: totalPersistenceStats.updatedCount,
      jobsLinkedToRun: totalPersistenceStats.linkedToRunCount,
      indexedEventsEmitted: totalPersistenceStats.indexedEventCount,
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
      Array.from(providerPersistenceTotals.entries()),
      diagnostics.backgroundPersistence?.skippedReason,
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

async function ensureBackgroundSearch(repository: JobCrawlerRepository, now: Date) {
  const existing = await repository.findMostRecentSearchByFilters(backgroundIngestionSearchFilters);
  if (existing) {
    return existing;
  }

  return repository.createSearch(backgroundIngestionSearchFilters, now.toISOString());
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
  }) => Promise<InventoryExpansionResult>;
}): Promise<InventoryExpansionResult> {
  if (input.expandInventory) {
    return input.expandInventory({
      repository: input.repository,
      now: input.now,
      fetchImpl: input.fetchImpl,
      intervalMs: input.intervalMs,
      maxSources: input.maxSources,
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

function accumulateProviderPersistenceStats(
  totals: Map<CrawlSourceResult["provider"], BackgroundPersistenceCounts>,
  provider: CrawlSourceResult["provider"],
  next: BackgroundPersistenceCounts,
) {
  const current = totals.get(provider) ?? createEmptyPersistenceStats();
  accumulatePersistenceStats(current, next);
  totals.set(provider, current);
}

function buildBackgroundPersistenceDiagnostics(
  total: BackgroundPersistenceCounts,
  providerStats: Array<[CrawlSourceResult["provider"], BackgroundPersistenceCounts]>,
  skippedReason?: string,
): NonNullable<CrawlDiagnostics["backgroundPersistence"]> {
  return {
    jobsInserted: total.insertedCount,
    jobsUpdated: total.updatedCount,
    jobsLinkedToRun: total.linkedToRunCount,
    indexedEventsEmitted: total.indexedEventCount,
    skippedReason,
    providerStats: providerStats.map(([provider, stats]) => ({
      provider,
      savedCount: stats.linkedToRunCount,
      insertedCount: stats.insertedCount,
      updatedCount: stats.updatedCount,
      linkedToRunCount: stats.linkedToRunCount,
      indexedEventCount: stats.indexedEventCount,
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
      reason: "mongo_unavailable" | "bootstrap_failed";
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

    const [{ JobCrawlerRepository }, { getMongoDb }] = await Promise.all([
      import("@/lib/server/db/repository"),
      import("@/lib/server/mongodb"),
    ]);

    return { repository: new JobCrawlerRepository((await getMongoDb()) as never) };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "MongoDB is unavailable for recurring background ingestion.";
    const failure = classifyBackgroundRepositoryFailure(error);
    console.warn(
      failure.reason === "bootstrap_failed"
        ? "[background-ingestion:bootstrap-failed]"
        : "[background-ingestion:mongo-unavailable]",
      {
        message,
        reason: failure.reason,
        phase: failure.phase,
        bootstrapFailure: failure.reason === "bootstrap_failed",
      },
    );
    return failure;
  }
}

export function classifyBackgroundRepositoryFailure(error: unknown): {
  repository: null;
  reason: "mongo_unavailable" | "bootstrap_failed";
  phase: "repository_resolution" | "index_initialization";
  message: string;
} {
  const message =
    error instanceof Error
      ? error.message
      : "MongoDB is unavailable for recurring background ingestion.";
  const reason =
    /bootstrap failed|migration|index initialization|index init|jobs_canonical_job_key/i.test(
      message,
    )
      ? "bootstrap_failed"
      : "mongo_unavailable";
  const phase = reason === "bootstrap_failed" ? "index_initialization" : "repository_resolution";

  return {
    repository: null,
    reason,
    phase,
    message,
  };
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

function createSourceResult(input: Omit<CrawlSourceResult, "_id">): CrawlSourceResult {
  return {
    _id: `${input.crawlRunId}:${input.provider}`,
    ...input,
  };
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
      skippedByReason: {},
      freshnessBuckets: {},
      selectedByPlatform: {},
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
  const failedCount = sourceResults.filter((result) => result.status === "failed").length;
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
  status: "success" | "partial" | "failed" | "unsupported",
): CrawlSourceResult["status"] {
  return status;
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
