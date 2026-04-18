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
  refreshPersistentSourceInventory,
} from "@/lib/server/inventory/service";
import { resolveObservedSourceNextEligibleAt } from "@/lib/server/inventory/selection";
import { createDefaultProviders } from "@/lib/server/providers";
import type { CrawlProvider } from "@/lib/server/providers/types";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
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
};

type BackgroundIngestionTriggerResult =
  | { status: "started"; searchId: string; crawlRunId: string }
  | { status: "skipped-disabled" | "skipped-active" | "skipped-no-mongo"; searchId?: string };

type BackgroundSchedulerState = {
  startedAt: string;
  intervalMs: number;
  timer: ReturnType<typeof setTimeout> | null;
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

  const repository = await resolveDurableBackgroundRepository(runtime.repository);
  if (!repository) {
    return { status: "skipped-no-mongo" };
  }

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
    const inventory = await (runtime.refreshInventory ?? refreshPersistentSourceInventory)({
      repository,
      now: runtime.now,
    });
    diagnostics.discoveredSources = inventory.length;
    diagnostics.performance.stageTimingsMs.discovery = Date.now() - discoveryStartedMs;
    const selectionPlan = planPersistentInventoryRecurringCrawl({
      inventory,
      providers,
      now: runtime.now,
      maxSources: runtime.maxSources,
      intervalMs: runtime.schedulingIntervalMs,
    });
    diagnostics.inventoryScheduling = selectionPlan.diagnostics;
    const selectedRecordById = new Map(
      selectionPlan.selectedRecords.map((record) => [record._id, record] as const),
    );
    const inventorySources = selectionPlan.selectedRecords.map(toDiscoveredSourceFromInventory);

    console.info("[background-ingestion:source-selection]", {
      inventorySources: selectionPlan.diagnostics.inventorySources,
      crawlableSources: selectionPlan.diagnostics.crawlableSources,
      eligibleSources: selectionPlan.diagnostics.eligibleSources,
      selectedSources: selectionPlan.diagnostics.selectedSources,
      skippedByReason: selectionPlan.diagnostics.skippedByReason,
      freshnessBuckets: selectionPlan.diagnostics.freshnessBuckets,
      selectedByPlatform: selectionPlan.diagnostics.selectedByPlatform,
      selectedByHealth: selectionPlan.diagnostics.selectedByHealth,
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
        const savedJobs = await repository.persistJobs(target.crawlRunId, persistableJobs, {
          searchSessionId: target.searchSession._id,
        });
        totalSavedJobs += savedJobs.length;
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
    const finishedAt = runTimestamp();
    const status = resolveFinalStatus(sourceResults, totalSavedJobs);

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

async function resolveDurableBackgroundRepository(repository?: JobCrawlerRepository) {
  if (repository) {
    return repository;
  }

  try {
    const [{ JobCrawlerRepository }, { getMongoDb }] = await Promise.all([
      import("@/lib/server/db/repository"),
      import("@/lib/server/mongodb"),
    ]);

    return new JobCrawlerRepository((await getMongoDb()) as never);
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "MongoDB is unavailable for recurring background ingestion.";
    console.warn("[background-ingestion:mongo-unavailable]", {
      message,
      bootstrapFailure:
        /bootstrap failed|migration|index initialization|jobs_canonical_job_key/i.test(message),
    });
    return null;
  }
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
