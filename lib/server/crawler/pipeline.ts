import "server-only";

import { buildCanonicalJobIdentity } from "@/lib/job-identity";
import {
  dedupeJobsWithDiagnostics,
  dedupeStoredJobs,
} from "@/lib/server/crawler/dedupe";
import {
  buildContentFingerprint,
  buildSourceLookupKey,
  createId,
  evaluateSearchFilters,
  type ExperienceFilterResult,
  isValidationStale,
  normalizeComparableText,
  resolveJobExperienceClassification,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import { parseGreenhouseUrl } from "@/lib/server/discovery/greenhouse-url";
import type { LocationMatchResult } from "@/lib/server/location-resolution";
import {
  toStoredValidation,
  validateJobLink,
  type LinkValidationDraft,
} from "@/lib/server/crawler/link-validation";
import { sortJobsForPersistence, sortJobsWithDiagnostics } from "@/lib/server/crawler/sort";
import { getEnv } from "@/lib/server/env";
import type { JobCrawlerRepository, PersistableJob } from "@/lib/server/db/repository";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider, NormalizedJobSeed } from "@/lib/server/providers/types";
import type { TitleMatchResult } from "@/lib/server/title-retrieval";
import {
  activeCrawlerPlatforms,
  crawlResponseSchema,
  crawlSourceResultSchema,
  resolveOperationalCrawlerPlatforms,
  searchFiltersSchema,
  type ActiveCrawlerPlatform,
  type CrawlerPlatform,
  type CrawlProviderSummary,
  type CrawlResponse,
  type CrawlDiagnostics,
  type CrawlMode,
  type CrawlRun,
  type CrawlRunStage,
  type CrawlRunStatus,
  type CrawlSourceResult,
  type CrawlValidationMode,
  type JobListing,
  type SearchDocument,
} from "@/lib/types";

export type CrawlLinkValidationMode = CrawlValidationMode;

export const defaultCrawlLinkValidationMode: CrawlLinkValidationMode = "deferred";

const defaultInlineValidationTopN = 0;
const defaultBalancedInlineValidationTopN = 5;

type ExecuteCrawlPipelineInput = {
  search: SearchDocument;
  crawlRun?: CrawlRun;
  repository: JobCrawlerRepository;
  discovery: DiscoveryService;
  providers: CrawlProvider[];
  fetchImpl: typeof fetch;
  now: Date;
  deepExperienceInference?: boolean;
  linkValidationMode?: CrawlLinkValidationMode;
  inlineValidationTopN?: number;
  providerTimeoutMs?: number;
  progressUpdateIntervalMs?: number;
};

export async function executeCrawlPipeline(
  input: ExecuteCrawlPipelineInput,
): Promise<CrawlResponse> {
  const crawlStartedMs = Date.now();
  const normalizedFilters = searchFiltersSchema.parse(input.search.filters);
  const selectedProviders = selectProvidersForSearch(
    input.providers,
    normalizedFilters.platforms,
  );
  const search = {
    ...input.search,
    filters: normalizedFilters,
  };
  const validationStrategy = resolveValidationStrategy(
    input.linkValidationMode,
    input.inlineValidationTopN,
    normalizedFilters.crawlMode,
  );
  const env = getEnv();
  const providerTimeoutMs =
    input.providerTimeoutMs ?? env.CRAWL_PROVIDER_TIMEOUT_MS;
  const progressUpdateIntervalMs =
    input.progressUpdateIntervalMs ?? env.CRAWL_PROGRESS_UPDATE_INTERVAL_MS;
  const crawlRun =
    input.crawlRun ??
    (await input.repository.createCrawlRun(search._id, input.now.toISOString(), {
      validationMode: validationStrategy.mode,
      stage: "queued",
    }));
  if (!input.crawlRun) {
    await input.repository.updateSearchLatestRun(
      search._id,
      crawlRun._id,
      "running",
      input.now.toISOString(),
    );
  }

  try {
    const stageTimingsMs = {
      discovery: 0,
      providerExecution: 0,
      filtering: 0,
      dedupe: 0,
      persistence: 0,
      validation: 0,
      responseAssembly: 0,
    };
    const diagnostics = createEmptyDiagnostics();
    const sourceResultsByProvider = new Map<CrawlSourceResult["provider"], CrawlSourceResult>();
    const providerSavedJobIds = new Map<CrawlSourceResult["provider"], Set<string>>();
    const savedJobIds = new Set<string>();
    const providerDurationsMs: Array<{
      provider: CrawlSourceResult["provider"];
      duration: number;
      sourceCount: number;
      timedOut: boolean;
    }> = [];
    let totalFetchedJobs = 0;
    let totalMatchedJobs = 0;
    let firstVisibleResultAtMs: number | undefined;
    let mutationQueue = Promise.resolve();
    const mutationErrors: unknown[] = [];
    let progressUpdateCount = 0;
    let persistenceBatchCount = 0;
    let pendingProgressStage: CrawlRunStage | undefined;
    let pendingProgressNow: string | undefined;
    let lastProgressFlushAtMs = 0;
    let progressFlushPromise: Promise<void> | null = null;

    const queueMutation = <T,>(task: () => Promise<T>) => {
      const run = mutationQueue.then(task, task);
      mutationQueue = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    };

    const buildSortedSourceResults = () =>
      Array.from(sourceResultsByProvider.values()).sort((left, right) =>
        left.provider.localeCompare(right.provider),
      );

    const updateRunProgress = async (
      stage: CrawlRunStage,
      now = new Date().toISOString(),
    ) => {
      progressUpdateCount += 1;
      syncPerformanceDiagnostics({
        diagnostics,
        stageTimingsMs,
        providerDurationsMs,
        crawlStartedMs,
        firstVisibleResultAtMs,
        progressUpdateCount,
        persistenceBatchCount,
      });
      diagnostics.jobsAfterDedupe = savedJobIds.size;
      diagnostics.dedupedOut = Math.max(0, diagnostics.jobsBeforeDedupe - diagnostics.jobsAfterDedupe);

      await input.repository.updateCrawlRunProgress(crawlRun._id, {
        status: "running",
        stage,
        totalFetchedJobs,
        totalMatchedJobs,
        dedupedJobs: savedJobIds.size,
        diagnostics,
        validationMode: validationStrategy.mode,
        providerSummary: buildProviderSummary(buildSortedSourceResults()),
      });
      await input.repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now);
    };

    const flushProgressUpdate = async (force = false) => {
      if (!pendingProgressStage || !pendingProgressNow) {
        return;
      }

      const nowMs = Date.now();
      if (!force && nowMs - lastProgressFlushAtMs < progressUpdateIntervalMs) {
        return;
      }

      const stage = pendingProgressStage;
      const now = pendingProgressNow;
      pendingProgressStage = undefined;
      pendingProgressNow = undefined;
      lastProgressFlushAtMs = nowMs;
      await updateRunProgress(stage, now);
    };

    const scheduleProgressUpdate = (
      stage: CrawlRunStage,
      now = new Date().toISOString(),
      options: { immediate?: boolean } = {},
    ) => {
      pendingProgressStage = stage;
      pendingProgressNow = now;

      if (options.immediate) {
        progressFlushPromise = queueMutation(() => flushProgressUpdate(true))
          .then(
            () => undefined,
            (error) => {
              mutationErrors.push(error);
            },
          )
          .finally(() => {
            progressFlushPromise = null;
          });
        return;
      }

      const nowMs = Date.now();
      if (progressFlushPromise || nowMs - lastProgressFlushAtMs < progressUpdateIntervalMs) {
        return;
      }

      progressFlushPromise = queueMutation(() => flushProgressUpdate()).then(
        () => undefined,
        (error) => {
          mutationErrors.push(error);
        },
      ).finally(() => {
        progressFlushPromise = null;
      });
    };

    const persistSeedBatch = async (payload: {
      batchLabel: string;
      provider?: CrawlSourceResult["provider"];
      seeds: NormalizedJobSeed[];
      fetchedCount: number;
      sourceCount: number;
      status?: CrawlSourceResult["status"];
      warningCount?: number;
      errorMessage?: string;
      startedAt: string;
      finishedAt: string;
      accumulateProviderTotals?: boolean;
    }) => {
      const filteringStartedMs = Date.now();
      persistenceBatchCount += 1;
      const filteredSeeds = filterSeedsForSearch(payload.seeds, normalizedFilters, {
        deepExperienceInference: input.deepExperienceInference ?? false,
      });
      stageTimingsMs.filtering += Date.now() - filteringStartedMs;

      diagnostics.excludedByTitle += filteredSeeds.excludedByTitle;
      diagnostics.excludedByLocation += filteredSeeds.excludedByLocation;
      diagnostics.excludedByExperience += filteredSeeds.excludedByExperience;
      diagnostics.filterDecisionTraces.push(...filteredSeeds.filterDecisionTraces);
      Object.entries(filteredSeeds.dropReasonCounts).forEach(([reason, count]) => {
        diagnostics.dropReasonCounts[reason] = (diagnostics.dropReasonCounts[reason] ?? 0) + count;
      });
      totalMatchedJobs += filteredSeeds.jobs.length;

      const dedupeStartedMs = Date.now();
      const hydratedJobs = hydrateJobs(filteredSeeds.jobs, input.now);
      diagnostics.jobsBeforeDedupe += hydratedJobs.length;
      const dedupeResult = dedupeJobsWithDiagnostics(
        hydratedJobs,
        (job) => getPipelineTraceId(job),
      );
      diagnostics.dedupeDecisionTraces.push(
        ...buildDedupeTraceRecords(dedupeResult.dropped, hydratedJobs),
      );
      dedupeResult.dropped.forEach((trace) => incrementDropReasonCount(diagnostics, trace.dropReason));
      const dedupedJobs = sortJobsForPersistence(dedupeResult.jobs, normalizedFilters.title);
      stageTimingsMs.dedupe += Date.now() - dedupeStartedMs;

      const persistenceStartedMs = Date.now();
      const savedJobs = await input.repository.persistJobs(crawlRun._id, dedupedJobs);
      stageTimingsMs.persistence += Date.now() - persistenceStartedMs;

      let newVisibleJobCount = 0;
      for (const job of savedJobs) {
        if (!savedJobIds.has(job._id)) {
          savedJobIds.add(job._id);
          newVisibleJobCount += 1;
        }
      }

      if (!firstVisibleResultAtMs && savedJobIds.size > 0) {
        firstVisibleResultAtMs = Date.now();
      }

      if (payload.provider) {
        const existing = sourceResultsByProvider.get(payload.provider);
        if (existing) {
          let providerSavedCount = existing.savedCount;
          let providerSavedSet = providerSavedJobIds.get(payload.provider);
          if (!providerSavedSet) {
            providerSavedSet = new Set<string>();
            providerSavedJobIds.set(payload.provider, providerSavedSet);
          }

          savedJobs.forEach((job) => {
            if (!job.sourceProvenance.some((record) => record.sourcePlatform === payload.provider)) {
              return;
            }

            providerSavedSet?.add(job._id);
          });
          providerSavedCount = providerSavedSet.size;

          const updated = crawlSourceResultSchema.parse({
            ...existing,
            status: payload.status ?? existing.status,
            sourceCount: payload.accumulateProviderTotals ? existing.sourceCount : payload.sourceCount,
            fetchedCount: payload.accumulateProviderTotals
              ? existing.fetchedCount + payload.fetchedCount
              : payload.fetchedCount,
            matchedCount: payload.accumulateProviderTotals
              ? existing.matchedCount + filteredSeeds.jobs.length
              : filteredSeeds.jobs.length,
            savedCount: providerSavedCount,
            warningCount: payload.accumulateProviderTotals
              ? existing.warningCount + (payload.warningCount ?? 0)
              : payload.warningCount ?? existing.warningCount,
            errorMessage: payload.errorMessage ?? existing.errorMessage,
            finishedAt: payload.finishedAt,
          });
          sourceResultsByProvider.set(payload.provider, updated);
          await input.repository.updateCrawlSourceResult(updated);
        }
      }

      console.info("[crawl:persistence-batch]", {
        searchId: search._id,
        batch: payload.batchLabel,
        provider: payload.provider ?? "discovery_harvest",
        sourceCount: payload.sourceCount,
        fetchedCount: payload.fetchedCount,
        matchedCount: filteredSeeds.jobs.length,
        dedupeInputCount: hydratedJobs.length,
        dedupeOutputCount: dedupedJobs.length,
        touchedSavedCount: savedJobs.length,
        newVisibleJobCount,
        totalVisibleJobCount: savedJobIds.size,
        warningCount: payload.warningCount ?? 0,
        errorMessage: payload.errorMessage,
      });

      scheduleProgressUpdate("crawling", payload.finishedAt, {
        immediate: newVisibleJobCount > 0 || payload.status !== "running",
      });
    };

    const initializedSourceResults = selectedProviders.map((provider) =>
      crawlSourceResultSchema.parse({
        _id: createId(),
        crawlRunId: crawlRun._id,
        searchId: search._id,
        provider: provider.provider,
        status: "running",
        sourceCount: 0,
        fetchedCount: 0,
        matchedCount: 0,
        savedCount: 0,
        warningCount: 0,
        startedAt: crawlRun.startedAt,
        finishedAt: crawlRun.startedAt,
      }),
    );

    if (initializedSourceResults.length > 0) {
      initializedSourceResults.forEach((sourceResult) => {
        sourceResultsByProvider.set(sourceResult.provider, sourceResult);
      });
      await input.repository.saveCrawlSourceResults(initializedSourceResults);
    }

    const discoveredSourceIds = new Set<string>();
    const enqueuedProviders = new Set<CrawlSourceResult["provider"]>();
    let totalHarvestedDiscoveryJobs = 0;

    const runProviderStage = async (
      provider: CrawlProvider,
      sourcesForProvider: Parameters<CrawlProvider["crawlSources"]>[1],
    ) => {
      if (sourcesForProvider.length === 0) {
        return;
      }

      const startedAt = new Date().toISOString();
      const startedMs = Date.now();
      let emittedLiveBatch = false;
      const runningSourceResult = sourceResultsByProvider.get(provider.provider);

      if (runningSourceResult) {
        const updatedRunningResult = crawlSourceResultSchema.parse({
          ...runningSourceResult,
          startedAt: runningSourceResult.fetchedCount === 0 ? startedAt : runningSourceResult.startedAt,
          finishedAt: startedAt,
          sourceCount: runningSourceResult.sourceCount + sourcesForProvider.length,
          status: "running",
        });
        sourceResultsByProvider.set(provider.provider, updatedRunningResult);
        await input.repository.updateCrawlSourceResult(updatedRunningResult);
      }

      try {
        const result = await runProviderWithTimeout(
          provider.crawlSources(
            {
              fetchImpl: input.fetchImpl,
              now: input.now,
              filters: normalizedFilters,
              onBatch: async (batch) => {
                emittedLiveBatch = true;
                totalFetchedJobs += batch.fetchedCount;
                void queueMutation(() =>
                  persistSeedBatch({
                    batchLabel: `${batch.provider}:batch`,
                    provider: batch.provider,
                    seeds: batch.jobs,
                    fetchedCount: batch.fetchedCount,
                    sourceCount: batch.sourceCount ?? sourcesForProvider.length,
                    status: "running",
                    startedAt,
                    finishedAt: new Date().toISOString(),
                    accumulateProviderTotals: true,
                  }),
                ).catch((error) => {
                  mutationErrors.push(error);
                });
              },
            },
            sourcesForProvider,
          ),
          provider.provider,
          providerTimeoutMs,
        );

        const finishedAt = new Date().toISOString();
        const durationMs = Date.now() - startedMs;
        providerDurationsMs.push({
          provider: result.provider,
          duration: durationMs,
          sourceCount: result.sourceCount ?? sourcesForProvider.length,
          timedOut: false,
        });

        if (result.status === "failed" || result.status === "partial") {
          diagnostics.providerFailures += 1;
        }

        if (!emittedLiveBatch) {
          totalFetchedJobs += result.fetchedCount;

          await queueMutation(() =>
            persistSeedBatch({
              batchLabel: result.provider,
              provider: result.provider,
              seeds: result.jobs,
              fetchedCount: result.fetchedCount,
              sourceCount: result.sourceCount ?? sourcesForProvider.length,
              status: result.status,
              warningCount: result.warningCount,
              errorMessage: result.errorMessage,
              startedAt,
              finishedAt,
              accumulateProviderTotals: true,
            }),
          );
          return;
        }

        await queueMutation(async () => {
          const existing = sourceResultsByProvider.get(result.provider);
          if (!existing) {
            return;
          }

          const updated = crawlSourceResultSchema.parse({
            ...existing,
            status: result.status,
            warningCount: existing.warningCount + (result.warningCount ?? 0),
            errorMessage: result.errorMessage ?? existing.errorMessage,
            finishedAt,
          });
          sourceResultsByProvider.set(result.provider, updated);
          await input.repository.updateCrawlSourceResult(updated);
          await updateRunProgress("crawling", finishedAt);
        });
      } catch (reason) {
        const finishedAt = new Date().toISOString();
        const durationMs = Date.now() - startedMs;
        const timedOut = isTimeoutError(reason);
        providerDurationsMs.push({
          provider: provider.provider,
          duration: durationMs,
          sourceCount: sourcesForProvider.length,
          timedOut,
        });

        await queueMutation(async () => {
          diagnostics.providerFailures += 1;
          const existing = sourceResultsByProvider.get(provider.provider);
          const failedResult = crawlSourceResultSchema.parse({
            _id: existing?._id ?? createId(),
            crawlRunId: crawlRun._id,
            searchId: search._id,
            provider: provider.provider,
            status: "failed",
            sourceCount: existing?.sourceCount ?? sourcesForProvider.length,
            fetchedCount: existing?.fetchedCount ?? 0,
            matchedCount: existing?.matchedCount ?? 0,
            savedCount: existing?.savedCount ?? 0,
            warningCount: (existing?.warningCount ?? 0) + 1,
            errorMessage:
              reason instanceof Error
                ? reason.message
                : `Provider ${provider.provider} failed unexpectedly.`,
            startedAt: existing?.startedAt ?? startedAt,
            finishedAt,
          });
          sourceResultsByProvider.set(provider.provider, failedResult);
          await input.repository.updateCrawlSourceResult(failedResult);
          await updateRunProgress("crawling", finishedAt);
        });
      }
    };

    const processDiscoveryStage = async (payload: {
      label: string;
      sources: DiscoveredSource[];
      jobs: NormalizedJobSeed[];
      diagnostics?: CrawlDiagnostics["discovery"];
    }) => {
      const newSources = payload.sources.filter((source) => {
        if (discoveredSourceIds.has(source.id)) {
          return false;
        }

        discoveredSourceIds.add(source.id);
        return true;
      });
      const providerSources = selectedProviders.map((provider) =>
        newSources.filter((source) => provider.supportsSource(source)),
      );

      diagnostics.discoveredSources = discoveredSourceIds.size;
      diagnostics.crawledSources += providerSources.reduce((total, sources) => total + sources.length, 0);
      totalHarvestedDiscoveryJobs += payload.jobs.length;
      diagnostics.directJobsHarvested = totalHarvestedDiscoveryJobs;
      if (payload.diagnostics) {
        diagnostics.discovery = payload.diagnostics;
      }

      const providerRouting = selectedProviders.map((provider, index) => ({
        provider: provider.provider,
        sourceCount: providerSources[index].length,
      }));
      providerRouting
        .filter((entry) => entry.sourceCount > 0)
        .forEach((entry) => enqueuedProviders.add(entry.provider));
      diagnostics.providersEnqueued = enqueuedProviders.size;

      console.info("[crawl:provider-routing]", {
        searchId: search._id,
        stage: payload.label,
        normalizedFilters,
        discoveredSourceCount: newSources.length,
        harvestedDiscoveryJobCount: payload.jobs.length,
        discoveredPlatformCounts: newSources.reduce<Record<string, number>>((counts, source) => {
          counts[source.platform] = (counts[source.platform] ?? 0) + 1;
          return counts;
        }, {}),
        selectedProviders: selectedProviders.map((provider) => provider.provider),
        providerRouting,
      });

      if (payload.jobs.length > 0) {
        await queueMutation(() =>
          persistSeedBatch({
            batchLabel: `${payload.label}:discovery_harvest`,
            seeds: payload.jobs,
            fetchedCount: 0,
            sourceCount: 0,
            startedAt: crawlRun.startedAt,
            finishedAt: new Date().toISOString(),
          }),
        );
      }

      if (providerRouting.every((entry) => entry.sourceCount === 0)) {
        console.info("[crawl:provider-routing]", {
          searchId: search._id,
          reason: "No sources matched any provider, skipping provider execution stage."
        });
        return;
      }

      scheduleProgressUpdate("crawling", new Date().toISOString(), { immediate: true });

      await Promise.all(
        selectedProviders.map((provider, index) =>
          runProviderStage(provider, providerSources[index]),
        ),
      );
      await mutationQueue;
    };

    await updateRunProgress("discovering", crawlRun.startedAt);

    Object.assign(diagnostics, createEmptyDiagnostics());

    if (input.discovery.discoverBaseline && input.discovery.discoverSupplemental) {
      const baselineDiscoveryStartedMs = Date.now();
      const baselineStage = await input.discovery.discoverBaseline({
        filters: normalizedFilters,
        now: input.now,
        fetchImpl: input.fetchImpl,
      });
      stageTimingsMs.discovery += Date.now() - baselineDiscoveryStartedMs;
      const baselineProcessingPromise = processDiscoveryStage({
        label: baselineStage.label,
        sources: baselineStage.sources,
        jobs: baselineStage.jobs ?? [],
        diagnostics: baselineStage.diagnostics,
      });

      const supplementalDiscoveryStartedMs = Date.now();
      const supplementalStage = await input.discovery.discoverSupplemental(
        {
          filters: normalizedFilters,
          now: input.now,
          fetchImpl: input.fetchImpl,
        },
        {
          baselineSources: baselineStage.sources,
        },
      );
      stageTimingsMs.discovery += Date.now() - supplementalDiscoveryStartedMs;
      const supplementalProcessingPromise = processDiscoveryStage({
        label: supplementalStage.label,
        sources: supplementalStage.sources,
        jobs: supplementalStage.jobs ?? [],
        diagnostics: supplementalStage.diagnostics,
      });
      await Promise.all([baselineProcessingPromise, supplementalProcessingPromise]);
    } else if (input.discovery.discoverInStages) {
      const stagedDiscoveryStartedMs = Date.now();
      const discoveryStages = await input.discovery.discoverInStages({
        filters: normalizedFilters,
        now: input.now,
        fetchImpl: input.fetchImpl,
      });
      stageTimingsMs.discovery += Date.now() - stagedDiscoveryStartedMs;
      for (const discoveryStage of discoveryStages) {
        await processDiscoveryStage({
          label: discoveryStage.label,
          sources: discoveryStage.sources,
          jobs: discoveryStage.jobs ?? [],
          diagnostics: discoveryStage.diagnostics,
        });
      }
    } else {
      const fullDiscoveryStartedMs = Date.now();
      const discoveryExecution = input.discovery.discoverWithDiagnostics
        ? await input.discovery.discoverWithDiagnostics({
            filters: normalizedFilters,
            now: input.now,
            fetchImpl: input.fetchImpl,
          })
        : {
            sources: await input.discovery.discover({
              filters: normalizedFilters,
              now: input.now,
              fetchImpl: input.fetchImpl,
            }),
            jobs: [],
            diagnostics: undefined,
          };
      stageTimingsMs.discovery += Date.now() - fullDiscoveryStartedMs;

      await processDiscoveryStage({
        label: "full",
        sources: discoveryExecution.sources,
        jobs: discoveryExecution.jobs ?? [],
        diagnostics: discoveryExecution.diagnostics,
      });
    }
    stageTimingsMs.providerExecution = providerDurationsMs.reduce(
      (total, entry) => total + entry.duration,
      0,
    );
    if (mutationErrors.length > 0) {
      throw mutationErrors[0];
    }
    if (discoveredSourceIds.size === 0) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Discovery returned zero runnable sources before provider routing began.",
      });
    } else if (enqueuedProviders.size === 0) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Sources were discovered, but no provider accepted them.",
      });
    }

    let savedJobs = await input.repository.getJobsByCrawlRun(crawlRun._id);
    if (validationStrategy.mode === "deferred") {
      diagnostics.validationDeferred = savedJobs.length;
    } else if (savedJobs.length > 0) {
      await flushProgressUpdate(true);
      await updateRunProgress("validating");
      const validationStartedMs = Date.now();
      const {
        jobs: jobsReadyToPersist,
        validations: inlineValidations,
        deferredCount,
      } = await applyInlineValidationStrategy(
        savedJobs,
        validationStrategy,
        input.repository,
        input.fetchImpl,
        input.now,
      );
      diagnostics.validationDeferred = deferredCount;
      const validatedJobs = await input.repository.persistJobs(crawlRun._id, jobsReadyToPersist);
      stageTimingsMs.validation = Date.now() - validationStartedMs;

      const validationJobIds = resolveValidationJobIds(validatedJobs, jobsReadyToPersist, inlineValidations);
      for (let index = 0; index < inlineValidations.length; index += 1) {
        const jobId = validationJobIds[index];
        if (!jobId) {
          continue;
        }

        await input.repository.saveLinkValidation(
          toStoredValidation(jobId, inlineValidations[index].validation),
        );
      }

      validatedJobs.forEach((job) => savedJobIds.add(job._id));
      savedJobs = await input.repository.getJobsByCrawlRun(crawlRun._id);
      await updateRunProgress("validating");
    }

    await flushProgressUpdate(true);
    await updateRunProgress("finalizing");

    const sourceResults = buildSortedSourceResults();
    const status = deriveRunStatus(sourceResults, savedJobIds.size);
    const finishedAt = new Date().toISOString();

    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status,
      stage: "finalizing",
      totalFetchedJobs,
      totalMatchedJobs,
      dedupedJobs: savedJobIds.size,
      diagnostics,
      validationMode: validationStrategy.mode,
      providerSummary: buildProviderSummary(sourceResults),
      finishedAt,
    });
    await input.repository.updateSearchLatestRun(search._id, crawlRun._id, status, finishedAt);

    const responseAssemblyStartedMs = Date.now();
    const finalizedRun = (await input.repository.getCrawlRun(crawlRun._id)) as CrawlRun;
    const response = crawlResponseSchema.parse({
      search: {
        ...search,
        latestCrawlRunId: crawlRun._id,
        lastStatus: status,
        updatedAt: finishedAt,
      },
      crawlRun: finalizedRun,
      sourceResults,
      jobs: sortJobsWithDiagnostics(
        savedJobs.map(applyResolvedExperienceLevel),
        normalizedFilters.title,
        input.now,
      ),
      diagnostics: finalizedRun.diagnostics,
    });
    stageTimingsMs.responseAssembly = Date.now() - responseAssemblyStartedMs;
    syncPerformanceDiagnostics({
      diagnostics,
      stageTimingsMs,
      providerDurationsMs,
      crawlStartedMs,
      firstVisibleResultAtMs,
      progressUpdateCount,
      persistenceBatchCount,
    });

    console.info("[crawl:summary]", {
      searchId: search._id,
      fetchedCount: totalFetchedJobs,
      directJobsHarvested: totalHarvestedDiscoveryJobs,
      matchedCount: totalMatchedJobs,
      jobsBeforeDedupe: diagnostics.jobsBeforeDedupe,
      jobsAfterDedupe: diagnostics.jobsAfterDedupe,
      savedCount: savedJobIds.size,
      providerDurationsMs,
      timeToFirstVisibleResultMs: diagnostics.performance?.timeToFirstVisibleResultMs,
    });
    console.info("[crawl:ranking-sample]", {
      searchId: search._id,
      titleQuery: normalizedFilters.title,
      topJobs: response.jobs.slice(0, 5).map((job) => {
        const ranking =
          job.rawSourceMetadata.crawlRanking &&
          typeof job.rawSourceMetadata.crawlRanking === "object"
            ? (job.rawSourceMetadata.crawlRanking as Record<string, unknown>)
            : undefined;

        return {
          title: job.title,
          company: job.company,
          postingDate: job.postingDate ?? job.postedAt,
          discoveredAt: job.discoveredAt,
          crawledAt: job.crawledAt,
          relevanceScore:
            typeof ranking?.relevanceScore === "number" ? ranking.relevanceScore : undefined,
          relevanceTier:
            typeof ranking?.relevanceTier === "string" ? ranking.relevanceTier : undefined,
          dateScore: typeof ranking?.dateScore === "number" ? ranking.dateScore : undefined,
          dateSource: typeof ranking?.dateSource === "string" ? ranking.dateSource : undefined,
          finalScore: typeof ranking?.finalScore === "number" ? ranking.finalScore : undefined,
        };
      }),
    });
    console.info("[crawl:timings]", {
      searchId: search._id,
      discoveryMs: stageTimingsMs.discovery,
      providerExecutionMs: stageTimingsMs.providerExecution,
      filteringMs: stageTimingsMs.filtering,
      dedupeMs: stageTimingsMs.dedupe,
      persistenceMs: stageTimingsMs.persistence,
      validationMs: stageTimingsMs.validation,
      responseAssemblyMs: stageTimingsMs.responseAssembly,
      totalMs: diagnostics.performance?.stageTimingsMs?.total ?? Date.now() - crawlStartedMs,
    });

    return response;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Crawl failed unexpectedly.";
    const finishedAt = new Date().toISOString();

    await input.repository.finalizeCrawlRun(crawlRun._id, {
      status: "failed",
      stage: "finalizing",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
      diagnostics: createEmptyDiagnostics(),
      validationMode: validationStrategy.mode,
      errorMessage: message,
      finishedAt,
    });
    await input.repository.updateSearchLatestRun(
      search._id,
      crawlRun._id,
      "failed",
      finishedAt,
    );
    throw error;
  }
}

export async function refreshStaleJobs(
  jobs: JobListing[],
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const ttl = getEnv().LINK_VALIDATION_TTL_MINUTES;

  return runWithConcurrency(
    dedupeStoredJobs(jobs),
    async (job) => {
      if (!job.lastValidatedAt) {
        // Deferred crawls intentionally leave fresh jobs unvalidated. Keep search loads
        // read-only for those records so validation happens only through a separate pass
        // such as manual revalidation or an explicit background job.
        return job;
      }

      if (!isValidationStale(job.lastValidatedAt, ttl, now)) {
        return job;
      }

      const validation = await validateJobLink(job.applyUrl, fetchImpl, now);
      const experienceClassification = resolveJobExperienceClassification(job);
      const refreshed: PersistableJob = {
        ...job,
        experienceLevel:
          experienceClassification.explicitLevel ??
          experienceClassification.inferredLevel,
        experienceClassification,
        resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
        canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
        linkStatus: validation.status,
        lastValidatedAt: validation.checkedAt,
      };

      const latestRunId = job.crawlRunIds[job.crawlRunIds.length - 1] ?? job.crawlRunIds[0];
      const [savedJob] = await repository.persistJobs(latestRunId, [refreshed]);
      await repository.saveLinkValidation(toStoredValidation(savedJob._id, validation));
      return savedJob;
    },
    3,
  );
}

export function applyResolvedExperienceLevel(job: JobListing) {
  const experienceClassification = resolveJobExperienceClassification(job);
  const experienceLevel =
    experienceClassification.explicitLevel ??
    experienceClassification.inferredLevel;
  return (
    experienceLevel === job.experienceLevel &&
    sameExperienceClassification(job.experienceClassification, experienceClassification)
  )
    ? job
    : {
        ...job,
        experienceLevel,
        experienceClassification,
      };
}

function filterSeedsForSearch(
  seeds: NormalizedJobSeed[],
  filters: SearchDocument["filters"],
  options: { deepExperienceInference: boolean },
) {
  // Keep the request path fast: synchronous crawls only use provider payload fields,
  // title signals, and metadata already attached to the seed.
  //
  // If we ever reintroduce deep page-level inference, it should run behind an
  // explicit opt-in and ideally outside the main crawl request path.
  void options.deepExperienceInference;

  const matchedSeeds: NormalizedJobSeed[] = [];
  let excludedByTitle = 0;
  let excludedByLocation = 0;
  let excludedByExperience = 0;
  const filterDecisionTraces: CrawlDiagnostics["filterDecisionTraces"] = [];
  const dropReasonCounts: Record<string, number> = {};

  for (const seed of seeds) {
    const evaluation = evaluateSearchFilters(seed, filters, {
      includeExperience: true,
    });
    const trace = buildFilterDecisionTrace(seed, evaluation, filters);
    filterDecisionTraces.push(trace);

    if (evaluation.matches && evaluation.locationMatch) {
      console.info("[crawl:location-filter]", {
        traceId: trace.traceId,
        queryLocation: evaluation.locationMatch.queryDiagnostics.original,
        rawLocation: evaluation.locationMatch.jobDiagnostics.raw,
        matched: evaluation.locationMatch.matches,
        explanation: evaluation.locationMatch.explanation,
        matchedTerms: evaluation.locationMatch.matchedTerms,
        queryDiagnostics: evaluation.locationMatch.queryDiagnostics,
        jobDiagnostics: evaluation.locationMatch.jobDiagnostics,
      });
    }

    if (evaluation.matches) {
      console.info("[crawl:title-filter]", {
        traceId: trace.traceId,
        queryTitle: filters.title,
        jobTitle: seed.title,
        matched: true,
        tier: evaluation.titleMatch.tier,
        score: evaluation.titleMatch.score,
        explanation: evaluation.titleMatch.explanation,
        queryDiagnostics: evaluation.titleMatch.queryDiagnostics,
        jobDiagnostics: evaluation.titleMatch.jobDiagnostics,
      });
      console.info("[crawl:filter-trace]", trace);
      matchedSeeds.push(
        withFilterMatchMetadata(
          seed,
          trace.traceId,
          evaluation.titleMatch,
          evaluation.locationMatch,
          evaluation.experienceMatch,
        ),
      );
      continue;
    }

    if (evaluation.reason === "title") {
      console.info("[crawl:title-filter]", {
        queryTitle: filters.title,
        jobTitle: seed.title,
        matched: false,
        reason: evaluation.reason,
        traceId: trace.traceId,
        tier: evaluation.titleMatch?.tier,
        score: evaluation.titleMatch?.score,
        explanation: evaluation.titleMatch?.explanation,
        queryDiagnostics: evaluation.titleMatch?.queryDiagnostics,
        jobDiagnostics: evaluation.titleMatch?.jobDiagnostics,
      });
      incrementCount(dropReasonCounts, trace.dropReason ?? "filter:title_mismatch");
      console.info("[crawl:filter-trace]", trace);
      excludedByTitle += 1;
      continue;
    }

    if (evaluation.reason === "location") {
      console.info("[crawl:location-filter]", {
        queryLocation: evaluation.locationMatch?.queryDiagnostics.original,
        rawLocation: evaluation.locationMatch?.jobDiagnostics.raw ?? seed.locationText,
        matched: false,
        reason: evaluation.reason,
        traceId: trace.traceId,
        explanation: evaluation.locationMatch?.explanation,
        matchedTerms: evaluation.locationMatch?.matchedTerms,
        queryDiagnostics: evaluation.locationMatch?.queryDiagnostics,
        jobDiagnostics: evaluation.locationMatch?.jobDiagnostics,
      });
      incrementCount(dropReasonCounts, trace.dropReason ?? "filter:location_mismatch");
      console.info("[crawl:filter-trace]", trace);
      excludedByLocation += 1;
      continue;
    }

    incrementCount(dropReasonCounts, trace.dropReason ?? "filter:experience_mismatch");
    console.info("[crawl:filter-trace]", trace);
    excludedByExperience += 1;
  }

  return {
    jobs: matchedSeeds,
    excludedByTitle,
    excludedByLocation,
    excludedByExperience,
    filterDecisionTraces,
    dropReasonCounts,
  };
}

function withFilterMatchMetadata(
  seed: NormalizedJobSeed,
  traceId: string,
  titleMatch: TitleMatchResult,
  locationMatch?: LocationMatchResult,
  experienceMatch?: ExperienceFilterResult,
) {
  return {
    ...seed,
    rawSourceMetadata: {
      ...seed.rawSourceMetadata,
      crawlTraceId: traceId,
      crawlTitleMatch: {
        originalQueryTitle: titleMatch.queryDiagnostics.original,
        normalizedQueryTitle: titleMatch.queryDiagnostics.normalized,
        queryAliasesUsed: titleMatch.queryDiagnostics.aliasesUsed,
        originalJobTitle: titleMatch.jobDiagnostics.original,
        normalizedJobTitle: titleMatch.jobDiagnostics.normalized,
        jobAliasesUsed: titleMatch.jobDiagnostics.aliasesUsed,
        tier: titleMatch.tier,
        score: titleMatch.score,
        canonicalQueryTitle: titleMatch.canonicalQueryTitle,
        canonicalJobTitle: titleMatch.canonicalJobTitle,
        explanation: titleMatch.explanation,
        matchedTerms: titleMatch.matchedTerms,
        penalties: titleMatch.penalties,
      },
      ...(locationMatch
        ? {
            crawlLocationMatch: {
              originalQueryLocation: locationMatch.queryDiagnostics.original,
              normalizedQueryLocation: locationMatch.queryDiagnostics.normalized,
              queryExpandedTermsUsed: locationMatch.queryDiagnostics.expandedTerms,
              queryScopesApplied: locationMatch.queryDiagnostics.scopesApplied,
              queryWorkplaceMode: locationMatch.queryDiagnostics.workplaceMode,
              rawJobLocation: locationMatch.jobDiagnostics.raw,
              normalizedJobLocation: locationMatch.jobDiagnostics.normalized,
              jobLocationAliasesUsed: locationMatch.jobDiagnostics.aliasesUsed,
              jobWorkplaceMode: locationMatch.jobDiagnostics.workplaceMode,
              matchedTerms: locationMatch.matchedTerms,
              explanation: locationMatch.explanation,
              country: locationMatch.jobDiagnostics.country,
              state: locationMatch.jobDiagnostics.state,
              stateCode: locationMatch.jobDiagnostics.stateCode,
              city: locationMatch.jobDiagnostics.city,
              isRemote: locationMatch.jobDiagnostics.isRemote,
              isUnitedStates: locationMatch.jobDiagnostics.isUnitedStates,
            },
          }
        : {}),
      ...(experienceMatch
        ? {
            crawlExperienceMatch: {
              selectedLevels: experienceMatch.selectedLevels,
              mode: experienceMatch.mode,
              includeUnspecified: experienceMatch.includeUnspecified,
              matchedLevel: experienceMatch.matchedLevel,
              explanation: experienceMatch.explanation,
              confidence: experienceMatch.classification.confidence,
              source: experienceMatch.classification.source,
              reasons: experienceMatch.classification.reasons,
              explicitLevel: experienceMatch.classification.explicitLevel,
              inferredLevel: experienceMatch.classification.inferredLevel,
            },
          }
        : {}),
      ...(seed.resolvedLocation
        ? {
            crawlResolvedLocation: {
              country: seed.resolvedLocation.country,
              state: seed.resolvedLocation.state,
              stateCode: seed.resolvedLocation.stateCode,
              city: seed.resolvedLocation.city,
              isRemote: seed.resolvedLocation.isRemote,
              isUnitedStates: seed.resolvedLocation.isUnitedStates,
              confidence: seed.resolvedLocation.confidence,
            },
          }
        : {}),
    },
  };
}

function buildFilterDecisionTrace(
  seed: NormalizedJobSeed,
  evaluation: ReturnType<typeof evaluateSearchFilters>,
  filters: SearchDocument["filters"],
): CrawlDiagnostics["filterDecisionTraces"][number] {
  const traceId = getPipelineTraceId(seed);
  const locationDiagnostics = evaluation.locationMatch?.jobDiagnostics ?? {
    raw: seed.locationText,
    normalized: normalizeComparableText(
      `${seed.city ?? ""} ${seed.state ?? ""} ${seed.country ?? ""} ${seed.locationText}`,
    ),
    country: seed.resolvedLocation?.country ?? seed.country,
    state: seed.resolvedLocation?.state ?? seed.state,
    stateCode: seed.resolvedLocation?.stateCode,
    city: seed.resolvedLocation?.city ?? seed.city,
    workplaceMode: "unknown" as const,
    isRemote: seed.resolvedLocation?.isRemote ?? false,
    isUnitedStates: seed.resolvedLocation?.isUnitedStates ?? false,
    aliasesUsed: [],
  };
  const experienceClassification =
    evaluation.experienceMatch?.classification ?? resolveJobExperienceClassification(seed);
  const experienceLevel =
    experienceClassification.explicitLevel ?? experienceClassification.inferredLevel;
  const experienceMode = filters.experienceMatchMode ?? "balanced";
  const includeUnspecified =
    filters.includeUnspecifiedExperience === true || experienceMode === "broad";

  if (evaluation.matches) {
    return {
      traceId,
      sourcePlatform: seed.sourcePlatform,
      sourceJobId: seed.sourceJobId,
      sourceUrl: seed.sourceUrl,
      applyUrl: seed.applyUrl,
      canonicalUrl: seed.canonicalUrl,
      company: seed.company,
      title: seed.title,
      locationText: seed.locationText,
      filterStage: filters.experienceLevels?.length
        ? "experience"
        : evaluation.locationMatch
          ? "location"
          : "title",
      outcome: "passed",
      dropReason: undefined,
      titleDiagnostics: {
        original: evaluation.titleMatch.jobDiagnostics.original,
        normalized: evaluation.titleMatch.jobDiagnostics.normalized,
        canonical: evaluation.titleMatch.canonicalJobTitle,
        family: evaluation.titleMatch.jobFamily,
        tier: evaluation.titleMatch.tier,
        score: evaluation.titleMatch.score,
        threshold: evaluation.titleMatch.threshold,
        explanation: evaluation.titleMatch.explanation,
        matchedTerms: evaluation.titleMatch.matchedTerms,
        penalties: evaluation.titleMatch.penalties,
        passed: true,
      },
      locationDiagnostics: {
        raw: locationDiagnostics.raw,
        normalized: locationDiagnostics.normalized,
        country: locationDiagnostics.country,
        state: locationDiagnostics.state,
        stateCode: locationDiagnostics.stateCode,
        city: locationDiagnostics.city,
        isRemote: locationDiagnostics.isRemote,
        isUnitedStates: locationDiagnostics.isUnitedStates,
        explanation: evaluation.locationMatch?.explanation,
        matchedTerms: evaluation.locationMatch?.matchedTerms ?? [],
        passed: true,
      },
      experienceDiagnostics: {
        level: evaluation.experienceMatch?.matchedLevel ?? experienceLevel,
        finalSeniority:
          experienceClassification.diagnostics?.finalSeniority ??
          (experienceLevel ?? "unknown"),
        normalizedTitle: experienceClassification.diagnostics?.normalizedTitle ?? "",
        source: experienceClassification.source,
        confidence: experienceClassification.confidence,
        selectedLevels: evaluation.experienceMatch?.selectedLevels ?? (filters.experienceLevels ?? []),
        mode: evaluation.experienceMatch?.mode ?? experienceMode,
        includeUnspecified:
          evaluation.experienceMatch?.includeUnspecified ?? includeUnspecified,
        explanation: evaluation.experienceMatch?.explanation ?? "Experience filtering passed.",
        passed: true,
        reasons: experienceClassification.reasons,
        matchedSignals: experienceClassification.diagnostics?.matchedSignals ?? [],
      },
    };
  }

  const filterStage = evaluation.reason;
  const dropReason =
    filterStage === "title"
      ? buildTitleDropReason(evaluation.titleMatch)
      : filterStage === "location"
        ? buildLocationDropReason(evaluation.locationMatch)
        : buildExperienceDropReason(evaluation.experienceMatch);

  return {
    traceId,
    sourcePlatform: seed.sourcePlatform,
    sourceJobId: seed.sourceJobId,
    sourceUrl: seed.sourceUrl,
    applyUrl: seed.applyUrl,
    canonicalUrl: seed.canonicalUrl,
    company: seed.company,
    title: seed.title,
    locationText: seed.locationText,
    filterStage,
    outcome: "dropped",
    dropReason,
    titleDiagnostics: {
      original: evaluation.titleMatch?.jobDiagnostics.original ?? seed.title,
      normalized:
        evaluation.titleMatch?.jobDiagnostics.normalized ??
        normalizeComparableText(seed.title),
      canonical: evaluation.titleMatch?.canonicalJobTitle,
      family: evaluation.titleMatch?.jobFamily,
      tier: evaluation.titleMatch?.tier,
      score: evaluation.titleMatch?.score,
      threshold: evaluation.titleMatch?.threshold,
      explanation: evaluation.titleMatch?.explanation,
      matchedTerms: evaluation.titleMatch?.matchedTerms ?? [],
      penalties: evaluation.titleMatch?.penalties ?? [],
      passed: filterStage !== "title",
    },
    locationDiagnostics: {
      raw: locationDiagnostics.raw,
      normalized: locationDiagnostics.normalized,
      country: locationDiagnostics.country,
      state: locationDiagnostics.state,
      stateCode: locationDiagnostics.stateCode,
      city: locationDiagnostics.city,
      isRemote: locationDiagnostics.isRemote,
      isUnitedStates: locationDiagnostics.isUnitedStates,
      explanation: evaluation.locationMatch?.explanation,
      matchedTerms: evaluation.locationMatch?.matchedTerms ?? [],
      passed: filterStage !== "location",
    },
    experienceDiagnostics: {
      level: experienceLevel,
      finalSeniority:
        experienceClassification.diagnostics?.finalSeniority ??
        (experienceLevel ?? "unknown"),
      normalizedTitle: experienceClassification.diagnostics?.normalizedTitle ?? "",
      source: experienceClassification.source,
      confidence: experienceClassification.confidence,
      selectedLevels: evaluation.experienceMatch?.selectedLevels ?? (filters.experienceLevels ?? []),
      mode: evaluation.experienceMatch?.mode ?? experienceMode,
      includeUnspecified:
        evaluation.experienceMatch?.includeUnspecified ?? includeUnspecified,
      explanation:
        evaluation.experienceMatch?.explanation ??
        "Experience filtering did not run because an earlier stage dropped the job.",
      passed: filterStage !== "experience",
      reasons: experienceClassification.reasons,
      matchedSignals: experienceClassification.diagnostics?.matchedSignals ?? [],
    },
  };
}

function buildTitleDropReason(titleMatch?: TitleMatchResult) {
  if (!titleMatch) {
    return "filter:title_missing_match";
  }

  if (titleMatch.penalties.some((penalty) => penalty.includes("conflicting role families"))) {
    return "filter:title_family_conflict";
  }

  return titleMatch.tier === "none"
    ? "filter:title_below_threshold"
    : `filter:title_${titleMatch.tier}`;
}

function buildLocationDropReason(locationMatch?: LocationMatchResult) {
  if (!locationMatch) {
    return "filter:location_missing_match";
  }

  if (locationMatch.explanation.includes("United States")) {
    return "filter:location_not_in_requested_country";
  }

  if (locationMatch.explanation.includes("resolved state")) {
    return "filter:location_state_mismatch";
  }

  if (locationMatch.explanation.includes("resolved city")) {
    return "filter:location_city_mismatch";
  }

  if (locationMatch.explanation.includes("requires a")) {
    return "filter:location_workplace_mismatch";
  }

  return "filter:location_mismatch";
}

function buildExperienceDropReason(experienceMatch?: ExperienceFilterResult) {
  if (!experienceMatch) {
    return "filter:experience_mismatch";
  }

  if (experienceMatch.classification.isUnspecified) {
    return "filter:experience_unspecified";
  }

  return "filter:experience_level_mismatch";
}

function getPipelineTraceId(
  seedOrJob: Pick<
    NormalizedJobSeed,
    "sourcePlatform" | "sourceJobId" | "canonicalUrl" | "applyUrl" | "sourceUrl"
  >,
) {
  return [
    seedOrJob.sourcePlatform,
    seedOrJob.sourceJobId,
    seedOrJob.canonicalUrl ?? seedOrJob.applyUrl ?? seedOrJob.sourceUrl,
  ].join(":");
}

function incrementCount(counts: Record<string, number>, key: string) {
  counts[key] = (counts[key] ?? 0) + 1;
}

function incrementDropReasonCount(diagnostics: CrawlDiagnostics, reason: string) {
  diagnostics.dropReasonCounts[reason] = (diagnostics.dropReasonCounts[reason] ?? 0) + 1;
}

function buildDedupeTraceRecords(
  dropped: Array<{
    traceId: string;
    keptTraceId: string;
    matchedKeys: string[];
    dropReason: string;
  }>,
  hydratedJobs: PersistableJob[],
): CrawlDiagnostics["dedupeDecisionTraces"] {
  const jobsByTraceId = new Map(
    hydratedJobs.map((job) => [getPipelineTraceId(job), job] as const),
  );
  const droppedByTraceId = new Map(dropped.map((trace) => [trace.traceId, trace] as const));
  const survivorTraceIds = new Set(dropped.map((trace) => trace.keptTraceId));

  return hydratedJobs.map((job) => {
    const traceId = getPipelineTraceId(job);
    const droppedTrace = droppedByTraceId.get(traceId);
    const identity = buildCanonicalJobIdentity(job);

    if (droppedTrace) {
      return {
        traceId,
        keptTraceId: droppedTrace.keptTraceId,
        originalIdentifiers: {
          databaseId: identity.databaseId,
          ...identity.originalIdentifiers,
        },
        normalizedIdentity: identity.normalizedIdentity,
        sourcePlatform: job.sourcePlatform,
        sourceJobId: job.sourceJobId,
        sourceUrl: job.sourceUrl,
        canonicalUrl: job.canonicalUrl,
        applyUrl: job.applyUrl,
        title: job.title,
        company: job.company,
        locationText: job.locationText,
        outcome: "deduped" as const,
        dropReason: droppedTrace.dropReason,
        decisionReason: `merged into ${droppedTrace.keptTraceId} via ${droppedTrace.dropReason}`,
        matchedKeys: droppedTrace.matchedKeys,
      };
    }

    if (!jobsByTraceId.has(traceId)) {
      throw new Error(`Missing hydrated job for dedupe trace ${traceId}.`);
    }

    return {
      traceId,
      keptTraceId: traceId,
      originalIdentifiers: {
        databaseId: identity.databaseId,
        ...identity.originalIdentifiers,
      },
      normalizedIdentity: identity.normalizedIdentity,
      sourcePlatform: job.sourcePlatform,
      sourceJobId: job.sourceJobId,
      sourceUrl: job.sourceUrl,
      canonicalUrl: job.canonicalUrl,
      applyUrl: job.applyUrl,
      title: job.title,
      company: job.company,
      locationText: job.locationText,
      outcome: "kept" as const,
      dropReason: undefined,
      decisionReason: survivorTraceIds.has(traceId)
        ? "preserved as the strongest candidate in a duplicate group"
        : "preserved as a unique canonical identity",
      matchedKeys: [],
    };
  });
}

function hydrateJobs(
  seeds: NormalizedJobSeed[],
  now: Date,
) {
  return seeds.map((seed) => seedToPersistableJob(seed, now));
}

async function applyInlineValidationStrategy(
  jobs: PersistableJob[],
  strategy: ResolvedValidationStrategy,
  repository: JobCrawlerRepository,
  fetchImpl: typeof fetch,
  now: Date,
) {
  const jobsToPersist = [...jobs];
  const indexesToValidate = pickInlineValidationIndexes(jobsToPersist, strategy);

  if (indexesToValidate.length === 0) {
    return {
      jobs: jobsToPersist,
      validations: [] as InlineValidationResult[],
      deferredCount: jobsToPersist.length,
    };
  }

  const checkedAfter = new Date(
    now.getTime() - getEnv().LINK_VALIDATION_TTL_MINUTES * 60_000,
  ).toISOString();

  const validations = await runWithConcurrency(
    indexesToValidate,
    async (jobIndex) => {
      const job = jobsToPersist[jobIndex];
      const cachedValidation = await repository.getFreshValidation(job.applyUrl, checkedAfter);
      const validation = cachedValidation
        ? buildCachedValidationDraft(job.applyUrl, job.canonicalUrl, cachedValidation, now)
        : await validateJobLink(job.applyUrl, fetchImpl, now);

      jobsToPersist[jobIndex] = applyValidationDraft(job, validation);

      return {
        jobIndex,
        validation,
      };
    },
    5,
  );

  return {
    jobs: jobsToPersist,
    validations,
    deferredCount: Math.max(0, jobsToPersist.length - indexesToValidate.length),
  };
}

function seedToPersistableJob(seed: NormalizedJobSeed, now: Date): PersistableJob {
  const resolvedLocation = seed.resolvedLocation;
  const locationRaw = seed.locationRaw ?? seed.locationText;
  const locationNormalized =
    seed.normalizedLocation ??
    normalizeComparableText(
      `${resolvedLocation?.city ?? seed.city ?? ""} ${resolvedLocation?.state ?? seed.state ?? ""} ${resolvedLocation?.country ?? seed.country ?? ""} ${locationRaw}`,
    );
  const experienceClassification = resolveJobExperienceClassification(seed);
  const discoveredAt = seed.discoveredAt || now.toISOString();
  const postingDate = seed.postingDate ?? seed.postedAt;
  const normalizedCompany = seed.normalizedCompany ?? normalizeComparableText(seed.company);
  const normalizedTitle = seed.normalizedTitle ?? normalizeComparableText(seed.title);
  const seniority =
    seed.seniority ??
    experienceClassification.explicitLevel ??
    experienceClassification.inferredLevel;
  const dedupeFingerprint =
    seed.dedupeFingerprint ??
    buildContentFingerprint({
      company: seed.company,
      title: seed.title,
      location: locationNormalized,
    });

  return {
    title: seed.title,
    company: seed.company,
    normalizedCompany,
    normalizedTitle,
    country: resolvedLocation?.country ?? seed.country,
    state: resolvedLocation?.state ?? seed.state,
    city: resolvedLocation?.city ?? seed.city,
    locationRaw,
    normalizedLocation: locationNormalized,
    locationText: seed.locationText,
    resolvedLocation,
    remoteType: seed.remoteType ?? (resolvedLocation?.isRemote ? "remote" : "unknown"),
    employmentType: seed.employmentType,
    seniority,
    experienceLevel:
      experienceClassification.explicitLevel ??
      experienceClassification.inferredLevel,
    experienceClassification,
    sourcePlatform: seed.sourcePlatform,
    sourceCompanySlug: seed.sourceCompanySlug,
    sourceJobId: seed.sourceJobId,
    sourceUrl: seed.sourceUrl,
    applyUrl: seed.applyUrl,
    canonicalUrl: seed.canonicalUrl,
    postingDate,
    postedAt: postingDate,
    discoveredAt,
    crawledAt: seed.crawledAt ?? discoveredAt,
    descriptionSnippet: seed.descriptionSnippet,
    salaryInfo: seed.salaryInfo,
    sponsorshipHint: seed.sponsorshipHint ?? "unknown",
    linkStatus: "unknown",
    rawSourceMetadata: seed.rawSourceMetadata,
    sourceProvenance: [
      {
        sourcePlatform: seed.sourcePlatform,
        sourceJobId: seed.sourceJobId,
        sourceUrl: seed.sourceUrl,
        applyUrl: seed.applyUrl,
        canonicalUrl: seed.canonicalUrl,
        discoveredAt,
        rawSourceMetadata: seed.rawSourceMetadata,
      },
    ],
    sourceLookupKeys: buildSeedSourceLookupKeys(seed),
    dedupeFingerprint,
    companyNormalized: normalizedCompany,
    titleNormalized: normalizedTitle,
    locationNormalized,
    contentFingerprint: dedupeFingerprint,
  };
}

function buildSeedSourceLookupKeys(seed: NormalizedJobSeed) {
  if (seed.sourcePlatform !== "greenhouse") {
    return [buildSourceLookupKey(seed.sourcePlatform, seed.sourceJobId)];
  }

  const boardToken = resolveGreenhouseBoardToken(seed);
  if (!boardToken) {
    return [buildSourceLookupKey(seed.sourcePlatform, seed.sourceJobId)];
  }

  return [
    `greenhouse:${normalizeComparableText(boardToken)}:${normalizeComparableText(seed.sourceJobId)}`,
  ];
}

function resolveGreenhouseBoardToken(seed: NormalizedJobSeed) {
  const metadataBoardToken =
    typeof seed.rawSourceMetadata?.greenhouseBoardToken === "string"
      ? seed.rawSourceMetadata.greenhouseBoardToken
      : typeof seed.rawSourceMetadata?.boardToken === "string"
        ? seed.rawSourceMetadata.boardToken
        : undefined;
  if (typeof metadataBoardToken === "string" && metadataBoardToken.trim()) {
    return metadataBoardToken.trim().toLowerCase();
  }

  return (
    parseGreenhouseUrl(seed.canonicalUrl ?? "")?.boardSlug ??
    parseGreenhouseUrl(seed.sourceUrl)?.boardSlug ??
    parseGreenhouseUrl(seed.applyUrl)?.boardSlug
  );
}

function applyValidationDraft(job: PersistableJob, validation: LinkValidationDraft): PersistableJob {
  return {
    ...job,
    resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
    canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
    linkStatus: validation.status,
    lastValidatedAt: validation.checkedAt,
    sourceProvenance: job.sourceProvenance.map((record) =>
      record.applyUrl === job.applyUrl
        ? {
            ...record,
            resolvedUrl: validation.resolvedUrl ?? record.resolvedUrl,
            canonicalUrl: validation.canonicalUrl ?? record.canonicalUrl,
          }
        : record,
    ),
  };
}

function buildCachedValidationDraft(
  applyUrl: string,
  canonicalUrl: string | undefined,
  cachedValidation: Awaited<ReturnType<JobCrawlerRepository["getFreshValidation"]>>,
  now: Date,
): LinkValidationDraft {
  if (!cachedValidation) {
    throw new Error("Cached validation is required to build a cached validation draft.");
  }

  return {
    applyUrl,
    resolvedUrl: cachedValidation.resolvedUrl,
    canonicalUrl: cachedValidation.canonicalUrl ?? canonicalUrl,
    status: cachedValidation.status,
    method: "CACHE",
    httpStatus: cachedValidation.httpStatus,
    checkedAt: now.toISOString(),
    errorMessage: cachedValidation.errorMessage,
    staleMarkers: cachedValidation.staleMarkers,
  };
}

function pickInlineValidationIndexes(
  jobs: PersistableJob[],
  strategy: ResolvedValidationStrategy,
) {
  if (strategy.mode === "deferred") {
    return [];
  }

  if (strategy.mode === "full_inline") {
    return jobs.map((_, index) => index);
  }

  if (strategy.inlineTopN <= 0) {
    return [];
  }

  return jobs
    .map((job, index) => ({
      index,
      recency: job.postedAt ?? job.discoveredAt,
    }))
    .sort(
      (left, right) =>
        compareRecency(left.recency, right.recency) ||
        left.index - right.index,
    )
    .slice(0, strategy.inlineTopN)
    .map((entry) => entry.index);
}

function compareRecency(left: string, right: string) {
  if (left === right) {
    return 0;
  }

  return left > right ? -1 : 1;
}

function resolveValidationStrategy(
  mode: CrawlLinkValidationMode | undefined,
  inlineTopN: number | undefined,
  crawlMode: CrawlMode | undefined,
): ResolvedValidationStrategy {
  if (mode) {
    return {
      mode,
      inlineTopN: Number.isFinite(inlineTopN)
        ? Math.max(0, Math.floor(inlineTopN ?? 0))
        : defaultInlineValidationTopN,
    };
  }

  if (crawlMode === "deep") {
    return {
      mode: "full_inline",
      inlineTopN: 0,
    };
  }

  if (crawlMode === "balanced") {
    return {
      mode: "inline_top_n",
      inlineTopN: defaultBalancedInlineValidationTopN,
    };
  }

  return {
    mode: defaultCrawlLinkValidationMode,
    inlineTopN: defaultInlineValidationTopN,
  };
}

function sameExperienceClassification(
  left: JobListing["experienceClassification"],
  right: JobListing["experienceClassification"],
) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function resolveValidationJobIds(
  savedJobs: JobListing[],
  jobsReadyToPersist: PersistableJob[],
  inlineValidations: InlineValidationResult[],
) {
  const byCanonicalUrl = new Map<string, string>();
  const byResolvedUrl = new Map<string, string>();
  const byApplyUrl = new Map<string, string>();
  const bySourceLookupKey = new Map<string, string>();
  const byContentFingerprint = new Map<string, string>();

  savedJobs.forEach((job) => {
    const identity = buildCanonicalJobIdentity(job);

    if (job.canonicalUrl) {
      byCanonicalUrl.set(job.canonicalUrl, job._id);
    }

    if (job.resolvedUrl) {
      byResolvedUrl.set(job.resolvedUrl, job._id);
    }

    byApplyUrl.set(job.applyUrl, job._id);
    identity.normalizedIdentity.platformJobKeys.forEach((lookupKey) => {
      bySourceLookupKey.set(lookupKey, job._id);
    });
    if (!identity.hasStrongIdentity) {
      byContentFingerprint.set(identity.normalizedIdentity.fallbackFingerprint, job._id);
    }
  });

  return inlineValidations.map(({ jobIndex }) => {
    const job = jobsReadyToPersist[jobIndex];
    if (!job) {
      return undefined;
    }

    const identity = buildCanonicalJobIdentity(job);

    return (
      (job.canonicalUrl ? byCanonicalUrl.get(job.canonicalUrl) : undefined) ??
      (job.resolvedUrl ? byResolvedUrl.get(job.resolvedUrl) : undefined) ??
      byApplyUrl.get(job.applyUrl) ??
      identity.normalizedIdentity.platformJobKeys
        .map((lookupKey) => bySourceLookupKey.get(lookupKey))
        .find(Boolean) ??
      (!identity.hasStrongIdentity
        ? byContentFingerprint.get(identity.normalizedIdentity.fallbackFingerprint)
        : undefined)
    );
  });
}

function deriveRunStatus(
  sourceResults: CrawlSourceResult[],
  savedJobCount: number,
): CrawlRunStatus {
  const hasFailures = sourceResults.some((result) => result.status === "failed");
  const hasPartials = sourceResults.some((result) => result.status === "partial");
  const hasSupported = sourceResults.some(
    (result) =>
      result.status === "success" ||
      result.status === "partial" ||
      result.status === "running",
  );

  if (hasFailures && !hasSupported && savedJobCount === 0) {
    return "failed";
  }

  if (hasFailures || hasPartials) {
    return "partial";
  }

  return "completed";
}

type ResolvedValidationStrategy = {
  mode: CrawlLinkValidationMode;
  inlineTopN: number;
};

type InlineValidationResult = {
  jobIndex: number;
  validation: LinkValidationDraft;
};

function createEmptyDiagnostics(
  overrides: Partial<CrawlDiagnostics> = {},
): CrawlDiagnostics {
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
    performance: {
      timeToFirstVisibleResultMs: undefined,
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
    ...overrides,
  };
}

function syncPerformanceDiagnostics(input: {
  diagnostics: CrawlDiagnostics;
  stageTimingsMs: {
    discovery: number;
    providerExecution: number;
    filtering: number;
    dedupe: number;
    persistence: number;
    validation: number;
    responseAssembly: number;
  };
  providerDurationsMs: Array<{
    provider: CrawlSourceResult["provider"];
    duration: number;
    sourceCount: number;
    timedOut: boolean;
  }>;
  crawlStartedMs: number;
  firstVisibleResultAtMs?: number;
  progressUpdateCount: number;
  persistenceBatchCount: number;
}) {
  input.diagnostics.performance = {
    timeToFirstVisibleResultMs:
      typeof input.firstVisibleResultAtMs === "number"
        ? input.firstVisibleResultAtMs - input.crawlStartedMs
        : undefined,
    stageTimingsMs: {
      ...input.stageTimingsMs,
      total: Date.now() - input.crawlStartedMs,
    },
    providerTimingsMs: input.providerDurationsMs.map((entry) => ({
      provider: entry.provider,
      duration: entry.duration,
      sourceCount: entry.sourceCount,
      timedOut: entry.timedOut,
    })),
    progressUpdateCount: input.progressUpdateCount,
    persistenceBatchCount: input.persistenceBatchCount,
  };
}

async function runProviderWithTimeout<P>(
  task: Promise<P>,
  provider: CrawlSourceResult["provider"],
  timeoutMs: number,
) {
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(
        new Error(
          `Provider ${provider} exceeded the ${timeoutMs}ms crawl budget and was failed fast.`,
        ),
      );
    }, timeoutMs);
  });

  try {
    return await Promise.race([task, timeoutPromise]);
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
}

function isTimeoutError(error: unknown) {
  return error instanceof Error && error.message.includes("crawl budget");
}

function buildProviderSummary(
  sourceResults: CrawlSourceResult[],
): CrawlProviderSummary[] {
  return sourceResults.map((sourceResult) => ({
    provider: sourceResult.provider,
    status: sourceResult.status,
    sourceCount: sourceResult.sourceCount,
    fetchedCount: sourceResult.fetchedCount,
    matchedCount: sourceResult.matchedCount,
    savedCount: sourceResult.savedCount,
    warningCount: sourceResult.warningCount,
    errorMessage: sourceResult.errorMessage,
  }));
}

function selectProvidersForSearch(
  providers: CrawlProvider[],
  selectedPlatforms: CrawlerPlatform[] | undefined,
) {
  const allowedPlatforms = new Set(
    resolveOperationalCrawlerPlatforms(selectedPlatforms),
  );

  // Provider execution is limited to the implemented families that survived the
  // requested platform scope, so disabled platforms never reach crawlSources.
  return providers.filter(
    (provider) =>
      isOperationalProvider(provider.provider) && allowedPlatforms.has(provider.provider),
  );
}

function isOperationalProvider(
  provider: CrawlProvider["provider"],
): provider is ActiveCrawlerPlatform {
  return activeCrawlerPlatforms.includes(provider as ActiveCrawlerPlatform);
}
