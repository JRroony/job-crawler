import "server-only";

import { dedupeJobs, dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import {
  buildContentFingerprint,
  buildSourceLookupKey,
  createId,
  evaluateSearchFilters,
  isValidationStale,
  normalizeComparableText,
  resolveJobExperienceClassification,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import { parseGreenhouseUrl } from "@/lib/server/discovery/greenhouse-url";
import {
  toStoredValidation,
  validateJobLink,
  type LinkValidationDraft,
} from "@/lib/server/crawler/link-validation";
import { sortJobs } from "@/lib/server/crawler/sort";
import { getEnv } from "@/lib/server/env";
import type { JobCrawlerRepository, PersistableJob } from "@/lib/server/db/repository";
import type { DiscoveryService } from "@/lib/server/discovery/types";
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
    }> = [];
    let totalFetchedJobs = 0;
    let totalMatchedJobs = 0;
    let firstVisibleResultAtMs: number | undefined;
    let mutationQueue = Promise.resolve();

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
      const filteredSeeds = filterSeedsForSearch(payload.seeds, normalizedFilters, {
        deepExperienceInference: input.deepExperienceInference ?? false,
      });
      stageTimingsMs.filtering += Date.now() - filteringStartedMs;

      diagnostics.excludedByTitle += filteredSeeds.excludedByTitle;
      diagnostics.excludedByLocation += filteredSeeds.excludedByLocation;
      diagnostics.excludedByExperience += filteredSeeds.excludedByExperience;
      totalMatchedJobs += filteredSeeds.jobs.length;

      const dedupeStartedMs = Date.now();
      const hydratedJobs = hydrateJobs(filteredSeeds.jobs, input.now);
      diagnostics.jobsBeforeDedupe += hydratedJobs.length;
      const dedupedJobs = sortJobs(dedupeJobs(hydratedJobs), normalizedFilters.title);
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

      await updateRunProgress("crawling", payload.finishedAt);
    };

    await updateRunProgress("discovering", crawlRun.startedAt);

    const discoveryStartedMs = Date.now();
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
    stageTimingsMs.discovery = Date.now() - discoveryStartedMs;

    const discoveredSources = discoveryExecution.sources;
    const harvestedDiscoveryJobs = discoveryExecution.jobs ?? [];
    const providerSources = selectedProviders.map((provider) =>
      discoveredSources.filter((source) => provider.supportsSource(source)),
    );
    const providerRouting = selectedProviders.map((provider, index) => ({
      provider: provider.provider,
      sourceCount: providerSources[index].length,
    }));

    console.info("[crawl:provider-routing]", {
      searchId: search._id,
      normalizedFilters,
      discoveredSourceCount: discoveredSources.length,
      harvestedDiscoveryJobCount: harvestedDiscoveryJobs.length,
      discoveredPlatformCounts: discoveredSources.reduce<Record<string, number>>((counts, source) => {
        counts[source.platform] = (counts[source.platform] ?? 0) + 1;
        return counts;
      }, {}),
      selectedProviders: selectedProviders.map((provider) => provider.provider),
      providerRouting,
    });

    if (discoveredSources.length === 0) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Discovery returned zero runnable sources before provider routing began.",
      });
    } else if (providerRouting.every((entry) => entry.sourceCount === 0)) {
      console.warn("[crawl:provider-routing]", {
        searchId: search._id,
        reason: "Sources were discovered, but no provider accepted them.",
      });
    }

    Object.assign(
      diagnostics,
      createEmptyDiagnostics({
        discoveredSources: discoveredSources.length,
        crawledSources: providerSources.reduce((total, sources) => total + sources.length, 0),
        providersEnqueued: providerRouting.filter((entry) => entry.sourceCount > 0).length,
        directJobsHarvested: harvestedDiscoveryJobs.length,
        discovery: discoveryExecution.diagnostics,
      }),
    );

    const initializedSourceResults = selectedProviders.map((provider, index) =>
      crawlSourceResultSchema.parse({
        _id: createId(),
        crawlRunId: crawlRun._id,
        searchId: search._id,
        provider: provider.provider,
        status: "running",
        sourceCount: providerSources[index].length,
        fetchedCount: 0,
        matchedCount: 0,
        savedCount: 0,
        warningCount: 0,
        startedAt: new Date().toISOString(),
        finishedAt: new Date().toISOString(),
      }),
    );

    if (initializedSourceResults.length > 0) {
      initializedSourceResults.forEach((sourceResult) => {
        sourceResultsByProvider.set(sourceResult.provider, sourceResult);
      });
      await input.repository.saveCrawlSourceResults(initializedSourceResults);
    }

    await updateRunProgress("crawling");

    if (harvestedDiscoveryJobs.length > 0) {
      await queueMutation(() =>
        persistSeedBatch({
          batchLabel: "discovery_harvest",
          seeds: harvestedDiscoveryJobs,
          fetchedCount: 0,
          sourceCount: 0,
          startedAt: crawlRun.startedAt,
          finishedAt: new Date().toISOString(),
        }),
      );
    }

    const providerExecutionStartedMs = Date.now();
    await Promise.all(
      selectedProviders.map(async (provider, index) => {
        const startedAt = new Date().toISOString();
        const startedMs = Date.now();
        const sourcesForProvider = providerSources[index];
        let emittedLiveBatch = false;

        const runningSourceResult = sourceResultsByProvider.get(provider.provider);
        if (runningSourceResult) {
          const updatedRunningResult = {
            ...runningSourceResult,
            startedAt,
            finishedAt: startedAt,
            sourceCount: sourcesForProvider.length,
          };
          sourceResultsByProvider.set(provider.provider, updatedRunningResult);
          await input.repository.updateCrawlSourceResult(updatedRunningResult);
        }

        try {
          const result = await provider.crawlSources(
            {
              fetchImpl: input.fetchImpl,
              now: input.now,
              filters: normalizedFilters,
              onBatch: async (batch) => {
                emittedLiveBatch = true;
                totalFetchedJobs += batch.fetchedCount;
                await queueMutation(() =>
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
                );
              },
            },
            sourcesForProvider,
          );

          const finishedAt = new Date().toISOString();
          const durationMs = Date.now() - startedMs;
          providerDurationsMs.push({
            provider: result.provider,
            duration: durationMs,
            sourceCount: result.sourceCount ?? sourcesForProvider.length,
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
              }),
            );
          } else {
            await queueMutation(async () => {
              const existing = sourceResultsByProvider.get(result.provider);
              if (!existing) {
                return;
              }

              const updated = crawlSourceResultSchema.parse({
                ...existing,
                status: result.status,
                warningCount: result.warningCount ?? existing.warningCount,
                errorMessage: result.errorMessage ?? existing.errorMessage,
                finishedAt,
              });
              sourceResultsByProvider.set(result.provider, updated);
              await input.repository.updateCrawlSourceResult(updated);
              await updateRunProgress("crawling", finishedAt);
            });
          }
        } catch (reason) {
          const finishedAt = new Date().toISOString();
          const durationMs = Date.now() - startedMs;
          providerDurationsMs.push({
            provider: provider.provider,
            duration: durationMs,
            sourceCount: sourcesForProvider.length,
          });

          await queueMutation(async () => {
            diagnostics.providerFailures += 1;
            const failedResult = crawlSourceResultSchema.parse({
              _id:
                sourceResultsByProvider.get(provider.provider)?._id ??
                createId(),
              crawlRunId: crawlRun._id,
              searchId: search._id,
              provider: provider.provider,
              status: "failed",
              sourceCount: sourcesForProvider.length,
              fetchedCount: 0,
              matchedCount: 0,
              savedCount: 0,
              warningCount: 1,
              errorMessage:
                reason instanceof Error
                  ? reason.message
                  : `Provider ${provider.provider} failed unexpectedly.`,
              startedAt,
              finishedAt,
            });
            sourceResultsByProvider.set(provider.provider, failedResult);
            await input.repository.updateCrawlSourceResult(failedResult);
            await updateRunProgress("crawling", finishedAt);
          });
        }
      }),
    );
    await mutationQueue;
    stageTimingsMs.providerExecution = Date.now() - providerExecutionStartedMs;

    let savedJobs = await input.repository.getJobsByCrawlRun(crawlRun._id);
    if (validationStrategy.mode === "deferred") {
      diagnostics.validationDeferred = savedJobs.length;
    } else if (savedJobs.length > 0) {
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
      jobs: sortJobs(
        dedupeStoredJobs(savedJobs).map(applyResolvedExperienceLevel),
        normalizedFilters.title,
      ),
      diagnostics: finalizedRun.diagnostics,
    });
    stageTimingsMs.responseAssembly = Date.now() - responseAssemblyStartedMs;

    console.info("[crawl:summary]", {
      searchId: search._id,
      fetchedCount: totalFetchedJobs,
      directJobsHarvested: harvestedDiscoveryJobs.length,
      matchedCount: totalMatchedJobs,
      jobsBeforeDedupe: diagnostics.jobsBeforeDedupe,
      jobsAfterDedupe: diagnostics.jobsAfterDedupe,
      savedCount: savedJobIds.size,
      providerDurationsMs,
      timeToFirstVisibleResultMs:
        typeof firstVisibleResultAtMs === "number"
          ? firstVisibleResultAtMs - crawlStartedMs
          : undefined,
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
      totalMs: Date.now() - crawlStartedMs,
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

  for (const seed of seeds) {
    const evaluation = evaluateSearchFilters(seed, filters, {
      includeExperience: true,
    });

    if (evaluation.matches) {
      matchedSeeds.push(withTitleMatchMetadata(seed, evaluation.titleMatch));
      continue;
    }

    if (evaluation.reason === "title") {
      excludedByTitle += 1;
      continue;
    }

    if (evaluation.reason === "location") {
      excludedByLocation += 1;
      continue;
    }

    excludedByExperience += 1;
  }

  return {
    jobs: matchedSeeds,
    excludedByTitle,
    excludedByLocation,
    excludedByExperience,
  };
}

function withTitleMatchMetadata(seed: NormalizedJobSeed, titleMatch: TitleMatchResult) {
  return {
    ...seed,
    rawSourceMetadata: {
      ...seed.rawSourceMetadata,
      crawlTitleMatch: {
        tier: titleMatch.tier,
        score: titleMatch.score,
        canonicalQueryTitle: titleMatch.canonicalQueryTitle,
        canonicalJobTitle: titleMatch.canonicalJobTitle,
        explanation: titleMatch.explanation,
        matchedTerms: titleMatch.matchedTerms,
        penalties: titleMatch.penalties,
      },
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
  const locationNormalized = normalizeComparableText(
    `${resolvedLocation?.city ?? seed.city ?? ""} ${resolvedLocation?.state ?? seed.state ?? ""} ${resolvedLocation?.country ?? seed.country ?? ""} ${seed.locationText}`,
  );
  const experienceClassification = resolveJobExperienceClassification(seed);
  const discoveredAt = seed.discoveredAt || now.toISOString();

  return {
    title: seed.title,
    company: seed.company,
    country: resolvedLocation?.country ?? seed.country,
    state: resolvedLocation?.state ?? seed.state,
    city: resolvedLocation?.city ?? seed.city,
    locationText: seed.locationText,
    resolvedLocation,
    experienceLevel:
      experienceClassification.explicitLevel ??
      experienceClassification.inferredLevel,
    experienceClassification,
    sourcePlatform: seed.sourcePlatform,
    sourceJobId: seed.sourceJobId,
    sourceUrl: seed.sourceUrl,
    applyUrl: seed.applyUrl,
    canonicalUrl: seed.canonicalUrl,
    postedAt: seed.postedAt,
    discoveredAt,
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
    companyNormalized: normalizeComparableText(seed.company),
    titleNormalized: normalizeComparableText(seed.title),
    locationNormalized,
    contentFingerprint: buildContentFingerprint({
      company: seed.company,
      title: seed.title,
      location: locationNormalized,
    }),
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
    if (job.canonicalUrl) {
      byCanonicalUrl.set(job.canonicalUrl, job._id);
    }

    if (job.resolvedUrl) {
      byResolvedUrl.set(job.resolvedUrl, job._id);
    }

    byApplyUrl.set(job.applyUrl, job._id);
    byContentFingerprint.set(job.contentFingerprint, job._id);
    job.sourceLookupKeys.forEach((lookupKey) => {
      bySourceLookupKey.set(lookupKey, job._id);
    });
  });

  return inlineValidations.map(({ jobIndex }) => {
    const job = jobsReadyToPersist[jobIndex];
    if (!job) {
      return undefined;
    }

    return (
      (job.canonicalUrl ? byCanonicalUrl.get(job.canonicalUrl) : undefined) ??
      (job.resolvedUrl ? byResolvedUrl.get(job.resolvedUrl) : undefined) ??
      byApplyUrl.get(job.applyUrl) ??
      job.sourceLookupKeys.map((lookupKey) => bySourceLookupKey.get(lookupKey)).find(Boolean) ??
      byContentFingerprint.get(job.contentFingerprint)
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
    ...overrides,
  };
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
