import "server-only";

import type { DiscoveredSource } from "@/lib/server/discovery/types";
import {
  createSignalAwareFetch,
  isProviderSourceTimeoutError,
  runProviderSourceWithTimeout,
} from "@/lib/server/providers/budget";
import {
  finalizeProviderResult,
  normalizeProviderJobSeed,
  unsupportedProviderResult,
  validateProviderSeedBatch,
} from "@/lib/server/providers/shared";
import {
  defineProvider,
  type CrawlProvider,
  type NormalizedJobSeed,
  type ProviderDiagnostics,
  type ProviderExecutionContext,
  type ProviderResult,
  type ProviderSourceObservation,
  type ProviderSourceFor,
} from "@/lib/server/providers/types";

export type ProviderSourceAdapterRun<
  P extends ProviderResult["provider"],
> = {
  fetchedCount: number;
  fetchCount?: number;
  sourceSucceeded?: boolean;
  sourceTimedOut?: boolean;
  sourceFailed?: boolean;
  sourceSkipped?: boolean;
  jobsEmittedViaOnBatch?: number;
  jobs: NormalizedJobSeed[];
  warnings?: string[];
  parseSuccessCount?: number;
  parseFailureCount?: number;
  dropReasons?: string[];
  parsedSeedCount?: number;
  validSeedCount?: number;
  invalidSeedCount?: number;
  sampleInvalidSeeds?: ProviderDiagnostics<P>["sampleInvalidSeeds"];
};

export type ProviderSourceAdapterDefinition<
  P extends ProviderResult["provider"],
> = {
  provider: P;
  supportsSource(source: DiscoveredSource): source is ProviderSourceFor<P>;
  unsupportedMessage: string;
  concurrency: number | ((context: ProviderExecutionContext) => number);
  dedupeSources?(sources: readonly ProviderSourceFor<P>[]): ProviderSourceFor<P>[];
  crawlSource(
    context: ProviderExecutionContext,
    source: ProviderSourceFor<P>,
  ): Promise<ProviderSourceAdapterRun<P>>;
};

export function createAdapterProvider<P extends ProviderResult["provider"]>(
  definition: ProviderSourceAdapterDefinition<P>,
): CrawlProvider {
  return defineProvider({
    provider: definition.provider,
    sourceTimeoutIsolation: true,
    supportsSource: definition.supportsSource,
    async crawlSources(context, sources) {
      await context.throwIfCanceled?.();
      if (sources.length === 0) {
        return unsupportedProviderResult(
          definition.provider,
          definition.unsupportedMessage,
          sources.length,
        );
      }

      const adapterSources =
        definition.dedupeSources?.(sources as ProviderSourceFor<P>[]) ??
        (sources as ProviderSourceFor<P>[]);
      const concurrency =
        typeof definition.concurrency === "function"
          ? definition.concurrency(context)
          : definition.concurrency;
      const providerStartedMs = Date.now();
      const providerBudgetMs = Math.max(0, Math.floor(context.providerTimeoutMs ?? 0));
      const sourceTimeoutMs = Math.max(0, Math.floor(context.sourceTimeoutMs ?? 0));

      const sourceRuns = await runSourcesWithSoftProviderBudget(
        adapterSources,
        async (source, remainingProviderBudgetMs) => {
          await context.throwIfCanceled?.();
          const effectiveSourceTimeoutMs = resolveEffectiveSourceTimeoutMs(
            sourceTimeoutMs,
            remainingProviderBudgetMs,
          );
          const emittedJobs: NormalizedJobSeed[] = [];
          let jobsEmittedViaOnBatch = 0;
          let fetchedCountEmittedViaOnBatch = 0;

          try {
            const run = await runProviderSourceWithTimeout({
              provider: definition.provider,
              sourceId: source.id,
              timeoutMs: effectiveSourceTimeoutMs,
              parentSignal: context.signal,
              task: async (sourceSignal) => {
                const run = await definition.crawlSource(
                  {
                    ...context,
                    fetchImpl: createSignalAwareFetch(context.fetchImpl, sourceSignal),
                    signal: sourceSignal ?? context.signal,
                    throwIfCanceled: async () => {
                      await context.throwIfCanceled?.();
                      if (sourceSignal?.aborted) {
                        throw sourceSignal.reason instanceof Error
                          ? sourceSignal.reason
                          : new Error("Provider source was aborted.");
                      }
                    },
                    onBatch: context.onBatch
                      ? (batch) => {
                          const validation = validateProviderSeedBatch({
                            provider: batch.provider,
                            jobs: batch.jobs.map(normalizeProviderJobSeed),
                          });

                          if (validation.jobs.length === 0) {
                            return undefined;
                          }

                          emittedJobs.push(...validation.jobs);
                          jobsEmittedViaOnBatch += validation.jobs.length;
                          fetchedCountEmittedViaOnBatch += batch.fetchedCount;
                          console.info("[provider:batch-emitted]", {
                            provider: definition.provider,
                            sourceId: source.id,
                            sourceCount: batch.sourceCount ?? 1,
                            fetchedCount: batch.fetchedCount,
                            jobCount: validation.jobs.length,
                            droppedCount: validation.dropped.length,
                          });

                          return context.onBatch?.({
                            ...batch,
                            jobs: validation.jobs,
                          });
                        }
                      : undefined,
                  },
                  source,
                );

                return finalizeProviderSourceRun(definition.provider, run);
              },
            });

            return {
              ...run,
              jobs: run.jobs.map(normalizeProviderJobSeed),
              sourceSucceeded:
                run.sourceSucceeded ??
                (run.jobs.length > 0 ||
                  run.fetchedCount > 0 ||
                  jobsEmittedViaOnBatch > 0 ||
                  fetchedCountEmittedViaOnBatch > 0),
              jobsEmittedViaOnBatch:
                (run.jobsEmittedViaOnBatch ?? 0) + jobsEmittedViaOnBatch,
            };
          } catch (error) {
            if (context.signal?.aborted) {
              throw error;
            }

            if (!isProviderSourceTimeoutError(error)) {
              const message =
                error instanceof Error
                  ? error.message
                  : `Provider ${definition.provider} source ${source.id} failed unexpectedly.`;

              return {
                fetchedCount: fetchedCountEmittedViaOnBatch,
                fetchCount: 0,
                jobs: emittedJobs,
                warnings: [message],
                parseSuccessCount: emittedJobs.length,
                parseFailureCount: emittedJobs.length > 0 ? 0 : 1,
                dropReasons: ["source_failed"],
                sourceSucceeded: emittedJobs.length > 0,
                sourceFailed: emittedJobs.length === 0,
                jobsEmittedViaOnBatch,
              };
            }

            return {
              fetchedCount: fetchedCountEmittedViaOnBatch,
              fetchCount: 0,
              jobs: emittedJobs,
              warnings: [error.message],
              parseSuccessCount: emittedJobs.length,
              parseFailureCount: 1,
              dropReasons: ["source_timeout"],
              sourceSucceeded: emittedJobs.length > 0,
              sourceTimedOut: true,
              jobsEmittedViaOnBatch,
            };
          }
        },
        concurrency,
        {
          provider: definition.provider,
          providerStartedMs,
          providerBudgetMs,
        },
      );

      const warnings = sourceRuns.flatMap((run) => run.warnings ?? []);
      const jobs = sourceRuns.flatMap((run) => run.jobs);
      const fetchedCount = sourceRuns.reduce((total, run) => total + run.fetchedCount, 0);
      const diagnostics = buildProviderDiagnostics(
        definition.provider,
        adapterSources,
        sourceRuns,
        {
          providerElapsedMs: Date.now() - providerStartedMs,
          providerBudgetMs,
          sourceTimeoutMs,
        },
      );

      console.info(`[${definition.provider}:adapter-summary]`, diagnostics);

      const result = finalizeProviderResult({
        provider: definition.provider,
        jobs,
        sourceCount: adapterSources.length,
        fetchedCount,
        warnings,
        diagnostics,
        didExecuteSuccessfully: sourceRuns.some((run) => run.sourceSucceeded ?? run.jobs.length > 0),
      });

      return applyAdapterStatusRules(result, sourceRuns, diagnostics);
    },
  });
}

function finalizeProviderSourceRun<P extends ProviderResult["provider"]>(
  provider: P,
  run: ProviderSourceAdapterRun<P>,
): ProviderSourceAdapterRun<P> {
  const parsedSeedCount = run.parsedSeedCount ?? run.jobs.length;
  const validation = validateProviderSeedBatch({
    provider,
    jobs: run.jobs,
    warnings: run.warnings,
  });
  const existingInvalidSeedCount = run.invalidSeedCount ?? 0;

  return {
    ...run,
    jobs: validation.jobs,
    warnings: validation.warnings,
    parseSuccessCount: validation.jobs.length,
    parseFailureCount:
      (run.parseFailureCount ?? 0) + validation.dropped.length,
    dropReasons: [
      ...(run.dropReasons ?? []),
      ...validation.dropped.map((drop) => drop.reason),
    ],
    parsedSeedCount,
    validSeedCount: validation.jobs.length,
    invalidSeedCount: existingInvalidSeedCount + validation.dropped.length,
    sampleInvalidSeeds: [
      ...(run.sampleInvalidSeeds ?? []),
      ...validation.sampleInvalidSeeds,
    ].slice(0, 8),
  };
}

async function runSourcesWithSoftProviderBudget<
  P extends ProviderResult["provider"],
  TSource extends ProviderSourceFor<P>,
>(
  sources: readonly TSource[],
  worker: (
    source: TSource,
    effectiveSourceTimeoutMs: number | undefined,
  ) => Promise<ProviderSourceAdapterRun<P>>,
  concurrency: number,
  budget: {
    provider: P;
    providerStartedMs: number;
    providerBudgetMs: number;
  },
) {
  const results: Array<ProviderSourceAdapterRun<P>> = [];
  let currentIndex = 0;

  const takeNextIndex = () => {
    if (budget.providerBudgetMs > 0 && Date.now() - budget.providerStartedMs >= budget.providerBudgetMs) {
      return undefined;
    }

    if (currentIndex >= sources.length) {
      return undefined;
    }

    const index = currentIndex;
    currentIndex += 1;
    return index;
  };

  const markRemainingSkipped = () => {
    while (currentIndex < sources.length) {
      const index = currentIndex;
      currentIndex += 1;
      results[index] = createProviderBudgetSkippedRun(
        budget.provider,
        sources[index].id,
        budget.providerBudgetMs,
      );
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(Math.max(1, Math.floor(concurrency)), sources.length) }, async () => {
      while (true) {
        const index = takeNextIndex();
        if (index === undefined) {
          markRemainingSkipped();
          return;
        }

        const remainingBudgetMs =
          budget.providerBudgetMs > 0
            ? Math.max(1, budget.providerBudgetMs - (Date.now() - budget.providerStartedMs))
            : undefined;
        results[index] = await worker(sources[index], remainingBudgetMs);
      }
    }),
  );

  return results;
}

function createProviderBudgetSkippedRun<P extends ProviderResult["provider"]>(
  provider: P,
  sourceId: string,
  providerBudgetMs: number,
): ProviderSourceAdapterRun<P> {
  return {
    fetchedCount: 0,
    fetchCount: 0,
    jobs: [],
    warnings: [
      `Provider ${provider} skipped source ${sourceId} because the ${providerBudgetMs}ms provider crawl budget was exhausted.`,
    ],
    parseSuccessCount: 0,
    parseFailureCount: 0,
    dropReasons: ["source_skipped_provider_budget"],
    sourceSucceeded: false,
    sourceSkipped: true,
  };
}

function resolveEffectiveSourceTimeoutMs(
  sourceTimeoutMs: number,
  remainingProviderBudgetMs: number | undefined,
) {
  if (sourceTimeoutMs > 0 && remainingProviderBudgetMs !== undefined) {
    return Math.max(1, Math.min(sourceTimeoutMs, remainingProviderBudgetMs));
  }

  if (sourceTimeoutMs > 0) {
    return sourceTimeoutMs;
  }

  return remainingProviderBudgetMs;
}

function buildProviderDiagnostics<P extends ProviderResult["provider"]>(
  provider: P,
  sources: Array<ProviderSourceFor<P>>,
  sourceRuns: Array<ProviderSourceAdapterRun<P>>,
  timing: {
    providerElapsedMs: number;
    providerBudgetMs: number;
    sourceTimeoutMs: number;
  },
): ProviderDiagnostics<P> {
  const dropReasonCounts = sourceRuns
    .flatMap((run) => run.dropReasons ?? [])
    .reduce<Record<string, number>>((counts, reason) => {
      counts[reason] = (counts[reason] ?? 0) + 1;
      return counts;
    }, {});
  const sourceObservations = sourceRuns.map((run, index) =>
    buildProviderSourceObservation(provider, sources[index]?.id ?? `unknown:${index}`, run),
  );

  return {
    provider,
    discoveryCount: sources.length,
    sourceCount: sources.length,
    sourceSucceededCount: sourceObservations.filter((observation) => observation.succeeded).length,
    sourceTimedOutCount: sourceObservations.filter((observation) => observation.errorType === "source_timeout").length,
    sourceFailedCount: sourceObservations.filter((observation) => observation.errorType === "source_failed").length,
    sourceSkippedCount: sourceObservations.filter((observation) => observation.errorType === "source_skipped").length,
    fetchCount: sourceRuns.reduce(
      (total, run) => total + (run.fetchCount ?? (run.fetchedCount > 0 || (run.warnings?.length ?? 0) > 0 ? 1 : 0)),
      0,
    ),
    fetchedCount: sourceRuns.reduce((total, run) => total + run.fetchedCount, 0),
    parseSuccessCount: sourceRuns.reduce(
      (total, run) => total + (run.parseSuccessCount ?? run.jobs.length),
      0,
    ),
    parseFailureCount: sourceRuns.reduce(
      (total, run) => total + (run.parseFailureCount ?? 0),
      0,
    ),
    rawFetchedCount: sourceRuns.reduce((total, run) => total + run.fetchedCount, 0),
    parsedSeedCount: sourceRuns.reduce(
      (total, run) => total + (run.parsedSeedCount ?? run.jobs.length),
      0,
    ),
    validSeedCount: sourceRuns.reduce(
      (total, run) => total + (run.validSeedCount ?? run.jobs.length),
      0,
    ),
    invalidSeedCount: sourceRuns.reduce(
      (total, run) => total + (run.invalidSeedCount ?? 0),
      0,
    ),
    jobsEmittedViaOnBatch: sourceRuns.reduce(
      (total, run) => total + (run.jobsEmittedViaOnBatch ?? 0),
      0,
    ),
    providerElapsedMs: timing.providerElapsedMs,
    providerBudgetMs: timing.providerBudgetMs,
    sourceTimeoutMs: timing.sourceTimeoutMs,
    sourceObservations,
    dropReasonCounts,
    sampleDropReasons: Object.keys(dropReasonCounts).slice(0, 8),
    sampleInvalidSeeds: sourceRuns
      .flatMap((run) => run.sampleInvalidSeeds ?? [])
      .slice(0, 8),
  };
}

function buildProviderSourceObservation<P extends ProviderResult["provider"]>(
  provider: P,
  sourceId: string,
  run: ProviderSourceAdapterRun<P>,
): ProviderSourceObservation {
  if (run.sourceSucceeded) {
    return {
      sourceId,
      succeeded: true,
      errorType: "none",
    };
  }

  if (run.sourceTimedOut) {
    return {
      sourceId,
      succeeded: false,
      errorType: "source_timeout",
      failureReason:
        firstNonEmptyString(run.warnings) ??
        `Provider ${provider} source ${sourceId} exceeded the source crawl budget.`,
    };
  }

  if (run.sourceSkipped) {
    return {
      sourceId,
      succeeded: false,
      errorType: "source_skipped",
      failureReason:
        firstNonEmptyString(run.warnings) ??
        `Provider ${provider} skipped source ${sourceId}.`,
    };
  }

  if (
    run.sourceFailed ||
    (run.jobs.length === 0 &&
      ((run.warnings?.length ?? 0) > 0 ||
        (run.dropReasons?.length ?? 0) > 0 ||
        (run.parseFailureCount ?? 0) > 0))
  ) {
    return {
      sourceId,
      succeeded: false,
      errorType: "source_failed",
      failureReason:
        firstNonEmptyString(run.warnings) ??
        `Provider ${provider} source ${sourceId} failed without emitting jobs.`,
    };
  }

  return {
    sourceId,
    succeeded: true,
    errorType: "none",
  };
}

function firstNonEmptyString(values: string[] | undefined) {
  return values?.find((value) => value.trim().length > 0);
}

function applyAdapterStatusRules<P extends ProviderResult["provider"]>(
  result: ProviderResult<P>,
  sourceRuns: Array<ProviderSourceAdapterRun<P>>,
  diagnostics: ProviderDiagnostics<P>,
): ProviderResult<P> {
  const sourceCount = sourceRuns.length;
  const sourceSucceededCount = diagnostics.sourceSucceededCount ?? 0;
  const sourceTimedOutCount = diagnostics.sourceTimedOutCount ?? 0;
  const sourceFailedCount = diagnostics.sourceFailedCount ?? 0;
  const sourceSkippedCount = diagnostics.sourceSkippedCount ?? 0;
  const emittedJobCount = result.jobs.length + (diagnostics.jobsEmittedViaOnBatch ?? 0);
  const hasSourceProblem = sourceTimedOutCount > 0 || sourceFailedCount > 0 || sourceSkippedCount > 0;

  if (sourceCount > 0 && sourceSkippedCount === sourceCount && emittedJobCount === 0) {
    return {
      ...result,
      status: "unsupported",
      errorMessage:
        result.errorMessage ??
        `No usable ${result.provider} source could start before the provider budget was exhausted.`,
    };
  }

  if (sourceSucceededCount > 0 && hasSourceProblem) {
    return {
      ...result,
      status: "partial",
      errorMessage:
        result.errorMessage ??
        summarizeAdapterSourceProblems(result.provider, diagnostics),
    };
  }

  if (sourceSucceededCount > 0) {
    return {
      ...result,
      status: result.warningCount && result.warningCount > 0 ? "partial" : "success",
      errorMessage: result.warningCount && result.warningCount > 0 ? result.errorMessage : undefined,
    };
  }

  if (sourceTimedOutCount === sourceCount && emittedJobCount === 0) {
    return {
      ...result,
      status: "failed",
      errorMessage: `All ${result.provider} sources timed out before emitting jobs.`,
    };
  }

  if (sourceFailedCount + sourceTimedOutCount + sourceSkippedCount >= sourceCount && emittedJobCount === 0) {
    return {
      ...result,
      status: "failed",
      errorMessage:
        result.errorMessage ??
        summarizeAdapterSourceProblems(result.provider, diagnostics),
    };
  }

  return result;
}

function summarizeAdapterSourceProblems(
  provider: ProviderResult["provider"],
  diagnostics: ProviderDiagnostics,
) {
  const parts = [
    diagnostics.sourceTimedOutCount
      ? `${diagnostics.sourceTimedOutCount} source(s) timed out`
      : undefined,
    diagnostics.sourceFailedCount
      ? `${diagnostics.sourceFailedCount} source(s) failed`
      : undefined,
    diagnostics.sourceSkippedCount
      ? `${diagnostics.sourceSkippedCount} source(s) were skipped`
      : undefined,
  ].filter((value): value is string => Boolean(value));

  return parts.length > 0
    ? `Provider ${provider} completed with partial source results: ${parts.join(", ")}.`
    : `Provider ${provider} completed with partial source results.`;
}
