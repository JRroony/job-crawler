import "server-only";

import { runWithConcurrency } from "@/lib/server/crawler/helpers";
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
  type ProviderSourceFor,
} from "@/lib/server/providers/types";

export type ProviderSourceAdapterRun<
  P extends ProviderResult["provider"],
> = {
  fetchedCount: number;
  fetchCount?: number;
  sourceSucceeded?: boolean;
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

      const sourceRuns = await runWithConcurrency(
        adapterSources,
        async (source) => {
          await context.throwIfCanceled?.();
          try {
            const run = await runProviderSourceWithTimeout({
              provider: definition.provider,
              sourceId: source.id,
              timeoutMs: context.sourceTimeoutMs,
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
            };
          } catch (error) {
            if (!isProviderSourceTimeoutError(error)) {
              throw error;
            }

            return {
              fetchedCount: 0,
              fetchCount: 0,
              jobs: [],
              warnings: [error.message],
              parseSuccessCount: 0,
              parseFailureCount: 1,
              dropReasons: ["source_timeout"],
              sourceSucceeded: false,
            };
          }
        },
        concurrency,
      );

      const warnings = sourceRuns.flatMap((run) => run.warnings ?? []);
      const jobs = sourceRuns.flatMap((run) => run.jobs);
      const fetchedCount = sourceRuns.reduce((total, run) => total + run.fetchedCount, 0);
      const diagnostics = buildProviderDiagnostics(
        definition.provider,
        adapterSources.length,
        sourceRuns,
      );

      console.info(`[${definition.provider}:adapter-summary]`, diagnostics);

      return finalizeProviderResult({
        provider: definition.provider,
        jobs,
        sourceCount: adapterSources.length,
        fetchedCount,
        warnings,
        diagnostics,
        didExecuteSuccessfully: sourceRuns.some((run) => run.sourceSucceeded ?? run.jobs.length > 0),
      });
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

function buildProviderDiagnostics<P extends ProviderResult["provider"]>(
  provider: P,
  discoveryCount: number,
  sourceRuns: Array<ProviderSourceAdapterRun<P>>,
): ProviderDiagnostics<P> {
  const dropReasonCounts = sourceRuns
    .flatMap((run) => run.dropReasons ?? [])
    .reduce<Record<string, number>>((counts, reason) => {
      counts[reason] = (counts[reason] ?? 0) + 1;
      return counts;
    }, {});

  return {
    provider,
    discoveryCount,
    fetchCount: sourceRuns.reduce(
      (total, run) => total + (run.fetchCount ?? (run.fetchedCount > 0 || (run.warnings?.length ?? 0) > 0 ? 1 : 0)),
      0,
    ),
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
    dropReasonCounts,
    sampleDropReasons: Object.keys(dropReasonCounts).slice(0, 8),
    sampleInvalidSeeds: sourceRuns
      .flatMap((run) => run.sampleInvalidSeeds ?? [])
      .slice(0, 8),
  };
}
