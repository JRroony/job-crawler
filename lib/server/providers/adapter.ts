import "server-only";

import { runWithConcurrency } from "@/lib/server/crawler/helpers";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import {
  finalizeProviderResult,
  unsupportedProviderResult,
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
  jobs: NormalizedJobSeed[];
  warnings?: string[];
  parseSuccessCount?: number;
  parseFailureCount?: number;
  dropReasons?: string[];
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
          return definition.crawlSource(context, source);
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
      });
    },
  });
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
    dropReasonCounts,
    sampleDropReasons: Object.keys(dropReasonCounts).slice(0, 8),
  };
}
