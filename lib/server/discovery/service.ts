import "server-only";

import { slugToLabel } from "@/lib/server/crawler/helpers";
import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { expandCompanyPageSources } from "@/lib/server/discovery/company-page-expansion";
import { lookupGreenhouseCompanyHint } from "@/lib/server/discovery/greenhouse-registry";
import { discoverSourcesFromPublicSearchDetailed } from "@/lib/server/discovery/public-search";
import { resolveOperationalCrawlerPlatforms, type CrawlMode } from "@/lib/types";
import type {
  DiscoveryExecution,
  DiscoveryExecutionStage,
  DiscoveredSource,
  DiscoveryInput,
  DiscoveryService,
} from "@/lib/server/discovery/types";
import { getEnv, type AppEnv } from "@/lib/server/env";

type DiscoveryEnvSnapshot = Pick<
  AppEnv,
  | "greenhouseBoardTokens"
  | "leverSiteTokens"
  | "ashbyBoardTokens"
  | "companyPageSources"
  | "PUBLIC_SEARCH_DISCOVERY_ENABLED"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES"
  | "PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY"
  | "GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES"
>;

export const defaultDiscoveryService: DiscoveryService = {
  async discover(input) {
    const result = await discoverSourcesDetailed({
      ...input,
      env: getEnv(),
    });
    return result.sources;
  },
  async discoverWithDiagnostics(input) {
    return discoverSourcesDetailed({
      ...input,
      env: getEnv(),
    });
  },
  async discoverInStages(input) {
    return discoverSourcesInStages({
      ...input,
      env: getEnv(),
    });
  },
  async discoverBaseline(input) {
    return discoverBaselineSourcesDetailed({
      ...input,
      env: getEnv(),
    });
  },
  async discoverSupplemental(input, context) {
    return discoverSupplementalSourcesDetailed(
      {
        ...input,
        env: getEnv(),
      },
      context,
    );
  },
};

export async function discoverSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const result = await discoverSourcesDetailed(input);
  return result.sources;
}

export async function discoverSourcesDetailed(
  input: DiscoveryInput & { env: DiscoveryEnvSnapshot },
): Promise<DiscoveryExecution> {
  const stages = await discoverSourcesInStages(input);
  const finalStage = stages[stages.length - 1];
  const allSources = dedupeDiscoveredSources(stages.flatMap((stage) => stage.sources));
  const allJobs = stages.flatMap((stage) => stage.jobs ?? []);

  return {
    sources: allSources,
    jobs: allJobs,
    diagnostics: finalStage?.diagnostics ?? {
      configuredSources: 0,
      curatedSources: 0,
      publicSources: 0,
      publicJobs: 0,
      discoveredBeforeFiltering: allSources.length,
      discoveredAfterFiltering: allSources.length,
      platformCounts: summarizePlatformCounts(allSources),
      publicJobPlatformCounts: summarizePublicJobPlatformCounts(allJobs),
    },
  };
}

export async function discoverSourcesInStages(
  input: DiscoveryInput & { env: DiscoveryEnvSnapshot },
): Promise<DiscoveryExecutionStage[]> {
  const baseline = await discoverBaselineSourcesDetailed(input);
  const supplemental = await discoverSupplementalSourcesDetailed(input, {
    baselineSources: baseline.sources,
  });

  return [baseline, supplemental];
}

export async function discoverBaselineSourcesDetailed(
  input: DiscoveryInput & { env: DiscoveryEnvSnapshot },
): Promise<DiscoveryExecutionStage> {
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const unfilteredConfiguredSources = buildConfiguredSources(input);
  const configuredSources = filterDiscoveredSources(
    unfilteredConfiguredSources,
    selectedPlatforms,
  );
  const curatedSources = filterDiscoveredSources(
    discoverCatalogSources(input.filters.platforms),
    selectedPlatforms,
  );
  const companyPageExpandedSources = dedupeDiscoveredSources(
    await expandCompanyPageSources(
      dedupeDiscoveredSources([...unfilteredConfiguredSources, ...curatedSources]),
      input.fetchImpl,
    ),
  );
  const sources = filterDiscoveredSources(
    dedupeDiscoveredSources([
      ...configuredSources,
      ...curatedSources,
      ...companyPageExpandedSources,
    ]),
    selectedPlatforms,
  );

  return {
    label: "baseline",
    sources,
    jobs: [],
  };
}

export async function discoverSupplementalSourcesDetailed(
  input: DiscoveryInput & { env: DiscoveryEnvSnapshot },
  context: { baselineSources: DiscoveredSource[] },
): Promise<DiscoveryExecutionStage> {
  const discoveryStartedMs = Date.now();
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const unfilteredConfiguredSources = buildConfiguredSources(input);
  const configuredSources = filterDiscoveredSources(
    unfilteredConfiguredSources,
    selectedPlatforms,
  );
  const curatedSources = filterDiscoveredSources(
    discoverCatalogSources(input.filters.platforms),
    selectedPlatforms,
  );
  const companyPageExpansionStartedMs = Date.now();
  const companyPageExpandedSources = dedupeDiscoveredSources(
    await expandCompanyPageSources(
      dedupeDiscoveredSources([...unfilteredConfiguredSources, ...curatedSources]),
      input.fetchImpl,
    ),
  );
  const companyPageExpansionMs = Date.now() - companyPageExpansionStartedMs;
  const publicSearchOptions = resolvePublicSearchExecutionOptions(
    input.filters.crawlMode,
    input.env,
    input.filters.platforms,
  );
  const publicSearchSkippedReason = input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED
    ? resolvePublicSearchSkippedReason(
        input.filters.crawlMode,
        configuredSources.length + curatedSources.length + companyPageExpandedSources.length > 0,
        input.filters.platforms,
      )
    : undefined;
  const shouldExecutePublicSearch =
    input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED && !publicSearchSkippedReason;
  const publicSearchStartedMs = Date.now();
  const publicSearchResult = shouldExecutePublicSearch
    ? await discoverSourcesFromPublicSearchDetailed(input.filters, {
        fetchImpl: input.fetchImpl,
        maxResultsPerQuery: publicSearchOptions.maxResultsPerQuery,
        maxSources: publicSearchOptions.maxSources,
        maxQueries: publicSearchOptions.maxQueries,
        queryConcurrency: publicSearchOptions.queryConcurrency,
        maxGreenhouseLocationClauses: publicSearchOptions.maxLocationClauses,
        maxDirectJobs: publicSearchOptions.maxDirectJobs,
        maxRoleQueries: publicSearchOptions.maxRoleQueries,
      })
    : {
        sources: [] as DiscoveredSource[],
        jobs: [],
        diagnostics: undefined,
      };
  const publicSearchMs = shouldExecutePublicSearch
    ? Date.now() - publicSearchStartedMs
    : 0;
  const publicSources = publicSearchResult.sources;
  const publicJobs = publicSearchResult.jobs;
  const baselineSources = dedupeDiscoveredSources(context.baselineSources);
  const discoveredBeforeFiltering = dedupeDiscoveredSources([
    ...baselineSources,
    ...publicSources,
  ]);
  const discoveredSources = filterDiscoveredSources(
    discoveredBeforeFiltering,
    selectedPlatforms,
  );
  const filteredBaselineSources = filterDiscoveredSources(
    baselineSources,
    selectedPlatforms,
  );
  const publicSourcesOnly = discoveredSources.filter(
    (source) => !filteredBaselineSources.some((baseline) => baseline.id === source.id),
  );

  logDiscoveryTrace(input.filters, {
    configuredSources,
    curatedSources,
    companyPageExpandedSources,
    publicSources,
    publicJobs,
    discoveredBeforeFiltering,
    discoveredSources,
    publicSearchEnabled: input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED,
    publicSearchSkippedReason,
    publicSearchDiagnostics: publicSearchResult.diagnostics,
    timingsMs: {
      companyPageExpansion: companyPageExpansionMs,
      publicSearch: publicSearchMs,
      total: Date.now() - discoveryStartedMs,
    },
  });

  return {
    label: shouldExecutePublicSearch ? "public_search" : "baseline",
    sources: publicSourcesOnly,
    jobs: publicJobs,
    diagnostics: {
      configuredSources: configuredSources.length,
      curatedSources: curatedSources.length,
      publicSources: publicSources.length,
      publicJobs: publicJobs.length,
      discoveredBeforeFiltering: discoveredBeforeFiltering.length,
      discoveredAfterFiltering: discoveredSources.length,
      platformCounts: summarizePlatformCounts(discoveredSources),
      publicJobPlatformCounts: summarizePublicJobPlatformCounts(publicJobs),
      zeroCoverageReason:
        discoveredSources.length === 0
          ? resolveZeroCoverageReason({
              discoveredBeforeFiltering,
              publicSearchEnabled: input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED,
            })
          : undefined,
      ...(publicSearchSkippedReason
        ? {
            publicSearchSkippedReason,
          }
        : {}),
      ...(publicSearchResult.diagnostics
        ? {
            publicSearch: publicSearchResult.diagnostics,
          }
        : {}),
    },
  };
}

function resolvePublicSearchExecutionOptions(
  crawlMode: CrawlMode | undefined,
  env: DiscoveryEnvSnapshot,
  platforms?: readonly string[],
) {
  const greenhouseFirstOnly =
    Array.isArray(platforms) &&
    platforms.length > 0 &&
    platforms.every((platform) => platform === "greenhouse");
  const base = {
    maxResultsPerQuery: env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS,
    maxSources: env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
    maxQueries: env.PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES,
    queryConcurrency: env.PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY,
    maxLocationClauses: env.GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES,
    maxDirectJobs: Math.min(env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES, 24),
    maxRoleQueries: 18,
  };

  if (crawlMode === "deep") {
    return base;
  }

  if (crawlMode === "balanced") {
    if (greenhouseFirstOnly) {
      return {
        ...base,
        maxQueries: Math.min(base.maxQueries, 48),
        maxLocationClauses: Math.min(base.maxLocationClauses, 16),
        maxDirectJobs: Math.min(base.maxDirectJobs, 12),
        maxRoleQueries: 16,
      };
    }

    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 36),
      maxLocationClauses: Math.min(base.maxLocationClauses, 12),
      maxDirectJobs: Math.min(base.maxDirectJobs, 16),
      maxRoleQueries: 12,
    };
  }

  if (greenhouseFirstOnly) {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 24),
      maxLocationClauses: Math.min(base.maxLocationClauses, 10),
      maxDirectJobs: Math.min(base.maxDirectJobs, 6),
      maxRoleQueries: 12,
    };
  }

  return {
    ...base,
    maxQueries: Math.min(base.maxQueries, 12),
    maxLocationClauses: Math.min(base.maxLocationClauses, 4),
    maxDirectJobs: Math.min(base.maxDirectJobs, 8),
    maxRoleQueries: 8,
  };
}

function resolvePublicSearchSkippedReason(
  crawlMode: CrawlMode | undefined,
  hasBaselineCoverage: boolean,
  platforms?: readonly string[],
) {
  const greenhouseFirstOnly =
    Array.isArray(platforms) &&
    platforms.length > 0 &&
    platforms.every((platform) => platform === "greenhouse");
  if (crawlMode !== "fast" || !hasBaselineCoverage) {
    return undefined;
  }

  if (greenhouseFirstOnly) {
    return undefined;
  }

  return "Fast mode skipped public ATS search because configured or expanded sources already provided runnable coverage.";
}

export function discoverConfiguredSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const discoveredSources = buildConfiguredSources(input);

  // Discovery is the first place we can honestly narrow the platform scope:
  // when a user selects provider families, we stop surfacing configured sources
  // for the unselected families before provider routing even begins.
  if (!selectedPlatforms) {
    return discoveredSources;
  }

  return filterDiscoveredSources(discoveredSources, selectedPlatforms);
}

function buildConfiguredSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const candidates = [
    ...input.env.greenhouseBoardTokens.map((token) =>
      classifySourceCandidate({
        url: `https://boards.greenhouse.io/${token}`,
        token,
        companyHint: lookupGreenhouseCompanyHint(token) ?? slugToLabel(token),
        confidence: "high",
        discoveryMethod: "platform_registry",
      }),
    ),
    ...input.env.leverSiteTokens.map((token) =>
      classifySourceCandidate({
        url: `https://jobs.lever.co/${token}`,
        token,
        companyHint: slugToLabel(token),
        confidence: "high",
        discoveryMethod: "configured_env",
      }),
    ),
    ...input.env.ashbyBoardTokens.map((token) =>
      classifySourceCandidate({
        url: `https://jobs.ashbyhq.com/${token}`,
        token,
        companyHint: slugToLabel(token),
        confidence: "high",
        discoveryMethod: "configured_env",
      }),
    ),
    ...input.env.companyPageSources.map((source) =>
      classifySourceCandidate({
        url: source.url,
        companyHint: source.company,
        pageType: source.type,
        confidence: source.type === "json_feed" ? "high" : "medium",
        discoveryMethod: "manual_config",
      }),
    ),
  ];

  return dedupeDiscoveredSources(candidates);
}

function dedupeDiscoveredSources(sources: DiscoveredSource[]) {
  const deduped = new Map<string, DiscoveredSource>();

  for (const source of sources) {
    if (!deduped.has(source.id)) {
      deduped.set(source.id, source);
    }
  }

  return Array.from(deduped.values());
}

function filterDiscoveredSources(
  sources: DiscoveredSource[],
  selectedPlatforms: Set<string> | null,
) {
  if (!selectedPlatforms) {
    return sources;
  }

  return sources.filter((source) => selectedPlatforms.has(source.platform));
}

function logDiscoveryTrace(
  filters: DiscoveryInput["filters"],
  trace: {
    configuredSources: DiscoveredSource[];
    curatedSources: DiscoveredSource[];
    companyPageExpandedSources: DiscoveredSource[];
    publicSources: DiscoveredSource[];
    publicJobs: { sourcePlatform: string }[];
    discoveredBeforeFiltering: DiscoveredSource[];
    discoveredSources: DiscoveredSource[];
    publicSearchEnabled: boolean;
    publicSearchSkippedReason?: string;
    publicSearchDiagnostics?: Awaited<
      ReturnType<typeof discoverSourcesFromPublicSearchDetailed>
    >["diagnostics"];
    timingsMs: {
      companyPageExpansion: number;
      publicSearch: number;
      total: number;
    };
  },
) {
  console.info("[discovery:summary]", {
    filters,
    configuredCount: trace.configuredSources.length,
    curatedCount: trace.curatedSources.length,
    companyPageExpandedCount: trace.companyPageExpandedSources.length,
    publicCount: trace.publicSources.length,
    publicJobCount: trace.publicJobs.length,
    discoveredBeforeFilteringCount: trace.discoveredBeforeFiltering.length,
    discoveredAfterFilteringCount: trace.discoveredSources.length,
    platformCounts: summarizePlatformCounts(trace.discoveredSources),
    discoveryMethodCounts: summarizeDiscoveryMethodCounts(trace.discoveredSources),
    publicSearchEnabled: trace.publicSearchEnabled,
    publicSearchSkippedReason: trace.publicSearchSkippedReason,
    publicSearch: trace.publicSearchDiagnostics,
    timingsMs: trace.timingsMs,
  });

  if (trace.discoveredSources.length > 0) {
    return;
  }

  const reason =
    resolveZeroCoverageReason({
      discoveredBeforeFiltering: trace.discoveredBeforeFiltering,
      publicSearchEnabled: trace.publicSearchEnabled,
    });

  console.warn("[discovery:zero-sources]", {
    filters,
    reason,
  });
}

function summarizePlatformCounts(sources: DiscoveredSource[]) {
  return sources.reduce<Record<string, number>>((counts, source) => {
    counts[source.platform] = (counts[source.platform] ?? 0) + 1;
    return counts;
  }, {});
}

function summarizeDiscoveryMethodCounts(sources: DiscoveredSource[]) {
  return sources.reduce<Record<string, number>>((counts, source) => {
    counts[source.discoveryMethod] = (counts[source.discoveryMethod] ?? 0) + 1;
    return counts;
  }, {});
}

function summarizePublicJobPlatformCounts(jobs: { sourcePlatform: string }[]) {
  return jobs.reduce<Record<string, number>>((counts, job) => {
    counts[job.sourcePlatform] = (counts[job.sourcePlatform] ?? 0) + 1;
    return counts;
  }, {});
}

function resolveZeroCoverageReason(input: {
  discoveredBeforeFiltering: DiscoveredSource[];
  publicSearchEnabled: boolean;
}) {
  return input.discoveredBeforeFiltering.length > 0
    ? "Selected platforms filtered every discovered source before provider routing."
    : input.publicSearchEnabled
      ? "Registry-backed seeds, supplemental catalog sources, and public ATS search all returned zero runnable sources."
      : "Registry-backed seeds and supplemental catalog sources returned zero runnable sources while public ATS search was disabled.";
}
