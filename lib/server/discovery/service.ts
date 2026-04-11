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
};

export async function discoverSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const result = await discoverSourcesDetailed(input);
  return result.sources;
}

export async function discoverSourcesDetailed(
  input: DiscoveryInput & { env: DiscoveryEnvSnapshot },
): Promise<DiscoveryExecution> {
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
  const publicSearchOptions = resolvePublicSearchExecutionOptions(
    input.filters.crawlMode,
    input.env,
  );
  const publicSearchResult = input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED
    ? await discoverSourcesFromPublicSearchDetailed(input.filters, {
        fetchImpl: input.fetchImpl,
        maxResultsPerQuery: publicSearchOptions.maxResultsPerQuery,
        maxSources: publicSearchOptions.maxSources,
        maxQueries: publicSearchOptions.maxQueries,
        queryConcurrency: publicSearchOptions.queryConcurrency,
        maxGreenhouseLocationClauses: publicSearchOptions.maxLocationClauses,
        maxDirectJobs: publicSearchOptions.maxDirectJobs,
      })
    : {
        sources: [] as DiscoveredSource[],
        jobs: [],
        diagnostics: undefined,
      };
  const publicSources = publicSearchResult.sources;
  const publicJobs = publicSearchResult.jobs;
  const discoveredBeforeFiltering = dedupeDiscoveredSources([
    ...configuredSources,
    ...curatedSources,
    ...companyPageExpandedSources,
    ...publicSources,
  ]);
  const discoveredSources = filterDiscoveredSources(
    discoveredBeforeFiltering,
    selectedPlatforms,
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
    publicSearchDiagnostics: publicSearchResult.diagnostics,
  });

  return {
    sources: discoveredSources,
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
) {
  const base = {
    maxResultsPerQuery: env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS,
    maxSources: env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
    maxQueries: env.PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES,
    queryConcurrency: env.PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY,
    maxLocationClauses: env.GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES,
    maxDirectJobs: Math.min(env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES, 24),
  };

  if (crawlMode === "deep") {
    return base;
  }

  if (crawlMode === "balanced") {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 72),
      maxLocationClauses: Math.min(base.maxLocationClauses, 24),
      maxDirectJobs: Math.min(base.maxDirectJobs, 24),
    };
  }

  return {
    ...base,
    maxQueries: Math.min(base.maxQueries, 48),
    maxLocationClauses: Math.min(base.maxLocationClauses, 16),
    maxDirectJobs: Math.min(base.maxDirectJobs, 16),
  };
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
    publicSearchDiagnostics?: Awaited<
      ReturnType<typeof discoverSourcesFromPublicSearchDetailed>
    >["diagnostics"];
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
    publicSearch: trace.publicSearchDiagnostics,
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
