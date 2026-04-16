import "server-only";

import { slugToLabel } from "@/lib/server/crawler/helpers";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { expandCompanyPageSources } from "@/lib/server/discovery/company-page-expansion";
import { lookupGreenhouseCompanyHint } from "@/lib/server/discovery/greenhouse-registry";
import {
  buildSourceInventorySeeds,
  toDiscoveredSourceFromInventory,
  toSourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
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

type DiscoveryRuntime = {
  repository?: JobCrawlerRepository;
  env?: DiscoveryEnvSnapshot;
};

export function createDiscoveryService(runtime: DiscoveryRuntime = {}): DiscoveryService {
  return {
    async discover(input) {
      const result = await discoverSourcesDetailed({
        ...input,
        env: runtime.env ?? getEnv(),
        repository: runtime.repository,
      });
      return result.sources;
    },
    async discoverWithDiagnostics(input) {
      return discoverSourcesDetailed({
        ...input,
        env: runtime.env ?? getEnv(),
        repository: runtime.repository,
      });
    },
    async discoverInStages(input) {
      return discoverSourcesInStages({
        ...input,
        env: runtime.env ?? getEnv(),
        repository: runtime.repository,
      });
    },
    async discoverBaseline(input) {
      return discoverBaselineSourcesDetailed({
        ...input,
        env: runtime.env ?? getEnv(),
        repository: runtime.repository,
      });
    },
    async discoverSupplemental(input, context) {
      return discoverSupplementalSourcesDetailed(
        {
          ...input,
          env: runtime.env ?? getEnv(),
          repository: runtime.repository,
        },
        context,
      );
    },
  };
}

export const defaultDiscoveryService: DiscoveryService = createDiscoveryService();

type DiscoveryExecutionInput = DiscoveryInput & {
  env: DiscoveryEnvSnapshot;
  repository?: JobCrawlerRepository;
};

export async function discoverSources(input: DiscoveryExecutionInput) {
  const result = await discoverSourcesDetailed(input);
  return result.sources;
}

export async function discoverSourcesDetailed(
  input: DiscoveryExecutionInput,
): Promise<DiscoveryExecution> {
  const stages = await discoverSourcesInStages(input);
  const finalStage = stages[stages.length - 1];
  const allSources = dedupeDiscoveredSources(stages.flatMap((stage) => stage.sources));
  const allJobs = stages.flatMap((stage) => stage.jobs ?? []);

  return {
    sources: allSources,
    jobs: allJobs,
    diagnostics: finalStage?.diagnostics ?? {
      inventorySources: 0,
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
  input: DiscoveryExecutionInput,
): Promise<DiscoveryExecutionStage[]> {
  const baseline = await discoverBaselineSourcesDetailed(input);
  const supplemental = await discoverSupplementalSourcesDetailed(input, {
    baselineSources: baseline.sources,
  });

  return [baseline, supplemental];
}

export async function discoverBaselineSourcesDetailed(
  input: DiscoveryExecutionInput,
): Promise<DiscoveryExecutionStage> {
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const inventorySources = await loadInventorySources(input, selectedPlatforms);
  const inventoryBackedPlatforms = new Set(inventorySources.map((source) => source.platform));
  const inventoryPrimaryPlatforms = new Set<string>(
    Array.from(inventoryBackedPlatforms).filter((platform) => platform === "greenhouse"),
  );
  const unfilteredConfiguredSources = buildConfiguredSources(input).filter(
    (source) => !inventoryPrimaryPlatforms.has(source.platform),
  );
  const configuredSources = filterDiscoveredSources(
    unfilteredConfiguredSources,
    selectedPlatforms,
  );
  const curatedSources = filterDiscoveredSources(
    discoverCatalogSources(input.filters.platforms).filter(
      (source) => !inventoryPrimaryPlatforms.has(source.platform),
    ),
    selectedPlatforms,
  );
  const sources = filterDiscoveredSources(
    dedupeDiscoveredSources([...inventorySources, ...configuredSources, ...curatedSources]),
    selectedPlatforms,
  );

  return {
    label: "baseline",
    sources,
    jobs: [],
    diagnostics: {
      inventorySources: inventorySources.length,
      configuredSources: configuredSources.length,
      curatedSources: curatedSources.length,
      publicSources: 0,
      publicJobs: 0,
      discoveredBeforeFiltering: sources.length,
      discoveredAfterFiltering: sources.length,
      platformCounts: summarizePlatformCounts(sources),
      publicJobPlatformCounts: {},
    },
  };
}

export async function discoverSupplementalSourcesDetailed(
  input: DiscoveryExecutionInput,
  context: { baselineSources: DiscoveredSource[] },
): Promise<DiscoveryExecutionStage> {
  const discoveryStartedMs = Date.now();
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const inventorySources = await loadInventorySources(input, selectedPlatforms);
  const inventoryBackedPlatforms = new Set(inventorySources.map((source) => source.platform));
  const inventoryPrimaryPlatforms = new Set<string>(
    Array.from(inventoryBackedPlatforms).filter((platform) => platform === "greenhouse"),
  );
  const unfilteredConfiguredSources = buildConfiguredSources(input).filter(
    (source) => !inventoryPrimaryPlatforms.has(source.platform),
  );
  const configuredSources = filterDiscoveredSources(
    unfilteredConfiguredSources,
    selectedPlatforms,
  );
  const curatedSources = filterDiscoveredSources(
    discoverCatalogSources(input.filters.platforms).filter(
      (source) => !inventoryPrimaryPlatforms.has(source.platform),
    ),
    selectedPlatforms,
  );
  const baselineSources = filterDiscoveredSources(
    dedupeDiscoveredSources(context.baselineSources),
    selectedPlatforms,
  );
  const expansionCandidates = dedupeDiscoveredSources([
    ...inventorySources,
    ...unfilteredConfiguredSources,
    ...curatedSources,
  ]);
  const companyPageExpansionStartedMs = Date.now();
  const companyPageExpandedSources = dedupeDiscoveredSources(
    await expandCompanyPageSources(
      expansionCandidates,
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
        inventorySources.length +
          configuredSources.length +
          curatedSources.length +
          companyPageExpandedSources.length >
          0,
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
  const discoveredBeforeFiltering = dedupeDiscoveredSources([
    ...baselineSources,
    ...companyPageExpandedSources,
    ...publicSources,
  ]);
  const discoveredSources = filterDiscoveredSources(
    discoveredBeforeFiltering,
    selectedPlatforms,
  );
  const publicSourcesOnly = discoveredSources.filter(
    (source) => !baselineSources.some((baseline) => baseline.id === source.id),
  );

  logDiscoveryTrace(input.filters, {
    inventorySources,
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
      inventorySources: inventorySources.length,
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

export function resolvePublicSearchExecutionOptions(
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
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 24),
      maxSources: Math.min(base.maxSources, 50),
      maxLocationClauses: Math.min(base.maxLocationClauses, 8),
      maxDirectJobs: Math.min(base.maxDirectJobs, 12),
      maxRoleQueries: 12,
    };
  }

  // "fast" mode (default) — aggressive caps to finish in ~15-30 seconds
  if (greenhouseFirstOnly) {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 12),
      maxSources: Math.min(base.maxSources, 30),
      maxLocationClauses: Math.min(base.maxLocationClauses, 4),
      maxDirectJobs: Math.min(base.maxDirectJobs, 6),
      maxRoleQueries: 8,
    };
  }

  return {
    ...base,
    maxQueries: Math.min(base.maxQueries, 12),
    maxSources: Math.min(base.maxSources, 30),
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
  void crawlMode;
  void hasBaselineCoverage;
  void platforms;
  return undefined;
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

export async function refreshSourceInventory(input: {
  repository: JobCrawlerRepository;
  now: Date;
  env?: DiscoveryEnvSnapshot;
  fetchImpl?: typeof fetch;
}) {
  const env = input.env ?? getEnv();
  const now = input.now.toISOString();
  const existingRecords = await input.repository.listSourceInventory();
  const carryForwardRecords = existingRecords.map((record) => ({
    ...record,
    lastRefreshedAt: now,
  }));
  const seededRecords = buildSourceInventorySeeds(env).map((record) => ({
    ...record,
    firstSeenAt: now,
    lastSeenAt: now,
    lastRefreshedAt: now,
  }));
  const refreshCandidates = dedupeDiscoveredSources([
    ...existingRecords.map(toDiscoveredSourceFromInventory),
    ...seededRecords.map(toDiscoveredSourceFromInventory),
  ]);
  const expandedRecords = (await expandCompanyPageSources(
    refreshCandidates,
    input.fetchImpl,
  )).map((source, index) =>
    toSourceInventoryRecord(source, {
      now,
      inventoryOrigin: "public_search",
      inventoryRank: 50_000 + index,
    }),
  );

  const persisted = await input.repository.upsertSourceInventory([
    ...carryForwardRecords,
    ...seededRecords,
    ...expandedRecords,
  ]);

  console.info("[discovery:inventory-refresh]", {
    persistedCount: persisted.length,
    platformCounts: persisted.reduce<Record<string, number>>((counts, record) => {
      counts[record.platform] = (counts[record.platform] ?? 0) + 1;
      return counts;
    }, {}),
    greenhouseCount: persisted.filter((record) => record.platform === "greenhouse").length,
    leverCount: persisted.filter((record) => record.platform === "lever").length,
    ashbyCount: persisted.filter((record) => record.platform === "ashby").length,
    smartRecruitersCount: persisted.filter((record) => record.platform === "smartrecruiters").length,
  });

  return persisted;
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

async function loadInventorySources(
  input: DiscoveryExecutionInput,
  selectedPlatforms: Set<string> | null,
) {
  if (!input.repository) {
    return [] as DiscoveredSource[];
  }

  const selectedInventoryPlatforms = selectedPlatforms
    ? Array.from(selectedPlatforms).filter(isInventoryPlatform)
    : undefined;
  const records = await input.repository.listSourceInventory(
    selectedInventoryPlatforms,
  );

  return records.map(toDiscoveredSourceFromInventory);
}

function isInventoryPlatform(
  platform: string,
): platform is Awaited<ReturnType<JobCrawlerRepository["listSourceInventory"]>>[number]["platform"] {
  return (
    platform === "greenhouse" ||
    platform === "lever" ||
    platform === "ashby" ||
    platform === "smartrecruiters" ||
    platform === "company_page" ||
    platform === "workday"
  );
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
    inventorySources: DiscoveredSource[];
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
    inventoryCount: trace.inventorySources.length,
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
      ? "Inventory-backed sources, registry-backed seeds, supplemental catalog sources, and public ATS search all returned zero runnable sources."
      : "Inventory-backed sources, registry-backed seeds, and supplemental catalog sources returned zero runnable sources while public ATS search was disabled.";
}
