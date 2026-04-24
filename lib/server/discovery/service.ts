import "server-only";

import {
  listBackgroundSystemSearchProfiles,
  selectBackgroundSystemSearchProfiles,
} from "@/lib/server/background/constants";
import { slugToLabel } from "@/lib/server/crawler/helpers";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { expandCompanyPageSources } from "@/lib/server/discovery/company-page-expansion";
import { lookupGreenhouseCompanyHint } from "@/lib/server/discovery/greenhouse-registry";
import type { SourceRegistryEntryInput } from "@/lib/server/discovery/source-registry";
import {
  buildSourceInventorySeeds,
  inventoryOriginFromDiscoveryMethod,
  toDiscoveredSourceFromInventory,
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import { discoverSourcesFromPublicSearchDetailed } from "@/lib/server/discovery/public-search";
import { analyzeTitle, normalizeTitleText } from "@/lib/server/title-retrieval";
import { resolveOperationalCrawlerPlatforms, type CrawlMode } from "@/lib/types";
import type { CrawlDiagnostics, SearchFilters } from "@/lib/types";
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
> & {
  sourceRegistryEntries?: SourceRegistryEntryInput[];
};

type DiscoveryRuntime = {
  repository?: JobCrawlerRepository;
  env?: DiscoveryEnvSnapshot;
};

type SourceInventoryExpansionDiagnostics = NonNullable<CrawlDiagnostics["inventoryExpansion"]>;

export type SourceInventoryExpansionResult = {
  inventory: SourceInventoryRecord[];
  diagnostics: SourceInventoryExpansionDiagnostics;
};

const defaultInventoryExpansionSearchesPerCycle = 2;

export function listBackgroundInventoryExpansionPortfolio() {
  return listBackgroundSystemSearchProfiles().map((profile) =>
    copyExpansionFilter(profile.filters),
  );
}

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
    input.filters.title,
    input.filters,
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
        executionStrategy: publicSearchOptions.executionStrategy,
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
  title?: string,
  filters?: Pick<SearchFilters, "country" | "state" | "city">,
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

  const titleAnalysis = analyzeTitle(normalizeTitleText(title));
  const sparseAiScience = isSparseAiScienceSearch(title, titleAnalysis);
  const canadaHighDemand = resolveCanadaHighDemandDiscoveryProfile({
    title,
    filters,
    titleFamily: titleAnalysis.family,
    titleConcept: titleAnalysis.primaryConceptId,
  });
  const requestedMode = crawlMode ?? "fast";
  const baseStrategy = {
    requestedMode,
    title,
    country: filters?.country,
    state: filters?.state,
    city: filters?.city,
    titleFamily: titleAnalysis.family,
    titleConcept: titleAnalysis.primaryConceptId,
    canadaHighDemandRole: canadaHighDemand.matches,
  };

  if (crawlMode === "deep") {
    return {
      ...base,
      executionStrategy: {
        ...baseStrategy,
        effectiveMode: "deep",
        reason: "Requested crawl mode is deep, so public search uses the full configured discovery budget.",
      },
    };
  }

  if (crawlMode === "balanced" && canadaHighDemand.matches) {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, 48),
      maxSources: Math.min(base.maxSources, 80),
      maxLocationClauses: Math.min(base.maxLocationClauses, 20),
      maxDirectJobs: Math.min(base.maxDirectJobs, 18),
      maxRoleQueries: 18,
      executionStrategy: {
        ...baseStrategy,
        effectiveMode: "canada_high_demand_deepened",
        reason: canadaHighDemand.reason,
      },
    };
  }

  if (crawlMode === "balanced") {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, sparseAiScience ? 32 : 24),
      maxSources: Math.min(base.maxSources, 50),
      maxLocationClauses: Math.min(base.maxLocationClauses, sparseAiScience ? 20 : 8),
      maxDirectJobs: Math.min(base.maxDirectJobs, sparseAiScience ? 16 : 12),
      maxRoleQueries: sparseAiScience ? 16 : 12,
      executionStrategy: {
        ...baseStrategy,
        effectiveMode: "balanced",
        reason: sparseAiScience
          ? "Balanced mode uses a modest sparse AI/science uplift while staying below deep-mode caps."
          : "Balanced mode uses conservative public-search caps for general profiles.",
      },
    };
  }

  // "fast" mode (default) — aggressive caps to finish in ~15-30 seconds
  if (greenhouseFirstOnly) {
    return {
      ...base,
      maxQueries: Math.min(base.maxQueries, sparseAiScience ? 18 : 12),
      maxSources: Math.min(base.maxSources, 30),
      maxLocationClauses: Math.min(base.maxLocationClauses, sparseAiScience ? 18 : 4),
      maxDirectJobs: Math.min(base.maxDirectJobs, sparseAiScience ? 10 : 6),
      maxRoleQueries: sparseAiScience ? 12 : 8,
      executionStrategy: {
        ...baseStrategy,
        effectiveMode: "fast",
        reason: sparseAiScience
          ? "Fast Greenhouse-only discovery uses a small sparse AI/science uplift."
          : "Fast Greenhouse-only discovery uses the smallest direct-job and location budgets.",
      },
    };
  }

  return {
    ...base,
    maxQueries: Math.min(base.maxQueries, sparseAiScience ? 18 : 12),
    maxSources: Math.min(base.maxSources, 30),
    maxLocationClauses: Math.min(base.maxLocationClauses, sparseAiScience ? 18 : 4),
    maxDirectJobs: Math.min(base.maxDirectJobs, sparseAiScience ? 12 : 8),
    maxRoleQueries: sparseAiScience ? 12 : 8,
    executionStrategy: {
      ...baseStrategy,
      effectiveMode: "fast",
      reason: sparseAiScience
        ? "Fast discovery uses a small sparse AI/science uplift."
        : "Fast discovery uses conservative public-search caps.",
    },
  };
}

function isSparseAiScienceSearch(
  title?: string,
  analysis = analyzeTitle(normalizeTitleText(title)),
) {
  const normalized = normalizeTitleText(title);
  if (!normalized) {
    return false;
  }

  return (
    analysis.family === "ai_ml_science" ||
    /\b(?:applied scientist|research scientist|ai engineer|llm engineer|large language model|generative ai|genai)\b/.test(
      normalized,
    )
  );
}

function resolveCanadaHighDemandDiscoveryProfile(input: {
  title?: string;
  filters?: Pick<SearchFilters, "country" | "state" | "city">;
  titleFamily?: string;
  titleConcept?: string;
}) {
  const country = normalizeDiscoveryText(input.filters?.country);
  const city = normalizeDiscoveryText(input.filters?.city);
  const title = normalizeTitleText(input.title);
  const isCanada =
    country === "canada" ||
    city === "toronto" ||
    city === "vancouver" ||
    city === "montreal" ||
    city === "calgary" ||
    /\bcanada\b/.test(title);

  if (!isCanada) {
    return {
      matches: false,
      reason: "Profile is not Canada-scoped, so Canada high-demand discovery caps do not apply.",
    };
  }

  const conceptMatches =
    input.titleConcept === "software_engineer" ||
    input.titleConcept === "data_analyst" ||
    input.titleConcept === "business_analyst" ||
    input.titleConcept === "product_manager" ||
    input.titleConcept === "technical_product_manager" ||
    input.titleConcept === "machine_learning_engineer" ||
    input.titleConcept === "ai_engineer" ||
    input.titleConcept === "applied_scientist" ||
    input.titleConcept === "research_scientist";
  const phraseMatches =
    /\b(?:software engineer|software developer|software development engineer|data analyst|business analyst|product manager|machine learning engineer|ml engineer|ai engineer|artificial intelligence engineer|applied scientist|research scientist)\b/.test(
      title,
    );
  const familyMatches =
    input.titleFamily === "ai_ml_science" &&
    /\b(?:machine learning|ml|ai|artificial intelligence|applied scientist|research scientist)\b/.test(
      title,
    );

  if (!conceptMatches && !phraseMatches && !familyMatches) {
    return {
      matches: false,
      reason: "Canada profile is outside the configured high-demand role families, so balanced or fast caps remain in force.",
    };
  }

  return {
    matches: true,
    reason:
      "Canada high-demand role profile matched; using partially deep public-search budgets to improve lower-density Canada source recall without full deep-mode caps.",
  };
}

function isCanadaHighDemandDiscoveryFilters(filters: SearchFilters) {
  const analysis = analyzeTitle(normalizeTitleText(filters.title));
  return resolveCanadaHighDemandDiscoveryProfile({
    title: filters.title,
    filters,
    titleFamily: analysis.family,
    titleConcept: analysis.primaryConceptId,
  }).matches;
}

function normalizeDiscoveryText(value?: string) {
  return (value ?? "").trim().toLowerCase();
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
    existingPlatformCounts: summarizeInventoryRecordPlatformCounts(existingRecords),
    seededPlatformCounts: summarizeInventoryRecordPlatformCounts(seededRecords),
    expandedPlatformCounts: summarizeInventoryRecordPlatformCounts(expandedRecords),
    persistedCount: persisted.length,
    platformCounts: persisted.reduce<Record<string, number>>((counts, record) => {
      counts[record.platform] = (counts[record.platform] ?? 0) + 1;
      return counts;
    }, {}),
    greenhouseCount: persisted.filter((record) => record.platform === "greenhouse").length,
    leverCount: persisted.filter((record) => record.platform === "lever").length,
    ashbyCount: persisted.filter((record) => record.platform === "ashby").length,
    smartRecruitersCount: persisted.filter((record) => record.platform === "smartrecruiters").length,
    workdayCount: persisted.filter((record) => record.platform === "workday").length,
  });

  return persisted;
}

export async function expandSourceInventory(input: {
  repository: JobCrawlerRepository;
  now: Date;
  env?: DiscoveryEnvSnapshot;
  fetchImpl?: typeof fetch;
  intervalMs: number;
  maxSources: number;
  refreshedInventory?: SourceInventoryRecord[];
  maxExpansionSearches?: number;
  expansionFilters?: SearchFilters[];
}): Promise<SourceInventoryExpansionResult> {
  const env = input.env ?? getEnv();
  const beforeRecords = input.refreshedInventory ?? await input.repository.listSourceInventory();
  const beforeIds = new Set(beforeRecords.map((record) => record._id));
  const selectedFilters =
    input.expansionFilters?.map(copyExpansionFilter) ??
    selectBackgroundInventoryExpansionFilters({
      now: input.now,
      intervalMs: input.intervalMs,
      maxSearches: input.maxExpansionSearches ?? defaultInventoryExpansionSearchesPerCycle,
    });
  const baseDiagnostics: SourceInventoryExpansionDiagnostics = {
    beforeCount: beforeRecords.length,
    afterRefreshCount: beforeRecords.length,
    afterExpansionCount: beforeRecords.length,
    selectedSearches: selectedFilters.length,
    candidateSources: 0,
    newSourcesAdded: 0,
    selectedSearchTitles: selectedFilters.map(formatExpansionFilterLabel).slice(0, 12),
    selectedSearchFilters: selectedFilters.map(copyExpansionFilter).slice(0, 12),
    selectedSourceIds: [],
    newSourceIds: [],
    platformCountsBefore: summarizeInventoryRecordPlatformCounts(beforeRecords),
    platformCountsAfter: summarizeInventoryRecordPlatformCounts(beforeRecords),
    searchDiagnostics: [],
  };

  if (!env.PUBLIC_SEARCH_DISCOVERY_ENABLED) {
    return {
      inventory: beforeRecords,
      diagnostics: {
        ...baseDiagnostics,
        selectedSearches: 0,
        skippedReason: "public_search_disabled",
      },
    };
  }

  if (selectedFilters.length === 0) {
    return {
      inventory: beforeRecords,
      diagnostics: {
        ...baseDiagnostics,
        skippedReason: "no_expansion_searches_selected",
      },
    };
  }

  const hasCanadaHighDemandExpansion = selectedFilters.some(isCanadaHighDemandDiscoveryFilters);
  const sourceBudget = Math.max(
    1,
    Math.min(
      env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
      Math.max(
        input.maxSources,
        selectedFilters.length * 4,
        hasCanadaHighDemandExpansion ? selectedFilters.length * 16 : 0,
      ),
    ),
  );
  const perSearchSourceBudget = Math.max(
    1,
    Math.ceil(sourceBudget / selectedFilters.length),
  );
  const candidates = new Map<string, DiscoveredSource>();
  const searchDiagnostics: SourceInventoryExpansionDiagnostics["searchDiagnostics"] = [];

  for (const filters of selectedFilters) {
    const remainingBudget = sourceBudget - candidates.size;
    if (remainingBudget <= 0) {
      break;
    }

    const useCanadaHighDemandExpansion = isCanadaHighDemandDiscoveryFilters(filters);
    const discovery = await discoverSourcesDetailed({
      filters,
      now: input.now,
      fetchImpl: input.fetchImpl,
      repository: input.repository,
      env: {
        ...env,
        PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: Math.min(
          env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS,
          useCanadaHighDemandExpansion ? 10 : 8,
        ),
        PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: Math.min(
          useCanadaHighDemandExpansion
            ? Math.max(perSearchSourceBudget, Math.min(80, remainingBudget))
            : perSearchSourceBudget,
          remainingBudget,
        ),
        PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: Math.min(
          env.PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES,
          useCanadaHighDemandExpansion ? 48 : 16,
        ),
        GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: Math.min(
          env.GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES,
          useCanadaHighDemandExpansion ? 20 : 6,
        ),
      },
    });

    searchDiagnostics.push({
      title: filters.title,
      country: filters.country,
      state: filters.state,
      city: filters.city,
      discoveredSources: discovery.sources.length,
      publicSources: discovery.diagnostics.publicSources,
      publicJobs: discovery.diagnostics.publicJobs,
      publicSearch: discovery.diagnostics.publicSearch,
    });

    for (const source of discovery.sources) {
      if (
        source.discoveryMethod !== "future_search" ||
        beforeIds.has(source.id) ||
        source.platform === "unknown"
      ) {
        continue;
      }

      if (!candidates.has(source.id)) {
        candidates.set(source.id, source);
      }

      if (candidates.size >= sourceBudget) {
        break;
      }
    }
  }

  const now = input.now.toISOString();
  const candidateSources = Array.from(candidates.values());
  if (candidateSources.length > 0) {
    await input.repository.upsertSourceInventory(
      candidateSources.map((source, index) =>
        toSourceInventoryRecord(source, {
          now,
          inventoryOrigin: inventoryOriginFromDiscoveryMethod(source.discoveryMethod),
          inventoryRank: 60_000 + index,
        }),
      ),
    );
  }

  const afterRecords = await input.repository.listSourceInventory();
  const newSourceIds = afterRecords
    .filter((record) => !beforeIds.has(record._id))
    .map((record) => record._id);

  return {
    inventory: afterRecords,
    diagnostics: {
      ...baseDiagnostics,
      candidateSources: candidateSources.length,
      newSourcesAdded: newSourceIds.length,
      selectedSourceIds: candidateSources.slice(0, 12).map((source) => source.id),
      newSourceIds: newSourceIds.slice(0, 12),
      afterExpansionCount: afterRecords.length,
      platformCountsAfter: summarizeInventoryRecordPlatformCounts(afterRecords),
      searchDiagnostics: searchDiagnostics.slice(0, 12),
    },
  };
}

export function selectBackgroundInventoryExpansionFilters(input: {
  now: Date;
  intervalMs: number;
  maxSearches: number;
}) {
  return selectBackgroundSystemSearchProfiles({
    now: input.now,
    intervalMs: input.intervalMs,
    maxProfiles: input.maxSearches,
  }).map((profile) => copyExpansionFilter(profile.filters));
}

function copyExpansionFilter(filters: SearchFilters): SearchFilters {
  return {
    title: filters.title,
    ...(filters.country ? { country: filters.country } : {}),
    ...(filters.state ? { state: filters.state } : {}),
    ...(filters.city ? { city: filters.city } : {}),
    ...(filters.platforms ? { platforms: [...filters.platforms] } : {}),
    ...(filters.crawlMode ? { crawlMode: filters.crawlMode } : {}),
  };
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

function summarizeInventoryRecordPlatformCounts(records: SourceInventoryRecord[]) {
  return records.reduce<Record<string, number>>((counts, record) => {
    counts[record.platform] = (counts[record.platform] ?? 0) + 1;
    return counts;
  }, {});
}

function formatExpansionFilterLabel(filters: SearchFilters) {
  return [filters.title, filters.city, filters.state, filters.country]
    .filter(Boolean)
    .join(" / ");
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
