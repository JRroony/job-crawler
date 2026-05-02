import "server-only";

import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  inventoryOriginFromDiscoveryMethod,
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import { discoverSourcesFromPublicSearchDetailed } from "@/lib/server/discovery/public-search";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { getEnv, type AppEnv } from "@/lib/server/env";
import type { SearchFilters } from "@/lib/types";

export const supportedSourceDiscoveryJobFamilies = [
  "Software Engineer",
  "Data Analyst",
  "Product Manager",
  "Machine Learning Engineer",
  "Business Analyst",
  "Backend Engineer",
  "Full Stack Engineer",
  "Data Scientist",
] as const;

export type SourceDiscoveryJobFamily =
  (typeof supportedSourceDiscoveryJobFamilies)[number];

type SourceDiscoveryEnvSnapshot = Pick<
  AppEnv,
  | "SOURCE_DISCOVERY_ENABLED"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES"
  | "PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES"
  | "PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY"
  | "GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES"
>;

export type RecurringSourceDiscoveryStats = {
  beforeCount: number;
  afterCount: number;
  discoveredSourceCount: number;
  newSourceCount: number;
  updatedSourceCount: number;
  duplicateSourceCount: number;
  invalidSourceCount: number;
  platformCounts: Record<string, number>;
  discoveredSourceIds: string[];
  newSourceIds: string[];
  updatedSourceIds: string[];
  discoveryQueries: string[];
  skippedReason?: "source_discovery_disabled";
};

export type RecurringSourceDiscoveryResult = {
  inventory: SourceInventoryRecord[];
  stats: RecurringSourceDiscoveryStats;
};

export async function runRecurringSourceDiscovery(input: {
  repository: JobCrawlerRepository;
  now?: Date;
  env?: SourceDiscoveryEnvSnapshot;
  fetchImpl?: typeof fetch;
  candidateUrls?: string[];
  maxSources?: number;
  jobFamilies?: readonly SourceDiscoveryJobFamily[];
}): Promise<RecurringSourceDiscoveryResult> {
  const env = input.env ?? getEnv();
  const now = input.now ?? new Date();
  const beforeRecords = await input.repository.listSourceInventory();
  const beforeById = new Map(beforeRecords.map((record) => [record._id, record]));
  const baseStats: RecurringSourceDiscoveryStats = {
    beforeCount: beforeRecords.length,
    afterCount: beforeRecords.length,
    discoveredSourceCount: 0,
    newSourceCount: 0,
    updatedSourceCount: 0,
    duplicateSourceCount: 0,
    invalidSourceCount: 0,
    platformCounts: {},
    discoveredSourceIds: [],
    newSourceIds: [],
    updatedSourceIds: [],
    discoveryQueries: buildRecurringSourceDiscoveryQueries(
      input.jobFamilies ?? supportedSourceDiscoveryJobFamilies,
    ),
  };

  if (!env.SOURCE_DISCOVERY_ENABLED) {
    return {
      inventory: beforeRecords,
      stats: {
        ...baseStats,
        skippedReason: "source_discovery_disabled",
      },
    };
  }

  const discovered = await discoverRecurringSources({
    now,
    env,
    fetchImpl: input.fetchImpl,
    candidateUrls: input.candidateUrls,
    maxSources: input.maxSources,
    jobFamilies: input.jobFamilies ?? supportedSourceDiscoveryJobFamilies,
  });
  const { uniqueSources, duplicateSourceCount, invalidSourceCount } =
    normalizeSourceDiscoveryCandidates(discovered);
  const sourceBudget = Math.max(
    1,
    input.maxSources ?? env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
  );
  const selectedSources = uniqueSources.slice(0, sourceBudget);
  const observedAt = now.toISOString();

  if (selectedSources.length > 0) {
    await input.repository.upsertSourceInventory(
      selectedSources.map((source, index) =>
        toSourceInventoryRecord(source, {
          now: observedAt,
          inventoryOrigin: inventoryOriginFromDiscoveryMethod(source.discoveryMethod),
          inventoryRank: 70_000 + index,
        }),
      ),
    );
  }

  const afterRecords = await input.repository.listSourceInventory();
  const afterIds = new Set(afterRecords.map((record) => record._id));
  const discoveredSourceIds = selectedSources.map((source) => source.id);
  const newSourceIds = discoveredSourceIds.filter((id) => !beforeById.has(id) && afterIds.has(id));
  const updatedSourceIds = discoveredSourceIds.filter((id) => beforeById.has(id) && afterIds.has(id));
  const stats: RecurringSourceDiscoveryStats = {
    ...baseStats,
    afterCount: afterRecords.length,
    discoveredSourceCount: selectedSources.length,
    newSourceCount: newSourceIds.length,
    updatedSourceCount: updatedSourceIds.length,
    duplicateSourceCount,
    invalidSourceCount,
    platformCounts: summarizeSourcePlatformCounts(selectedSources),
    discoveredSourceIds,
    newSourceIds,
    updatedSourceIds,
  };

  console.info("[source-discovery:cycle-result]", stats);

  return {
    inventory: afterRecords,
    stats,
  };
}

export function buildRecurringSourceDiscoveryFilters(
  jobFamilies: readonly SourceDiscoveryJobFamily[] = supportedSourceDiscoveryJobFamilies,
): SearchFilters[] {
  return jobFamilies.map((title) => ({
    title,
    country: "United States",
    crawlMode: "balanced",
    platforms: [
      "greenhouse",
      "lever",
      "ashby",
      "workday",
      "smartrecruiters",
      "company_page",
    ],
  }));
}

export function buildRecurringSourceDiscoveryQueries(
  jobFamilies: readonly SourceDiscoveryJobFamily[] = supportedSourceDiscoveryJobFamilies,
) {
  return buildRecurringSourceDiscoveryFilters(jobFamilies).map((filters) =>
    `${filters.title ?? "jobs"} ${filters.country ?? ""}`.trim(),
  );
}

async function discoverRecurringSources(input: {
  now: Date;
  env: SourceDiscoveryEnvSnapshot;
  fetchImpl?: typeof fetch;
  candidateUrls?: string[];
  maxSources?: number;
  jobFamilies: readonly SourceDiscoveryJobFamily[];
}) {
  const directSources = (input.candidateUrls ?? []).map((url) =>
    classifySourceCandidate({
      url,
      discoveryMethod: "future_search",
    }),
  );

  if (input.candidateUrls && input.candidateUrls.length > 0) {
    return directSources;
  }

  const sourceBudget = Math.max(
    1,
    input.maxSources ?? input.env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
  );
  const filters = buildRecurringSourceDiscoveryFilters(input.jobFamilies);
  const perFamilyBudget = Math.max(1, Math.ceil(sourceBudget / filters.length));
  const sources = new Map<string, DiscoveredSource>();

  for (const filter of filters) {
    if (sources.size >= sourceBudget) {
      break;
    }

    const remainingBudget = sourceBudget - sources.size;
    const discovery = await discoverSourcesFromPublicSearchDetailed(filter, {
      fetchImpl: input.fetchImpl,
      maxResultsPerQuery: Math.min(input.env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS, 10),
      maxSources: Math.min(perFamilyBudget, remainingBudget),
      maxQueries: Math.min(input.env.PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES, 24),
      queryConcurrency: input.env.PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY,
      maxGreenhouseLocationClauses: Math.min(
        input.env.GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES,
        8,
      ),
      maxDirectJobs: Math.min(perFamilyBudget, 8),
      maxRoleQueries: 12,
      executionStrategy: {
        requestedMode: "balanced",
        effectiveMode: "source_discovery",
        title: filter.title,
        country: filter.country,
        canadaHighDemandRole: false,
        reason:
          "Recurring source discovery uses supported job families to find ATS boards and career pages independently of job ingestion.",
      },
    });

    for (const source of discovery.sources) {
      if (!sources.has(source.id)) {
        sources.set(source.id, source);
      }
      if (sources.size >= sourceBudget) {
        break;
      }
    }
  }

  return [...directSources, ...sources.values()];
}

function normalizeSourceDiscoveryCandidates(sources: DiscoveredSource[]) {
  const uniqueSources: DiscoveredSource[] = [];
  const seen = new Set<string>();
  let duplicateSourceCount = 0;
  let invalidSourceCount = 0;

  for (const source of sources) {
    if (source.platform === "unknown") {
      invalidSourceCount += 1;
      continue;
    }

    if (seen.has(source.id)) {
      duplicateSourceCount += 1;
      continue;
    }

    seen.add(source.id);
    uniqueSources.push(source);
  }

  return {
    uniqueSources,
    duplicateSourceCount,
    invalidSourceCount,
  };
}

function summarizeSourcePlatformCounts(sources: DiscoveredSource[]) {
  return sources.reduce<Record<string, number>>((counts, source) => {
    counts[source.platform] = (counts[source.platform] ?? 0) + 1;
    return counts;
  }, {});
}
