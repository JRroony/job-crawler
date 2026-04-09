import "server-only";

import { slugToLabel } from "@/lib/server/crawler/helpers";
import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { lookupGreenhouseCompanyHint } from "@/lib/server/discovery/greenhouse-registry";
import { discoverSourcesFromPublicSearch } from "@/lib/server/discovery/public-search";
import { resolveOperationalCrawlerPlatforms } from "@/lib/types";
import type {
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
>;

export const defaultDiscoveryService: DiscoveryService = {
  async discover(input) {
    return discoverSources({
      ...input,
      env: getEnv(),
    });
  },
};

export async function discoverSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
  const configuredSources = discoverConfiguredSources(input);
  const curatedSources = filterDiscoveredSources(
    discoverCatalogSources(input.filters.platforms),
    selectedPlatforms,
  );
  const publicSources = input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED
    ? await discoverSourcesFromPublicSearch(input.filters, {
        fetchImpl: input.fetchImpl,
        maxResultsPerQuery: input.env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS,
      })
    : [];
  const discoveredBeforeFiltering = dedupeDiscoveredSources([
    ...configuredSources,
    ...curatedSources,
    ...publicSources,
  ]);
  const discoveredSources = filterDiscoveredSources(
    discoveredBeforeFiltering,
    selectedPlatforms,
  );

  logDiscoveryTrace(input.filters, {
    configuredSources,
    curatedSources,
    publicSources,
    discoveredBeforeFiltering,
    discoveredSources,
    publicSearchEnabled: input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED,
  });

  return discoveredSources;
}

export function discoverConfiguredSources(input: DiscoveryInput & { env: DiscoveryEnvSnapshot }) {
  const selectedPlatforms = input.filters.platforms
    ? new Set<string>(resolveOperationalCrawlerPlatforms(input.filters.platforms))
    : null;
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

  const discoveredSources = dedupeDiscoveredSources(candidates);

  // Discovery is the first place we can honestly narrow the platform scope:
  // when a user selects provider families, we stop surfacing configured sources
  // for the unselected families before provider routing even begins.
  if (!selectedPlatforms) {
    return discoveredSources;
  }

  return filterDiscoveredSources(discoveredSources, selectedPlatforms);
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
    publicSources: DiscoveredSource[];
    discoveredBeforeFiltering: DiscoveredSource[];
    discoveredSources: DiscoveredSource[];
    publicSearchEnabled: boolean;
  },
) {
  console.info("[discovery:summary]", {
    filters,
    configuredCount: trace.configuredSources.length,
    curatedCount: trace.curatedSources.length,
    publicCount: trace.publicSources.length,
    discoveredBeforeFilteringCount: trace.discoveredBeforeFiltering.length,
    discoveredAfterFilteringCount: trace.discoveredSources.length,
    platformCounts: summarizePlatformCounts(trace.discoveredSources),
    discoveryMethodCounts: summarizeDiscoveryMethodCounts(trace.discoveredSources),
    publicSearchEnabled: trace.publicSearchEnabled,
  });

  if (trace.discoveredSources.length > 0) {
    return;
  }

  const reason =
    trace.discoveredBeforeFiltering.length > 0
      ? "Selected platforms filtered every discovered source before provider routing."
      : trace.publicSearchEnabled
        ? "Registry-backed seeds, supplemental catalog sources, and public ATS search all returned zero runnable sources."
        : "Registry-backed seeds and supplemental catalog sources returned zero runnable sources while public ATS search was disabled.";

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
