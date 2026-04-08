import "server-only";

import { slugToLabel } from "@/lib/server/crawler/helpers";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
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
  const configuredSources = discoverConfiguredSources(input);
  if (!input.env.PUBLIC_SEARCH_DISCOVERY_ENABLED) {
    return configuredSources;
  }

  const publicSources = await discoverSourcesFromPublicSearch(input.filters, {
    fetchImpl: input.fetchImpl,
    maxResultsPerQuery: input.env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS,
  });

  return dedupeDiscoveredSources([...configuredSources, ...publicSources]);
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
        companyHint: slugToLabel(token),
        confidence: "high",
        discoveryMethod: "configured_env",
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

  return discoveredSources.filter((source) => selectedPlatforms.has(source.platform));
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
