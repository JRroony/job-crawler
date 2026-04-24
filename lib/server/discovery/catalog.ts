import "server-only";

import {
  getDefaultSourceRegistryEntries,
  sourceRegistryEntryToDiscoveredSource,
} from "@/lib/server/discovery/source-registry";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import {
  resolveOperationalCrawlerPlatforms,
  type CrawlerPlatform,
} from "@/lib/types";

export function discoverCatalogSources(
  platforms?: readonly CrawlerPlatform[],
): DiscoveredSource[] {
  const selectedPlatforms = new Set(
    resolveOperationalCrawlerPlatforms(platforms ? [...platforms] : undefined),
  );

  return getDefaultSourceRegistryEntries()
    .filter((entry) => selectedPlatforms.has(entry.platform))
    .map((entry) => ({
      ...sourceRegistryEntryToDiscoveredSource(entry),
      confidence: "medium" as const,
      discoveryMethod: "curated_catalog" as const,
    }));
}
