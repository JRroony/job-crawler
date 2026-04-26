import "server-only";

import type { CrawlProvider } from "@/lib/server/providers/types";
import {
  activeCrawlerPlatforms,
  resolveOperationalCrawlerPlatforms,
  type ActiveCrawlerPlatform,
  type CrawlerPlatform,
  type CrawlMode,
} from "@/lib/types";

export const fastCrawlerProviders = [
  "greenhouse",
  "lever",
  "ashby",
  "smartrecruiters",
] as const satisfies readonly ActiveCrawlerPlatform[];

export const slowCrawlerProviders = [
  "workday",
  "company_page",
] as const satisfies readonly ActiveCrawlerPlatform[];

export type CrawlProviderTier = "fast" | "slow";

const fastProviderSet = new Set<ActiveCrawlerPlatform>(fastCrawlerProviders);
const slowProviderSet = new Set<ActiveCrawlerPlatform>(slowCrawlerProviders);

export type TieredProviderSelection = {
  selectedProviders: CrawlProvider[];
  selectedFastProviders: ActiveCrawlerPlatform[];
  selectedSlowProviders: ActiveCrawlerPlatform[];
  skippedSlowProviders: ActiveCrawlerPlatform[];
  reason: string;
};

export function selectProvidersForTieredCrawl(input: {
  providers: CrawlProvider[];
  selectedPlatforms?: CrawlerPlatform[];
  crawlMode?: CrawlMode;
  includeSlowProviders: boolean;
}): TieredProviderSelection {
  const allowedPlatforms = new Set(
    resolveOperationalCrawlerPlatforms(input.selectedPlatforms),
  );
  const operationalProviders = input.providers.filter(
    (provider): provider is CrawlProvider & { provider: ActiveCrawlerPlatform } =>
      isOperationalProvider(provider.provider) && allowedPlatforms.has(provider.provider),
  );
  const selectedProviders = operationalProviders.filter((provider) =>
    input.includeSlowProviders || resolveProviderTier(provider.provider) === "fast",
  );
  const selectedFastProviders = selectedProviders
    .filter((provider) => resolveProviderTier(provider.provider) === "fast")
    .map((provider) => provider.provider);
  const selectedSlowProviders = selectedProviders
    .filter((provider) => resolveProviderTier(provider.provider) === "slow")
    .map((provider) => provider.provider);
  const skippedSlowProviders = operationalProviders
    .filter((provider) => resolveProviderTier(provider.provider) === "slow")
    .filter((provider) => !selectedProviders.includes(provider))
    .map((provider) => provider.provider);

  return {
    selectedProviders,
    selectedFastProviders,
    selectedSlowProviders,
    skippedSlowProviders,
    reason: input.includeSlowProviders
      ? "deep_or_background_crawl_includes_slow_providers"
      : "request_time_crawl_defaults_to_fast_providers",
  };
}

export function resolveProviderTier(provider: ActiveCrawlerPlatform): CrawlProviderTier {
  if (fastProviderSet.has(provider)) {
    return "fast";
  }

  if (slowProviderSet.has(provider)) {
    return "slow";
  }

  return "slow";
}

export function isSlowCrawlerProvider(provider: CrawlProvider["provider"]) {
  return isOperationalProvider(provider) && slowProviderSet.has(provider);
}

function isOperationalProvider(
  provider: CrawlProvider["provider"],
): provider is ActiveCrawlerPlatform {
  return activeCrawlerPlatforms.includes(provider as ActiveCrawlerPlatform);
}
