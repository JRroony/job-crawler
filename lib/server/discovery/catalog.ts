import "server-only";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import {
  resolveOperationalCrawlerPlatforms,
  type ActiveCrawlerPlatform,
  type CrawlerPlatform,
} from "@/lib/types";

type CatalogPlatform = Extract<ActiveCrawlerPlatform, "greenhouse" | "lever" | "ashby">;

type CatalogEntry = {
  platform: CatalogPlatform;
  token: string;
  companyHint: string;
};

const curatedPublicSourceCatalog: CatalogEntry[] = [
  {
    platform: "greenhouse",
    token: "openai",
    companyHint: "OpenAI",
  },
  {
    platform: "greenhouse",
    token: "stripe",
    companyHint: "Stripe",
  },
  {
    platform: "greenhouse",
    token: "coinbase",
    companyHint: "Coinbase",
  },
  {
    platform: "lever",
    token: "figma",
    companyHint: "Figma",
  },
  {
    platform: "lever",
    token: "plaid",
    companyHint: "Plaid",
  },
  {
    platform: "lever",
    token: "robinhood",
    companyHint: "Robinhood",
  },
  {
    platform: "ashby",
    token: "notion",
    companyHint: "Notion",
  },
  {
    platform: "ashby",
    token: "ramp",
    companyHint: "Ramp",
  },
  {
    platform: "ashby",
    token: "replit",
    companyHint: "Replit",
  },
];

export function discoverCatalogSources(
  platforms?: readonly CrawlerPlatform[],
): DiscoveredSource[] {
  const selectedPlatforms = new Set(
    resolveOperationalCrawlerPlatforms(platforms ? [...platforms] : undefined),
  );

  return curatedPublicSourceCatalog
    .filter((entry) => selectedPlatforms.has(entry.platform))
    .map((entry) =>
      classifySourceCandidate({
        url: buildCatalogUrl(entry.platform, entry.token),
        token: entry.token,
        companyHint: entry.companyHint,
        confidence: "medium",
        discoveryMethod: "curated_catalog",
      }),
    );
}

function buildCatalogUrl(platform: CatalogPlatform, token: string) {
  if (platform === "greenhouse") {
    return `https://boards.greenhouse.io/${token}`;
  }

  if (platform === "lever") {
    return `https://jobs.lever.co/${token}`;
  }

  return `https://jobs.ashbyhq.com/${token}`;
}
