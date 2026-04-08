import "server-only";

import { safeFetchText } from "@/lib/server/net/fetcher";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { ActiveCrawlerPlatform, SearchFilters } from "@/lib/types";

type PublicSearchOptions = {
  fetchImpl?: typeof fetch;
  maxResultsPerQuery: number;
  timeoutMs?: number;
};

const duckDuckGoHtmlBaseUrl = "https://html.duckduckgo.com/html/";
const defaultSearchTimeoutMs = 8_000;

const searchablePlatforms: ActiveCrawlerPlatform[] = [
  "greenhouse",
  "lever",
  "ashby",
];

const searchHostQueries: Record<Extract<ActiveCrawlerPlatform, "greenhouse" | "lever" | "ashby">, string[]> = {
  greenhouse: [
    "site:boards.greenhouse.io",
    "site:job-boards.greenhouse.io",
  ],
  lever: ["site:jobs.lever.co"],
  ashby: ["site:jobs.ashbyhq.com"],
};

export async function discoverSourcesFromPublicSearch(
  filters: SearchFilters,
  options: PublicSearchOptions,
) {
  const queries = buildPublicSearchQueries(filters, options.maxResultsPerQuery);
  if (queries.length === 0) {
    return [];
  }

  const discovered = new Map<string, DiscoveredSource>();

  for (const query of queries) {
    const urls = await searchPublicWeb(query.query, {
      fetchImpl: options.fetchImpl,
      timeoutMs: options.timeoutMs ?? defaultSearchTimeoutMs,
      limit: query.limit,
    });

    for (const url of urls) {
      const source = classifySourceCandidate({
        url,
        confidence: "medium",
        discoveryMethod: "future_search",
      });

      if (source.platform !== query.platform) {
        continue;
      }

      if (!discovered.has(source.id)) {
        discovered.set(source.id, source);
      }
    }
  }

  return Array.from(discovered.values());
}

function buildPublicSearchQueries(
  filters: SearchFilters,
  maxResultsPerQuery: number,
) {
  const requestedPlatforms = filters.platforms ?? searchablePlatforms;
  const selectedPlatforms = requestedPlatforms.filter(
    (
      platform,
    ): platform is Extract<ActiveCrawlerPlatform, "greenhouse" | "lever" | "ashby"> =>
      platform === "greenhouse" || platform === "lever" || platform === "ashby",
  );
  const roleTerms = buildRoleTerms(filters.title);
  if (!roleTerms) {
    return [];
  }

  const locationTerms = buildLocationTerms(filters);

  return selectedPlatforms.flatMap((platform) =>
    searchHostQueries[platform].map((hostQuery: string) => ({
      platform,
      limit: maxResultsPerQuery,
      query: [hostQuery, roleTerms, locationTerms].filter(Boolean).join(" "),
    })),
  );
}

function buildRoleTerms(title: string) {
  const normalized = title.trim();
  if (!normalized) {
    return "";
  }

  if (!normalized.includes(" ")) {
    return `"${normalized}"`;
  }

  return `"${normalized}"`;
}

function buildLocationTerms(filters: SearchFilters) {
  const parts = [filters.city, filters.state, filters.country]
    .map((value) => value?.trim())
    .filter(Boolean) as string[];

  return parts.map((part) => `"${part}"`).join(" ");
}

async function searchPublicWeb(
  query: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) {
  const url = new URL(duckDuckGoHtmlBaseUrl);
  url.searchParams.set("q", query);

  const result = await safeFetchText(url, {
    fetchImpl: options.fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": "job-crawler/0.1 (+public-source-discovery)",
    },
    cache: "no-store",
    timeoutMs: options.timeoutMs,
    retries: 1,
  });

  if (!result.ok || !result.data) {
    return [];
  }

  return extractSearchResultUrls(result.data).slice(0, options.limit);
}

function extractSearchResultUrls(html: string) {
  const discovered = new Set<string>();
  const hrefMatches = Array.from(
    html.matchAll(/href=["']([^"'#]+)["']/gi),
  );

  for (const match of hrefMatches) {
    const rawHref = match[1];
    const resolved = resolveSearchResultUrl(rawHref);
    if (!resolved) {
      continue;
    }

    discovered.add(resolved);
  }

  return Array.from(discovered);
}

function resolveSearchResultUrl(rawHref: string) {
  const trimmed = rawHref.trim();
  if (!trimmed) {
    return undefined;
  }

  try {
    const direct = new URL(trimmed, duckDuckGoHtmlBaseUrl);
    const uddg = direct.searchParams.get("uddg");
    if (uddg) {
      const decoded = decodeURIComponent(uddg);
      return normalizeCandidateUrl(decoded);
    }

    return normalizeCandidateUrl(direct.toString());
  } catch {
    return undefined;
  }
}

function normalizeCandidateUrl(value: string) {
  try {
    const url = new URL(value);

    if (!["http:", "https:"].includes(url.protocol)) {
      return undefined;
    }

    if (url.hostname.includes("duckduckgo.com")) {
      return undefined;
    }

    url.hash = "";
    return url.toString();
  } catch {
    return undefined;
  }
}
