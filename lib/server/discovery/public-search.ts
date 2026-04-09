import "server-only";

import { normalizeTitleToCanonicalForm } from "@/lib/server/crawler/helpers";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  buildUsDiscoveryLocationClauses,
  type UsLocationIntent,
} from "@/lib/server/locations/us";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { safeFetchText } from "@/lib/server/net/fetcher";
import type { ActiveCrawlerPlatform, SearchFilters } from "@/lib/types";

type PublicSearchOptions = {
  fetchImpl?: typeof fetch;
  maxResultsPerQuery: number;
  timeoutMs?: number;
};

type SearchablePublicPlatform = Extract<
  ActiveCrawlerPlatform,
  "greenhouse" | "lever" | "ashby"
>;

type PublicSearchQuery = {
  platform: SearchablePublicPlatform;
  hostQuery: string;
  roleQuery: string;
  locationClause?: string;
  query: string;
  limit: number;
  priority: number;
};

type PublicSearchPlatformPlan = {
  platform: SearchablePublicPlatform;
  locationIntent: UsLocationIntent;
  locationClauses: string[];
  queries: PublicSearchQuery[];
};

export type PublicSearchQueryPlan = {
  maxResultsPerQuery: number;
  maxSources: number;
  roleQueries: string[];
  platformPlans: PublicSearchPlatformPlan[];
  queries: PublicSearchQuery[];
};

const duckDuckGoHtmlBaseUrl = "https://html.duckduckgo.com/html/";
const bingRssBaseUrl = "https://www.bing.com/search";
const defaultSearchTimeoutMs = 8_000;
const maxGreenhouseLocationClauses = 12;
const maxPublicSearchSources = 24;

const searchablePlatforms: ActiveCrawlerPlatform[] = [
  "greenhouse",
  "lever",
  "ashby",
];

const searchHostQueries: Record<
  Extract<ActiveCrawlerPlatform, "greenhouse" | "lever" | "ashby">,
  string[]
> = {
  greenhouse: [
    "site:boards.greenhouse.io",
    "site:job-boards.greenhouse.io",
  ],
  lever: ["site:jobs.lever.co"],
  ashby: ["site:jobs.ashbyhq.com"],
};

const publicSearchEngines = [
  {
    name: "duckduckgo_html" as const,
    search: searchDuckDuckGoHtml,
  },
  {
    name: "bing_rss" as const,
    search: searchBingRss,
  },
];

export async function discoverSourcesFromPublicSearch(
  filters: SearchFilters,
  options: PublicSearchOptions,
) {
  const plan = buildPublicSearchQueryPlan(filters, {
    maxResultsPerQuery: options.maxResultsPerQuery,
  });
  if (plan.queries.length === 0) {
    return [];
  }

  logQueryPlan(filters, plan);
  const discovered = new Map<string, DiscoveredSource>();

  for (const query of plan.queries) {
    const remainingSourceBudget = plan.maxSources - discovered.size;
    if (remainingSourceBudget <= 0) {
      break;
    }

    const urls = await searchPublicWeb(query.query, {
      fetchImpl: options.fetchImpl,
      timeoutMs: options.timeoutMs ?? defaultSearchTimeoutMs,
      limit: Math.min(query.limit, remainingSourceBudget),
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

      if (discovered.size >= plan.maxSources) {
        break;
      }
    }

    if (discovered.size >= plan.maxSources) {
      break;
    }
  }

  return Array.from(discovered.values());
}

export function buildPublicSearchQueryPlan(
  filters: SearchFilters,
  options: {
    maxResultsPerQuery: number;
  },
): PublicSearchQueryPlan {
  const requestedPlatforms = filters.platforms ?? searchablePlatforms;
  const selectedPlatforms = requestedPlatforms.filter(
    (
      platform,
    ): platform is SearchablePublicPlatform =>
      platform === "greenhouse" || platform === "lever" || platform === "ashby",
  );
  const roleQueries = buildRoleQueries(filters.title);
  if (roleQueries.length === 0) {
    return {
      maxResultsPerQuery: options.maxResultsPerQuery,
      maxSources: maxPublicSearchSources,
      roleQueries,
      platformPlans: [],
      queries: [],
    };
  }

  const platformPlans = selectedPlatforms.map((platform) => {
    const { locationIntent, locationClauses } = buildPlatformLocationPlan(
      platform,
      filters,
    );
    const queries = locationClauses.flatMap((locationClause, clauseIndex) =>
      searchHostQueries[platform].flatMap((hostQuery, hostIndex) =>
        roleQueries.map((roleQuery) => ({
          platform,
          hostQuery,
          roleQuery,
          locationClause: locationClause || undefined,
          query: [hostQuery, roleQuery, locationClause].filter(Boolean).join(" "),
          limit: options.maxResultsPerQuery,
          priority: clauseIndex * searchHostQueries[platform].length + hostIndex,
        })),
      ),
    );

    return {
      platform,
      locationIntent,
      locationClauses,
      queries,
    } satisfies PublicSearchPlatformPlan;
  });

  return {
    maxResultsPerQuery: options.maxResultsPerQuery,
    maxSources: maxPublicSearchSources,
    roleQueries,
    platformPlans,
    queries: platformPlans.flatMap((platformPlan) => platformPlan.queries),
  };
}

function buildRoleQueries(title: string) {
  const canonical = normalizeTitleToCanonicalForm(title);
  return canonical ? [canonical] : [];
}

function buildPlatformLocationPlan(
  platform: SearchablePublicPlatform,
  filters: SearchFilters,
) {
  if (platform !== "greenhouse") {
    return {
      locationIntent: {
        kind: "none",
      } satisfies UsLocationIntent,
      locationClauses: [""],
    };
  }

  const locationPlan = buildUsDiscoveryLocationClauses(filters, {
    maxClauses: maxGreenhouseLocationClauses,
  });

  if (locationPlan.intent.kind === "none" || locationPlan.intent.kind === "non_us") {
    return {
      locationIntent: locationPlan.intent,
      locationClauses: [""],
    };
  }

  return {
    locationIntent: locationPlan.intent,
    locationClauses: locationPlan.clauses.length > 0 ? locationPlan.clauses : [""],
  };
}

function logQueryPlan(filters: SearchFilters, plan: PublicSearchQueryPlan) {
  console.info("[discovery:query-plan]", {
    filters,
    maxResultsPerQuery: plan.maxResultsPerQuery,
    maxSources: plan.maxSources,
    totalQueries: plan.queries.length,
    platformPlans: plan.platformPlans.map((platformPlan) => ({
      platform: platformPlan.platform,
      locationIntent: platformPlan.locationIntent,
      locationClauseCount: platformPlan.locationClauses.length,
      sampleLocationClauses: platformPlan.locationClauses.slice(0, 6),
      queryCount: platformPlan.queries.length,
    })),
    sampleQueries: plan.queries.slice(0, 6).map((query) => query.query),
  });
}

async function searchPublicWeb(
  query: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) {
  const discovered = new Set<string>();

  for (const engine of publicSearchEngines) {
    const remaining = options.limit - discovered.size;
    if (remaining <= 0) {
      break;
    }

    const urls = await engine.search(query, {
      ...options,
      limit: remaining,
    });

    if (urls.length > 0) {
      console.info("[discovery:public-search]", {
        engine: engine.name,
        query,
        resultCount: urls.length,
      });
    }

    for (const url of urls) {
      discovered.add(url);
      if (discovered.size >= options.limit) {
        break;
      }
    }
  }

  return Array.from(discovered).slice(0, options.limit);
}

async function searchDuckDuckGoHtml(
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

  if (looksLikeDuckDuckGoChallenge(result.data)) {
    console.warn("[discovery:public-search]", {
      engine: "duckduckgo_html",
      query,
      reason: "challenge_page",
    });
    return [];
  }

  return extractDuckDuckGoUrls(result.data).slice(0, options.limit);
}

async function searchBingRss(
  query: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) {
  const url = new URL(bingRssBaseUrl);
  url.searchParams.set("format", "rss");
  url.searchParams.set("q", query);

  const result = await safeFetchText(url, {
    fetchImpl: options.fetchImpl,
    method: "GET",
    headers: {
      Accept: "application/rss+xml,application/xml,text/xml,text/plain",
      "User-Agent": "job-crawler/0.1 (+public-source-discovery)",
    },
    cache: "no-store",
    timeoutMs: options.timeoutMs,
    retries: 1,
  });

  if (!result.ok || !result.data) {
    return [];
  }

  return extractBingRssUrls(result.data).slice(0, options.limit);
}

function extractDuckDuckGoUrls(html: string) {
  const discovered = new Set<string>();
  const hrefMatches = Array.from(
    html.matchAll(/href=["']([^"'#]+)["']/gi),
  );

  for (const match of hrefMatches) {
    const rawHref = match[1];
    const resolved = resolveDuckDuckGoResultUrl(rawHref);
    if (!resolved) {
      continue;
    }

    discovered.add(resolved);
  }

  return Array.from(discovered);
}

function extractBingRssUrls(xml: string) {
  const discovered = new Set<string>();
  const itemMatches = Array.from(
    xml.matchAll(/<item\b[\s\S]*?<link>([^<]+)<\/link>/gi),
  );

  for (const match of itemMatches) {
    const resolved = normalizeCandidateUrl(match[1]?.trim() ?? "");
    if (!resolved) {
      continue;
    }

    discovered.add(resolved);
  }

  return Array.from(discovered);
}

function resolveDuckDuckGoResultUrl(rawHref: string) {
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

function looksLikeDuckDuckGoChallenge(html: string) {
  return (
    html.includes("Unfortunately, bots use DuckDuckGo too.") ||
    html.includes('id="challenge-form"') ||
    html.includes("anomaly-modal")
  );
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
