import "server-only";

import {
  buildDiscoveryRoleQueries,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  buildUsDiscoveryLocationClauses,
  type UsLocationIntent,
} from "@/lib/server/locations/us";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { safeFetchText } from "@/lib/server/net/fetcher";
import type {
  ActiveCrawlerPlatform,
  PublicSearchDiscoveryDiagnostics,
  SearchFilters,
} from "@/lib/types";

type PublicSearchOptions = {
  fetchImpl?: typeof fetch;
  maxResultsPerQuery: number;
  maxSources?: number;
  maxQueries?: number;
  queryConcurrency?: number;
  maxGreenhouseLocationClauses?: number;
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
  maxQueries: number;
  maxSources: number;
  roleQueries: string[];
  platformPlans: PublicSearchPlatformPlan[];
  queries: PublicSearchQuery[];
};

type PublicSearchResult = {
  sources: DiscoveredSource[];
  diagnostics: PublicSearchDiscoveryDiagnostics;
};

type SearchEngineResult = {
  urls: string[];
  rawResultCount: number;
};

type QueryExecutionResult = {
  sources: DiscoveredSource[];
  rawResultCount: number;
  normalizedUrlCount: number;
  platformMatchedUrlCount: number;
  platformMismatchCount: number;
  duplicateSourceCount: number;
  engineRequestCounts: Record<string, number>;
  engineResultCounts: Record<string, number>;
};

const duckDuckGoHtmlBaseUrl = "https://html.duckduckgo.com/html/";
const bingRssBaseUrl = "https://www.bing.com/search";
const defaultSearchTimeoutMs = 8_000;
const defaultBingTimeoutMs = 5_000;
const defaultDuckDuckGoTimeoutMs = 2_500;
const defaultMaxGreenhouseLocationClauses = 24;
const defaultMaxPublicSearchSources = 120;
const defaultMaxPublicSearchQueries = 72;
const defaultPublicSearchQueryConcurrency = 4;
const sufficientPlatformMatchesPerQuery = 8;
const stagnantQueryThreshold = 12;

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
    name: "bing_rss" as const,
    timeoutMs: defaultBingTimeoutMs,
    search: searchBingRss,
  },
  {
    name: "duckduckgo_html" as const,
    timeoutMs: defaultDuckDuckGoTimeoutMs,
    search: searchDuckDuckGoHtml,
  },
];

export async function discoverSourcesFromPublicSearch(
  filters: SearchFilters,
  options: PublicSearchOptions,
) {
  const result = await discoverSourcesFromPublicSearchDetailed(filters, options);
  return result.sources;
}

export async function discoverSourcesFromPublicSearchDetailed(
  filters: SearchFilters,
  options: PublicSearchOptions,
): Promise<PublicSearchResult> {
  const plan = buildPublicSearchQueryPlan(filters, {
    maxResultsPerQuery: options.maxResultsPerQuery,
    maxSources: options.maxSources,
    maxQueries: options.maxQueries,
    maxGreenhouseLocationClauses: options.maxGreenhouseLocationClauses,
  });
  const diagnostics = createEmptyPublicSearchDiagnostics(plan);
  if (plan.queries.length === 0) {
    return {
      sources: [],
      diagnostics,
    };
  }

  logQueryPlan(filters, plan);
  const discovered = new Map<string, DiscoveredSource>();
  const queriesToExecute = selectQueriesForExecution(plan);
  const skippedByQueryBudget = Math.max(0, plan.queries.length - queriesToExecute.length);
  let consecutiveQueriesWithoutNewSources = 0;
  const queryConcurrency = Math.min(
    options.queryConcurrency ?? defaultPublicSearchQueryConcurrency,
    Math.max(1, queriesToExecute.length),
  );

  for (let offset = 0; offset < queriesToExecute.length; offset += queryConcurrency) {
    if (discovered.size >= plan.maxSources) {
      incrementDiagnosticCount(diagnostics.dropReasonCounts, "source_budget");
      break;
    }

    const batch = queriesToExecute.slice(offset, offset + queryConcurrency);
    const batchResults = await runWithConcurrency(
      batch,
      async (query) =>
        executePublicSearchQuery(query, {
          fetchImpl: options.fetchImpl,
          timeoutMs: options.timeoutMs ?? defaultSearchTimeoutMs,
          limit: Math.min(query.limit, plan.maxSources),
        }),
      queryConcurrency,
    );

    batchResults.forEach((result, index) => {
      const query = batch[index];
      diagnostics.executedQueries += 1;
      diagnostics.rawResultsHarvested += result.rawResultCount;
      diagnostics.normalizedUrlsHarvested += result.normalizedUrlCount;
      diagnostics.platformMatchedUrls += result.platformMatchedUrlCount;
      mergeDiagnosticCounts(diagnostics.engineRequestCounts, result.engineRequestCounts);
      mergeDiagnosticCounts(diagnostics.engineResultCounts, result.engineResultCounts);
      incrementDiagnosticCount(
        diagnostics.dropReasonCounts,
        "platform_mismatch",
        result.platformMismatchCount,
      );
      incrementDiagnosticCount(
        diagnostics.dropReasonCounts,
        "duplicate_within_query",
        result.duplicateSourceCount,
      );

      if (diagnostics.sampleExecutedQueries.length < 12) {
        diagnostics.sampleExecutedQueries.push(query.query);
      }

      let addedSourceCount = 0;
      for (const source of result.sources) {
        if (discovered.size >= plan.maxSources) {
          incrementDiagnosticCount(diagnostics.dropReasonCounts, "source_budget");
          break;
        }

        if (discovered.has(source.id)) {
          incrementDiagnosticCount(diagnostics.dropReasonCounts, "duplicate_across_queries");
          continue;
        }

        discovered.set(source.id, source);
        diagnostics.sourcesAdded += 1;
        addedSourceCount += 1;
      }

      if (result.rawResultCount > 0 || result.platformMatchedUrlCount > 0) {
        console.info("[discovery:public-search-query]", {
          query: query.query,
          rawResultCount: result.rawResultCount,
          normalizedUrlCount: result.normalizedUrlCount,
          platformMatchedUrlCount: result.platformMatchedUrlCount,
          addedSourceCount,
          platformMismatchCount: result.platformMismatchCount,
          duplicateSourceCount: result.duplicateSourceCount,
          engineRequestCounts: result.engineRequestCounts,
        });
      }

      if (addedSourceCount === 0 && discovered.size > 0) {
        consecutiveQueriesWithoutNewSources += 1;
      } else {
        consecutiveQueriesWithoutNewSources = 0;
      }
    });

    if (consecutiveQueriesWithoutNewSources >= stagnantQueryThreshold) {
      incrementDiagnosticCount(
        diagnostics.dropReasonCounts,
        "stagnant_query_plateau",
        Math.max(0, queriesToExecute.length - diagnostics.executedQueries),
      );
      break;
    }
  }

  diagnostics.skippedQueries = Math.max(0, plan.queries.length - diagnostics.executedQueries);
  if (skippedByQueryBudget > 0) {
    incrementDiagnosticCount(
      diagnostics.dropReasonCounts,
      "query_budget",
      skippedByQueryBudget,
    );
  }

  return {
    sources: Array.from(discovered.values()),
    diagnostics,
  };
}

export function buildPublicSearchQueryPlan(
  filters: SearchFilters,
  options: {
    maxResultsPerQuery: number;
    maxSources?: number;
    maxQueries?: number;
    maxGreenhouseLocationClauses?: number;
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
      maxQueries: options.maxQueries ?? defaultMaxPublicSearchQueries,
      maxSources: options.maxSources ?? defaultMaxPublicSearchSources,
      roleQueries,
      platformPlans: [],
      queries: [],
    };
  }

  const platformPlans = selectedPlatforms.map((platform) => {
    const { locationIntent, locationClauses } = buildPlatformLocationPlan(
      platform,
      filters,
      options.maxGreenhouseLocationClauses ?? defaultMaxGreenhouseLocationClauses,
    );
    const hostCount = searchHostQueries[platform].length;
    const queries = roleQueries.flatMap((roleQuery, roleIndex) =>
      locationClauses.flatMap((locationClause, clauseIndex) =>
        searchHostQueries[platform].map((hostQuery, hostIndex) => ({
          platform,
          hostQuery,
          roleQuery,
          locationClause: locationClause || undefined,
          query: [hostQuery, roleQuery, locationClause].filter(Boolean).join(" "),
          limit: options.maxResultsPerQuery,
          priority:
            roleIndex * locationClauses.length * hostCount +
            clauseIndex * hostCount +
            hostIndex,
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
    maxQueries: options.maxQueries ?? defaultMaxPublicSearchQueries,
    maxSources: options.maxSources ?? defaultMaxPublicSearchSources,
    roleQueries,
    platformPlans,
    queries: platformPlans
      .flatMap((platformPlan) => platformPlan.queries)
      .sort((left, right) => left.priority - right.priority || left.query.localeCompare(right.query)),
  };
}

function buildRoleQueries(title: string) {
  return buildDiscoveryRoleQueries(title);
}

function buildPlatformLocationPlan(
  platform: SearchablePublicPlatform,
  filters: SearchFilters,
  maxLocationClauses: number,
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
    maxClauses: maxLocationClauses,
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
    maxQueries: plan.maxQueries,
    maxSources: plan.maxSources,
    roleQueryCount: plan.roleQueries.length,
    sampleRoleQueries: plan.roleQueries.slice(0, 8),
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

async function executePublicSearchQuery(
  query: PublicSearchQuery,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) : Promise<QueryExecutionResult> {
  const matchedSources = new Map<string, DiscoveredSource>();
  const seenUrls = new Set<string>();
  const engineRequestCounts: Record<string, number> = {};
  const engineResultCounts: Record<string, number> = {};
  let rawResultCount = 0;
  let normalizedUrlCount = 0;
  let platformMismatchCount = 0;
  let duplicateSourceCount = 0;

  for (const engine of publicSearchEngines) {
    const remaining = options.limit - matchedSources.size;
    if (remaining <= 0) {
      break;
    }

    incrementDiagnosticCount(engineRequestCounts, engine.name);
    const result = await engine.search(query.query, {
      fetchImpl: options.fetchImpl,
      timeoutMs: Math.min(options.timeoutMs, engine.timeoutMs),
      limit: options.limit,
    });
    rawResultCount += result.rawResultCount;
    normalizedUrlCount += result.urls.length;
    incrementDiagnosticCount(engineResultCounts, engine.name, result.urls.length);

    if (result.urls.length > 0) {
      console.info("[discovery:public-search]", {
        engine: engine.name,
        query: query.query,
        rawResultCount: result.rawResultCount,
        normalizedUrlCount: result.urls.length,
      });
    }

    for (const url of result.urls) {
      if (seenUrls.has(url)) {
        continue;
      }

      seenUrls.add(url);
      const source = classifySourceCandidate({
        url,
        confidence: "medium",
        discoveryMethod: "future_search",
      });

      if (source.platform !== query.platform) {
        platformMismatchCount += 1;
        continue;
      }

      if (matchedSources.has(source.id)) {
        duplicateSourceCount += 1;
        continue;
      }

      matchedSources.set(source.id, source);
      if (matchedSources.size >= options.limit) {
        break;
      }
    }

    if (
      matchedSources.size >= options.limit ||
      matchedSources.size >= Math.min(options.limit, sufficientPlatformMatchesPerQuery)
    ) {
      break;
    }
  }

  return {
    sources: Array.from(matchedSources.values()).slice(0, options.limit),
    rawResultCount,
    normalizedUrlCount,
    platformMatchedUrlCount: matchedSources.size,
    platformMismatchCount,
    duplicateSourceCount,
    engineRequestCounts,
    engineResultCounts,
  };
}

async function searchDuckDuckGoHtml(
  query: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) : Promise<SearchEngineResult> {
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
    return emptySearchEngineResult();
  }

  if (looksLikeDuckDuckGoChallenge(result.data)) {
    console.warn("[discovery:public-search]", {
      engine: "duckduckgo_html",
      query,
      reason: "challenge_page",
    });
    return emptySearchEngineResult();
  }

  return limitSearchEngineResult(extractDuckDuckGoUrls(result.data), options.limit);
}

async function searchBingRss(
  query: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs: number;
    limit: number;
  },
) : Promise<SearchEngineResult> {
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
    return emptySearchEngineResult();
  }

  return limitSearchEngineResult(extractBingRssUrls(result.data), options.limit);
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

  return {
    urls: Array.from(discovered),
    rawResultCount: hrefMatches.length,
  } satisfies SearchEngineResult;
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

  return {
    urls: Array.from(discovered),
    rawResultCount: itemMatches.length,
  } satisfies SearchEngineResult;
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

function createEmptyPublicSearchDiagnostics(
  plan: PublicSearchQueryPlan,
): PublicSearchDiscoveryDiagnostics {
  return {
    generatedQueries: plan.queries.length,
    executedQueries: 0,
    skippedQueries: plan.queries.length,
    maxQueries: plan.maxQueries,
    maxSources: plan.maxSources,
    maxResultsPerQuery: plan.maxResultsPerQuery,
    roleQueryCount: plan.roleQueries.length,
    locationClauseCount: plan.platformPlans.reduce(
      (total, platformPlan) => total + platformPlan.locationClauses.length,
      0,
    ),
    rawResultsHarvested: 0,
    normalizedUrlsHarvested: 0,
    platformMatchedUrls: 0,
    sourcesAdded: 0,
    engineRequestCounts: {},
    engineResultCounts: {},
    dropReasonCounts: {},
    sampleGeneratedQueries: plan.queries.slice(0, 12).map((query) => query.query),
    sampleExecutedQueries: [],
  };
}

function incrementDiagnosticCount(
  counts: Record<string, number>,
  key: string,
  amount = 1,
) {
  if (amount <= 0) {
    return;
  }

  counts[key] = (counts[key] ?? 0) + amount;
}

function mergeDiagnosticCounts(
  target: Record<string, number>,
  source: Record<string, number>,
) {
  Object.entries(source).forEach(([key, amount]) => {
    incrementDiagnosticCount(target, key, amount);
  });
}

function emptySearchEngineResult(): SearchEngineResult {
  return {
    urls: [],
    rawResultCount: 0,
  };
}

function limitSearchEngineResult(result: SearchEngineResult, limit: number): SearchEngineResult {
  return {
    urls: result.urls.slice(0, limit),
    rawResultCount: result.rawResultCount,
  };
}

function selectQueriesForExecution(plan: PublicSearchQueryPlan) {
  if (plan.queries.length <= plan.maxQueries) {
    return plan.queries;
  }

  const selected: PublicSearchQuery[] = [];
  const seen = new Set<string>();
  const primaryRoleQuery = plan.roleQueries[0];
  const preferredExpansionClauses = new Set([
    "",
    "remote us",
    "remote usa",
    "remote united states",
  ]);

  const pushQuery = (query: PublicSearchQuery | undefined) => {
    if (!query || selected.length >= plan.maxQueries || seen.has(query.query)) {
      return;
    }

    seen.add(query.query);
    selected.push(query);
  };

  plan.queries
    .filter((query) => query.roleQuery === primaryRoleQuery)
    .forEach(pushQuery);

  if (selected.length >= plan.maxQueries) {
    return selected;
  }

  const secondaryRoleQueries = plan.roleQueries.slice(1);
  const preferredExpansionGroups = secondaryRoleQueries.map((roleQuery) =>
    plan.queries.filter(
      (query) =>
        query.roleQuery === roleQuery &&
        preferredExpansionClauses.has(query.locationClause ?? ""),
    ),
  );
  const remainingExpansionGroups = secondaryRoleQueries.map((roleQuery) =>
    plan.queries.filter(
      (query) =>
        query.roleQuery === roleQuery &&
        !preferredExpansionClauses.has(query.locationClause ?? ""),
    ),
  );

  roundRobinQueryGroups(preferredExpansionGroups, pushQuery);
  roundRobinQueryGroups(remainingExpansionGroups, pushQuery);

  return selected;
}

function roundRobinQueryGroups(
  groups: PublicSearchQuery[][],
  pushQuery: (query: PublicSearchQuery | undefined) => void,
) {
  let addedInRound = true;

  while (addedInRound) {
    addedInRound = false;

    for (const group of groups) {
      const next = group.shift();
      if (!next) {
        continue;
      }

      pushQuery(next);
      addedInRound = true;
    }
  }
}
