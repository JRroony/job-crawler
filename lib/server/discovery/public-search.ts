import "server-only";

import {
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  classifyPublicSearchCandidate,
  type PublicSearchCandidate,
} from "@/lib/server/discovery/public-search-candidates";
import { extractDirectJobFromPublicSearchCandidate } from "@/lib/server/discovery/public-search-detail";
import {
  buildUsDiscoveryLocationClauses,
  type UsDiscoveryLocationClause,
  type UsLocationIntent,
} from "@/lib/server/locations/us";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import { safeFetchText } from "@/lib/server/net/fetcher";
import type { NormalizedJobSeed } from "@/lib/server/providers/types";
import {
  buildTitleQueryVariants,
  type TitleQueryVariant,
} from "@/lib/server/title-retrieval";
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
  maxRoleQueries?: number;
  queryConcurrency?: number;
  maxGreenhouseLocationClauses?: number;
  maxDirectJobs?: number;
  timeoutMs?: number;
};

type SearchablePublicPlatform = Extract<
  ActiveCrawlerPlatform,
  "greenhouse" | "lever" | "ashby" | "workday"
>;

type PublicSearchQuery = {
  platform: SearchablePublicPlatform;
  hostQuery: string;
  roleQuery: string;
  roleKind: TitleQueryVariant["kind"];
  rolePriority: number;
  locationClause?: string;
  locationKind: UsDiscoveryLocationClause["kind"];
  locationPriority: number;
  query: string;
  limit: number;
  priority: number;
};

type PublicSearchPlatformPlan = {
  platform: SearchablePublicPlatform;
  locationIntent: UsLocationIntent;
  locationClauses: string[];
  locationOptions: UsDiscoveryLocationClause[];
  queries: PublicSearchQuery[];
};

export type PublicSearchQueryPlan = {
  maxResultsPerQuery: number;
  maxQueries: number;
  maxSources: number;
  roleQueries: string[];
  roleQueryVariants: TitleQueryVariant[];
  platformPlans: PublicSearchPlatformPlan[];
  queries: PublicSearchQuery[];
};

type PublicSearchResult = {
  sources: DiscoveredSource[];
  jobs: NormalizedJobSeed[];
  diagnostics: PublicSearchDiscoveryDiagnostics;
};

type SearchEngineResult = {
  urls: string[];
  rawResultCount: number;
};

type QueryExecutionResult = {
  sources: DiscoveredSource[];
  detailCandidates: PublicSearchCandidate[];
  rawResultCount: number;
  normalizedUrlCount: number;
  platformMatchedUrlCount: number;
  candidateUrlCount: number;
  detailUrlCount: number;
  sourceUrlCount: number;
  recoveredSourceCount: number;
  platformMismatchCount: number;
  duplicateSourceCount: number;
  engineRequestCounts: Record<string, number>;
  engineResultCounts: Record<string, number>;
  sampleCandidateUrls: string[];
  sampleDetailUrls: string[];
  sampleSourceUrls: string[];
  sampleRecoveredSourceUrls: string[];
};

const duckDuckGoHtmlBaseUrl = "https://html.duckduckgo.com/html/";
const bingRssBaseUrl = "https://www.bing.com/search";
const defaultSearchTimeoutMs = 8_000;
const defaultBingTimeoutMs = 5_000;
const defaultDuckDuckGoTimeoutMs = 2_500;
const defaultMaxGreenhouseLocationClauses = 32;
const defaultMaxPublicSearchSources = 120;
const defaultMaxPublicSearchQueries = 96;
const defaultMaxDirectJobExtractions = 32;
const defaultPublicSearchQueryConcurrency = 4;
const sufficientPlatformMatchesPerQuery = 12;
const stagnantQueryThreshold = 32;

const searchablePlatforms: ActiveCrawlerPlatform[] = [
  "greenhouse",
  "lever",
  "ashby",
  "workday",
];

const searchHostQueries: Record<
  Extract<ActiveCrawlerPlatform, "greenhouse" | "lever" | "ashby" | "workday">,
  string[]
> = {
  greenhouse: [
    "site:boards.greenhouse.io",
    "site:job-boards.greenhouse.io",
  ],
  lever: ["site:jobs.lever.co"],
  ashby: ["site:jobs.ashbyhq.com"],
  workday: ["site:myworkdayjobs.com", "myworkdayjobs"],
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
    maxRoleQueries: options.maxRoleQueries,
  });
  const diagnostics = createEmptyPublicSearchDiagnostics(plan);
  if (plan.queries.length === 0) {
    return {
      sources: [],
      jobs: [],
      diagnostics,
    };
  }

  logQueryPlan(filters, plan);
  const discoveredAt = new Date().toISOString();
  const discovered = new Map<string, DiscoveredSource>();
  const harvestedJobs = new Map<string, NormalizedJobSeed>();
  const seenDetailCandidates = new Set<string>();
  const queriesToExecute = selectQueriesForExecution(plan);
  const skippedByQueryBudget = Math.max(0, plan.queries.length - queriesToExecute.length);
  const maxDirectJobs = options.maxDirectJobs ?? Math.min(
    plan.maxSources,
    defaultMaxDirectJobExtractions,
  );
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

    for (let index = 0; index < batchResults.length; index += 1) {
      const result = batchResults[index];
      const query = batch[index];
      diagnostics.executedQueries += 1;
      diagnostics.rawResultsHarvested += result.rawResultCount;
      diagnostics.normalizedUrlsHarvested += result.normalizedUrlCount;
      diagnostics.platformMatchedUrls += result.platformMatchedUrlCount;
      diagnostics.candidateUrlsHarvested += result.candidateUrlCount;
      diagnostics.detailUrlsHarvested += result.detailUrlCount;
      diagnostics.sourceUrlsHarvested += result.sourceUrlCount;
      diagnostics.recoveredSourcesFromDetailUrls += result.recoveredSourceCount;
      mergeDiagnosticCounts(diagnostics.engineRequestCounts, result.engineRequestCounts);
      mergeDiagnosticCounts(diagnostics.engineResultCounts, result.engineResultCounts);
      result.sampleCandidateUrls.forEach((url) =>
        pushDiagnosticSample(diagnostics.sampleHarvestedCandidateUrls, url),
      );
      result.sampleDetailUrls.forEach((url) =>
        pushDiagnosticSample(diagnostics.sampleHarvestedDetailUrls, url),
      );
      result.sampleSourceUrls.forEach((url) =>
        pushDiagnosticSample(diagnostics.sampleHarvestedSourceUrls, url),
      );
      result.sampleRecoveredSourceUrls.forEach((url) =>
        pushDiagnosticSample(diagnostics.sampleRecoveredSourceUrls, url),
      );
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

      if (
        diagnostics.sampleExecutedRoleQueries.length < 12 &&
        !diagnostics.sampleExecutedRoleQueries.includes(query.roleQuery)
      ) {
        diagnostics.sampleExecutedRoleQueries.push(query.roleQuery);
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

      const detailCandidates = result.detailCandidates.filter((candidate) => {
        if (seenDetailCandidates.has(candidate.url)) {
          incrementDiagnosticCount(diagnostics.dropReasonCounts, "duplicate_detail_candidate");
          return false;
        }

        seenDetailCandidates.add(candidate.url);
        return true;
      });
      const supportedDetailCandidates = detailCandidates.filter(supportsDirectDetailExtraction);
      incrementDiagnosticCount(
        diagnostics.dropReasonCounts,
        "direct_extraction_unsupported",
        detailCandidates.length - supportedDetailCandidates.length,
      );

      const remainingDirectBudget = Math.max(0, maxDirectJobs - harvestedJobs.size);
      const detailCandidatesToExtract = supportedDetailCandidates.slice(0, remainingDirectBudget);
      incrementDiagnosticCount(
        diagnostics.dropReasonCounts,
        "direct_job_budget",
        Math.max(0, supportedDetailCandidates.length - detailCandidatesToExtract.length),
      );

      let addedDirectJobCount = 0;
      if (detailCandidatesToExtract.length > 0) {
        const extractedJobs = await runWithConcurrency(
          detailCandidatesToExtract,
          async (candidate) =>
            extractDirectJobFromPublicSearchCandidate({
              candidate,
              companyHint: candidate.recoveredSource?.companyHint,
              discoveredAt,
              fetchImpl: options.fetchImpl ?? fetch,
            }),
          Math.min(2, detailCandidatesToExtract.length),
        );

        extractedJobs.forEach((job) => {
          if (!job) {
            incrementDiagnosticCount(diagnostics.dropReasonCounts, "direct_extraction_failed");
            return;
          }

          const key = buildHarvestedJobKey(job);
          if (harvestedJobs.has(key)) {
            incrementDiagnosticCount(diagnostics.dropReasonCounts, "duplicate_direct_job");
            return;
          }

          harvestedJobs.set(key, job);
          diagnostics.directJobsExtracted += 1;
          addedDirectJobCount += 1;
        });
      }

      if (result.rawResultCount > 0 || result.platformMatchedUrlCount > 0) {
        console.info("[discovery:public-search-query]", {
          query: query.query,
          rawResultCount: result.rawResultCount,
          normalizedUrlCount: result.normalizedUrlCount,
          platformMatchedUrlCount: result.platformMatchedUrlCount,
          candidateUrlCount: result.candidateUrlCount,
          detailUrlCount: result.detailUrlCount,
          sourceUrlCount: result.sourceUrlCount,
          recoveredSourceCount: result.recoveredSourceCount,
          addedSourceCount,
          addedDirectJobCount,
          platformMismatchCount: result.platformMismatchCount,
          duplicateSourceCount: result.duplicateSourceCount,
          engineRequestCounts: result.engineRequestCounts,
        });
      }

      if (addedSourceCount === 0 && addedDirectJobCount === 0 && (discovered.size > 0 || harvestedJobs.size > 0)) {
        consecutiveQueriesWithoutNewSources += 1;
      } else {
        consecutiveQueriesWithoutNewSources = 0;
      }
    }

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
  diagnostics.coverageNotes = buildCoverageNotes(diagnostics, {
    sourceCount: discovered.size,
    directJobCount: harvestedJobs.size,
  });

  return {
    sources: Array.from(discovered.values()),
    jobs: Array.from(harvestedJobs.values()),
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
    maxRoleQueries?: number;
  },
): PublicSearchQueryPlan {
  const requestedPlatforms = filters.platforms ?? searchablePlatforms;
  const selectedPlatforms = requestedPlatforms.filter(
    (
      platform,
    ): platform is SearchablePublicPlatform =>
      platform === "greenhouse" ||
      platform === "lever" ||
      platform === "ashby" ||
      platform === "workday",
  );
  const roleQueryVariants = buildRoleQueries(filters.title, options.maxRoleQueries);
  if (roleQueryVariants.length === 0) {
    return {
      maxResultsPerQuery: options.maxResultsPerQuery,
      maxQueries: options.maxQueries ?? defaultMaxPublicSearchQueries,
      maxSources: options.maxSources ?? defaultMaxPublicSearchSources,
      roleQueries: [],
      roleQueryVariants,
      platformPlans: [],
      queries: [],
    };
  }

  const roleQueries = roleQueryVariants.map((variant) => variant.query);

  const platformPlans = selectedPlatforms.map((platform) => {
    const { locationIntent, locationClauses, locationOptions } = buildPlatformLocationPlan(
      platform,
      filters,
      options.maxGreenhouseLocationClauses ?? defaultMaxGreenhouseLocationClauses,
    );
    const hostCount = searchHostQueries[platform].length;
    const queries = roleQueryVariants.flatMap((roleVariant, roleIndex) =>
      locationOptions.flatMap((locationOption, clauseIndex) =>
        searchHostQueries[platform].map((hostQuery, hostIndex) => ({
          platform,
          hostQuery,
          roleQuery: roleVariant.query,
          roleKind: roleVariant.kind,
          rolePriority: roleIndex,
          locationClause: locationOption.clause || undefined,
          locationKind: locationOption.kind,
          locationPriority: clauseIndex,
          query: [hostQuery, roleVariant.query, locationOption.clause].filter(Boolean).join(" "),
          limit: options.maxResultsPerQuery,
          priority:
            roleIndex * locationOptions.length * hostCount +
            clauseIndex * hostCount +
            hostIndex,
        })),
      ),
    );

    return {
      platform,
      locationIntent,
      locationClauses,
      locationOptions,
      queries,
    } satisfies PublicSearchPlatformPlan;
  });

  return {
    maxResultsPerQuery: options.maxResultsPerQuery,
    maxQueries: options.maxQueries ?? defaultMaxPublicSearchQueries,
    maxSources: options.maxSources ?? defaultMaxPublicSearchSources,
    roleQueries,
    roleQueryVariants,
    platformPlans,
    queries: platformPlans
      .flatMap((platformPlan) => platformPlan.queries)
      .sort((left, right) => left.priority - right.priority || left.query.localeCompare(right.query)),
  };
}

function buildRoleQueries(title: string, maxRoleQueries?: number) {
  return buildTitleQueryVariants(title, {
    maxQueries: maxRoleQueries ?? 18,
  });
}

function buildPlatformLocationPlan(
  _platform: SearchablePublicPlatform,
  filters: SearchFilters,
  maxLocationClauses: number,
): {
  locationIntent: UsLocationIntent;
  locationClauses: string[];
  locationOptions: UsDiscoveryLocationClause[];
} {
  const locationPlan = buildUsDiscoveryLocationClauses(filters, {
    maxClauses: maxLocationClauses,
  });

  if (locationPlan.intent.kind === "none" || locationPlan.intent.kind === "non_us") {
    return {
      locationIntent: locationPlan.intent,
      locationClauses: [""],
      locationOptions: [
        { clause: "", kind: "blank", priority: 0 } satisfies UsDiscoveryLocationClause,
      ],
    };
  }

  return {
    locationIntent: locationPlan.intent,
    locationClauses: locationPlan.clauses.length > 0 ? locationPlan.clauses : [""],
    locationOptions:
      locationPlan.detailedClauses.length > 0
        ? locationPlan.detailedClauses
        : [
            { clause: "", kind: "blank", priority: 0 } satisfies UsDiscoveryLocationClause,
          ],
  };
}

function logQueryPlan(filters: SearchFilters, plan: PublicSearchQueryPlan) {
  console.info("[discovery:query-plan]", {
    filters,
    titleExpansion: {
      originalQuery: filters.title,
      normalizedQuery: plan.roleQueryVariants[0]?.normalized ?? "",
      aliasesUsed: plan.roleQueryVariants.map((variant) => variant.query),
      variants: plan.roleQueryVariants.slice(0, 12).map((variant) => ({
        query: variant.query,
        kind: variant.kind,
        family: variant.family,
        conceptId: variant.conceptId,
      })),
    },
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
  const matchedDetailCandidates = new Map<string, PublicSearchCandidate>();
  const seenUrls = new Set<string>();
  const engineRequestCounts: Record<string, number> = {};
  const engineResultCounts: Record<string, number> = {};
  let rawResultCount = 0;
  let normalizedUrlCount = 0;
  let candidateUrlCount = 0;
  let detailUrlCount = 0;
  let sourceUrlCount = 0;
  let recoveredSourceCount = 0;
  let platformMatchedUrlCount = 0;
  let platformMismatchCount = 0;
  let duplicateSourceCount = 0;
  const sampleCandidateUrls: string[] = [];
  const sampleDetailUrls: string[] = [];
  const sampleSourceUrls: string[] = [];
  const sampleRecoveredSourceUrls: string[] = [];

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
      const candidate = classifyPublicSearchCandidate(url, "future_search");

      if (candidate.platform !== query.platform) {
        platformMismatchCount += 1;
        continue;
      }

      candidateUrlCount += 1;
      platformMatchedUrlCount += 1;
      pushDiagnosticSample(sampleCandidateUrls, candidate.url);

      if (candidate.kind === "detail") {
        detailUrlCount += 1;
        pushDiagnosticSample(sampleDetailUrls, candidate.url);
        if (!matchedDetailCandidates.has(candidate.url)) {
          matchedDetailCandidates.set(candidate.url, candidate);
        }
      } else if (candidate.kind === "source") {
        sourceUrlCount += 1;
        pushDiagnosticSample(sampleSourceUrls, candidate.url);
      }

      if (candidate.kind === "detail" && candidate.recoveredSource) {
        recoveredSourceCount += 1;
        pushDiagnosticSample(
          sampleRecoveredSourceUrls,
          candidate.recoveredSource.url,
        );
      }

      if (candidate.recoveredSource) {
        if (matchedSources.has(candidate.recoveredSource.id)) {
          duplicateSourceCount += 1;
        } else {
          matchedSources.set(candidate.recoveredSource.id, candidate.recoveredSource);
        }
      }

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
    detailCandidates: Array.from(matchedDetailCandidates.values()).slice(0, options.limit),
    rawResultCount,
    normalizedUrlCount,
    platformMatchedUrlCount,
    candidateUrlCount,
    detailUrlCount,
    sourceUrlCount,
    recoveredSourceCount,
    platformMismatchCount,
    duplicateSourceCount,
    engineRequestCounts,
    engineResultCounts,
    sampleCandidateUrls,
    sampleDetailUrls,
    sampleSourceUrls,
    sampleRecoveredSourceUrls,
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
    candidateUrlsHarvested: 0,
    detailUrlsHarvested: 0,
    sourceUrlsHarvested: 0,
    recoveredSourcesFromDetailUrls: 0,
    directJobsExtracted: 0,
    sourcesAdded: 0,
    engineRequestCounts: {},
    engineResultCounts: {},
    dropReasonCounts: {},
    sampleGeneratedRoleQueries: plan.roleQueries.slice(0, 12),
    sampleGeneratedQueries: plan.queries.slice(0, 12).map((query) => query.query),
    sampleExecutedRoleQueries: [],
    sampleExecutedQueries: [],
    sampleHarvestedCandidateUrls: [],
    sampleHarvestedDetailUrls: [],
    sampleHarvestedSourceUrls: [],
    sampleRecoveredSourceUrls: [],
    coverageNotes: [],
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

function pushDiagnosticSample(target: string[], value?: string) {
  const trimmed = value?.trim();
  if (!trimmed || target.length >= 12 || target.includes(trimmed)) {
    return;
  }

  target.push(trimmed);
}

function buildCoverageNotes(
  diagnostics: PublicSearchDiscoveryDiagnostics,
  input: {
    sourceCount: number;
    directJobCount: number;
  },
) {
  const notes: string[] = [];

  if (diagnostics.executedQueries === 0) {
    notes.push("No public search queries were executed.");
  }

  if (diagnostics.rawResultsHarvested === 0) {
    notes.push("Search engines returned zero raw results.");
  }

  if (diagnostics.platformMatchedUrls === 0 && diagnostics.rawResultsHarvested > 0) {
    notes.push("Search results did not classify into the requested ATS platforms.");
  }

  if (
    diagnostics.detailUrlsHarvested > 0 &&
    diagnostics.recoveredSourcesFromDetailUrls === 0
  ) {
    notes.push("Detail URLs were harvested but did not recover runnable sources.");
  }

  if (
    diagnostics.detailUrlsHarvested > 0 &&
    diagnostics.directJobsExtracted === 0
  ) {
    notes.push("Detail URLs were harvested but direct job extraction produced zero jobs.");
  }

  if (input.sourceCount === 0 && input.directJobCount === 0) {
    notes.push("Public search ended with zero recovered sources and zero direct jobs.");
  }

  if ((diagnostics.dropReasonCounts.query_budget ?? 0) > 0) {
    notes.push("Query budgeting skipped part of the generated search plan.");
  }

  if ((diagnostics.dropReasonCounts.stagnant_query_plateau ?? 0) > 0) {
    notes.push("Public search plateaued before exhausting the full query plan.");
  }

  return notes.slice(0, 12);
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

function supportsDirectDetailExtraction(candidate: PublicSearchCandidate) {
  return (
    candidate.platform === "greenhouse" ||
    candidate.platform === "lever" ||
    candidate.platform === "ashby" ||
    candidate.platform === "workday"
  );
}

function buildHarvestedJobKey(job: NormalizedJobSeed) {
  return [
    job.sourcePlatform,
    job.canonicalUrl ?? job.sourceUrl,
    job.sourceJobId,
  ]
    .filter(Boolean)
    .join("::");
}

export function selectQueriesForExecution(plan: PublicSearchQueryPlan) {
  if (plan.queries.length <= plan.maxQueries) {
    return plan.queries;
  }

  const selected: PublicSearchQuery[] = [];
  const seen = new Set<string>();
  const blankRoleWindow = Math.min(
    plan.roleQueries.length,
    Math.max(
      4,
      Math.floor((plan.maxQueries - 4) / Math.max(1, plan.platformPlans.length)),
    ),
  );

  const pushQuery = (query: PublicSearchQuery | undefined) => {
    if (!query || selected.length >= plan.maxQueries || seen.has(query.query)) {
      return;
    }

    seen.add(query.query);
    selected.push(query);
  };

  pushRoundRobinQueries(
    plan.queries.filter(
      (query) => query.rolePriority < blankRoleWindow && query.locationKind === "blank",
    ),
    (query) => `${query.platform}:${query.rolePriority}`,
    pushQuery,
  );

  if (selected.length >= plan.maxQueries) {
    return selected;
  }

  pushRoundRobinQueries(
    plan.queries.filter(
      (query) =>
        query.rolePriority === 0 &&
        (query.locationKind === "country" || query.locationKind === "remote"),
    ),
    (query) => `${query.platform}:${query.locationKind}:${query.locationPriority}`,
    pushQuery,
  );

  pushRoundRobinQueries(
    plan.queries.filter(
      (query) =>
        query.rolePriority === 0 &&
        (query.locationKind === "remote_state" ||
          query.locationKind === "metro" ||
          query.locationKind === "state"),
    ),
    (query) => `${query.platform}:${query.locationKind}:${query.locationPriority}`,
    pushQuery,
  );

  pushRoundRobinQueries(
    plan.queries.filter(
      (query) => query.rolePriority >= blankRoleWindow && query.locationKind === "blank",
    ),
    (query) => `${query.platform}:${query.rolePriority}`,
    pushQuery,
  );

  pushRoundRobinQueries(
    plan.queries.filter(
      (query) =>
        query.rolePriority > 0 &&
        (query.locationKind === "country" || query.locationKind === "remote"),
    ),
    (query) => `${query.platform}:${query.rolePriority}:${query.locationKind}`,
    pushQuery,
  );

  roundRobinQueryGroups(
    groupQueriesBy(
      plan.queries,
      (query) => `${query.platform}:${query.rolePriority}:${query.locationKind}`,
    ),
    pushQuery,
  );

  return selected;
}

function pushRoundRobinQueries(
  queries: PublicSearchQuery[],
  keyFn: (query: PublicSearchQuery) => string,
  pushQuery: (query: PublicSearchQuery | undefined) => void,
) {
  roundRobinQueryGroups(groupQueriesBy(queries, keyFn), pushQuery);
}

function groupQueriesBy(
  queries: PublicSearchQuery[],
  keyFn: (query: PublicSearchQuery) => string,
) {
  const groups = new Map<string, PublicSearchQuery[]>();

  queries.forEach((query) => {
    const key = keyFn(query);
    const current = groups.get(key) ?? [];
    current.push(query);
    groups.set(key, current);
  });

  return Array.from(groups.values());
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
