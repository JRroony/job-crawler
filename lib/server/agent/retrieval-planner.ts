import type { JobSearchIntent, RetrievalPlan, CrawlPlan, SourcePlan, ExpandedRoleQuery, AgentConfig } from "./types";
import { expandJobTitle, buildLocationClauses } from "./query-expander";
import type { PlatformSupportStatus } from "./types";

/**
 * Build a retrieval plan based on parsed search intent.
 * This determines what to query from the database and whether to crawl.
 */
export function buildRetrievalPlan(
  intent: JobSearchIntent,
  config: AgentConfig,
): RetrievalPlan {
  const expandedQueries = expandJobTitle(intent);
  const locationClauses = buildLocationClauses(
    intent.country,
    intent.state,
    intent.city,
    intent.remotePreference !== "none",
    config.maxLocationClauses,
  );

  // Build DB query filter
  const dbQueryFilter: Record<string, unknown> = {};

  // Add title variants to DB filter
  const titleVariants = expandedQueries.filter((q) => q.relevanceScore >= 0.7).map((q) => q.queryTitle);
  if (titleVariants.length > 0) {
    dbQueryFilter.titleVariants = titleVariants;
  }

  // Add location filter
  if (intent.country) {
    dbQueryFilter.country = intent.country;
  }
  if (intent.state) {
    dbQueryFilter.state = intent.state;
  }
  if (intent.city) {
    dbQueryFilter.city = intent.city;
  }
  if (intent.remotePreference === "required") {
    dbQueryFilter.remoteType = "remote";
  }

  // Add platform filter
  if (intent.platformFilters.length > 0) {
    dbQueryFilter.platforms = intent.platformFilters;
  }

  // Build crawl plan (initially set to no crawl — evaluation will update)
  const crawlPlan = buildInitialCrawlPlan(intent, expandedQueries, locationClauses, config);

  return {
    intent,
    expandedQueries,
    crawlPlan,
    useDbFirst: true,
    dbQueryFilter,
    dbCandidateLimit: 500,
    minResultsTarget: config.minResults,
    freshnessDays: config.freshnessDays,
    createdAt: new Date().toISOString(),
  };
}

/**
 * Build an initial crawl plan (not yet decided whether to execute).
 */
function buildInitialCrawlPlan(
  intent: JobSearchIntent,
  expandedQueries: ExpandedRoleQuery[],
  locationClauses: string[],
  config: AgentConfig,
): CrawlPlan {
  const sourcePlans = buildSourcePlans(intent, expandedQueries, locationClauses, config);

  return {
    shouldCrawl: false, // Will be decided after DB evaluation
    reason: "",
    mode: config.mode === "deep" ? "full" : "targeted",
    maxQueries: config.maxCrawlQueries,
    maxLocationClauses: config.maxLocationClauses,
    sourcePlans,
    estimatedDurationMs: estimateCrawlDuration(sourcePlans),
    abortable: true,
  };
}

/**
 * Build platform-specific source plans.
 */
function buildSourcePlans(
  intent: JobSearchIntent,
  expandedQueries: ExpandedRoleQuery[],
  locationClauses: string[],
  config: AgentConfig,
): SourcePlan[] {
  // Platform support statuses — honest about what's actually implemented
  const platformStatuses: Record<string, PlatformSupportStatus> = {
    greenhouse: "supported",
    lever: "supported",
    ashby: "supported",
    smartrecruiters: "partial",
    workday: "partial",
    company_page: "partial",
  };

  const titleQueries = expandedQueries
    .filter((q) => q.relevanceScore >= 0.7)
    .slice(0, config.maxCrawlQueries)
    .map((q) => q.queryTitle);

  const sourcePlans: SourcePlan[] = [];

  // Build plans for each platform
  const platformPriorities: Record<string, number> = {
    greenhouse: 9,
    lever: 8,
    ashby: 7,
    smartrecruiters: 5,
    workday: 4,
    company_page: 3,
  };

  for (const [platform, status] of Object.entries(platformStatuses)) {
    const priority = platformPriorities[platform] ?? 5;

    // Skip unsupported or disabled platforms
    if (status === "disabled" || status === "unavailable") {
      sourcePlans.push({
        platform,
        supportStatus: status,
        priority,
        maxSources: 0,
        searchQueries: [],
        locationClauses: [],
        cooldownMinutes: config.sourceCooldownMinutes,
        skipReason: `Platform is ${status}`,
      });
      continue;
    }

    // If user has specific platform filters, skip non-matching platforms
    if (intent.platformFilters.length > 0 && !intent.platformFilters.includes(platform)) {
      sourcePlans.push({
        platform,
        supportStatus: status,
        priority,
        maxSources: 0,
        searchQueries: [],
        locationClauses: [],
        cooldownMinutes: config.sourceCooldownMinutes,
        skipReason: "Not in user platform filter",
      });
      continue;
    }

    sourcePlans.push({
      platform,
      supportStatus: status,
      priority,
      maxSources: status === "supported" ? 40 : status === "partial" ? 15 : 0,
      searchQueries: titleQueries.slice(0, status === "supported" ? config.maxCrawlQueries : 6),
      locationClauses: locationClauses.slice(0, config.maxLocationClauses),
      cooldownMinutes: config.sourceCooldownMinutes,
    });
  }

  return sourcePlans;
}

/**
 * Estimate crawl duration based on source plans.
 */
function estimateCrawlDuration(sourcePlans: SourcePlan[]): number {
  const activePlans = sourcePlans.filter((p) => p.maxSources > 0);
  // Rough estimate: 500ms per query per location clause per source
  const totalQueries = activePlans.reduce(
    (sum, p) => sum + p.searchQueries.length * Math.max(1, p.locationClauses.length || 1),
    0,
  );
  return Math.min(totalQueries * 500, 60000); // Cap at 60 seconds
}