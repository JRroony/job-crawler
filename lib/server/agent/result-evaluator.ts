import type {
  RetrievalQualityReport,
  CrawlPlan,
  SourcePlan,
  ExpandedRoleQuery,
  AgentConfig,
  AgentDecision,
  RetrievalPlan,
} from "./types";

/**
 * Evaluate the quality of database results and decide whether to trigger a crawl.
 */
export function evaluateResults(
  plan: RetrievalPlan,
  dbResults: Array<{
    _id: string;
    title: string;
    indexedAt?: string;
    crawledAt?: string;
    discoveredAt?: string;
    sourcePlatform?: string;
  }>,
  config: AgentConfig,
): RetrievalQualityReport {
  const now = Date.now();
  const freshnessCutoff = now - config.freshnessDays * 24 * 60 * 60 * 1000;

  // Count fresh vs stale results
  const freshResults = dbResults.filter((job) => {
    const timestamp = job.indexedAt ?? job.crawledAt ?? job.discoveredAt;
    if (!timestamp) return false;
    const parsed = Date.parse(timestamp);
    return Number.isFinite(parsed) && parsed >= freshnessCutoff;
  });

  const dbCandidateCount = dbResults.length;
  const dbFreshCount = freshResults.length;

  // Calculate duplicate rate (simplified: check for duplicate titles)
  const seenTitles = new Map<string, number>();
  for (const job of dbResults) {
    const normalized = job.title.toLowerCase().trim();
    seenTitles.set(normalized, (seenTitles.get(normalized) ?? 0) + 1);
  }
  const duplicateCount = Array.from(seenTitles.values()).filter((c) => c > 1).reduce((sum, c) => sum + (c - 1), 0);
  const duplicateRate = dbCandidateCount > 0 ? duplicateCount / dbCandidateCount : 0;

  // Calculate quality score (0-1)
  const coverageRatio = Math.min(dbCandidateCount / config.minResults, 1);
  const freshnessRatio = dbCandidateCount > 0 ? dbFreshCount / dbCandidateCount : 0;
  const dedupeScore = 1 - duplicateRate;
  const qualityScore = (coverageRatio * 0.4) + (freshnessRatio * 0.3) + (dedupeScore * 0.3);

  // Determine if crawl is needed
  const needsCrawl = shouldTriggerCrawl(dbCandidateCount, dbFreshCount, duplicateRate, config);

  const titleVariants = plan.expandedQueries.map((q) => q.queryTitle);
  const locationClauses = plan.crawlPlan.sourcePlans.flatMap((p) => p.locationClauses);
  const plannedPlatforms = plan.crawlPlan.sourcePlans
    .filter((p) => p.maxSources > 0)
    .map((p) => p.platform);

  const skippedReasons = plan.crawlPlan.sourcePlans
    .filter((p) => p.skipReason)
    .map((p) => `${p.platform}: ${p.skipReason}`);

  return {
    dbCandidateCount,
    dbRelevantCount: dbCandidateCount,
    dbFreshCount,
    crawlTriggered: needsCrawl,
    crawlReason: needsCrawl
      ? buildCrawlReason(dbCandidateCount, dbFreshCount, duplicateRate, config)
      : `Database returned ${dbCandidateCount} results (${dbFreshCount} fresh), meeting the ${config.minResults} minimum target.`,
    plannedPlatforms,
    plannedQueries: titleVariants.slice(0, 12),
    plannedLocationClauses: locationClauses.slice(0, 8),
    skippedSourcesReason: skippedReasons.join("; "),
    duplicateRate,
    qualityScore,
    titleVariantsUsed: titleVariants,
    filteredByTitle: 0,
    filteredByLocation: 0,
    filteredBySeniority: 0,
    aborted: false,
  };
}

/**
 * Determine whether a crawl should be triggered based on DB result quality.
 */
function shouldTriggerCrawl(
  candidateCount: number,
  freshCount: number,
  duplicateRate: number,
  config: AgentConfig,
): boolean {
  // Not enough results
  if (candidateCount < config.minResults) return true;

  // Too many duplicates
  if (duplicateRate > 0.5 && candidateCount < config.minResults * 2) return true;

  // Too few fresh results
  if (freshCount < config.minResults * 0.5) return true;

  // In fast mode, be more aggressive about not crawling
  if (config.mode === "fast" && candidateCount >= config.minResults) return false;

  // In balanced mode: crawl if less than 50% of target met
  if (config.mode === "balanced" && candidateCount < config.minResults * 0.5) return true;

  return false;
}

/**
 * Build a human-readable reason for why crawl was triggered.
 */
function buildCrawlReason(
  candidateCount: number,
  freshCount: number,
  duplicateRate: number,
  config: AgentConfig,
): string {
  const reasons: string[] = [];

  if (candidateCount < config.minResults) {
    reasons.push(
      `Only ${candidateCount} results in DB (target: ${config.minResults})`,
    );
  }

  if (freshCount < config.minResults * 0.5) {
    reasons.push(
      `Only ${freshCount} fresh results (cutoff: ${config.freshnessDays} days)`,
    );
  }

  if (duplicateRate > 0.5) {
    reasons.push(
      `High duplicate rate: ${(duplicateRate * 100).toFixed(0)}%`,
    );
  }

  return reasons.join("; ");
}

/**
 * Make the agent decision: what action to take.
 */
export function makeAgentDecision(
  plan: RetrievalPlan,
  qualityReport: RetrievalQualityReport,
): AgentDecision {
  let action: AgentDecision["action"];
  let servedFrom: AgentDecision["servedFrom"];

  if (qualityReport.crawlTriggered) {
    // Determine if we should crawl now or defer to background ingestion
    if (qualityReport.dbCandidateCount === 0) {
      action = "trigger_crawl";
      servedFrom = "crawl";
    } else if (qualityReport.dbCandidateCount < plan.minResultsTarget * 0.3) {
      action = "trigger_crawl";
      servedFrom = "mixed";
    } else {
      action = "trigger_background_ingestion";
      servedFrom = "database";
    }
  } else {
    action = "return_db_results";
    servedFrom = "database";
  }

  // Update crawl plan if triggered
  const updatedCrawlPlan: CrawlPlan = {
    ...plan.crawlPlan,
    shouldCrawl: qualityReport.crawlTriggered,
    reason: qualityReport.crawlReason,
  };

  return {
    action,
    reason: qualityReport.crawlReason || "Sufficient DB results available.",
    retrievalPlan: {
      ...plan,
      crawlPlan: updatedCrawlPlan,
    },
    qualityReport,
    servedFrom,
    diagnostics: {},
  };
}