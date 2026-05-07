import "server-only";

import type { SearchFilters, JobListing } from "@/lib/types";
import { getEnv } from "@/lib/server/env";
import type { AgentConfig, AgentDiagnostics, AgentDecision, RetrievalPlan, JobSearchIntent, ExpandedRoleQuery } from "./types";
import { DEFAULT_AGENT_CONFIG } from "./types";
import { parseSearchIntent } from "./intent-parser";
import { buildRetrievalPlan } from "./retrieval-planner";
import { evaluateResults, makeAgentDecision } from "./result-evaluator";
import { buildAgentDiagnostics, buildDisabledAgentDiagnostics, validateAgentDiagnostics } from "./agent-diagnostics";

export type {
  AgentDiagnostics,
  AgentDecision,
  AgentConfig,
  RetrievalPlan,
  CrawlPlan,
  SourcePlan,
  JobSearchIntent,
  ExpandedRoleQuery,
} from "./types";

/**
 * Agent configuration from environment variables.
 */
export function resolveAgentConfig(): AgentConfig {
  const env = getEnv();

  return {
    enabled: process.env.JOB_AGENT_ENABLED !== "false",
    llmEnabled: process.env.JOB_AGENT_LLM_ENABLED === "true",
    mode: (process.env.JOB_AGENT_MODE as "fast" | "balanced" | "deep") ?? DEFAULT_AGENT_CONFIG.mode,
    minResults: parseInt(process.env.JOB_AGENT_MIN_RESULTS ?? String(DEFAULT_AGENT_CONFIG.minResults), 10),
    freshnessDays: parseInt(process.env.JOB_AGENT_FRESHNESS_DAYS ?? String(DEFAULT_AGENT_CONFIG.freshnessDays), 10),
    maxCrawlQueries: parseInt(process.env.JOB_AGENT_MAX_CRAWL_QUERIES ?? String(DEFAULT_AGENT_CONFIG.maxCrawlQueries), 10),
    maxLocationClauses: parseInt(process.env.JOB_AGENT_MAX_LOCATION_CLAUSES ?? String(DEFAULT_AGENT_CONFIG.maxLocationClauses), 10),
    sourceCooldownMinutes: parseInt(process.env.JOB_AGENT_SOURCE_COOLDOWN_MINUTES ?? String(DEFAULT_AGENT_CONFIG.sourceCooldownMinutes), 10),
    debug: process.env.JOB_AGENT_DEBUG === "true",
  };
}

/**
 * Run the agent orchestration pipeline:
 * 1. Parse user search intent
 * 2. Expand job titles
 * 3. Build retrieval plan
 * 4. Query database
 * 5. Evaluate quality
 * 6. Make decision (return / crawl / background ingest)
 * 7. Build diagnostics
 */
export async function runJobRetrievalAgent(
  filters: SearchFilters,
  options: {
    dbQueryFn: (filter: Record<string, unknown>, limit: number) => Promise<Array<{
      _id: string;
      title: string;
      indexedAt?: string;
      crawledAt?: string;
      discoveredAt?: string;
      sourcePlatform?: string;
    }>>;
    config?: AgentConfig;
  },
): Promise<{
  agentDecision: AgentDecision;
  agentDiagnostics: AgentDiagnostics;
  dbResults: Array<{
    _id: string;
    title: string;
    indexedAt?: string;
    crawledAt?: string;
    discoveredAt?: string;
    sourcePlatform?: string;
  }>;
}> {
  const totalStart = Date.now();
  const stageTimings: AgentDiagnostics["stageTimingsMs"] = {
    intentParsing: 0,
    queryExpansion: 0,
    dbQuery: 0,
    qualityEvaluation: 0,
    crawlPlanning: 0,
    total: 0,
  };
  const decisionChain: string[] = [];
  const warnings: string[] = [];
  const config = options.config ?? resolveAgentConfig();

  decisionChain.push("Agent initialized");

  if (!config.enabled) {
    warnings.push("Agent is disabled; returning empty diagnostics.");
    return {
      agentDecision: {
        action: "return_empty",
        reason: "Agent is disabled",
        retrievalPlan: {
          intent: {
            rawTitle: filters.title,
            normalizedTitle: "",
            rawLocation: "",
            resolvedLocationScope: "none",
            remotePreference: "none",
            sponsorshipPreference: "none",
            platformFilters: [],
            excludedSeniorities: [],
            preferredSeniorities: [],
            titleVariants: [],
          },
          expandedQueries: [],
          crawlPlan: {
            shouldCrawl: false,
            reason: "Agent disabled",
            mode: "none",
            maxQueries: 0,
            maxLocationClauses: 0,
            sourcePlans: [],
            estimatedDurationMs: 0,
            abortable: true,
          },
          useDbFirst: true,
          dbQueryFilter: {},
          dbCandidateLimit: 0,
          minResultsTarget: 0,
          freshnessDays: 0,
          createdAt: new Date().toISOString(),
        },
        qualityReport: {
          dbCandidateCount: 0,
          dbRelevantCount: 0,
          dbFreshCount: 0,
          crawlTriggered: false,
          crawlReason: "Agent disabled",
          plannedPlatforms: [],
          plannedQueries: [],
          plannedLocationClauses: [],
          skippedSourcesReason: "Agent disabled",
          duplicateRate: 0,
          qualityScore: 0,
          titleVariantsUsed: [],
          filteredByTitle: 0,
          filteredByLocation: 0,
          filteredBySeniority: 0,
          aborted: false,
        },
        servedFrom: "empty",
        diagnostics: {},
      },
      agentDiagnostics: buildDisabledAgentDiagnostics("Agent is disabled"),
      dbResults: [],
    };
  }

  // Stage 1: Parse intent
  decisionChain.push("Parsing search intent");
  const intentStart = Date.now();
  let intent: JobSearchIntent;
  try {
    intent = parseSearchIntent(filters);
    stageTimings.intentParsing = Date.now() - intentStart;
    decisionChain.push(
      `Intent parsed: title="${intent.normalizedTitle}", location=${intent.resolvedLocationScope}, family=${intent.roleFamily ?? "unknown"}`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown intent parsing error";
    warnings.push(`Intent parsing failed: ${message}`);
    stageTimings.intentParsing = Date.now() - intentStart;
    // Fallback intent
    intent = {
      rawTitle: filters.title,
      normalizedTitle: filters.title,
      rawLocation: [filters.city, filters.state, filters.country].filter(Boolean).join(", "),
      resolvedLocationScope: filters.country ? "country" : "none",
      country: filters.country,
      state: filters.state,
      city: filters.city,
      remotePreference: "none",
      sponsorshipPreference: "none",
      platformFilters: [],
      excludedSeniorities: [],
      preferredSeniorities: [],
      titleVariants: [],
    };
  }

  // Stage 2: Expand queries
  decisionChain.push("Expanding job title queries");
  const expandStart = Date.now();
  let expandedQueries: ExpandedRoleQuery[];
  try {
    const plan = buildRetrievalPlan(intent, config);
    expandedQueries = plan.expandedQueries;
    stageTimings.queryExpansion = Date.now() - expandStart;
    decisionChain.push(
      `Expanded to ${expandedQueries.length} title variants (anchor: ${expandedQueries[0]?.queryTitle ?? "none"})`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown query expansion error";
    warnings.push(`Query expansion failed: ${message}`);
    stageTimings.queryExpansion = Date.now() - expandStart;
    expandedQueries = [
      { queryTitle: intent.normalizedTitle, relevanceScore: 1.0, tier: "anchor", isBroadMatch: false },
    ];
  }

  // Stage 3: Build retrieval plan (timed separately)
  const planStart = Date.now();
  const plan = buildRetrievalPlan(intent, config);
  stageTimings.crawlPlanning = Date.now() - planStart;
  decisionChain.push("Retrieval plan built");

  // Stage 4: Query database
  decisionChain.push("Querying database");
  const dbStart = Date.now();
  let dbResults: Array<{
    _id: string;
    title: string;
    indexedAt?: string;
    crawledAt?: string;
    discoveredAt?: string;
    sourcePlatform?: string;
  }> = [];
  try {
    dbResults = await options.dbQueryFn(plan.dbQueryFilter, plan.dbCandidateLimit);
    stageTimings.dbQuery = Date.now() - dbStart;
    decisionChain.push(
      `Database returned ${dbResults.length} candidates`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown DB query error";
    warnings.push(`Database query failed: ${message}`);
    stageTimings.dbQuery = Date.now() - dbStart;
    dbResults = [];
  }

  // Stage 5: Evaluate quality
  decisionChain.push("Evaluating result quality");
  const evalStart = Date.now();
  const qualityReport = evaluateResults(plan, dbResults, config);
  stageTimings.qualityEvaluation = Date.now() - evalStart;
  decisionChain.push(
    `Quality: ${qualityReport.qualityScore.toFixed(2)}, crawl=${qualityReport.crawlTriggered}, reason=${qualityReport.crawlReason}`,
  );

  // Stage 6: Make decision
  const decision = makeAgentDecision(plan, qualityReport);
  decisionChain.push(`Decision: ${decision.action} (served from ${decision.servedFrom})`);

  // Stage 7: Build diagnostics
  stageTimings.total = Date.now() - totalStart;
  const agentDiagnostics = validateAgentDiagnostics(
    buildAgentDiagnostics(decision, intent, expandedQueries, config, stageTimings, decisionChain, warnings, dbResults),
  );

  if (config.debug) {
    console.log("[agent:diagnostics]", JSON.stringify({
      intentTitle: intent.normalizedTitle,
      intentLocation: intent.resolvedLocationScope,
      expandedQueryCount: expandedQueries.length,
      dbCandidateCount: qualityReport.dbCandidateCount,
      dbFreshCount: qualityReport.dbFreshCount,
      crawlTriggered: qualityReport.crawlTriggered,
      qualityScore: qualityReport.qualityScore.toFixed(2),
      action: decision.action,
      stageTimingsMs: stageTimings,
      decisionChain,
      warnings,
    }));
  }

  return {
    agentDecision: decision,
    agentDiagnostics,
    dbResults,
  };
}