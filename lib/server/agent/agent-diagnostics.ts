import type {
  AgentDiagnostics,
  AgentDecision,
  AgentConfig,
  JobSearchIntent,
  ExpandedRoleQuery,
  PlatformSupportStatus,
} from "./types";
import { platformSupportStatuses } from "./types";

/**
 * Build comprehensive agent diagnostics from the decision and supporting data.
 * This is the diagnostics payload exposed to the API/UI.
 */
export function buildAgentDiagnostics(
  decision: AgentDecision,
  intent: JobSearchIntent,
  expandedQueries: ExpandedRoleQuery[],
  config: AgentConfig,
  stageTimingsMs: AgentDiagnostics["stageTimingsMs"],
  decisionChain: string[],
  warnings: string[],
  dbResults: Array<{
    _id: string;
    title: string;
    indexedAt?: string;
    crawledAt?: string;
    discoveredAt?: string;
    sourcePlatform?: string;
  }> = [],
): AgentDiagnostics {
  // Build platform support statuses
  const platformSupportStatusesMap: Record<string, PlatformSupportStatus> = {};
  for (const sp of decision.retrievalPlan.crawlPlan.sourcePlans) {
    platformSupportStatusesMap[sp.platform] = sp.supportStatus;
  }
  // Ensure all known platforms are represented
  for (const platform of ["greenhouse", "lever", "ashby", "smartrecruiters", "workday", "company_page"]) {
    if (!platformSupportStatusesMap[platform]) {
      platformSupportStatusesMap[platform] = "unavailable";
    }
  }

  // Count filtered results (from DB evaluation)
  const filteredByTitle = dbResults.length > 0
    ? dbResults.filter((j) =>
        !expandedQueries.some((q) =>
          j.title.toLowerCase().includes(q.queryTitle.toLowerCase()),
        ),
      ).length
    : 0;

  const titleVariantsUsed = expandedQueries.map((q) => q.queryTitle);
  const locationClausesUsed = decision.retrievalPlan.crawlPlan.sourcePlans.flatMap(
    (p) => p.locationClauses,
  );

  return {
    agentEnabled: config.enabled,
    agentMode: config.mode,
    llmEnabled: config.llmEnabled,
    intent,
    expandedQueries: expandedQueries.slice(0, 20),
    dbQueryFilterSummary: decision.retrievalPlan.dbQueryFilter,
    dbCandidateCount: decision.qualityReport.dbCandidateCount,
    dbRelevantCount: decision.qualityReport.dbRelevantCount,
    dbFreshCount: decision.qualityReport.dbFreshCount,
    crawlTriggered: decision.qualityReport.crawlTriggered,
    crawlReason: decision.qualityReport.crawlReason,
    plannedPlatforms: decision.qualityReport.plannedPlatforms,
    plannedQueries: decision.qualityReport.plannedQueries,
    plannedLocationClauses: decision.qualityReport.plannedLocationClauses,
    platformSupportStatuses: platformSupportStatusesMap,
    filteredByTitle,
    filteredByLocation: decision.qualityReport.filteredByLocation,
    filteredBySeniority: decision.qualityReport.filteredBySeniority,
    titleVariantsUsed: titleVariantsUsed.slice(0, 20),
    locationClausesUsed: locationClausesUsed.slice(0, 8),
    duplicateRate: decision.qualityReport.duplicateRate,
    qualityScore: decision.qualityReport.qualityScore,
    skippedSourcesReason: decision.qualityReport.skippedSourcesReason,
    abortReason: decision.qualityReport.abortReason,
    decisionChain,
    warnings,
    totalDurationMs: stageTimingsMs.total,
    stageTimingsMs,
  };
}

/**
 * Build an empty diagnostics object for cases where the agent is disabled.
 */
export function buildDisabledAgentDiagnostics(
  reason: string = "Agent is disabled",
): AgentDiagnostics {
  return {
    agentEnabled: false,
    agentMode: "fast",
    llmEnabled: false,
    intent: {
      rawTitle: "",
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
    dbQueryFilterSummary: {},
    dbCandidateCount: 0,
    dbRelevantCount: 0,
    dbFreshCount: 0,
    crawlTriggered: false,
    crawlReason: reason,
    plannedPlatforms: [],
    plannedQueries: [],
    plannedLocationClauses: [],
    platformSupportStatuses: {},
    filteredByTitle: 0,
    filteredByLocation: 0,
    filteredBySeniority: 0,
    titleVariantsUsed: [],
    locationClausesUsed: [],
    duplicateRate: 0,
    qualityScore: 0,
    skippedSourcesReason: reason,
    decisionChain: [],
    warnings: [reason],
    totalDurationMs: 0,
    stageTimingsMs: {
      intentParsing: 0,
      queryExpansion: 0,
      dbQuery: 0,
      qualityEvaluation: 0,
      crawlPlanning: 0,
      total: 0,
    },
  };
}

/**
 * Validate diagnostics against the schema.
 * Returns the diagnostics if valid, or a fallback with a warning if invalid.
 */
export function validateAgentDiagnostics(
  diagnostics: AgentDiagnostics,
): AgentDiagnostics {
  try {
    // Basic structural validation
    if (!diagnostics.intent) {
      diagnostics.intent = {
        rawTitle: "",
        normalizedTitle: "",
        rawLocation: "",
        resolvedLocationScope: "none",
        remotePreference: "none",
        sponsorshipPreference: "none",
        platformFilters: [],
        excludedSeniorities: [],
        preferredSeniorities: [],
        titleVariants: [],
      };
    }

    if (diagnostics.qualityScore < 0 || diagnostics.qualityScore > 1) {
      diagnostics.qualityScore = Math.max(0, Math.min(1, diagnostics.qualityScore));
    }

    if (diagnostics.duplicateRate < 0 || diagnostics.duplicateRate > 1) {
      diagnostics.duplicateRate = Math.max(0, Math.min(1, diagnostics.duplicateRate));
    }

    return diagnostics;
  } catch {
    return {
      ...diagnostics,
      warnings: [...(diagnostics.warnings ?? []), "Diagnostics validation failed; using fallback values."],
    };
  }
}