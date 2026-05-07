import { z } from "zod";

import {
  experienceLevels,
  experienceLevelSchema,
  crawlerPlatforms,
  activeCrawlerPlatforms,
} from "@/lib/types";

// ─── Seniority Levels ────────────────────────────────────────────────
export const seniorityLevels = experienceLevels;
export const seniorityLevelSchema = experienceLevelSchema;

// ─── Platform Support Status ─────────────────────────────────────────
export const platformSupportStatuses = [
  "supported",
  "partial",
  "planned",
  "disabled",
  "unavailable",
] as const;

export const platformSupportStatusSchema = z.enum(platformSupportStatuses);

export type PlatformSupportStatus = z.infer<typeof platformSupportStatusSchema>;

// ─── Job Search Intent ───────────────────────────────────────────────
export const jobSearchIntentSchema = z.object({
  rawTitle: z.string(),
  normalizedTitle: z.string(),
  roleFamily: z.string().optional(),
  titleVariants: z.array(z.string()).default([]),
  excludedSeniorities: z.array(seniorityLevelSchema).default([]),
  preferredSeniorities: z.array(seniorityLevelSchema).default([]),
  rawLocation: z.string(),
  resolvedLocationScope: z.enum([
    "none",
    "city",
    "state",
    "country",
    "remote",
    "global_remote",
    "ambiguous",
  ]),
  country: z.string().optional(),
  state: z.string().optional(),
  city: z.string().optional(),
  remotePreference: z.enum(["none", "preferred", "required"]).default("none"),
  sponsorshipPreference: z.enum(["none", "preferred", "required"]).default("none"),
  platformFilters: z.array(z.string()).default([]),
});

export type JobSearchIntent = z.infer<typeof jobSearchIntentSchema>;

// ─── Expanded Role Query ─────────────────────────────────────────────
export const expandedRoleQuerySchema = z.object({
  queryTitle: z.string(),
  canonicalTitle: z.string().optional(),
  family: z.string().optional(),
  conceptId: z.string().optional(),
  relevanceScore: z.number().min(0).max(1).default(0.8),
  tier: z.enum(["anchor", "core", "adjacent", "supplemental"]).default("core"),
  isBroadMatch: z.boolean().default(false),
});

export type ExpandedRoleQuery = z.infer<typeof expandedRoleQuerySchema>;

// ─── Source Plan ─────────────────────────────────────────────────────
export const sourcePlanSchema = z.object({
  platform: z.string(),
  supportStatus: platformSupportStatusSchema,
  priority: z.number().int().min(0).max(10).default(5),
  maxSources: z.number().int().positive().default(40),
  searchQueries: z.array(z.string()).default([]),
  locationClauses: z.array(z.string()).default([]),
  cooldownMinutes: z.number().int().nonnegative().default(30),
  skipReason: z.string().optional(),
});

export type SourcePlan = z.infer<typeof sourcePlanSchema>;

// ─── Crawl Plan ──────────────────────────────────────────────────────
export const crawlPlanSchema = z.object({
  shouldCrawl: z.boolean(),
  reason: z.string().default(""),
  mode: z.enum(["none", "targeted", "full"]).default("targeted"),
  maxQueries: z.number().int().positive().default(12),
  maxLocationClauses: z.number().int().positive().default(8),
  sourcePlans: z.array(sourcePlanSchema).default([]),
  estimatedDurationMs: z.number().int().nonnegative().default(0),
  abortable: z.boolean().default(true),
});

export type CrawlPlan = z.infer<typeof crawlPlanSchema>;

// ─── Retrieval Plan ──────────────────────────────────────────────────
export const retrievalPlanSchema = z.object({
  intent: jobSearchIntentSchema,
  expandedQueries: z.array(expandedRoleQuerySchema),
  crawlPlan: crawlPlanSchema,
  useDbFirst: z.boolean().default(true),
  dbQueryFilter: z.record(z.string(), z.unknown()).default({}),
  dbCandidateLimit: z.number().int().positive().default(500),
  minResultsTarget: z.number().int().positive().default(50),
  freshnessDays: z.number().int().positive().default(14),
  createdAt: z.string().datetime(),
});

export type RetrievalPlan = z.infer<typeof retrievalPlanSchema>;

// ─── Retrieval Quality Report ────────────────────────────────────────
export const retrievalQualityReportSchema = z.object({
  dbCandidateCount: z.number().int().nonnegative().default(0),
  dbRelevantCount: z.number().int().nonnegative().default(0),
  dbFreshCount: z.number().int().nonnegative().default(0),
  crawlTriggered: z.boolean().default(false),
  crawlReason: z.string().default(""),
  plannedPlatforms: z.array(z.string()).default([]),
  plannedQueries: z.array(z.string()).default([]),
  plannedLocationClauses: z.array(z.string()).default([]),
  skippedSourcesReason: z.string().default(""),
  duplicateRate: z.number().min(0).max(1).default(0),
  qualityScore: z.number().min(0).max(1).default(0),
  titleVariantsUsed: z.array(z.string()).default([]),
  filteredByTitle: z.number().int().nonnegative().default(0),
  filteredByLocation: z.number().int().nonnegative().default(0),
  filteredBySeniority: z.number().int().nonnegative().default(0),
  aborted: z.boolean().default(false),
  abortReason: z.string().optional(),
});

export type RetrievalQualityReport = z.infer<typeof retrievalQualityReportSchema>;

// ─── Agent Decision ──────────────────────────────────────────────────
export const agentDecisionSchema = z.object({
  action: z.enum(["return_db_results", "trigger_crawl", "return_empty", "trigger_background_ingestion"]),
  reason: z.string(),
  retrievalPlan: retrievalPlanSchema,
  qualityReport: retrievalQualityReportSchema,
  servedFrom: z.enum(["database", "crawl", "mixed", "empty"]).default("database"),
  diagnostics: z.record(z.string(), z.unknown()).default({}),
});

export type AgentDecision = z.infer<typeof agentDecisionSchema>;

// ─── Agent Diagnostics ───────────────────────────────────────────────
export const agentDiagnosticsSchema = z.object({
  agentEnabled: z.boolean().default(true),
  agentMode: z.enum(["fast", "balanced", "deep"]).default("fast"),
  llmEnabled: z.boolean().default(false),
  intent: jobSearchIntentSchema,
  expandedQueries: z.array(expandedRoleQuerySchema).default([]),
  dbQueryFilterSummary: z.record(z.string(), z.unknown()).default({}),
  dbCandidateCount: z.number().int().nonnegative().default(0),
  dbRelevantCount: z.number().int().nonnegative().default(0),
  dbFreshCount: z.number().int().nonnegative().default(0),
  crawlTriggered: z.boolean().default(false),
  crawlReason: z.string().default(""),
  plannedPlatforms: z.array(z.string()).default([]),
  plannedQueries: z.array(z.string()).default([]),
  plannedLocationClauses: z.array(z.string()).default([]),
  platformSupportStatuses: z.record(z.string(), platformSupportStatusSchema).default({}),
  filteredByTitle: z.number().int().nonnegative().default(0),
  filteredByLocation: z.number().int().nonnegative().default(0),
  filteredBySeniority: z.number().int().nonnegative().default(0),
  titleVariantsUsed: z.array(z.string()).default([]),
  locationClausesUsed: z.array(z.string()).default([]),
  duplicateRate: z.number().min(0).max(1).default(0),
  qualityScore: z.number().min(0).max(1).default(0),
  skippedSourcesReason: z.string().default(""),
  abortReason: z.string().optional(),
  decisionChain: z.array(z.string()).default([]),
  warnings: z.array(z.string()).default([]),
  totalDurationMs: z.number().int().nonnegative().default(0),
  stageTimingsMs: z
    .object({
      intentParsing: z.number().int().nonnegative().default(0),
      queryExpansion: z.number().int().nonnegative().default(0),
      dbQuery: z.number().int().nonnegative().default(0),
      qualityEvaluation: z.number().int().nonnegative().default(0),
      crawlPlanning: z.number().int().nonnegative().default(0),
      total: z.number().int().nonnegative().default(0),
    })
    .default({}),
});

export type AgentDiagnostics = z.infer<typeof agentDiagnosticsSchema>;

// ─── Agent Config ────────────────────────────────────────────────────
export interface AgentConfig {
  enabled: boolean;
  llmEnabled: boolean;
  mode: "fast" | "balanced" | "deep";
  minResults: number;
  freshnessDays: number;
  maxCrawlQueries: number;
  maxLocationClauses: number;
  sourceCooldownMinutes: number;
  debug: boolean;
}

export const DEFAULT_AGENT_CONFIG: AgentConfig = {
  enabled: true,
  llmEnabled: false,
  mode: "fast",
  minResults: 50,
  freshnessDays: 14,
  maxCrawlQueries: 12,
  maxLocationClauses: 8,
  sourceCooldownMinutes: 30,
  debug: false,
};