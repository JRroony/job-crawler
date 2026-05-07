import { describe, it, expect } from "vitest";
import { parseSearchIntent, classifySeniority, mapSeniorityToExperienceLevel } from "@/lib/server/agent/intent-parser";
import { expandJobTitle, buildUSLocationClauses, buildLocationClauses } from "@/lib/server/agent/query-expander";
import { buildRetrievalPlan } from "@/lib/server/agent/retrieval-planner";
import { evaluateResults, makeAgentDecision } from "@/lib/server/agent/result-evaluator";
import { buildAgentDiagnostics, buildDisabledAgentDiagnostics, validateAgentDiagnostics } from "@/lib/server/agent/agent-diagnostics";
import { resolveAgentConfig } from "@/lib/server/agent/job-retrieval-agent";
import { DEFAULT_AGENT_CONFIG, type AgentConfig } from "@/lib/server/agent/types";
import type { SearchFilters } from "@/lib/types";

function makeFilters(overrides: Partial<SearchFilters> = {}): SearchFilters {
  return { title: "Software Engineer", country: "United States", ...overrides };
}

function makeDbResults(count: number, freshCount?: number) {
  const results: Array<{ _id: string; title: string; indexedAt?: string; crawledAt?: string; discoveredAt?: string; sourcePlatform?: string }> = [];
  const now = new Date();
  for (let i = 0; i < count; i++) {
    const isFresh = freshCount === undefined || i < freshCount;
    const date = isFresh ? now : new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    results.push({
      _id: `job-${i}`, title: `Software Engineer ${i}`,
      indexedAt: date.toISOString(), crawledAt: date.toISOString(), discoveredAt: date.toISOString(),
      sourcePlatform: "greenhouse",
    });
  }
  return results;
}

describe("Agent Configuration", () => {
  it("resolves default config with env defaults", () => {
    const config = resolveAgentConfig();
    expect(config.enabled).toBe(true);
    expect(config.llmEnabled).toBe(false);
    expect(config.mode).toBe("fast");
    expect(config.minResults).toBeGreaterThan(0);
    expect(config.freshnessDays).toBeGreaterThan(0);
    expect(config.maxCrawlQueries).toBeGreaterThan(0);
  });
});

describe("Intent Parser", () => {
  it("parses Software Engineer + US", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    expect(intent.normalizedTitle).toBeTruthy();
    expect(intent.roleFamily).toBeDefined();
    expect(intent.country).toBe("United States");
  });

  it("parses Data Analyst + US", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Data Analyst", country: "United States" }));
    expect(intent.normalizedTitle).toBeTruthy();
    expect(intent.country).toBe("United States");
  });

  it("detects remote preference for Product Manager Remote", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Product Manager", city: "Remote" }));
    expect(intent.remotePreference).toBe("preferred");
  });

  it("parses Machine Learning Engineer with city/state", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Machine Learning Engineer", city: "Seattle", state: "WA", country: "United States" }));
    expect(intent.city).toBe("Seattle");
    expect(intent.country).toBe("United States");
  });

  it("detects city/state US location scope", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", city: "Seattle", state: "WA", country: "United States" }));
    expect(intent.city).toBe("Seattle");
    expect(intent.state).toBe("Washington");
    expect(intent.country).toBe("United States");
  });
});

describe("Seniority Classification", () => {
  it("Senior Software Engineer → senior (high)", () => {
    const r = classifySeniority("Senior Software Engineer");
    expect(r[0].level).toBe("senior");
    expect(r[0].confidence).toBe("high");
  });
  it("Staff Backend Engineer → staff", () => {
    expect(classifySeniority("Staff Backend Engineer")[0].level).toBe("staff");
  });
  it("Principal Data Scientist → principal", () => {
    expect(classifySeniority("Principal Data Scientist")[0].level).toBe("principal");
  });
  it("Lead Product Manager → lead", () => {
    expect(classifySeniority("Lead Product Manager")[0].level).toBe("lead");
  });
  it("Software Engineer II → mid", () => {
    expect(classifySeniority("Software Engineer II")[0].level).toBe("mid");
  });
  it("Software Engineer, New Grad → new_grad", () => {
    expect(classifySeniority("Software Engineer, New Grad")[0].level).toBe("new_grad");
  });
  it("Associate Product Manager → junior", () => {
    expect(classifySeniority("Associate Product Manager")[0].level).toBe("junior");
  });
  it("Director of Engineering → director", () => {
    expect(classifySeniority("Director of Engineering")[0].level).toBe("director");
  });
  it("maps seniority to experience level correctly", () => {
    expect(mapSeniorityToExperienceLevel("senior")).toBe("senior");
    expect(mapSeniorityToExperienceLevel("staff")).toBe("staff");
    expect(mapSeniorityToExperienceLevel("director")).toBe("director");
    expect(mapSeniorityToExperienceLevel("executive")).toBe("executive");
    expect(mapSeniorityToExperienceLevel("unknown")).toBe("unknown");
    expect(mapSeniorityToExperienceLevel("invalid")).toBe("unknown");
  });
});

describe("Query Expansion", () => {
  it("expands Software Engineer to 5+ variants", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const queries = expandJobTitle(intent);
    expect(queries.length).toBeGreaterThan(5);
    const titles = queries.map((q) => q.queryTitle);
    expect(titles).toContain("software engineer");
    expect(titles.some((t) => /backend|frontend|full stack/i.test(t))).toBe(true);
    expect(queries[0].relevanceScore).toBeGreaterThanOrEqual(0.9);
  });

  it("expands Data Analyst to related roles", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Data Analyst" }));
    const queries = expandJobTitle(intent);
    const titles = queries.map((q) => q.queryTitle);
    expect(titles).toContain("data analyst");
    expect(titles.some((t) => /business|product analyst|reporting/i.test(t))).toBe(true);
  });

  it("expands Product Manager to related roles", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Product Manager" }));
    const queries = expandJobTitle(intent);
    const titles = queries.map((q) => q.queryTitle);
    expect(titles).toContain("product manager");
    expect(titles.some((t) => /associate|owner|growth/i.test(t))).toBe(true);
  });

  it("expands Machine Learning Engineer", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Machine Learning Engineer" }));
    const queries = expandJobTitle(intent);
    const titles = queries.map((q) => q.queryTitle);
    expect(titles).toContain("machine learning engineer");
    expect(titles.some((t) => /ai|ml|scientist/i.test(t))).toBe(true);
  });

  it("handles unknown title with fallback", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Flamingle Wrangler" }));
    const queries = expandJobTitle(intent);
    expect(queries.length).toBeGreaterThan(0);
    expect(queries[0].tier).toBe("anchor");
  });

  it("assigns tiers correctly", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const queries = expandJobTitle(intent);
    const tiers = queries.map((q) => q.tier);
    expect(tiers[0]).toBe("anchor");
    expect(tiers.some((t) => t === "core")).toBe(true);
    expect(tiers.some((t) => t === "adjacent")).toBe(true);
  });

  it("does not produce duplicate queries", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const queries = expandJobTitle(intent);
    const titles = queries.map((q) => q.queryTitle);
    expect(new Set(titles).size).toBe(titles.length);
  });
});

describe("Location Clauses", () => {
  it("builds US-wide location clauses up to max", () => {
    const clauses = buildUSLocationClauses(8);
    expect(clauses.length).toBeLessThanOrEqual(8);
    expect(clauses).toContain("Seattle WA");
    expect(clauses).toContain("San Francisco CA");
    expect(clauses).toContain("New York NY");
  });

  it("builds clauses for US country search", () => {
    const clauses = buildLocationClauses("United States", undefined, undefined, false, 8);
    expect(clauses.length).toBeGreaterThan(0);
  });

  it("builds clauses for city/state with nearby metros", () => {
    const clauses = buildLocationClauses("United States", "Washington", "Seattle", false, 8);
    expect(clauses.length).toBeGreaterThan(0);
    expect(clauses.some((c) => c.includes("Seattle"))).toBe(true);
  });

  it("includes remote clause when remote preferred", () => {
    const clauses = buildLocationClauses("United States", undefined, undefined, true, 8);
    expect(clauses.some((c) => c.toLowerCase().includes("remote"))).toBe(true);
  });
});

describe("Retrieval Planner", () => {
  it("builds a DB-first retrieval plan", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    expect(plan.useDbFirst).toBe(true);
    expect(plan.expandedQueries.length).toBeGreaterThan(0);
    expect(plan.crawlPlan.sourcePlans.length).toBeGreaterThan(0);
    expect(plan.dbQueryFilter).toBeDefined();
  });

  it("includes all active platforms in crawl plan", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const platforms = plan.crawlPlan.sourcePlans.map((sp) => sp.platform);
    expect(platforms).toContain("greenhouse");
    expect(platforms).toContain("lever");
    expect(platforms).toContain("ashby");
  });

  it("honors max crawl query budget", () => {
    const config: AgentConfig = { ...DEFAULT_AGENT_CONFIG, maxCrawlQueries: 5 };
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, config);
    expect(plan.crawlPlan.maxQueries).toBe(5);
  });

  it("sets crawl mode based on config", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    expect(buildRetrievalPlan(intent, { ...DEFAULT_AGENT_CONFIG, mode: "fast" }).crawlPlan.mode).toBe("targeted");
    expect(buildRetrievalPlan(intent, { ...DEFAULT_AGENT_CONFIG, mode: "deep" }).crawlPlan.mode).toBe("full");
  });
});

describe("Result Evaluator", () => {
  it("triggers crawl when DB results are too low", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const dbResults = makeDbResults(5, 5);
    const report = evaluateResults(plan, dbResults, DEFAULT_AGENT_CONFIG);
    expect(report.crawlTriggered).toBe(true);
    expect(report.crawlReason).toContain("Only 5 results");
  });

  it("does NOT trigger crawl when DB has enough fresh results", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const dbResults = makeDbResults(100, 100);
    const report = evaluateResults(plan, dbResults, DEFAULT_AGENT_CONFIG);
    expect(report.crawlTriggered).toBe(false);
  });

  it("triggers crawl when high duplicate rate with low count", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const results = Array.from({ length: 10 }, (_, i) => ({
      _id: `job-${i}`, title: "Software Engineer",
      indexedAt: new Date().toISOString(), crawledAt: new Date().toISOString(),
      discoveredAt: new Date().toISOString(), sourcePlatform: "greenhouse",
    }));
    const report = evaluateResults(plan, results, DEFAULT_AGENT_CONFIG);
    expect(report.duplicateRate).toBeGreaterThan(0.5);
    expect(report.crawlTriggered).toBe(true);
  });

  it("computes quality score between 0 and 1", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const report = evaluateResults(plan, makeDbResults(30, 20), DEFAULT_AGENT_CONFIG);
    expect(report.qualityScore).toBeGreaterThanOrEqual(0);
    expect(report.qualityScore).toBeLessThanOrEqual(1);
  });
});

describe("Agent Decision", () => {
  it("returns return_db_results when crawl is not triggered", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const dbResults = makeDbResults(100, 100);
    const report = evaluateResults(plan, dbResults, DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    expect(decision.action).toBe("return_db_results");
    expect(decision.servedFrom).toBe("database");
  });

  it("triggers crawl when DB is empty", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const report = evaluateResults(plan, [], DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    expect(decision.action).toBe("trigger_crawl");
  });

  it("triggers background ingestion when DB has some but not enough results", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    // 30 results, less than 50 min, but not zero
    const dbResults = makeDbResults(30, 30);
    const report = evaluateResults(plan, dbResults, DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    expect(["trigger_crawl", "trigger_background_ingestion"]).toContain(decision.action);
  });

  it("includes crawl reason in decision", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const report = evaluateResults(plan, makeDbResults(5, 5), DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    expect(decision.reason.length).toBeGreaterThan(0);
  });
});

describe("Agent Diagnostics", () => {
  it("builds diagnostics with all fields populated", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const dbResults = makeDbResults(30, 20);
    const report = evaluateResults(plan, dbResults, DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    const diag = buildAgentDiagnostics(decision, intent, plan.expandedQueries, DEFAULT_AGENT_CONFIG, { intentParsing: 1, queryExpansion: 2, dbQuery: 3, qualityEvaluation: 1, crawlPlanning: 1, total: 8 }, ["step1", "step2"], [], dbResults);
    expect(diag.agentEnabled).toBe(true);
    expect(diag.agentMode).toBe("fast");
    expect(diag.crawlTriggered).toBeDefined();
    expect(diag.dbCandidateCount).toBeGreaterThan(0);
    expect(diag.decisionChain.length).toBeGreaterThan(0);
    expect(diag.stageTimingsMs.total).toBeGreaterThan(0);
    expect(diag.platformSupportStatuses).toBeDefined();
  });

  it("builds disabled diagnostics", () => {
    const diag = buildDisabledAgentDiagnostics("Agent disabled for testing");
    expect(diag.agentEnabled).toBe(false);
    expect(diag.warnings).toContain("Agent disabled for testing");
  });

  it("validates diagnostics and clamps score range", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const report = evaluateResults(plan, makeDbResults(30, 30), DEFAULT_AGENT_CONFIG);
    const decision = makeAgentDecision(plan, report);
    const diag = buildAgentDiagnostics(decision, intent, plan.expandedQueries, DEFAULT_AGENT_CONFIG, { intentParsing: 0, queryExpansion: 0, dbQuery: 0, qualityEvaluation: 0, crawlPlanning: 0, total: 0 }, [], [], makeDbResults(30, 30));
    const validated = validateAgentDiagnostics(diag);
    expect(validated.qualityScore).toBeGreaterThanOrEqual(0);
    expect(validated.qualityScore).toBeLessThanOrEqual(1);
    expect(validated.duplicateRate).toBeGreaterThanOrEqual(0);
    expect(validated.duplicateRate).toBeLessThanOrEqual(1);
  });
});

describe("US Location Scope", () => {
  it("treats United States as all US locations, not requiring raw location text to contain US", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "United States" }));
    expect(intent.resolvedLocationScope).toBe("country");
    expect(intent.country).toBe("United States");
    // Location clauses should cover major US metros
    const clauses = buildLocationClauses(intent.country, intent.state, intent.city, intent.remotePreference !== "none", 8);
    expect(clauses.length).toBeGreaterThan(0);
  });

  it("handles US abbreviation", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", country: "US" }));
    expect(intent.country).toBe("United States");
  });
});

describe("Platform Support Status Honesty", () => {
  it("does not overclaim platform support in source plans", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer" }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const statuses = plan.crawlPlan.sourcePlans.map((sp) => ({ platform: sp.platform, status: sp.supportStatus }));
    for (const { platform, status } of statuses) {
      if (platform === "workday" || platform === "company_page") {
        expect(["partial", "planned"]).toContain(status);
      }
    }
    const unsupported = plan.crawlPlan.sourcePlans.filter((sp) => sp.skipReason?.includes("unavailable"));
    expect(unsupported.length).toBe(0);
  });

  it("honors platform filters from user intent", () => {
    const intent = parseSearchIntent(makeFilters({ title: "Software Engineer", platforms: ["greenhouse"] }));
    const plan = buildRetrievalPlan(intent, DEFAULT_AGENT_CONFIG);
    const active = plan.crawlPlan.sourcePlans.filter((sp) => sp.maxSources > 0);
    expect(active.length).toBe(1);
    expect(active[0].platform).toBe("greenhouse");
  });
});