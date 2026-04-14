import { describe, expect, it } from "vitest";

import {
  analyzeTitle,
  buildTitleQueryVariants,
  getTitleMatchResult,
  listSupportedRoleFamilies,
  normalizeTitleToCanonicalForm,
} from "@/lib/server/title-retrieval";

function queriesFor(title: string) {
  return buildTitleQueryVariants(title).map((variant) => variant.query);
}

describe("title retrieval analysis", () => {
  it("normalizes recognized titles to stable canonical forms", () => {
    expect(normalizeTitleToCanonicalForm("Senior SWE")).toBe("software engineer");
    expect(normalizeTitleToCanonicalForm("Lead Product Manager")).toBe("product manager");
    expect(normalizeTitleToCanonicalForm("Technical Recruiter")).toBe("technical recruiter");
    expect(normalizeTitleToCanonicalForm("Documentation Specialist")).toBe("technical writer");
  });

  it("strips seniority markers without removing the core role head", () => {
    expect(analyzeTitle("Senior Backend Engineer").strippedNormalized).toBe("backend engineer");
    expect(analyzeTitle("Staff Technical Writer").strippedNormalized).toBe("technical writer");
    expect(analyzeTitle("New Graduate QA Engineer").strippedNormalized).toBe("qa engineer");
    expect(analyzeTitle("Lead Product Manager").strippedNormalized).toBe("product manager");
  });

  it("ships the generalized role families used by the taxonomy", () => {
    expect(listSupportedRoleFamilies()).toEqual([
      "software_engineering",
      "data_engineering",
      "data_analytics",
      "product",
      "program_management",
      "recruiting",
      "quality_assurance",
      "writing_documentation",
      "support",
      "sales",
      "operations",
    ]);
  });

  it("keeps extra user modifiers when inferring a family from an arbitrary title", () => {
    expect(analyzeTitle("Cloud Platform Engineer")).toMatchObject({
      family: "software_engineering",
      primaryConceptId: undefined,
      candidateConceptIds: expect.arrayContaining(["platform_engineer"]),
      modifierTokens: ["cloud", "platform"],
    });
  });
});

describe("title retrieval query expansion", () => {
  it("expands software engineering searches beyond exact title matches", () => {
    expect(
      buildTitleQueryVariants("Software Engineer", { maxQueries: 32 }).map((variant) => variant.query),
    ).toEqual(
      expect.arrayContaining([
        "software engineer",
        "software developer",
        "software development engineer",
        "backend engineer",
        "frontend engineer",
        "full stack engineer",
        "application developer",
        "application engineer",
        "application software engineer",
        "web application developer",
        "platform engineer",
        "mobile engineer",
        "java developer",
        "member of technical staff",
        "mts",
        "api developer",
        "server engineer",
        "service engineer",
        "distributed systems engineer",
        "swe",
      ]),
    );
  });

  it("expands data analyst searches into nearby analyst concepts", () => {
    const queries = queriesFor("Data Analyst");
    // Core concept matches: synonyms and adjacent concepts
    expect(queries).toEqual(
      expect.arrayContaining([
        "data analyst",
        "business intelligence analyst",
        "reporting analyst",
        "product analyst",
        "business analyst",
        "operations analyst",
      ]),
    );
    // Should NOT contain unrelated roles
    expect(queries).not.toContain("data engineer");
    expect(queries).not.toContain("software engineer");
  });

  it("expands data engineer searches into nearby data-platform concepts without broad software spillover", () => {
    expect(queriesFor("Data Engineer")).toEqual(
      expect.arrayContaining([
        "data engineer",
        "analytics engineer",
        "data platform engineer",
        "etl engineer",
        "data warehouse engineer",
        "data pipeline engineer",
      ]),
    );
    expect(queriesFor("Data Engineer")).not.toContain("software engineer");
  });

  it("expands business analyst searches into adjacent analyst roles", () => {
    expect(queriesFor("Business Analyst")).toEqual(
      expect.arrayContaining([
        "business analyst",
        "business systems analyst",
        "systems analyst",
        "data analyst",
        "operations analyst",
      ]),
    );
  });

  it("expands product manager searches into common nearby PM variants", () => {
    expect(queriesFor("Product Manager")).toEqual(
      expect.arrayContaining([
        "product manager",
        "technical product manager",
        "growth product manager",
        "associate product manager",
        "apm",
      ]),
    );
  });

  it("expands program manager searches into TPM, delivery, and implementation variants", () => {
    expect(queriesFor("Program Manager")).toEqual(
      expect.arrayContaining([
        "program manager",
        "technical program manager",
        "delivery manager",
        "implementation manager",
        "tpm",
      ]),
    );
  });

  it("expands technical writer searches into documentation-oriented titles", () => {
    expect(queriesFor("Technical Writer")).toEqual(
      expect.arrayContaining([
        "technical writer",
        "documentation writer",
        "documentation specialist",
        "api writer",
      ]),
    );
  });

  it("expands recruiter searches into technical recruiting and sourcing variants", () => {
    expect(queriesFor("Recruiter")).toEqual(
      expect.arrayContaining([
        "recruiter",
        "technical recruiter",
        "talent acquisition partner",
        "sourcer",
      ]),
    );
  });

  it("expands qa engineer searches into quality, test, and sdet variants", () => {
    expect(queriesFor("QA Engineer")).toEqual(
      expect.arrayContaining([
        "qa engineer",
        "quality assurance engineer",
        "test engineer",
        "software engineer in test",
        "sdet",
      ]),
    );
  });

  it("falls back sensibly for unknown titles instead of degrading to zero expansion", () => {
    expect(queriesFor("Integration Engineer")).toEqual([
      "integration engineer",
      "integration developer",
      "integration software engineer",
    ]);
    expect(queriesFor("Integration Engineer")).not.toContain("software engineer");
  });

  it("keeps richer arbitrary input specific while borrowing nearby concept queries", () => {
    expect(queriesFor("Cloud Platform Engineer")).toEqual(
      expect.arrayContaining([
        "cloud platform engineer",
        "platform engineer",
        "platform developer",
      ]),
    );
    expect(queriesFor("Cloud Platform Engineer")).not.toContain("software engineer");
  });

  it("supports less-common operations titles with safe user-input-driven fallback", () => {
    expect(queriesFor("Revenue Operations Analyst")).toEqual(
      expect.arrayContaining([
        "revenue operations analyst",
        "operations analyst",
      ]),
    );
    expect(queriesFor("Revenue Operations Analyst")).not.toContain("data analyst");
  });
});

describe("title retrieval scoring", () => {
  it("scores synonym, adjacent, and alias matches with meaningful tiers", () => {
    expect(getTitleMatchResult("Software Developer", "Software Engineer")).toMatchObject({
      matches: true,
      tier: "synonym",
      canonicalQueryTitle: "software engineer",
    });
    expect(getTitleMatchResult("Backend Engineer", "Software Engineer")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
    expect(getTitleMatchResult("Documentation Specialist", "Technical Writer")).toMatchObject({
      matches: true,
      tier: "synonym",
    });
    expect(getTitleMatchResult("Operations Analyst", "Business Analyst")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
    expect(getTitleMatchResult("Analytics Engineer", "Data Engineer")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
    expect(getTitleMatchResult("Data Platform Engineer", "Data Engineer")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
  });

  it("uses thresholds so broad fallback overlap can match in balanced mode without acting like an exact match", () => {
    expect(
      getTitleMatchResult("Integration Developer", "Integration Engineer", {
        mode: "strict",
      }),
    ).toMatchObject({
      matches: false,
      tier: "same_family_related",
    });

    expect(
      getTitleMatchResult("Integration Developer", "Integration Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: "same_family_related",
    });
  });

  it("keeps generic same-family software titles like Developer in balanced mode without treating them as exact matches", () => {
    expect(
      getTitleMatchResult("Developer", "Software Engineer", {
        mode: "strict",
      }),
    ).toMatchObject({
      matches: false,
      tier: "same_family_related",
    });

    expect(
      getTitleMatchResult("Developer", "Software Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: "same_family_related",
    });
  });

  it.each([
    "Backend Developer",
    "Frontend Developer",
    "Full Stack Developer",
    "Java Engineer",
  ])("keeps %s as a realistic software-engineering sibling in balanced mode", (title) => {
    expect(
      getTitleMatchResult(title, "Software Engineer", {
        mode: "strict",
      }),
    ).toMatchObject({
      matches: false,
    });

    expect(
      getTitleMatchResult(title, "Software Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: expect.stringMatching(/^(synonym|adjacent_concept)$/),
    });
  });

  it.each([
    "Application Developer",
    "Application Engineer",
    "Web Application Developer",
  ])("treats %s as a direct software-engineering concept match", (title) => {
    expect(
      getTitleMatchResult(title, "Software Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: "synonym",
    });
  });

  it.each([
    "Member of Technical Staff",
    "MTS",
  ])("keeps %s as a closely related software-engineering sibling", (title) => {
    expect(
      getTitleMatchResult(title, "Software Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
  });

  it("surfaces match diagnostics with normalized query data and expansion aliases", () => {
    const result = getTitleMatchResult("Member of Technical Staff", "Software Engineer");

    expect(result.matches).toBe(true);
    expect(result.queryDiagnostics).toMatchObject({
      original: "Software Engineer",
      normalized: "software engineer",
    });
    expect(result.queryDiagnostics.aliasesUsed).toEqual(
      expect.arrayContaining([
        "software engineer",
        "software development engineer",
        "backend engineer",
        "member of technical staff",
        "mts",
      ]),
    );
    expect(result.jobDiagnostics).toMatchObject({
      original: "Member of Technical Staff",
      normalized: "member of technical staff",
      canonical: "member of technical staff",
    });
    expect(result.jobDiagnostics.aliasesUsed).toEqual(
      expect.arrayContaining(["member of technical staff"]),
    );
  });

  it.each([
    "Business Analyst",
    "Reporting Analyst",
    "BI Analyst",
  ])("keeps %s as a realistic data-analyst sibling in balanced mode", (title) => {
    expect(
      getTitleMatchResult(title, "Data Analyst", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
    });
  });

  it.each([
    "Technical Product Manager",
    "Associate Product Manager",
    "APM",
  ])("keeps %s as a realistic product-manager sibling in balanced mode", (title) => {
    expect(
      getTitleMatchResult(title, "Product Manager", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
    });
  });

  it("does not overmatch unrelated families", () => {
    expect(getTitleMatchResult("Technical Program Manager", "Product Manager")).toMatchObject({
      matches: false,
      tier: "none",
    });
    expect(getTitleMatchResult("Sales Engineer", "Software Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
    expect(getTitleMatchResult("Data Engineer", "Data Analyst")).toMatchObject({
      matches: false,
      tier: "none",
    });
    expect(getTitleMatchResult("Software Engineer", "Data Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
    expect(getTitleMatchResult("Software Engineer", "Recruiter")).toMatchObject({
      matches: false,
      tier: "none",
    });
  });

  it("allows strong data-platform evidence to outrank generic software phrasing", () => {
    expect(
      getTitleMatchResult("Software Engineer, Data Platform", "Data Engineer"),
    ).toMatchObject({
      matches: true,
    });
    expect(getTitleMatchResult("Software Engineer", "Data Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
  });

  it("keeps richer arbitrary titles specific instead of broadening them into generic adjacent engineering roles", () => {
    expect(getTitleMatchResult("Platform Engineer", "Cloud Platform Engineer")).toMatchObject({
      matches: true,
    });
    expect(getTitleMatchResult("Software Engineer", "Cloud Platform Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
    expect(
      getTitleMatchResult("Data Analyst", "Revenue Operations Analyst"),
    ).toMatchObject({
      matches: false,
      tier: "none",
    });
  });

  it("keeps qa matching inside the QA and test family", () => {
    expect(getTitleMatchResult("Test Engineer", "QA Engineer")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
    expect(getTitleMatchResult("SDET", "QA Engineer")).toMatchObject({
      matches: true,
      tier: "adjacent_concept",
    });
    expect(getTitleMatchResult("Software Engineer", "QA Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
  });

  it.each([
    "Recruiter",
    "Sales Engineer",
    "Support Engineer",
    "Technical Writer",
    "QA Engineer",
    "SDET",
  ])("rejects obvious false positives for a software engineer query: %s", (title) => {
    expect(
      getTitleMatchResult(title, "Software Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: false,
      tier: "none",
    });
  });
});

describe("semantic title expansion", () => {
  describe("software engineer semantic expansion", () => {
    it("generates token-synonym variants for software engineer", () => {
      const queries = buildTitleQueryVariants("Software Engineer", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      // Token synonym swaps: software->application, engineer->developer
      expect(queryList).toEqual(
        expect.arrayContaining([
          "application developer",
          "software developer",
          "application engineer",
        ]),
      );
    });

    it("matches Java Developer as a related role through adjacent concept + token synonyms", () => {
      const result = getTitleMatchResult("Java Developer", "Software Engineer");
      expect(result.matches).toBe(true);
      expect(["adjacent_concept", "synonym", "same_family_related"]).toContain(result.tier);
    });

    it("matches Backend Developer through head-word alternation (engineer->developer)", () => {
      const result = getTitleMatchResult("Backend Developer", "Software Engineer");
      expect(result.matches).toBe(true);
    });

    it("matches Frontend Developer through semantic expansion", () => {
      const result = getTitleMatchResult("Frontend Developer", "Software Engineer");
      expect(result.matches).toBe(true);
    });

    it("matches Full Stack Developer through semantic expansion", () => {
      const result = getTitleMatchResult("Full Stack Developer", "Software Engineer");
      expect(result.matches).toBe(true);
    });

    it("keeps software engineer queries within software family", () => {
      const queries = buildTitleQueryVariants("Software Engineer", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      // Should NOT generate queries from unrelated families
      expect(queryList).not.toContain("data engineer");
      expect(queryList).not.toContain("product manager");
      expect(queryList).not.toContain("recruiter");
      expect(queryList).not.toContain("sales engineer");
    });
  });

  describe("data analyst semantic expansion", () => {
    it("generates token-synonym variants for data analyst", () => {
      const queries = buildTitleQueryVariants("Data Analyst", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      // Token synonym swaps: data->analytics/insights, analyst->scientist
      expect(queryList).toEqual(
        expect.arrayContaining([
          "analytics analyst",
          "insights analyst",
          "decision scientist",
        ]),
      );
    });

    it("matches Business Intelligence Analyst as a synonym", () => {
      const result = getTitleMatchResult("Business Intelligence Analyst", "Data Analyst");
      expect(result.matches).toBe(true);
      expect(result.tier).toBe("synonym");
    });

    it("keeps data analyst queries within analytics family", () => {
      const queries = buildTitleQueryVariants("Data Analyst", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      expect(queryList).not.toContain("data engineer");
      expect(queryList).not.toContain("software engineer");
    });
  });

  describe("product manager semantic expansion", () => {
    it("generates token-synonym variants for product manager", () => {
      const queries = buildTitleQueryVariants("Product Manager", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      // Token synonym swaps: product->platform/growth, manager->owner/lead
      expect(queryList).toEqual(
        expect.arrayContaining([
          "product owner",
          "product lead",
          "growth product manager",
        ]),
      );
    });

    it("keeps product manager queries within product family", () => {
      const queries = buildTitleQueryVariants("Product Manager", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      expect(queryList).not.toContain("program manager");
      expect(queryList).not.toContain("technical program manager");
    });
  });

  describe("QA engineer semantic expansion", () => {
    it("generates token-synonym variants for qa engineer", () => {
      const queries = buildTitleQueryVariants("QA Engineer", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      // Token synonym swaps: qa->quality/test, engineer->analyst
      expect(queryList).toEqual(
        expect.arrayContaining([
          "quality engineer",
          "test engineer",
          "qa analyst",
        ]),
      );
    });

    it("matches SDET through adjacent concept", () => {
      const result = getTitleMatchResult("SDET", "QA Engineer");
      expect(result.matches).toBe(true);
    });

    it("keeps qa engineer queries within quality family", () => {
      const queries = buildTitleQueryVariants("QA Engineer", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      expect(queryList).not.toContain("software engineer");
      expect(queryList).not.toContain("backend engineer");
    });
  });

  describe("recruiter semantic expansion", () => {
    it("generates token-synonym variants for recruiter", () => {
      const queries = buildTitleQueryVariants("Recruiter", { maxQueries: 32 });
      const queryList = queries.map((q) => q.query);

      expect(queryList).toEqual(
        expect.arrayContaining([
          "talent acquisition partner",
          "sourcer",
          "technical recruiter",
        ]),
      );
    });
  });

  describe("unknown title semantic expansion", () => {
    it("generates head-word alternation variants for unknown engineering titles", () => {
      const queries = buildTitleQueryVariants("Integration Engineer", { maxQueries: 16 });
      const queryList = queries.map((q) => q.query);

      // Should try engineer->developer swap
      expect(queryList).toContain("integration developer");
    });

    it("generates head-word alternation variants for unknown analyst titles", () => {
      const queries = buildTitleQueryVariants("Risk Analyst", { maxQueries: 16 });
      const queryList = queries.map((q) => q.query);

      // Should try analyst->scientist swap
      expect(queryList).toContain("risk scientist");
    });
  });

  describe("cross-family precision", () => {
    it("does not match data engineer titles against software engineer query", () => {
      const result = getTitleMatchResult("Data Engineer", "Software Engineer");
      expect(result.matches).toBe(false);
      expect(result.tier).toBe("none");
    });

    it("does not match software engineer titles against data engineer query", () => {
      const result = getTitleMatchResult("Software Engineer", "Data Engineer");
      expect(result.matches).toBe(false);
      expect(result.tier).toBe("none");
    });

    it("does not match product manager titles against program manager query", () => {
      const result = getTitleMatchResult("Product Manager", "Program Manager");
      expect(result.matches).toBe(false);
    });

    it("does not match sales engineer titles against software engineer query", () => {
      const result = getTitleMatchResult("Sales Engineer", "Software Engineer");
      expect(result.matches).toBe(false);
    });
  });
});
