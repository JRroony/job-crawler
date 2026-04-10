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
});

describe("title retrieval query expansion", () => {
  it("expands software engineering searches beyond exact title matches", () => {
    expect(queriesFor("Software Engineer")).toEqual(
      expect.arrayContaining([
        "software engineer",
        "software developer",
        "software development engineer",
        "backend engineer",
        "frontend engineer",
        "full stack engineer",
        "platform engineer",
        "mobile engineer",
        "java developer",
        "swe",
      ]),
    );
  });

  it("expands data analyst searches into nearby analyst concepts", () => {
    expect(queriesFor("Data Analyst")).toEqual(
      expect.arrayContaining([
        "data analyst",
        "business intelligence analyst",
        "reporting analyst",
        "product analyst",
        "business analyst",
        "operations analyst",
        "bi analyst",
      ]),
    );
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
    ]);
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
      tier: "generic_token_overlap",
    });

    expect(
      getTitleMatchResult("Integration Developer", "Integration Engineer", {
        mode: "balanced",
      }),
    ).toMatchObject({
      matches: true,
      tier: "generic_token_overlap",
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
});
