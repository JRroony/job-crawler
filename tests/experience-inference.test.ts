import { describe, expect, it } from "vitest";

import {
  classifyExperience,
  inferExperienceLevel,
  resolveJobExperienceClassification,
} from "@/lib/server/crawler/helpers";

describe("experience classification", () => {
  it("emits explicit title classifications with structured diagnostics", () => {
    const classification = classifyExperience({
      title: "Principal Software Engineer",
    });

    expect(classification).toMatchObject({
      experienceVersion: 2,
      experienceBand: "advanced",
      experienceSource: "title",
      experienceConfidence: "high",
      explicitLevel: "principal",
      confidence: "high",
      source: "title",
      isUnspecified: false,
      diagnostics: {
        originalTitle: "Principal Software Engineer",
        normalizedTitle: "principal software engineer",
        finalSeniority: "principal",
      },
    });
    expect(classification.diagnostics?.matchedSignals).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          ruleId: "title_principal_keyword",
          level: "principal",
        }),
      ]),
    );
  });

  it("uses structured metadata ahead of description heuristics", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      structuredExperienceHints: ["SMTS"],
      descriptionExperienceHints: [
        "Minimum qualifications: 5+ years of experience building backend systems.",
      ],
    });

    expect(classification).toMatchObject({
      experienceBand: "advanced",
      experienceSource: "structured_metadata",
      experienceConfidence: "high",
      inferredLevel: "staff",
      confidence: "high",
      source: "structured_metadata",
      isUnspecified: false,
      diagnostics: {
        finalSeniority: "staff",
      },
    });
  });

  it("keeps description-based inference inferred and conservative", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      descriptionExperienceHints: [
        "This entry-level role is designed for recent graduates joining our product engineering team.",
      ],
    });

    expect(classification).toMatchObject({
      experienceBand: "entry",
      experienceSource: "description",
      experienceConfidence: "medium",
      inferredLevel: "new_grad",
      confidence: "medium",
      source: "description",
      isUnspecified: false,
      diagnostics: {
        finalSeniority: "new_grad",
      },
    });
  });

  it.each([
    {
      label: "level iii metadata",
      hints: ["Software Engineer III"],
      expected: "senior",
    },
    {
      label: "member of technical staff metadata",
      hints: ["Member of Technical Staff"],
      expected: "staff",
    },
    {
      label: "principal metadata",
      hints: ["Principal Software Engineer"],
      expected: "principal",
    },
    {
      label: "l6 metadata",
      hints: ["L6"],
      expected: "staff",
    },
    {
      label: "lmts metadata",
      hints: ["LMTS"],
      expected: "principal",
    },
    {
      label: "lead backend engineer metadata",
      hints: ["Lead Backend Engineer"],
      expected: "lead",
    },
  ])("does not down-level $label to mid", ({ hints, expected }) => {
    const classification = classifyExperience({
      title: "Software Engineer",
      structuredExperienceHints: hints,
    });

    expect(classification).toMatchObject({
      inferredLevel: expected,
      source: "structured_metadata",
      isUnspecified: false,
      diagnostics: {
        finalSeniority: expected,
      },
    });
  });

  it("falls back to unknown when no reliable clues exist", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
    });

    expect(classification).toMatchObject({
      experienceBand: "unknown",
      experienceSource: "unknown",
      experienceConfidence: "none",
      confidence: "none",
      source: "unknown",
      reasons: [],
      isUnspecified: true,
      diagnostics: {
        originalTitle: "Software Engineer",
        normalizedTitle: "software engineer",
        finalSeniority: "unknown",
        matchedSignals: [],
      },
    });
  });

  it("does not misclassify role-family manager titles as seniority markers", () => {
    const classification = classifyExperience({
      title: "Product Manager",
    });

    expect(classification).toMatchObject({
      experienceBand: "unknown",
      experienceSource: "unknown",
      experienceConfidence: "none",
      confidence: "none",
      source: "unknown",
      reasons: [],
      isUnspecified: true,
      diagnostics: {
        finalSeniority: "unknown",
      },
    });
  });

  it("keeps ambiguous company-specific acronyms unknown when confidence is low", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      structuredExperienceHints: ["IC3"],
    });

    expect(classification).toMatchObject({
      experienceBand: "unknown",
      experienceSource: "unknown",
      experienceConfidence: "none",
      confidence: "none",
      source: "unknown",
      isUnspecified: true,
      diagnostics: {
        finalSeniority: "unknown",
      },
    });
  });

  it("normalizes legacy stored levels into explicit high-confidence classifications", () => {
    const classification = resolveJobExperienceClassification({
      title: "Software Engineer",
      experienceLevel: "principal",
      rawSourceMetadata: {},
    });

    expect(classification).toMatchObject({
      experienceBand: "advanced",
      experienceSource: "unknown",
      experienceConfidence: "high",
      explicitLevel: "principal",
      confidence: "high",
      source: "unknown",
      isUnspecified: false,
      diagnostics: {
        finalSeniority: "principal",
      },
    });
  });

  it("keeps conflicting weak inferred levels unknown instead of promoting the highest one", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      structuredExperienceHints: ["Software Engineer L6 / L7"],
    });

    expect(classification).toMatchObject({
      experienceBand: "unknown",
      experienceSource: "unknown",
      experienceConfidence: "none",
      confidence: "none",
      source: "unknown",
      isUnspecified: true,
      diagnostics: {
        finalSeniority: "unknown",
      },
    });
  });
});

describe("inferExperienceLevel", () => {
  it.each([
    ["Software Engineer Intern", "intern"],
    ["New Graduate Software Engineer", "new_grad"],
    ["Junior Software Engineer", "junior"],
    ["Software Engineer II", "mid"],
    ["Senior Software Engineer", "senior"],
    ["Lead Backend Engineer", "lead"],
    ["Engineering Manager", "lead"],
    ["Director of Engineering", "lead"],
    ["Staff Data Engineer", "staff"],
    ["Principal Software Engineer", "principal"],
    ["Software Engineer III", "senior"],
    ["Software Engineer IV", "staff"],
    ["Software Engineer V", "principal"],
    ["Member of Technical Staff", "staff"],
    ["SMTS", "staff"],
    ["LMTS", "principal"],
    ["L5 Software Engineer", "senior"],
    ["L6 Software Engineer", "staff"],
    ["L7 Software Engineer", "principal"],
    ["Fellow", "principal"],
    ["Sr./Staff Software Engineer", "staff"],
    ["Sr./Principal Software Engineer", "principal"],
  ] as const)("infers %s as %s", (value, expected) => {
    expect(inferExperienceLevel(value)).toBe(expected);
  });

  it.each([
    {
      label: "internship prompt",
      values: [
        "Software Engineer",
        "<p>Join our 2026 summer internship program for software engineering students.</p>",
      ],
      expected: "intern",
    },
    {
      label: "senior years prompt",
      values: [
        "Software Engineer",
        "Minimum qualifications: 5+ years of experience building distributed systems.",
      ],
      expected: "senior",
    },
    {
      label: "staff years prompt",
      values: [
        "Software Engineer",
        "Candidates should bring 8+ years of experience building distributed systems.",
      ],
      expected: "staff",
    },
    {
      label: "principal years prompt",
      values: [
        "Software Engineer",
        "Candidates should bring 12+ years of experience leading large-scale platform work.",
      ],
      expected: "principal",
    },
  ])("infers $label", ({ values, expected }) => {
    expect(inferExperienceLevel(...values)).toBe(expected);
  });

  it.each([
    "Software Engineer",
    "Product Manager",
    "Business Analyst",
    "Customer Success Manager",
  ])("returns unknown for ambiguous title %s", (title) => {
    expect(inferExperienceLevel(title)).toBeUndefined();
  });
});
