import { describe, expect, it } from "vitest";

import {
  classifyExperience,
  inferExperienceLevel,
  resolveJobExperienceClassification,
} from "@/lib/server/crawler/helpers";

describe("experience classification", () => {
  it("treats direct title seniority markers as explicit title classifications", () => {
    const classification = classifyExperience({
      title: "Senior Software Engineer",
    });

    expect(classification).toMatchObject({
      explicitLevel: "senior",
      confidence: "high",
      source: "title",
      isUnspecified: false,
    });
    expect(classification.reasons[0]).toContain("title");
  });

  it("uses structured metadata ahead of description heuristics", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      structuredExperienceHints: ["Student Program"],
      descriptionExperienceHints: [
        "Minimum qualifications: 5+ years of experience building backend systems.",
      ],
    });

    expect(classification).toMatchObject({
      inferredLevel: "intern",
      confidence: "high",
      source: "structured_metadata",
      isUnspecified: false,
    });
  });

  it("keeps description-based inference as inferred instead of explicit", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
      descriptionExperienceHints: [
        "This entry-level role is designed for recent graduates joining our product engineering team.",
      ],
    });

    expect(classification).toMatchObject({
      inferredLevel: "new_grad",
      confidence: "medium",
      source: "description",
      isUnspecified: false,
    });
  });

  it("falls back to unspecified when no experience clues exist", () => {
    const classification = classifyExperience({
      title: "Software Engineer",
    });

    expect(classification).toEqual({
      confidence: "none",
      source: "unknown",
      reasons: [],
      isUnspecified: true,
    });
  });

  it("normalizes legacy stored levels into explicit high-confidence classifications", () => {
    const classification = resolveJobExperienceClassification({
      title: "Software Engineer",
      experienceLevel: "mid",
      rawSourceMetadata: {},
    });

    expect(classification).toMatchObject({
      explicitLevel: "mid",
      confidence: "high",
      source: "unknown",
      isUnspecified: false,
    });
  });
});

describe("inferExperienceLevel", () => {
  it.each([
    {
      label: "software engineer intern title",
      values: ["Software Engineer Intern"],
      expected: "intern",
    },
    {
      label: "new graduate title",
      values: ["New Graduate Software Engineer"],
      expected: "new_grad",
    },
    {
      label: "junior title",
      values: ["Junior Software Engineer"],
      expected: "junior",
    },
    {
      label: "level ii title",
      values: ["Software Engineer II"],
      expected: "mid",
    },
    {
      label: "senior title",
      values: ["Senior Software Engineer"],
      expected: "senior",
    },
    {
      label: "staff title",
      values: ["Staff Data Engineer"],
      expected: "staff",
    },
    {
      label: "summer internship description maps to intern",
      values: [
        "Software Engineer",
        "<p>Join our 2026 summer internship program for software engineering students.</p>",
      ],
      expected: "intern",
    },
    {
      label: "minimum qualifications years prompt maps to senior",
      values: [
        "Software Engineer",
        "Minimum qualifications: 5+ years of experience building distributed systems.",
      ],
      expected: "senior",
    },
  ])("infers $label", ({ values, expected }) => {
    expect(inferExperienceLevel(...values)).toBe(expected);
  });
});
