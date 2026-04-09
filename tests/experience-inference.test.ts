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

  it.each([
    {
      label: "lead description",
      hints: ["Lead Software Engineer, distributed systems"],
      expected: "senior",
    },
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
      expected: "staff",
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
      label: "software architect title",
      values: ["Software Architect"],
      expected: "senior",
    },
    {
      label: "director title",
      values: ["Director of Engineering"],
      expected: "senior",
    },
    {
      label: "software engineer iii title",
      values: ["Software Engineer III"],
      expected: "senior",
    },
    {
      label: "software engineer iv title",
      values: ["Software Engineer IV"],
      expected: "staff",
    },
    {
      label: "fellow title",
      values: ["Fellow"],
      expected: "staff",
    },
    {
      label: "member of technical staff title",
      values: ["Member of Technical Staff"],
      expected: "staff",
    },
    {
      label: "mixed punctuation title chooses the higher seniority bucket",
      values: ["Sr./Staff Software Engineer"],
      expected: "staff",
    },
    {
      label: "mixed case lead title",
      values: ["LEAD software engineer"],
      expected: "senior",
    },
    {
      label: "roman numeral title with suffix",
      values: ["Software Engineer III - Platform"],
      expected: "senior",
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

  it.each([
    {
      title: "Senior Software Engineer",
      expected: "senior",
    },
    {
      title: "Sr. Software Engineer",
      expected: "senior",
    },
    {
      title: "Lead Software Engineer",
      expected: "senior",
    },
    {
      title: "Software Architect",
      expected: "senior",
    },
    {
      title: "Engineering Manager",
      expected: "senior",
    },
    {
      title: "Director of Engineering",
      expected: "senior",
    },
    {
      title: "Staff Software Engineer",
      expected: "staff",
    },
    {
      title: "Principal Software Engineer",
      expected: "staff",
    },
    {
      title: "Distinguished Engineer",
      expected: "staff",
    },
    {
      title: "Fellow",
      expected: "staff",
    },
    {
      title: "Member of Technical Staff",
      expected: "staff",
    },
    {
      title: "Software Engineer II",
      expected: "mid",
    },
    {
      title: "Software Engineer III",
      expected: "senior",
    },
    {
      title: "Software Engineer IV",
      expected: "staff",
    },
    {
      title: "New Grad Software Engineer",
      expected: "new_grad",
    },
    {
      title: "Software Engineer Intern",
      expected: "intern",
    },
    {
      title: "Junior Software Engineer",
      expected: "junior",
    },
    {
      title: "Sr./Staff Software Engineer",
      expected: "staff",
    },
    {
      title: "LEAD software engineer",
      expected: "senior",
    },
    {
      title: "Software Engineer III - Platform",
      expected: "senior",
    },
  ])("keeps title-based experience classification conservative for $title", ({ title, expected }) => {
    expect(classifyExperience({ title })).toMatchObject({
      explicitLevel: expected,
      confidence: "high",
      source: "title",
      isUnspecified: false,
    });
  });
});
