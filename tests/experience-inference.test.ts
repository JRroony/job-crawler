import { describe, expect, it } from "vitest";

import { inferExperienceLevel } from "@/lib/server/crawler/helpers";

describe("inferExperienceLevel", () => {
  it.each([
    {
      label: "software engineer intern title",
      values: ["Software Engineer Intern"],
      expected: "intern",
    },
    {
      label: "software engineering intern title",
      values: ["Software Engineering Intern"],
      expected: "intern",
    },
    {
      label: "internship hint",
      values: ["Software Engineer", "Internship"],
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
      label: "0-2 years maps to junior",
      values: ["Software Engineer", "0-2 years of experience"],
      expected: "junior",
    },
    {
      label: "2-5 years maps to mid",
      values: ["Software Engineer", "2-5 years of experience"],
      expected: "mid",
    },
    {
      label: "5-10 years maps to senior",
      values: ["Software Engineer", "5-10 years of experience"],
      expected: "senior",
    },
    {
      label: "10+ years maps to staff",
      values: ["Software Engineer", "10+ years of experience"],
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
      label: "recent graduate description maps to new grad",
      values: [
        "Software Engineer",
        "This entry-level role is designed for recent graduates starting their careers.",
      ],
      expected: "new_grad",
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
