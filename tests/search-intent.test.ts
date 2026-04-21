import { describe, expect, it } from "vitest";

import {
  normalizeSearchIntent,
  normalizeSearchIntentInput,
} from "@/lib/server/crawler/search-intent";

describe("search intent normalization", () => {
  it("extracts obvious platform and country hints from the title", () => {
    expect(normalizeSearchIntent("Greenhouse software engineer jobs in the US")).toEqual({
      title: "software engineer",
      platforms: ["greenhouse"],
      country: "United States",
    });
  });

  it("infers Canada from free-text country hints while keeping titles clean", () => {
    expect(normalizeSearchIntent("ai engineer canada")).toEqual({
      title: "ai engineer",
      country: "Canada",
    });

    expect(normalizeSearchIntent("software engineer in canada")).toEqual({
      title: "software engineer",
      country: "Canada",
    });

    expect(normalizeSearchIntent("machine learning engineer canadian jobs")).toEqual({
      title: "machine learning engineer",
      country: "Canada",
    });
  });

  it("infers Canada from realistic required retrieval prompts", () => {
    const scenarios = [
      ["software engineer canada", { title: "software engineer", country: "Canada" }],
      ["ai engineer in Canada", { title: "ai engineer", country: "Canada" }],
      ["data analyst canada jobs", { title: "data analyst", country: "Canada" }],
      ["business analyst canada jobs", { title: "business analyst", country: "Canada" }],
      ["product manager roles in canada", { title: "product manager", country: "Canada" }],
      [
        "greenhouse software engineer canada",
        { title: "software engineer", platforms: ["greenhouse"], country: "Canada" },
      ],
    ] as const;

    for (const [input, expected] of scenarios) {
      expect(normalizeSearchIntent(input)).toEqual(expected);
    }
  });

  it("keeps platform inference when Canada is embedded in title text", () => {
    expect(normalizeSearchIntent("greenhouse data analyst canada")).toEqual({
      title: "data analyst",
      platforms: ["greenhouse"],
      country: "Canada",
    });

    expect(
      normalizeSearchIntentInput({
        title: "greenhouse data analyst canada",
      }),
    ).toEqual({
      title: "data analyst",
      platforms: ["greenhouse"],
      country: "Canada",
    });
  });

  it("removes Canada hints without over-normalizing other location words", () => {
    expect(normalizeSearchIntent("product manager toronto canada")).toEqual({
      title: "product manager toronto",
      country: "Canada",
    });
  });

  it("continues to infer United States from free-text country hints", () => {
    expect(normalizeSearchIntent("lever software engineer roles in the United States")).toEqual({
      title: "software engineer",
      platforms: ["lever"],
      country: "United States",
    });
  });

  it("preserves explicit structured filters over inferred title hints", () => {
    expect(
      normalizeSearchIntentInput({
        title: "Greenhouse software engineer USA",
        country: "Canada",
        platforms: ["lever"],
      }),
    ).toEqual({
      title: "software engineer",
      country: "Canada",
      platforms: ["lever"],
    });
  });

  it("normalizes nullable optional location fields before validation", () => {
    expect(
      normalizeSearchIntentInput({
        title: "Software Engineer",
        country: " United States ",
        state: null,
        city: "   ",
        experienceClassification: null,
      }),
    ).toEqual({
      title: "Software Engineer",
      country: "United States",
    });
  });
});
