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
