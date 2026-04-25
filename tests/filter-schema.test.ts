import { describe, expect, it } from "vitest";

import { searchFiltersSchema } from "@/lib/types";

describe("searchFiltersSchema", () => {
  it("accepts a valid payload, trims optional values, and preserves multiple levels", () => {
    const parsed = searchFiltersSchema.parse({
      title: "  Software Engineer  ",
      country: "  United States ",
      state: " California ",
      city: " San Francisco ",
      experienceLevels: ["mid", "senior"],
      experienceMatchMode: "balanced",
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      country: "United States",
      state: "California",
      city: "San Francisco",
      experienceLevels: ["mid", "senior"],
      experienceMatchMode: "balanced",
    });
  });

  it("normalizes nullable optional location fields and strips legacy search-only fields", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      country: " United States ",
      state: null,
      city: "   ",
      experienceClassification: null,
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      country: "United States",
    });
  });

  it("accepts the legacy single experience level payload and normalizes it to an array", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      experienceLevel: "intern",
    });

    expect(parsed.experienceLevels).toEqual(["intern"]);
  });

  it("merges legacy and multi-select experience filters without duplicates", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      experienceLevel: "senior",
      experienceLevels: ["mid", "senior", "staff"],
    });

    expect(parsed.experienceLevels).toEqual(["mid", "senior", "staff"]);
  });

  it("treats broad mode as including unspecified experience", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      experienceLevels: ["mid"],
      experienceMatchMode: "broad",
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      experienceLevels: ["mid"],
      experienceMatchMode: "broad",
      includeUnspecifiedExperience: true,
    });
  });

  it("accepts active platform scope and crawl mode selections", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      platforms: ["greenhouse", "ashby"],
      crawlMode: "deep",
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      platforms: ["greenhouse", "ashby"],
      crawlMode: "deep",
    });
  });

  it("accepts Workday as an active platform value", () => {
    const parsed = searchFiltersSchema.parse({
      title: "Software Engineer",
      platforms: ["workday"],
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      platforms: ["workday"],
    });
  });

  it("rejects titles that are too short", () => {
    expect(() =>
      searchFiltersSchema.parse({
        title: "A",
      }),
    ).toThrow();
  });
});
