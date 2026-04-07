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
    });

    expect(parsed).toEqual({
      title: "Software Engineer",
      country: "United States",
      state: "California",
      city: "San Francisco",
      experienceLevels: ["mid", "senior"],
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

  it("rejects titles that are too short", () => {
    expect(() =>
      searchFiltersSchema.parse({
        title: "A",
      }),
    ).toThrow();
  });
});
