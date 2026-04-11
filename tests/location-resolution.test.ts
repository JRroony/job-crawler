import { describe, expect, it } from "vitest";

import { evaluateSearchFilters } from "@/lib/server/crawler/helpers";
import { resolveJobLocation } from "@/lib/server/location-resolution";

describe("location resolution", () => {
  it.each([
    {
      locationText: "Austin, TX",
      expected: { city: "Austin", state: "Texas" },
    },
    {
      locationText: "Bellevue, WA",
      expected: { city: "Bellevue", state: "Washington" },
    },
    {
      locationText: "Seattle, Washington",
      expected: { city: "Seattle", state: "Washington" },
    },
    {
      locationText: "New York, NY",
      expected: { city: "New York", state: "New York" },
    },
    {
      locationText: "Remote, United States",
      expected: { isRemote: true },
    },
    {
      locationText: "Remote - California",
      expected: { state: "California", isRemote: true },
    },
  ])("resolves $locationText as a United States location", ({ locationText, expected }) => {
    expect(
      resolveJobLocation({
        locationText,
      }),
    ).toMatchObject({
      country: "United States",
      isUnitedStates: true,
      ...expected,
    });
  });

  it("passes a United States filter when the job only exposes a US city and state", () => {
    const evaluation = evaluateSearchFilters(
      {
        title: "Data Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Austin, TX",
        rawSourceMetadata: {},
      },
      {
        title: "Data Engineer",
        country: "United States",
      },
      {
        includeExperience: false,
      },
    );

    expect(evaluation.matches).toBe(true);
  });

  it("uses metadata clues when explicit country is missing but office evidence points to the US", () => {
    expect(
      resolveJobLocation({
        locationText: "Location unavailable",
        rawSourceMetadata: {
          greenhouseJob: {
            offices: [
              {
                name: "US",
                location: {
                  name: "Bellevue, WA",
                },
              },
            ],
          },
        },
      }),
    ).toMatchObject({
      country: "United States",
      state: "Washington",
      city: "Bellevue",
      isUnitedStates: true,
    });
  });
});
