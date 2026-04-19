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
      locationText: "San Jose, CA",
      expected: { city: "San Jose", state: "California" },
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
    {
      locationText: "Hybrid in Chicago, IL",
      expected: { city: "Chicago", state: "Illinois", isRemote: false },
    },
    {
      locationText: "Remote US",
      expected: { isRemote: true },
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

  it.each([
    {
      locationText: "Toronto, ON",
      expected: { country: "Canada", city: "Toronto", state: "Ontario", stateCode: "ON" },
    },
    {
      locationText: "Berlin, Germany",
      expected: { country: "Germany", city: "Berlin", state: "Berlin" },
    },
    {
      locationText: "London, UK",
      expected: { country: "United Kingdom", city: "London", state: "England" },
    },
  ])("resolves $locationText as a non-US location using country and regional evidence", ({
    locationText,
    expected,
  }) => {
    expect(
      resolveJobLocation({
        locationText,
      }),
    ).toMatchObject({
      isUnitedStates: false,
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
    expect(evaluation.locationMatch).toMatchObject({
      queryDiagnostics: {
        original: "United States",
        normalized: "united states",
      },
      jobDiagnostics: {
        raw: "Austin, TX",
        normalized: expect.stringContaining("austin"),
        isUnitedStates: true,
      },
      explanation: expect.stringContaining("resolved to the United States"),
    });
  });

  it("passes a Canada filter when the job only exposes a Canadian city and province", () => {
    const evaluation = evaluateSearchFilters(
      {
        title: "Data Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, ON",
        rawSourceMetadata: {},
      },
      {
        title: "Data Engineer",
        country: "Canada",
      },
      {
        includeExperience: false,
      },
    );

    expect(evaluation.matches).toBe(true);
    expect(evaluation.locationMatch).toMatchObject({
      jobDiagnostics: {
        country: "Canada",
        state: "Ontario",
        city: "Toronto",
        isUnitedStates: false,
      },
      explanation: expect.stringContaining('resolved country "Canada"'),
    });
  });

  it("treats a Remote US query as a remote-only United States filter", () => {
    const remoteMatch = evaluateSearchFilters(
      {
        title: "Data Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Remote - US",
        rawSourceMetadata: {},
      },
      {
        title: "Data Engineer",
        country: "Remote US",
      },
      {
        includeExperience: false,
      },
    );

    const onsiteMiss = evaluateSearchFilters(
      {
        title: "Data Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Seattle, WA",
        rawSourceMetadata: {},
      },
      {
        title: "Data Engineer",
        country: "Remote US",
      },
      {
        includeExperience: false,
      },
    );

    expect(remoteMatch.matches).toBe(true);
    expect(remoteMatch.locationMatch).toMatchObject({
      queryDiagnostics: {
        workplaceMode: "remote",
        expandedTerms: expect.arrayContaining(["remote us", "remote united states"]),
      },
      jobDiagnostics: {
        workplaceMode: "remote",
      },
    });
    expect(onsiteMiss).toMatchObject({
      matches: false,
      reason: "location",
      locationMatch: expect.objectContaining({
        explanation: expect.stringContaining("requires a remote role"),
      }),
    });
  });

  it("preserves remote evidence when structured country is less specific than location text", () => {
    expect(
      resolveJobLocation({
        country: "United States",
        locationText: "Remote - United States",
      }),
    ).toMatchObject({
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
      evidence: expect.arrayContaining([
        { source: "location_text", value: "Remote - United States" },
      ]),
    });

    expect(
      resolveJobLocation({
        country: "Canada",
        locationText: "Remote - Canada",
      }),
    ).toMatchObject({
      country: "Canada",
      isRemote: true,
      isUnitedStates: false,
      evidence: expect.arrayContaining([
        { source: "location_text", value: "Remote - Canada" },
      ]),
    });
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

  it("uses metadata clues when explicit country is missing but office evidence points to Canada", () => {
    expect(
      resolveJobLocation({
        locationText: "Location unavailable",
        rawSourceMetadata: {
          pageJsonLd: {
            office: {
              locationName: "Toronto, Ontario",
            },
          },
        },
      }),
    ).toMatchObject({
      country: "Canada",
      state: "Ontario",
      city: "Toronto",
      isUnitedStates: false,
    });
  });

  it("keeps noisy multi-part Greenhouse city and state strings in the United States without a literal country token", () => {
    expect(
      resolveJobLocation({
        locationText: "Seattle, WA; Austin, TX (Hybrid eligible)",
      }),
    ).toMatchObject({
      country: "United States",
      city: "Seattle",
      state: "Washington",
      isUnitedStates: true,
    });
  });

  it("does not let a non-US region string pass as United States during resolution", () => {
    expect(
      resolveJobLocation({
        locationText: "Toronto, Ontario",
      }),
    ).toMatchObject({
      country: "Canada",
      isUnitedStates: false,
    });
  });
});
