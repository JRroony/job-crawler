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
    {
      locationText: "Tel Aviv, Israel",
      expected: { country: "Israel", city: "Tel Aviv", state: "Tel Aviv District" },
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
      physicalLocations: expect.arrayContaining([expect.objectContaining(expected)]),
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
      explanation: expect.stringContaining("physical job location in United States"),
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
      explanation: expect.stringContaining("physical job location in Canada"),
    });
  });

  it("passes country filters for multiple locations within the same supported non-US country", () => {
    for (const locationText of ["Toronto, ON", "Vancouver, BC", "Montreal, Quebec"]) {
      const evaluation = evaluateSearchFilters(
        {
          title: "Software Engineer",
          company: "Acme",
          country: undefined,
          state: undefined,
          city: undefined,
          locationText,
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          country: "Canada",
        },
        {
          includeExperience: false,
        },
      );

      expect(evaluation.matches).toBe(true);
      expect(evaluation.locationMatch?.explanation).toContain("physical job location in Canada");
    }
  });

  it("matches Israel country filters and prevents wrong-country leakage", () => {
    const israelMatch = evaluateSearchFilters(
      {
        title: "Machine Learning Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Tel Aviv, Israel",
        rawSourceMetadata: {},
      },
      {
        title: "Machine Learning Engineer",
        country: "Israel",
      },
      {
        includeExperience: false,
      },
    );
    const canadaMiss = evaluateSearchFilters(
      {
        title: "Machine Learning Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, ON",
        rawSourceMetadata: {},
      },
      {
        title: "Machine Learning Engineer",
        country: "Israel",
      },
      {
        includeExperience: false,
      },
    );

    expect(israelMatch.matches).toBe(true);
    expect(canadaMiss).toMatchObject({
      matches: false,
      reason: "location",
    });
  });

  it("matches United Kingdom country filters without leaking other countries", () => {
    const ukMatch = evaluateSearchFilters(
      {
        title: "Data Analyst",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Manchester, UK",
        rawSourceMetadata: {},
      },
      {
        title: "Data Analyst",
        country: "United Kingdom",
      },
      {
        includeExperience: false,
      },
    );
    const germanyMiss = evaluateSearchFilters(
      {
        title: "Data Analyst",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Berlin, Germany",
        rawSourceMetadata: {},
      },
      {
        title: "Data Analyst",
        country: "United Kingdom",
      },
      {
        includeExperience: false,
      },
    );

    expect(ukMatch.matches).toBe(true);
    expect(germanyMiss).toMatchObject({
      matches: false,
      reason: "location",
    });
  });

  it.each([
    {
      title: "Software Engineer",
      country: "United States",
      locationText: "Seattle, WA",
    },
    {
      title: "Software Engineer",
      country: "Canada",
      locationText: "Vancouver, BC",
    },
    {
      title: "Machine Learning Engineer",
      country: "Israel",
      locationText: "Tel Aviv, Israel",
    },
    {
      title: "Product Manager",
      country: "Canada",
      locationText: "Toronto, ON",
    },
    {
      title: "Data Analyst",
      country: "United Kingdom",
      locationText: "London, UK",
    },
  ])("validates runtime country scenario $title + $country", ({
    title,
    country,
    locationText,
  }) => {
    const evaluation = evaluateSearchFilters(
      {
        title,
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText,
        rawSourceMetadata: {},
      },
      {
        title,
        country,
      },
      {
        includeExperience: false,
      },
    );

    expect(evaluation).toMatchObject({
      matches: true,
      locationMatch: expect.objectContaining({
        explanation: expect.stringContaining(`physical job location in ${country}`),
      }),
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
      eligibilityCountries: ["United States"],
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
      eligibilityCountries: ["Canada"],
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

  it("keeps physical locations separate from conflicting remote eligibility", () => {
    const resolved = resolveJobLocation({
      locationText: "Toronto, ON",
      rawSourceMetadata: {
        description: "This role may be performed remotely by candidates eligible to work within the United States.",
      },
    });

    expect(resolved).toMatchObject({
      country: "Canada",
      physicalLocations: [
        expect.objectContaining({
          country: "Canada",
          city: "Toronto",
          state: "Ontario",
        }),
      ],
      eligibilityCountries: ["United States"],
      conflicts: [
        expect.objectContaining({
          kind: "physical_remote_conflict",
          countries: expect.arrayContaining(["Canada", "United States"]),
        }),
      ],
    });

    const canadaEvaluation = evaluateSearchFilters(
      {
        title: "Software Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, ON",
        rawSourceMetadata: {
          description: "This role may be performed remotely by candidates eligible to work within the United States.",
        },
      },
      {
        title: "Software Engineer",
        country: "Canada",
      },
      {
        includeExperience: false,
      },
    );
    const usEvaluation = evaluateSearchFilters(
      {
        title: "Software Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, ON",
        rawSourceMetadata: {
          description: "This role may be performed remotely by candidates eligible to work within the United States.",
        },
      },
      {
        title: "Software Engineer",
        country: "United States",
      },
      {
        includeExperience: false,
      },
    );
    const remoteUsEvaluation = evaluateSearchFilters(
      {
        title: "Software Engineer",
        company: "Acme",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, ON",
        rawSourceMetadata: {
          description: "This role may be performed remotely by candidates eligible to work within the United States.",
        },
      },
      {
        title: "Software Engineer",
        country: "Remote US",
      },
      {
        includeExperience: false,
      },
    );

    expect(canadaEvaluation.matches).toBe(true);
    expect(usEvaluation).toMatchObject({
      matches: false,
      reason: "location",
      locationMatch: expect.objectContaining({
        explanation: expect.stringContaining("requires an explicit remote country filter"),
      }),
    });
    expect(remoteUsEvaluation).toMatchObject({
      matches: true,
      locationMatch: expect.objectContaining({
        explanation: expect.stringContaining("remote eligibility for United States"),
      }),
    });
  });
});
