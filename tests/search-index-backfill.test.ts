import { describe, expect, it } from "vitest";

import { buildSearchIndexBackfillRepair } from "@/lib/server/search/search-index-backfill";

describe("search index backfill repair", () => {
  it("generates United States location keys for a legacy Seattle, WA job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-seattle",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Seattle, WA",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      state: "Washington",
      stateCode: "WA",
      city: "Seattle",
      isUnitedStates: true,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "country_code:us",
        "region:united states:washington",
        "region_code:united states:wa",
        "city:united states:washington:seattle",
      ]),
    );
  });

  it("generates United States remote keys for a legacy Remote US job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-remote-us",
      title: "Software Engineer",
      company: "Acme",
      locationRaw: "Remote US",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      isRemote: true,
      isUnitedStates: true,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "country_code:us",
        "remote_country:united states",
      ]),
    );
  });

  it("does not invent a city for a state-only United States location", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-florida",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Florida",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "United States",
      state: "Florida",
      stateCode: "FL",
      isUnitedStates: true,
    });
    expect(repair.update.resolvedLocation?.city).toBeUndefined();
    expect(repair.update.searchIndex.locationSearchKeys).toEqual(
      expect.arrayContaining([
        "country:united states",
        "region:united states:florida",
        "region_code:united states:fl",
      ]),
    );
    expect(
      repair.update.searchIndex.locationSearchKeys.some((key) =>
        key.includes(":florida:florida"),
      ),
    ).toBe(false);
  });

  it("generates AI/ML role keys for a legacy Machine Learning Engineer job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-ml",
      title: "Machine Learning Engineer",
      company: "Acme",
      locationText: "Remote United States",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.searchIndex).toMatchObject({
      titleNormalized: "machine learning engineer",
      titleStrippedNormalized: "machine learning engineer",
      titleFamily: "ai_ml_science",
      titleRoleGroup: "engineering",
      titleConceptIds: expect.arrayContaining(["machine_learning_engineer"]),
      titleSearchKeys: expect.arrayContaining([
        "family:ai_ml_science",
        "family_role:ai_ml_science:engineering",
        "concept:machine_learning_engineer",
        "term:machine learning engineer",
        "term:ml engineer",
        "term:ai engineer",
        "term:applied ml engineer",
        "term:computer vision engineer",
        "term:nlp engineer",
      ]),
    });
  });

  it("does not generate United States keys for a non-US legacy job", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-canada",
      title: "Machine Learning Engineer",
      company: "Acme",
      locationText: "Toronto, Canada",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "Canada",
      isUnitedStates: false,
    });
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country:united states",
    );
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country_code:us",
    );
    expect(
      repair.update.searchIndex.locationSearchKeys.some((key) =>
        key.startsWith("remote_country:united states"),
      ),
    ).toBe(false);
  });

  it("keeps global remote text from becoming a guessed country", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-worldwide",
      title: "Linux Kernel Engineer",
      company: "Acme",
      locationText: "Home based - Worldwide",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      isRemote: true,
      isUnitedStates: false,
    });
    expect(repair.update.resolvedLocation?.country).toBeUndefined();
    expect(repair.update.searchIndex.locationSearchKeys).toContain("remote_global");
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain(
      "country:home based worldwide",
    );
  });

  it("canonicalizes clear two-letter non-US country codes in derived location data", () => {
    const repair = buildSearchIndexBackfillRepair({
      _id: "legacy-france",
      title: "Marketing Analyst",
      company: "Acme",
      locationText: "FR",
      sourcePlatform: "greenhouse",
      isActive: true,
      discoveredAt: "2026-04-01T00:00:00.000Z",
      crawledAt: "2026-04-01T00:00:00.000Z",
    });

    expect(repair.update.resolvedLocation).toMatchObject({
      country: "France",
      isUnitedStates: false,
    });
    expect(repair.update.searchIndex.locationSearchKeys).toContain("country:france");
    expect(repair.update.searchIndex.locationSearchKeys).not.toContain("country:fr");
  });
});
