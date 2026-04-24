import { describe, expect, it } from "vitest";

import {
  listBackgroundSystemGeographies,
  listBackgroundSystemRoleFamilies,
  listBackgroundSystemSearchProfiles,
  selectBackgroundSystemSearchProfiles,
} from "@/lib/server/background/constants";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { MongoLikeNullDb } from "@/tests/helpers/mongo-like-null-db";

describe("background system search profiles", () => {
  it("generates a scalable family-aware portfolio from role and geography templates", () => {
    const profiles = listBackgroundSystemSearchProfiles();
    const families = listBackgroundSystemRoleFamilies();
    const geographies = listBackgroundSystemGeographies();

    expect(profiles.length).toBeGreaterThanOrEqual(800);
    expect(profiles.length).toBeLessThan(1_000);
    expect(profiles.length).toBeLessThan(
      families.reduce((count, family) => count + family.variants.length, 0) *
        geographies.length *
        0.4,
    );

    expect(new Set(profiles.map((profile) => profile.id)).size).toBe(profiles.length);
    expect(
      new Set(
        profiles.map((profile) =>
          [
            profile.filters.title,
            profile.filters.country ?? "",
            profile.filters.state ?? "",
            profile.filters.city ?? "",
            profile.filters.platforms?.join(",") ?? "",
          ].join("|"),
        ),
      ).size,
    ).toBe(profiles.length);
  });

  it("covers required role families, title variants, and reusable geographies", () => {
    const profiles = listBackgroundSystemSearchProfiles();
    const familyIds = new Set(profiles.map((profile) => profile.canonicalJobFamily));
    const requiredFamilies = [
      "software_engineer",
      "backend_engineer",
      "frontend_engineer",
      "full_stack_engineer",
      "java_developer",
      "platform_engineer",
      "devops_engineer",
      "site_reliability_engineer",
      "data_engineer",
      "data_analyst",
      "business_analyst",
      "product_analyst",
      "machine_learning_engineer",
      "ai_engineer",
      "applied_scientist",
      "research_scientist",
      "product_manager",
      "program_manager",
      "technical_program_manager",
      "qa_engineer",
      "security_engineer",
      "solutions_engineer",
      "customer_success_manager",
      "sales_engineer",
      "technical_writer",
    ];

    for (const family of requiredFamilies) {
      expect(familyIds.has(family)).toBe(true);
    }

    for (const title of [
      "software development engineer",
      "application engineer",
      "java engineer",
      "java developer",
      "business intelligence analyst",
      "analytics analyst",
      "technical product manager",
      "product owner",
      "applied ai engineer",
      "ml platform engineer",
    ]) {
      expect(profiles).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ queryTitleVariant: title, filters: expect.objectContaining({ country: "United States" }) }),
          expect.objectContaining({ queryTitleVariant: title, filters: expect.objectContaining({ country: "Canada" }) }),
        ]),
      );
    }

    for (const state of ["CA", "WA", "NY", "TX", "MA", "IL", "NJ", "VA", "GA", "NC", "CO", "FL"]) {
      expect(profiles).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ filters: expect.objectContaining({ country: "United States", state }) }),
        ]),
      );
    }

    for (const city of ["Seattle", "Bellevue", "Redmond", "San Francisco", "San Jose", "New York City", "Austin", "Boston", "Toronto", "Vancouver", "Montreal"]) {
      expect(profiles).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ filters: expect.objectContaining({ city }) }),
        ]),
      );
    }
  });

  it("rotates fairly across cycles while respecting the per-cycle profile budget", () => {
    const intervalMs = 600_000;
    const selections = Array.from({ length: 12 }, (_, cycle) =>
      selectBackgroundSystemSearchProfiles({
        now: new Date(cycle * intervalMs),
        intervalMs,
        maxProfiles: 1,
      })[0],
    );

    expect(selections.every(Boolean)).toBe(true);
    expect(new Set(selections.map((profile) => profile?.id)).size).toBe(selections.length);

    const twoPerCycle = selectBackgroundSystemSearchProfiles({
      now: new Date(0),
      intervalMs,
      maxProfiles: 2,
    });

    expect(twoPerCycle).toHaveLength(2);
    expect(twoPerCycle.map((profile) => profile.filters)).toEqual([
      expect.objectContaining({ title: "software engineer", country: "United States" }),
      expect.objectContaining({ title: "software engineer", country: "Canada" }),
    ]);
  });

  it("honors profile freshness, cooldown, and health state during eligibility selection", () => {
    const intervalMs = 600_000;
    const now = new Date(0);
    const [firstProfile, secondProfile] = selectBackgroundSystemSearchProfiles({
      now,
      intervalMs,
      maxProfiles: 2,
    });

    expect(firstProfile).toBeTruthy();
    expect(secondProfile).toBeTruthy();

    const selected = selectBackgroundSystemSearchProfiles({
      now,
      intervalMs,
      maxProfiles: 1,
      profileRunStates: [
        {
          profileId: firstProfile.id,
          lastRunAt: now.toISOString(),
          lastStatus: "failed",
          failureCount: 3,
          consecutiveFailureCount: 3,
        },
        {
          profileId: secondProfile.id,
          lastRunAt: "1969-12-30T00:00:00.000Z",
          lastStatus: "completed",
          successCount: 4,
          failureCount: 0,
          consecutiveFailureCount: 0,
        },
      ],
    });

    expect(selected[0]?.id).toBe(secondProfile.id);
    expect(selected[0]?.successCount).toBe(4);
    expect(selected[0]?.failureCount).toBe(0);

    const noneEligible = selectBackgroundSystemSearchProfiles({
      now,
      intervalMs,
      maxProfiles: 1,
      profileRunStates: listBackgroundSystemSearchProfiles().map((profile) => ({
        profileId: profile.id,
        nextEligibleAt: "1970-01-01T01:00:00.000Z",
      })),
    });

    expect(noneEligible).toEqual([]);
  });

  it("hydrates scheduler selection from persisted profile run history", async () => {
    const repository = new JobCrawlerRepository(new MongoLikeNullDb());
    const [profile] = listBackgroundSystemSearchProfiles();
    const search = await repository.createSearch(
      profile.filters,
      "2026-04-15T12:00:00.000Z",
      {
        systemProfileId: profile.id,
        systemProfileLabel: profile.label,
      },
    );
    const run = await repository.createCrawlRun(search._id, "2026-04-15T12:00:00.000Z");

    await repository.finalizeCrawlRun(run._id, {
      status: "completed",
      stage: "finalizing",
      totalFetchedJobs: 10,
      totalMatchedJobs: 8,
      dedupedJobs: 7,
      finishedAt: "2026-04-15T12:03:00.000Z",
    });

    const states = await repository.listSystemSearchProfileRunStates();

    expect(states).toEqual([
      expect.objectContaining({
        profileId: profile.id,
        searchId: search._id,
        latestCrawlRunId: run._id,
        lastStatus: "completed",
        successCount: 1,
        failureCount: 0,
        consecutiveFailureCount: 0,
      }),
    ]);

    const selected = selectBackgroundSystemSearchProfiles({
      now: new Date("2026-04-15T12:05:00.000Z"),
      intervalMs: 600_000,
      maxProfiles: 1,
      profileRunStates: states,
    });

    expect(selected[0]?.id).not.toBe(profile.id);
  });
});
