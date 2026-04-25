import { describe, expect, it } from "vitest";

import {
  disabledPlatformFilterOptions,
  describeSelectedPlatforms,
  filterJobsForDisplay,
  isRemoteJob,
  isVisaFriendlyJob,
  matchesPostedDateFilter,
  parseLocationInput,
  platformFilterOptions,
} from "@/components/job-search/helpers";
import { activeCrawlerPlatforms } from "@/lib/types";
import type { JobListing, SearchFilters } from "@/lib/types";

function createJob(overrides: Partial<JobListing> = {}): JobListing {
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug = overrides.sourceCompanySlug ?? "acme";
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-03-29T00:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? "2026-03-29T00:00:00.000Z";

  return {
    _id: "job-1",
    canonicalJobKey:
      overrides.canonicalJobKey ??
      `platform:${sourcePlatform}:${sourceCompanySlug}:${sourceJobId.toLowerCase()}`,
    title: "Software Engineer",
    company: "Acme",
    normalizedCompany: "acme",
    normalizedTitle: "software engineer",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationRaw: "San Francisco, California, United States",
    normalizedLocation: "san francisco california united states",
    locationText: "San Francisco, California, United States",
    remoteType: "onsite",
    seniority: "mid",
    experienceLevel: "mid",
    sourcePlatform,
    sourceCompanySlug,
    sourceJobId,
    sourceUrl: "https://example.com/jobs/1",
    applyUrl: "https://example.com/jobs/1/apply",
    resolvedUrl: "https://example.com/jobs/1/apply",
    canonicalUrl: "https://example.com/jobs/1",
    postingDate: "2026-03-20T00:00:00.000Z",
    postedAt: "2026-03-20T00:00:00.000Z",
    discoveredAt,
    crawledAt,
    descriptionSnippet: "Team is open to visa sponsorship.",
    sponsorshipHint: "unknown",
    linkStatus: "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? crawledAt,
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform,
        sourceJobId,
        sourceUrl: "https://example.com/jobs/1",
        applyUrl: "https://example.com/jobs/1/apply",
        resolvedUrl: "https://example.com/jobs/1/apply",
        canonicalUrl: "https://example.com/jobs/1",
        discoveredAt,
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [`${sourcePlatform}:${sourceJobId}`],
    crawlRunIds: ["run-1"],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: "fingerprint-1",
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "san francisco california united states",
    contentFingerprint: "fingerprint-1",
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
    ...overrides,
  };
}

describe("job-search helpers", () => {
  it("keeps the result filter platform options aligned with active crawler platforms", () => {
    expect(platformFilterOptions.map((option) => option.value)).toEqual(
      activeCrawlerPlatforms,
    );
    expect(platformFilterOptions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          value: "workday",
          label: "Workday",
        }),
      ]),
    );
    expect(disabledPlatformFilterOptions.map((option) => option.label)).not.toContain(
      "Workday",
    );
    expect(describeSelectedPlatforms({ title: "Software Engineer" })).toContain("Workday");
  });

  it("promotes single country-like location input instead of treating Canada as a city", () => {
    expect(parseLocationInput("Canada")).toEqual({
      city: "",
      state: "",
      country: "Canada",
    });
    expect(parseLocationInput("canada")).toEqual({
      city: "",
      state: "",
      country: "Canada",
    });
    expect(parseLocationInput("Remote, Canada")).toEqual({
      city: "Remote",
      state: "",
      country: "Canada",
    });
  });

  it("uses normalized remote type before falling back to raw text", () => {
    expect(
      isRemoteJob(
        createJob({
          remoteType: "remote",
          locationRaw: "Austin, Texas, United States",
          locationText: "Austin, Texas, United States",
        }),
      ),
    ).toBe(true);
  });

  it("uses sponsorship hint before raw text heuristics", () => {
    expect(
      isVisaFriendlyJob(
        createJob({
          sponsorshipHint: "not_supported",
          descriptionSnippet: "We can sponsor visas for exceptional candidates.",
        }),
      ),
    ).toBe(false);
  });

  it("uses postingDate when applying posted-date filters", () => {
    const recentJob = createJob({
      postingDate: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      postedAt: undefined,
    });

    expect(matchesPostedDateFilter(recentJob, "7d")).toBe(true);
    expect(matchesPostedDateFilter(recentJob, "24h")).toBe(false);
  });

  it("keeps client-side experience filtering aligned with strict and balanced inference rules", () => {
    const inferredSeniorJob = createJob({
      experienceLevel: undefined,
      experienceClassification: {
        experienceVersion: 2,
        experienceBand: "senior",
        experienceSource: "description",
        experienceConfidence: "medium",
        experienceSignals: [],
        inferredLevel: "senior",
        confidence: "medium",
        source: "description",
        reasons: ["Detected senior markers in description."],
        isUnspecified: false,
      },
    });
    const resultFilters = {
      remoteOnly: false,
      visaFriendlyOnly: false,
      postedDate: "any" as const,
    };

    expect(
      filterJobsForDisplay(
        [inferredSeniorJob],
        {
          title: "Software Engineer",
          experienceLevels: ["senior"],
          experienceMatchMode: "strict",
        } satisfies SearchFilters,
        resultFilters,
      ),
    ).toEqual([]);

    expect(
      filterJobsForDisplay(
        [inferredSeniorJob],
        {
          title: "Software Engineer",
          experienceLevels: ["senior"],
          experienceMatchMode: "balanced",
        } satisfies SearchFilters,
        resultFilters,
      ),
    ).toHaveLength(1);
  });

  it("lets broad client-side filtering include unspecified experience jobs", () => {
    const unspecifiedJob = createJob({
      experienceLevel: undefined,
      experienceClassification: {
        experienceVersion: 2,
        experienceBand: "unknown",
        experienceSource: "unknown",
        experienceConfidence: "none",
        experienceSignals: [],
        confidence: "none",
        source: "unknown",
        reasons: [],
        isUnspecified: true,
      },
    });

    expect(
      filterJobsForDisplay(
        [unspecifiedJob],
        {
          title: "Software Engineer",
          experienceLevels: ["mid"],
          experienceMatchMode: "broad",
        } satisfies SearchFilters,
        {
          remoteOnly: false,
          visaFriendlyOnly: false,
          postedDate: "any",
        },
      ),
    ).toHaveLength(1);
  });
});
