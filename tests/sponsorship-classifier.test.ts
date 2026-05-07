import { describe, it, expect } from "vitest";

import {
  classifySponsorship,
  matchCompanyProfile,
  normalizeCompanyName,
  type CompanySponsorshipProfile,
} from "../lib/server/sponsorship/classifier";
import { InMemoryCompanySponsorshipProfileRepository } from "../lib/server/sponsorship/company-profile";

const sampleJob = (overrides: Record<string, unknown> = {}) => ({
  title: "Software Engineer",
  descriptionSnippet: "",
  company: "TestCompany",
  sourcePlatform: "greenhouse" as const,
  rawSourceMetadata: {},
  ...overrides,
});

const sponsorProfile: CompanySponsorshipProfile = {
  companyName: "SponsorCorp",
  companyNormalized: "sponsorcorp",
  aliases: ["Sponsor Corp International"],
  domain: "sponsorcorp.com",
  evidenceSources: ["company_historical_sponsor"],
  h1bSponsorLikely: true,
  confidence: "high",
  lastVerifiedAt: new Date().toISOString(),
  evidenceSummary: "Historical H-1B filings show regular sponsorship",
  positiveEvidenceCount: 50,
  negativeEvidenceCount: 0,
  recentEvidenceCount: 10,
  historicalEvidenceCount: 50,
};

const nonSponsorProfile: CompanySponsorshipProfile = {
  companyName: "NeverSponsor Inc",
  companyNormalized: "neversponsor inc",
  aliases: [],
  domain: "neversponsor.com",
  evidenceSources: ["company_policy_page"],
  h1bSponsorLikely: false,
  confidence: "high",
  lastVerifiedAt: new Date().toISOString(),
  evidenceSummary: "Company policy explicitly states no visa sponsorship",
  positiveEvidenceCount: 0,
  negativeEvidenceCount: 20,
  recentEvidenceCount: 5,
  historicalEvidenceCount: 5,
};

const unknownProfile: CompanySponsorshipProfile = {
  companyName: "UnknownCompany",
  companyNormalized: "unknowncompany",
  aliases: [],
  domain: "unknowncompany.com",
  evidenceSources: [],
  h1bSponsorLikely: null,
  confidence: "none",
  lastVerifiedAt: new Date().toISOString(),
  evidenceSummary: "No data available",
  positiveEvidenceCount: 0,
  negativeEvidenceCount: 0,
  recentEvidenceCount: 0,
  historicalEvidenceCount: 0,
};

describe("sponsorship classifier", () => {
  describe("job-level evidence", () => {
    it('classifies explicit "no visa sponsorship" as not_supported with high confidence', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "This position does not offer visa sponsorship. No visa sponsorship available.",
        }),
      );

      expect(result.sponsorshipHint).toBe("not_supported");
      expect(result.sponsorshipConfidence).toBe("high");
      expect(result.sponsorshipEvidence.some((e) => e.signal === "negative")).toBe(true);
      expect(result.sponsorshipReason).toMatch(/no visa sponsorship/i);
    });

    it('classifies explicit "we sponsor H-1B transfers" as supported with high confidence', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "We sponsor H-1B transfers for qualified candidates.",
        }),
      );

      expect(result.sponsorshipHint).toBe("supported");
      expect(result.sponsorshipConfidence).toBe("high");
      expect(result.sponsorshipEvidence.some((e) => e.signal === "positive")).toBe(true);
      expect(result.sponsorshipReason).toMatch(/visa sponsorship/i);
    });

    it('classifies ambiguous "authorized to work in the US" as unknown with low confidence', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "Must be authorized to work in the United States.",
        }),
      );

      expect(result.sponsorshipHint).toBe("unknown");
      expect(result.sponsorshipConfidence).toBe("low");
      // Must NOT be classified as not_supported
      expect(result.sponsorshipHint).not.toBe("not_supported");
    });

    it('classifies "we do not sponsor" as not_supported', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "Please note: we do not sponsor visas for this position.",
        }),
      );

      expect(result.sponsorshipHint).toBe("not_supported");
      expect(result.sponsorshipConfidence).toBe("high");
    });

    it('classifies "cannot sponsor" as not_supported', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "Unfortunately we cannot sponsor work visas at this time.",
        }),
      );

      expect(result.sponsorshipHint).toBe("not_supported");
    });

    it('classifies "work authorization sponsorship available" as supported', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "Work authorization sponsorship available for eligible candidates.",
        }),
      );

      expect(result.sponsorshipHint).toBe("supported");
    });

    it('classifies "US work authorization required without sponsorship" as not_supported', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet:
            "Candidates must have US work authorization required without sponsorship.",
        }),
      );

      expect(result.sponsorshipHint).toBe("not_supported");
    });

    it("classifies empty description as unknown", () => {
      const result = classifySponsorship(sampleJob());

      expect(result.sponsorshipHint).toBe("unknown");
      expect(result.sponsorshipConfidence).toBe("none");
    });
  });

  describe("company-level evidence", () => {
    it("marks job as supported when company historically sponsors and no job-level negative", () => {
      const result = classifySponsorship(
        sampleJob({ company: "SponsorCorp", descriptionSnippet: "" }),
        sponsorProfile,
      );

      expect(result.sponsorshipHint).toBe("supported");
      expect(result.sponsorshipConfidence).toBe("medium");
      expect(result.companySponsorshipProfileId).toBe(sponsorProfile._id);
    });

    it("overrides company sponsor history with explicit job-level negative", () => {
      const result = classifySponsorship(
        sampleJob({
          company: "SponsorCorp",
          descriptionSnippet: "We do not sponsor visas for this role.",
        }),
        sponsorProfile,
      );

      expect(result.sponsorshipHint).toBe("not_supported");
      expect(result.sponsorshipConfidence).toBe("high");
      // Should still reference the company profile
      expect(result.companySponsorshipLikely).toBe(true);
    });

    it("does not mark job as supported when company does not sponsor", () => {
      const result = classifySponsorship(
        sampleJob({ company: "NeverSponsor Inc", descriptionSnippet: "" }),
        nonSponsorProfile,
      );

      expect(result.sponsorshipHint).toBe("unknown");
    });

    it("returns unknown when company profile has null h1bSponsorLikely", () => {
      const result = classifySponsorship(
        sampleJob({ company: "UnknownCompany", descriptionSnippet: "" }),
        unknownProfile,
      );

      expect(result.sponsorshipHint).toBe("unknown");
    });
  });

  describe("company alias matching", () => {
    it("matches company by alias", async () => {
      const repo = new InMemoryCompanySponsorshipProfileRepository();
      await repo.upsert({
        ...sponsorProfile,
        companyNormalized: "sponsorcorp",
        aliases: ["Sponsor Corp International", "SCI"],
      });

      const result = await repo.findByName("SCI");
      expect(result).not.toBeNull();
      expect(result?.companyName).toBe("SponsorCorp");
    });

    it("matches normalized company names with punctuation differences", () => {
      const normalized1 = normalizeCompanyName("Test-Company, Inc.");
      const normalized2 = normalizeCompanyName("Test-Company Inc");
      expect(normalized1).toBe(normalized2);
      expect(normalized1).toBe("testcompany inc");
    });
  });

  describe("matchCompanyProfile", () => {
    it("matches exact normalized name", () => {
      const profiles: CompanySponsorshipProfile[] = [sponsorProfile];
      const result = matchCompanyProfile("SponsorCorp", profiles);
      expect(result).not.toBeNull();
      expect(result?.companyNormalized).toBe("sponsorcorp");
    });

    it("matches alias", () => {
      const profiles: CompanySponsorshipProfile[] = [sponsorProfile];
      const result = matchCompanyProfile("Sponsor Corp International", profiles);
      expect(result).not.toBeNull();
    });

    it("returns null for unmatched company", () => {
      const profiles: CompanySponsorshipProfile[] = [sponsorProfile];
      const result = matchCompanyProfile("Random Unknown Company", profiles);
      expect(result).toBeNull();
    });
  });

  describe("edge cases", () => {
    it("negative job signal overrides positive company history", () => {
      const result = classifySponsorship(
        sampleJob({
          company: "SponsorCorp",
          descriptionSnippet: "No visa sponsorship available for this position.",
        }),
        sponsorProfile,
      );

      expect(result.sponsorshipHint).toBe("not_supported");
    });

    it("positive job signal overrides unknown company", () => {
      const result = classifySponsorship(
        sampleJob({
          company: "UnknownCompany",
          descriptionSnippet: "Visa sponsorship available for qualified candidates.",
        }),
        unknownProfile,
      );

      expect(result.sponsorshipHint).toBe("supported");
    });

    it('ambiguous "work authorization required" alone is NOT classified as not_supported', () => {
      const result = classifySponsorship(
        sampleJob({
          descriptionSnippet: "Work authorization required for this position.",
        }),
      );

      expect(result.sponsorshipHint).not.toBe("not_supported");
      expect(result.sponsorshipHint).toBe("unknown");
    });

    it("unknown company with no evidence returns unknown", () => {
      const result = classifySponsorship(sampleJob({ company: "Random New Company" }));

      expect(result.sponsorshipHint).toBe("unknown");
      expect(result.sponsorshipConfidence).toBe("none");
    });

    it("does not mark unknown as not_supported", () => {
      const result = classifySponsorship(
        sampleJob({ company: "Any Company", descriptionSnippet: "" }),
        null,
      );

      expect(result.sponsorshipHint).toBe("unknown");
      expect(result.sponsorshipHint).not.toBe("not_supported");
    });
  });
});