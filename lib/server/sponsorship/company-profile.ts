import {
  type CompanySponsorshipProfile,
  type SponsorshipEvidenceType,
  type SponsorshipEvidenceConfidence,
  normalizeCompanyName,
} from "./classifier";

/**
 * Repository interface for company sponsorship profiles.
 * Implementations can use MongoDB, in-memory storage, or loaded files.
 */
export interface CompanySponsorshipProfileRepository {
  list(): Promise<CompanySponsorshipProfile[]>;
  upsert(profile: CompanySponsorshipProfile): Promise<CompanySponsorshipProfile>;
  findByName(
    companyName: string,
  ): Promise<CompanySponsorshipProfile | null>;
  count(): Promise<number>;
}

/**
 * In-memory implementation for diagnostic and import workflows.
 */
export class InMemoryCompanySponsorshipProfileRepository
  implements CompanySponsorshipProfileRepository
{
  private profiles: Map<string, CompanySponsorshipProfile> = new Map();

  async list(): Promise<CompanySponsorshipProfile[]> {
    return Array.from(this.profiles.values());
  }

  async upsert(
    profile: CompanySponsorshipProfile,
  ): Promise<CompanySponsorshipProfile> {
    const key = profile._id ?? profile.companyNormalized;
    this.profiles.set(key, profile);
    return profile;
  }

  async findByName(
    companyName: string,
  ): Promise<CompanySponsorshipProfile | null> {
    const normalized = normalizeCompanyName(companyName);
    for (const profile of this.profiles.values()) {
      if (profile.companyNormalized === normalized) {
        return profile;
      }
      for (const alias of profile.aliases) {
        if (normalizeCompanyName(alias) === normalized) {
          return profile;
        }
      }
    }
    return null;
  }

  async count(): Promise<number> {
    return this.profiles.size;
  }
}

/**
 * Create a company sponsorship profile from a normalized record.
 */
export function createCompanySponsorshipProfile(input: {
  companyName: string;
  aliases?: string[];
  domain?: string;
  sourcePlatform?: string;
  evidenceSources?: SponsorshipEvidenceType[];
  h1bSponsorLikely: boolean | null;
  confidence: SponsorshipEvidenceConfidence;
  evidenceSummary: string;
  positiveEvidenceCount?: number;
  negativeEvidenceCount?: number;
  recentEvidenceCount?: number;
  historicalEvidenceCount?: number;
}): CompanySponsorshipProfile {
  return {
    companyName: input.companyName,
    companyNormalized: normalizeCompanyName(input.companyName),
    aliases: (input.aliases ?? []).map((alias) => alias.trim()),
    domain: input.domain,
    sourcePlatform: input.sourcePlatform,
    evidenceSources: input.evidenceSources ?? [],
    h1bSponsorLikely: input.h1bSponsorLikely,
    confidence: input.confidence,
    lastVerifiedAt: new Date().toISOString(),
    evidenceSummary: input.evidenceSummary,
    positiveEvidenceCount: input.positiveEvidenceCount ?? 0,
    negativeEvidenceCount: input.negativeEvidenceCount ?? 0,
    recentEvidenceCount: input.recentEvidenceCount ?? 0,
    historicalEvidenceCount: input.historicalEvidenceCount ?? 0,
  };
}