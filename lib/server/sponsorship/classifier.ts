import type { JobListing } from "@/lib/types";

/**
 * Sponsorship evidence model for classification.
 *
 * Each evidence piece captures what text was found, where it was found,
 * and whether it indicates positive, negative, or ambiguous sponsorship.
 */

export type SponsorshipEvidenceType =
  | "job_description_positive"
  | "job_description_negative"
  | "company_historical_sponsor"
  | "company_policy_page"
  | "user_curated_company"
  | "external_dataset_import"
  | "unknown";

export type SponsorshipEvidenceConfidence = "high" | "medium" | "low" | "none";

export type SponsorshipEvidenceSignal = "positive" | "negative" | "ambiguous" | "neutral";

export type SponsorshipEvidence = {
  source: SponsorshipEvidenceType;
  signal: SponsorshipEvidenceSignal;
  confidence: SponsorshipEvidenceConfidence;
  matchedText: string;
  field: string;
  rationale: string;
};

export type SponsorshipClassification = {
  sponsorshipHint: "supported" | "not_supported" | "unknown";
  sponsorshipConfidence: SponsorshipEvidenceConfidence;
  sponsorshipEvidence: SponsorshipEvidence[];
  sponsorshipReason: string;
  companySponsorshipProfileId?: string;
  companySponsorshipLikely?: boolean;
};

/**
 * Keyword patterns for job-level sponsorship detection.
 *
 * Rules:
 * - Explicit job-level negative signal overrides company historical evidence.
 * - Explicit job-level positive signal overrides company unknown.
 * - Company historical sponsor evidence can mark job as likely supported
 *   only when the job does not explicitly reject sponsorship.
 * - Ambiguous "authorized to work" language alone should NOT be treated as not_supported.
 * - Do not mark unknown as not_supported.
 */

type SponsorshipSignalRule = {
  pattern: RegExp;
  signal: SponsorshipEvidenceSignal;
  confidence: SponsorshipEvidenceConfidence;
  type: SponsorshipEvidenceType;
  rationale: string;
};

const POSITIVE_SIGNALS: SponsorshipSignalRule[] = [
  {
    pattern: /\bwe\s+sponsor\b/i,
    signal: "positive",
    confidence: "high",
    type: "job_description_positive",
    rationale: 'Explicit "we sponsor" language',
  },
  {
    pattern: /visa\s+sponsorship\s+available/i,
    signal: "positive",
    confidence: "high",
    type: "job_description_positive",
    rationale: '"visa sponsorship available"',
  },
  {
    pattern: /H-?1B\s+transfer/i,
    signal: "positive",
    confidence: "high",
    type: "job_description_positive",
    rationale: '"H-1B transfer" mentioned',
  },
  {
    pattern: /\bwill\s+sponsor\b/i,
    signal: "positive",
    confidence: "high",
    type: "job_description_positive",
    rationale: '"will sponsor" language',
  },
  {
    pattern: /employment\s+visa/i,
    signal: "positive",
    confidence: "medium",
    type: "job_description_positive",
    rationale: '"employment visa" mentioned',
  },
  {
    pattern: /work\s+authorization\s+sponsorship\s+available/i,
    signal: "positive",
    confidence: "high",
    type: "job_description_positive",
    rationale: '"work authorization sponsorship available"',
  },
];

const NEGATIVE_SIGNALS: SponsorshipSignalRule[] = [
  {
    pattern: /\bwe\s+do\s+not\s+sponsor\b/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: 'Explicit "we do not sponsor" language',
  },
  {
    pattern: /no\s+visa\s+sponsorship/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: '"no visa sponsorship" stated',
  },
  {
    pattern: /must\s+be\s+authorized\s+to\s+work\s+without\s+sponsorship/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: '"must be authorized to work without sponsorship"',
  },
  {
    pattern: /now\s+or\s+in\s+the\s+future\s+require\s+sponsorship/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: '"now or in the future require sponsorship"',
  },
  {
    pattern: /\bcannot\s+sponsor\b/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: '"cannot sponsor" stated',
  },
  {
    pattern: /US\s+work\s+authorization\s+required\s+without\s+sponsorship/i,
    signal: "negative",
    confidence: "high",
    type: "job_description_negative",
    rationale: '"US work authorization required without sponsorship"',
  },
];

const AMBIGUOUS_SIGNALS: SponsorshipSignalRule[] = [
  {
    pattern: /\bmust\s+be\s+authorized\s+to\s+work\b/i,
    signal: "ambiguous",
    confidence: "low",
    type: "unknown",
    rationale: '"must be authorized to work" (ambiguous, not a denial)',
  },
  {
    pattern: /eligible\s+to\s+work\s+in\s+the\s+United\s+States/i,
    signal: "ambiguous",
    confidence: "low",
    type: "unknown",
    rationale: '"eligible to work in the United States" (ambiguous)',
  },
  {
    pattern: /work\s+authorization\s+required/i,
    signal: "ambiguous",
    confidence: "low",
    type: "unknown",
    rationale: '"work authorization required" (ambiguous)',
  },
];

/**
 * Parse job description and metadata for sponsorship signals.
 */
function parseJobLevelEvidence(
  job: Pick<
    JobListing,
    "title" | "descriptionSnippet" | "company" | "sourcePlatform" | "rawSourceMetadata"
  >,
): SponsorshipEvidence[] {
  const evidence: SponsorshipEvidence[] = [];
  const descriptionText = job.descriptionSnippet ?? "";

  // Combine all text fields for scanning
  const texts: Array<{ field: string; content: string }> = [
    { field: "descriptionSnippet", content: descriptionText },
  ];

  // Add raw source metadata if available
  if (job.rawSourceMetadata) {
    const metaText =
      typeof job.rawSourceMetadata === "object"
        ? JSON.stringify(job.rawSourceMetadata)
        : String(job.rawSourceMetadata);
    texts.push({ field: "rawSourceMetadata", content: metaText });
  }

  // Check positive signals first
  for (const rule of POSITIVE_SIGNALS) {
    for (const { field, content } of texts) {
      if (!content) continue;
      const match = content.match(rule.pattern);
      if (match) {
        evidence.push({
          source: rule.type,
          signal: rule.signal,
          confidence: rule.confidence,
          matchedText: match[0],
          field,
          rationale: rule.rationale,
        });
      }
    }
  }

  // Check negative signals
  for (const rule of NEGATIVE_SIGNALS) {
    for (const { field, content } of texts) {
      if (!content) continue;
      const match = content.match(rule.pattern);
      if (match) {
        evidence.push({
          source: rule.type,
          signal: rule.signal,
          confidence: rule.confidence,
          matchedText: match[0],
          field,
          rationale: rule.rationale,
        });
      }
    }
  }

  // Check ambiguous signals
  for (const rule of AMBIGUOUS_SIGNALS) {
    for (const { field, content } of texts) {
      if (!content) continue;
      const match = content.match(rule.pattern);
      if (match) {
        evidence.push({
          source: rule.type,
          signal: rule.signal,
          confidence: rule.confidence,
          matchedText: match[0],
          field,
          rationale: rule.rationale,
        });
      }
    }
  }

  return evidence;
}

export type CompanySponsorshipProfile = {
  companyName: string;
  companyNormalized: string;
  aliases: string[];
  domain?: string;
  sourcePlatform?: string;
  evidenceSources: SponsorshipEvidenceType[];
  h1bSponsorLikely: boolean | null; // true | false | null (unknown)
  confidence: SponsorshipEvidenceConfidence;
  lastVerifiedAt?: string;
  evidenceSummary: string;
  positiveEvidenceCount: number;
  negativeEvidenceCount: number;
  recentEvidenceCount: number;
  historicalEvidenceCount: number;
  _id?: string;
};

/**
 * Normalize a company name for matching (lowercase, stripped of excess whitespace).
 */
export function normalizeCompanyName(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.,/#!$%^&*;:{}=\-_`~()]/g, "")
    .trim();
}

/**
 * Match a job's company against a set of company sponsorship profiles.
 */
export function matchCompanyProfile(
  company: string,
  profiles: CompanySponsorshipProfile[],
): CompanySponsorshipProfile | null {
  const normalized = normalizeCompanyName(company);

  for (const profile of profiles) {
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

/**
 * Classify a job's sponsorship based on job-level evidence and company profile.
 *
 * Rules:
 * 1. Explicit job-level negative signal overrides historical company sponsor evidence.
 * 2. Explicit job-level positive signal overrides company unknown.
 * 3. Company historical sponsor evidence can mark job as likely supported
 *    only when the job does not explicitly reject sponsorship.
 * 4. Ambiguous "authorized to work" language alone should NOT be treated as not_supported.
 * 5. Do not mark unknown as not_supported.
 */
export function classifySponsorship(
  job: Pick<
    JobListing,
    "title" | "descriptionSnippet" | "company" | "sourcePlatform" | "rawSourceMetadata"
  >,
  profile?: CompanySponsorshipProfile | null,
): SponsorshipClassification {
  const jobEvidence = parseJobLevelEvidence(job);

  const hasJobLevelNegative = jobEvidence.some(
    (e) => e.signal === "negative" && e.confidence === "high",
  );
  const hasJobLevelPositive = jobEvidence.some(
    (e) => e.signal === "positive" && (e.confidence === "high" || e.confidence === "medium"),
  );
  const hasOnlyAmbiguous = jobEvidence.length > 0 && jobEvidence.every(
    (e) => e.signal === "ambiguous" || e.signal === "neutral",
  );

  // Build company evidence if profile exists
  const companyEvidence: SponsorshipEvidence[] = profile
    ? [
        {
          source: "company_historical_sponsor",
          signal:
            profile.h1bSponsorLikely === true
              ? "positive"
              : profile.h1bSponsorLikely === false
                ? "negative"
                : "neutral",
          confidence: profile.confidence,
          matchedText: profile.companyName,
          field: "company_profile",
          rationale: profile.evidenceSummary,
        },
      ]
    : [];

  const allEvidence = [...jobEvidence, ...companyEvidence];

  // Rule 1: Explicit job-level negative overrides everything
  if (hasJobLevelNegative) {
    return {
      sponsorshipHint: "not_supported",
      sponsorshipConfidence: "high",
      sponsorshipEvidence: allEvidence,
      sponsorshipReason:
        "Job explicitly states no visa sponsorship: " +
        jobEvidence
          .filter((e) => e.signal === "negative")
          .map((e) => e.matchedText)
          .join(", "),
      companySponsorshipProfileId: profile?._id,
      companySponsorshipLikely: profile?.h1bSponsorLikely ?? undefined,
    };
  }

  // Rule 2: Explicit job-level positive overrides company unknown
  if (hasJobLevelPositive) {
    return {
      sponsorshipHint: "supported",
      sponsorshipConfidence: "high",
      sponsorshipEvidence: allEvidence,
      sponsorshipReason:
        "Job explicitly mentions visa sponsorship: " +
        jobEvidence
          .filter((e) => e.signal === "positive")
          .map((e) => e.matchedText)
          .join(", "),
      companySponsorshipProfileId: profile?._id,
      companySponsorshipLikely: profile?.h1bSponsorLikely ?? undefined,
    };
  }

  // Rule 3: Company historical sponsor evidence
  if (profile && profile.h1bSponsorLikely === true) {
    const confidence =
      profile.confidence === "high" ? "medium" : profile.confidence === "medium" ? "low" : "low";
    return {
      sponsorshipHint: "supported",
      sponsorshipConfidence: confidence,
      sponsorshipEvidence: allEvidence,
      sponsorshipReason: `Company ${profile.companyName} has historical H-1B sponsorship evidence with ${profile.confidence} confidence`,
      companySponsorshipProfileId: profile._id,
      companySponsorshipLikely: true,
    };
  }

  // Rule 4: Ambiguous-only evidence should remain unknown
  if (hasOnlyAmbiguous) {
    return {
      sponsorshipHint: "unknown",
      sponsorshipConfidence: "low",
      sponsorshipEvidence: allEvidence,
      sponsorshipReason:
        "Ambiguous work authorization language found but no explicit sponsorship declaration: " +
        jobEvidence.map((e) => e.matchedText).join(", "),
      companySponsorshipProfileId: profile?._id,
      companySponsorshipLikely: profile?.h1bSponsorLikely ?? undefined,
    };
  }

  // No evidence at all
  const reason = profile
    ? profile.h1bSponsorLikely === false
      ? `Company ${profile.companyName} historically does not sponsor, but no explicit job statement`
      : `No sponsorship evidence found for ${profile.companyName}`
    : "No sponsorship evidence found (no company profile match)";

  return {
    sponsorshipHint: "unknown",
    sponsorshipConfidence: "none",
    sponsorshipEvidence: allEvidence,
    sponsorshipReason: reason,
    companySponsorshipProfileId: profile?._id,
    companySponsorshipLikely: profile?.h1bSponsorLikely ?? undefined,
  };
}