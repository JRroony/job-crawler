import { parseGeoIntentFromFilters } from "@/lib/server/geo/parse";
import { analyzeTitle } from "@/lib/server/title-retrieval";
import type { SearchFilters, ExperienceLevel } from "@/lib/types";
import type { JobSearchIntent } from "./types";
import { seniorityLevels } from "./types";

/**
 * Parse user search intent from raw search filters into a structured intent object.
 * This is a deterministic parser — no LLM calls required.
 */
export function parseSearchIntent(filters: SearchFilters): JobSearchIntent {
  const rawTitle = filters.title ?? "";
  const titleAnalysis = analyzeTitle(rawTitle);
  const geoIntent = parseGeoIntentFromFilters(filters);

  // Determine remote preference
  const rawLocation = [filters.city, filters.state, filters.country]
    .filter(Boolean)
    .join(", ");
  const remotePreference = inferRemotePreference(rawLocation, geoIntent.isRemote);

  // Determine sponsorship preference (simplified deterministic inference)
  const sponsorshipPreference = inferSponsorshipPreference(rawTitle, rawLocation);

  // Determine seniority preferences from title analysis
  const seniorities = extractSeniorityFromTitle(rawTitle, titleAnalysis);

  return {
    rawTitle,
    normalizedTitle: titleAnalysis.normalized,
    roleFamily: titleAnalysis.family,
    titleVariants: [],
    excludedSeniorities: seniorities.excluded,
    preferredSeniorities: seniorities.preferred,
    rawLocation,
    resolvedLocationScope: geoIntent.scope as JobSearchIntent["resolvedLocationScope"],
    country: geoIntent.country?.name,
    state: geoIntent.region?.name,
    city: geoIntent.city?.name,
    remotePreference,
    sponsorshipPreference,
    platformFilters: (filters.platforms as string[]) ?? [],
  };
}

/**
 * Infer remote work preference from location input and geo intent.
 */
function inferRemotePreference(
  rawLocation: string,
  isRemote: boolean,
): "none" | "preferred" | "required" {
  if (!isRemote) return "none";
  const lower = rawLocation.toLowerCase();
  if (/\b(remote only|fully remote|100% remote)\b/i.test(lower)) {
    return "required";
  }
  return "preferred";
}

/**
 * Deterministic sponsorship preference inference.
 * Heuristic: roles commonly needing sponsorship (entry-level, certain industries)
 * are flagged as "preferred" if user mentions specific terms.
 */
function inferSponsorshipPreference(
  _rawTitle: string,
  _rawLocation: string,
): "none" | "preferred" | "required" {
  // Default: none. This is a placeholder for future enhancement.
  return "none";
}

/**
 * Extract seniority hints from user's title search query.
 * Returns excluded and preferred lists.
 */
function extractSeniorityFromTitle(
  rawTitle: string,
  _titleAnalysis: ReturnType<typeof analyzeTitle>,
): { excluded: ExperienceLevel[]; preferred: ExperienceLevel[] } {
  const lower = rawTitle.toLowerCase();
  const excluded: ExperienceLevel[] = [];
  const preferred: ExperienceLevel[] = [];

  // Detect explicit seniority in the query itself (before variant expansion)
  const seniorityPatterns: Array<{ regex: RegExp; levels: string[] }> = [
    { regex: /\b(senior|sr\.?|sr)\b/i, levels: ["senior"] },
    { regex: /\b(staff)\b/i, levels: ["staff"] },
    { regex: /\b(principal|distinguished)\b/i, levels: ["principal"] },
    { regex: /\b(lead)\b/i, levels: ["lead"] },
    { regex: /\b(manager|head of|vp of|director of)\b/i, levels: ["manager", "director"] },
    { regex: /\b(director|head|vp|vice president)\b/i, levels: ["director", "executive"] },
    { regex: /\b(junior|jr\.?|jr|associate)\b/i, levels: ["junior", "mid"] },
    { regex: /\b(entry.level|new grad|graduate|intern)\b/i, levels: ["intern", "new_grad"] },
  ];

  const validLevels = new Set(seniorityLevels as readonly string[]);
  // If user explicitly requests a seniority, keep it; otherwise don't exclude anything
  for (const pattern of seniorityPatterns) {
    if (pattern.regex.test(lower)) {
      for (const level of pattern.levels) {
        if (validLevels.has(level)) {
          preferred.push(level as ExperienceLevel);
        }
      }
    }
  }

  return { excluded, preferred };
}

/**
 * Classify the seniority of a job title.
 * Returns an array of classifications from most to least specific.
 */
export function classifySeniority(title: string): {
  level: string;
  confidence: "high" | "medium" | "low";
  matchedText: string;
}[] {
  const normalized = title.trim();
  const lower = normalized.toLowerCase();
  const results: { level: string; confidence: "high" | "medium" | "low"; matchedText: string }[] = [];

  // High-confidence patterns (exact or near-exact matches)
  const highConfidencePatterns: Array<{ regex: RegExp; level: string }> = [
    { regex: /\b(distinguished engineer|fellow engineer|executive director|chief technology officer|cto|chief product officer|cpo)\b/i, level: "executive" },
    { regex: /\b(director of engineering|engineering director|director of product|vp of|vice president of|head of engineering|head of product)\b/i, level: "director" },
    { regex: /\b(principal engineer|principal scientist|principal architect|principal product manager|distinguished)\b/i, level: "principal" },
    { regex: /\b(staff engineer|staff scientist|staff product manager|staff data|staff software)\b/i, level: "staff" },
    { regex: /\b(lead engineer|lead developer|lead product|lead data|tech lead|technical lead|team lead)\b/i, level: "lead" },
    { regex: /\b(senior software|senior engineer|senior developer|senior product|senior data|senior manager|sr\. software|sr software)\b/i, level: "senior" },
    { regex: /\b(engineering manager|project manager|program manager)\b/i, level: "manager" },
    { regex: /\b(software engineer ii|software engineer 2|software developer ii|data analyst ii)\b/i, level: "mid" },
    { regex: /\b(software engineer iii|software engineer 3|software developer iii|data analyst iii|senior ii)\b/i, level: "senior" },
    { regex: /\b(software engineer i|software engineer 1|software developer i|data analyst i|associate software|associate product manager|associate engineer|associate developer)\b/i, level: "junior" },
    { regex: /\b(new grad|new graduate|university grad|campus hire|entry level|recent grad)\b/i, level: "new_grad" },
    { regex: /\b(intern|internship|co-op|co op)\b/i, level: "intern" },
  ];

  for (const { regex, level } of highConfidencePatterns) {
    if (regex.test(lower)) {
      results.push({ level, confidence: "high", matchedText: normalized.match(regex.source)?.[0] ?? "" });
    }
  }

  // If we have high-confidence matches, return the best one.
  // Tiebreak: prefer longest matched text (most specific pattern),
  // then highest seniority level.
  if (results.length > 0) {
    const seniorityOrder = ["intern", "new_grad", "junior", "mid", "senior", "lead", "staff", "principal", "manager", "director", "executive"];
    const bestMatch = results.reduce((best, curr) => {
      const currLen = curr.matchedText.length;
      const bestLen = best.matchedText.length;
      if (currLen !== bestLen) return currLen > bestLen ? curr : best;
      const currIdx = seniorityOrder.indexOf(curr.level);
      const bestIdx = seniorityOrder.indexOf(best.level);
      return currIdx > bestIdx ? curr : best;
    }, results[0]);
    return [bestMatch];
  }

  // Medium-confidence patterns
  const mediumPatterns: Array<{ regex: RegExp; level: string }> = [
    { regex: /\b(director)\b/i, level: "director" },
    { regex: /\b(principal|architect)\b/i, level: "principal" },
    { regex: /\b(staff)\b/i, level: "staff" },
    { regex: /\b(lead)\b/i, level: "lead" },
    { regex: /\b(senior|sr\.?)\b/i, level: "senior" },
    { regex: /\b(ii|iii|iv|v)\b/i, level: "senior" },
    { regex: /\b(manager|head of|vp)\b/i, level: "manager" },
    { regex: /\b(junior|jr\.?|associate)\b/i, level: "junior" },
    { regex: /\b(entry.level|graduate|grad)\b/i, level: "new_grad" },
    { regex: /\b(intern)\b/i, level: "intern" },
  ];

  for (const { regex, level } of mediumPatterns) {
    if (regex.test(lower)) {
      results.push({ level, confidence: "medium", matchedText: normalized.match(regex.source)?.[0] ?? "" });
    }
  }

  if (results.length > 0) {
    return [results[0]];
  }

  // Default: no explicit seniority → "mid" with low confidence for engineer/analyst roles
  // or "unknown" for everything else
  if (/\b(engineer|developer|analyst|scientist|designer)\b/i.test(lower)) {
    return [{ level: "unknown", confidence: "low", matchedText: "" }];
  }

  return [{ level: "unknown", confidence: "low", matchedText: "" }];
}

/**
 * Map classified seniority string to the experience level union.
 */
export function mapSeniorityToExperienceLevel(
  seniority: string,
): (typeof seniorityLevels)[number] | "unknown" {
  const lower = seniority.toLowerCase();
  const mapping: Record<string, (typeof seniorityLevels)[number]> = {
    intern: "intern",
    new_grad: "new_grad",
    junior: "junior",
    mid: "mid",
    senior: "senior",
    lead: "lead",
    staff: "staff",
    principal: "principal",
    manager: "manager",
    director: "director",
    executive: "executive",
  };
  return mapping[lower] ?? "unknown";
}