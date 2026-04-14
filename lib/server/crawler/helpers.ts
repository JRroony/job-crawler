import "server-only";

import { createHash, randomUUID } from "node:crypto";

import {
  analyzeUsLocation,
  isRecognizedUsCity,
  resolveUsState,
} from "@/lib/server/locations/us";
import {
  getLocationMatchResult,
  resolveJobLocation,
  resolveLocationText,
  type LocationMatchResult,
} from "@/lib/server/location-resolution";
import { canonicalizeGreenhouseUrl } from "@/lib/server/discovery/greenhouse-url";
import {
  buildDiscoveryRoleQueries as buildTitleRetrievalDiscoveryRoleQueries,
  getTitleMatchResult as getTitleRetrievalMatchResult,
  normalizeTitleToCanonicalForm as normalizeTitleRetrievalCanonicalForm,
} from "@/lib/server/title-retrieval";
import type {
  TitleMatchMode,
  TitleMatchResult,
} from "@/lib/server/title-retrieval";
import type {
  ExperienceClassification,
  ExperienceInferenceConfidence,
  ExperienceLevel,
  ExperienceMatchMode,
  JobListing,
  ProviderPlatform,
  ResolvedLocation,
  SearchFilters,
} from "@/lib/types";

type CountryAliasGroup = {
  concept: string;
  aliases: string[];
};

export type FilterExclusionReason = "title" | "location" | "experience";

export type FilterEvaluation =
  | {
      matches: true;
      titleMatch: TitleMatchResult;
      locationMatch?: LocationMatchResult;
      experienceMatch?: ExperienceFilterResult;
    }
  | {
      matches: false;
      reason: FilterExclusionReason;
      titleMatch?: TitleMatchResult;
      locationMatch?: LocationMatchResult;
      experienceMatch?: ExperienceFilterResult;
    };

type ExperienceFilterableJob = Pick<
  JobListing,
  | "title"
  | "company"
  | "country"
  | "state"
  | "city"
  | "locationText"
  | "resolvedLocation"
  | "experienceLevel"
  | "experienceClassification"
  | "rawSourceMetadata"
>;

type ExperienceSignal = {
  ruleId: string;
  signalType:
    | "title_keyword"
    | "acronym"
    | "level_code"
    | "leadership_context"
    | "years_of_experience"
    | "structured_hint"
    | "description_hint"
    | "metadata_hint";
  level: ExperienceLevel;
  confidence: ExperienceInferenceConfidence;
  source: ExperienceClassification["source"];
  reason: string;
  matchedText: string;
};

type ExperienceRule = {
  id: string;
  level: ExperienceLevel;
  signalType:
    | "title_keyword"
    | "acronym"
    | "level_code"
    | "leadership_context";
  rationale: string;
  patterns: RegExp[];
};

export type ExperienceFilterResult = {
  matches: boolean;
  classification: ExperienceClassification;
  selectedLevels: ExperienceLevel[];
  mode: ExperienceMatchMode;
  includeUnspecified: boolean;
  matchedLevel?: ExperienceLevel;
  explanation: string;
};

const experienceTitleRules: ExperienceRule[] = [
  {
    id: "title_intern",
    level: "intern",
    signalType: "title_keyword",
    rationale: "The title explicitly indicates an internship or student role.",
    patterns: [
      /\bintern\b/,
      /\binternship\b/,
      /\bco op\b/,
      /\bcooperative education\b/,
      /\bapprentice(?:ship)?\b/,
      /\bworking student\b/,
    ],
  },
  {
    id: "title_new_grad",
    level: "new_grad",
    signalType: "title_keyword",
    rationale: "The title explicitly targets new graduates or entry-level candidates.",
    patterns: [
      /\bnew grad(?:uate)?\b/,
      /\brecent grad(?:uate)?\b/,
      /\bentry level\b/,
      /\bearly career\b/,
      /\bearly talent\b/,
      /\bgraduate program\b/,
    ],
  },
  {
    id: "title_junior",
    level: "junior",
    signalType: "title_keyword",
    rationale: "The title explicitly marks the role as junior or associate-level.",
    patterns: [/\bjunior\b/, /\bjr\b/, /\bentry associate\b/, /\bassociate\b/],
  },
  {
    id: "title_mid",
    level: "mid",
    signalType: "level_code",
    rationale: "The title includes a mid-level ladder marker.",
    patterns: [
      /\bmid(?: level)?\b/,
      /\blevel 2\b/,
      /\bii\b/,
      /\bl[- ]?4\b/,
      /\bic[- ]?4\b/,
      /\be[- ]?4\b/,
    ],
  },
  {
    id: "title_senior_keyword",
    level: "senior",
    signalType: "title_keyword",
    rationale: "The title explicitly marks the role as senior.",
    patterns: [/\bsenior\b/, /\bsr\b/, /\barchitect\b/],
  },
  {
    id: "title_senior_level_code",
    level: "senior",
    signalType: "level_code",
    rationale: "The title includes a senior-level ladder marker.",
    patterns: [/\blevel 3\b/, /\biii\b/, /\bl[- ]?5\b/, /\bic[- ]?5\b/, /\be[- ]?5\b/],
  },
  {
    id: "title_leadership",
    level: "lead",
    signalType: "leadership_context",
    rationale: "The title explicitly indicates lead or people-management responsibility.",
    patterns: [
      /\blead\b/,
      /\btech(?:nical)? lead\b/,
      /\bteam lead\b/,
      /\b(?:engineering|software|data|machine learning|ml|ai|platform|infrastructure|security|it|devops|qa|quality|test|analytics)\s+manager\b/,
      /\b(?:engineering|software|data|machine learning|ml|ai|platform|infrastructure|security|it|devops|qa|quality|test|analytics)\s+director\b/,
      /\bdirector of (?:engineering|software|data|machine learning|ml|ai|platform|infrastructure|security|it|devops|qa|quality|test|analytics)\b/,
      /\bhead of (?:engineering|software|data|machine learning|ml|ai|platform|infrastructure|security|it|devops|qa|quality|test|analytics)\b/,
    ],
  },
  {
    id: "title_staff_keyword",
    level: "staff",
    signalType: "title_keyword",
    rationale: "The title explicitly indicates a staff-level technical role.",
    patterns: [/\bstaff\b/, /\bmember of technical staff\b/, /\bmts\b/, /\bsmts\b/],
  },
  {
    id: "title_staff_level_code",
    level: "staff",
    signalType: "level_code",
    rationale: "The title includes a staff-level ladder marker.",
    patterns: [/\blevel 4\b/, /\biv\b/, /\bl[- ]?6\b/, /\bic[- ]?6\b/, /\be[- ]?6\b/],
  },
  {
    id: "title_principal_keyword",
    level: "principal",
    signalType: "title_keyword",
    rationale: "The title explicitly indicates principal or distinguished seniority.",
    patterns: [/\bprincipal\b/, /\bdistinguished\b/, /\bfellow\b/, /\blmts\b/, /\bpmts\b/],
  },
  {
    id: "title_principal_level_code",
    level: "principal",
    signalType: "level_code",
    rationale: "The title includes a principal-level ladder marker.",
    patterns: [
      /\blevel 5\b/,
      /\bv\b/,
      /\bl[- ]?(?:7|8|9|10)\b/,
      /\bic[- ]?(?:7|8|9|10)\b/,
      /\be[- ]?(?:7|8|9|10)\b/,
    ],
  },
];

const structuredHintRules: ExperienceRule[] = [
  {
    id: "structured_student_program",
    level: "intern",
    signalType: "title_keyword",
    rationale: "Structured metadata indicates a student, internship, or campus program.",
    patterns: [
      /\bstudent program\b/,
      /\bstudent opportunity\b/,
      /\bstudent role\b/,
      /\bstudent position\b/,
      /\bfor students\b/,
      /\bcampus\b/,
      /\buniversity recruiting\b/,
    ],
  },
  ...experienceTitleRules,
];

const descriptionHintRules: ExperienceRule[] = experienceTitleRules.filter(
  (rule) =>
    !["title_leadership", "title_senior_keyword"].includes(rule.id) &&
    !(rule.level === "junior" && rule.id === "title_junior"),
);

const metadataHintRules: ExperienceRule[] = [
  ...structuredHintRules,
];

const experienceLevelPriority: Record<ExperienceLevel, number> = {
  intern: 1,
  new_grad: 2,
  junior: 3,
  mid: 4,
  senior: 5,
  lead: 6,
  staff: 7,
  principal: 8,
};

const experienceConfidencePriority: Record<ExperienceInferenceConfidence, number> = {
  none: 0,
  low: 1,
  medium: 2,
  high: 3,
};

const experienceSourcePriority: Record<ExperienceClassification["source"], number> = {
  unknown: 0,
  page_fetch: 1,
  description: 2,
  structured_metadata: 3,
  title: 4,
};

const prioritizedTitleExperienceRules = sortExperienceMatchersByPriority(
  experienceTitleRules,
);
const prioritizedStructuredHintRules = sortExperienceMatchersByPriority(
  structuredHintRules,
);
const prioritizedDescriptionHintRules = sortExperienceMatchersByPriority(
  descriptionHintRules,
);
const prioritizedMetadataHintRules = sortExperienceMatchersByPriority(
  metadataHintRules,
);

const experienceMetadataHintPattern =
  /\b(intern|internship|co op|cooperative education|apprentice|working student|student program|student opportunity|student role|student position|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|early talent|junior|associate|mid level|senior|staff|principal|distinguished|fellow|member of technical staff|mts|smts|lmts|pmts|lead|architect|manager|director|level [2-5]|ii|iii|iv|v|l[4-9]|ic[4-9]|e[4-9]|years|year|yrs|yoe|experience)\b/;

const negativeExperienceMetadataHintPatterns = [
  /\bif you are an? (?:intern|new grad|new graduate|staff|junior|senior|associate|working student|apprentice)\b/,
  /\bplease do not apply\b/,
  /\bdo not apply using this link\b/,
  /\bdoes not include internships?\b/,
  /\bnot for interns?\b/,
  /\bexcluding internships?\b/,
];

const experiencePromptSignalPattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|campus|university recruiting|new grads?|new graduates?|recent grads?|recent graduates?|graduate program|entry(?: |-)?level|early career|early talent|junior|jr|associate|mid(?: |-)?level|senior|sr|staff|principal|distinguished|fellow|member of technical staff|mts|smts|lmts|pmts|lead|architect|manager|director|career level|seniority|level [2-5]|ii|iii|iv|v|l[4-9]|ic[4-9]|e[4-9]|requirements?|qualifications?|minimum qualifications?|preferred qualifications?|years?|yrs?|yoe)\b/i;

const experiencePromptYearPattern =
  /\b\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe)\b/i;

const countryAliasGroups: CountryAliasGroup[] = [
  {
    concept: "united states",
    aliases: [
      "united states",
      "united states of america",
      "usa",
      "us",
      "u s a",
      "u s",
    ],
  },
];

const countryConceptByAlias = new Map(
  countryAliasGroups.flatMap((group) =>
    group.aliases.map((alias) => [normalizeComparableText(alias), group.concept] as const),
  ),
);

const countryAliasesByConcept = new Map(
  countryAliasGroups.map((group) => [
    group.concept,
    group.aliases.map((alias) => normalizeComparableText(alias)),
  ] as const),
);

const experienceMetadataIgnoredKeys = new Set([
  "crawlTraceId",
  "crawlTitleMatch",
  "crawlLocationMatch",
  "crawlExperienceMatch",
  "crawlResolvedLocation",
]);

export function createId() {
  return randomUUID();
}

export function normalizeComparableText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function slugToLabel(value: string) {
  return value
    .split(/[-_]/g)
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

export function buildLocationText(parts: Array<string | undefined>) {
  const unique = parts.map((part) => part?.trim()).filter(Boolean) as string[];
  return unique.join(", ");
}

export function parseLocationText(locationText?: string) {
  const cleaned = (locationText ?? "").trim();
  if (!cleaned) {
    return {
      city: undefined,
      state: undefined,
      country: undefined,
      locationText: "Location unavailable",
    };
  }

  const parts = cleaned.split(",").map((part) => part.trim()).filter(Boolean);
  const resolvedLocation = resolveLocationText(cleaned);
  if (parts.length === 0) {
    return {
      city: undefined,
      state: undefined,
      country: undefined,
      locationText: cleaned,
    };
  }

  if (parts.length === 1) {
    return {
      city: resolvedLocation.isRemote ? undefined : resolvedLocation.city,
      state: resolvedLocation.state,
      country: resolvedLocation.isUnitedStates ? "United States" : undefined,
      locationText: parts[0],
    };
  }

  if (parts.length === 2) {
    const normalizedSecondPart = normalizeComparableText(parts[1]);
    const usState = resolveUsState(normalizedSecondPart);
    const countryConcept = resolveCountryConcept(parts[1]);
    const isRemote = normalizeComparableText(parts[0]) === "remote";

    return {
      city:
        resolvedLocation.isRemote || isRemote
          ? undefined
          : resolvedLocation.city ?? parts[0],
      state: resolvedLocation.state ?? usState,
      country:
        resolvedLocation.isUnitedStates || usState || countryConcept === "united states"
          ? "United States"
          : parts[1],
      locationText: cleaned,
    };
  }

  return {
    city: resolvedLocation.isRemote ? undefined : resolvedLocation.city ?? parts[0],
    state: resolvedLocation.state ?? resolveUsState(parts[1]) ?? parts[1],
    country: resolvedLocation.isUnitedStates ? "United States" : parts[2],
    locationText: cleaned,
  };
}

export function inferExperienceLevel(...values: Array<string | undefined>) {
  return selectExperienceLevel(
    classifyExperience({
      title: values[0],
      descriptionExperienceHints: values.slice(1),
    }),
  );
}

export function buildExperienceInferencePrompt(...values: Array<string | undefined>) {
  const seen = new Set<string>();
  const segments: string[] = [];

  for (const value of values) {
    for (const segment of extractExperiencePromptSegments(value)) {
      const comparable = normalizeComparableText(segment);
      if (!comparable || seen.has(comparable)) {
        continue;
      }

      seen.add(comparable);
      segments.push(segment);

      if (segments.length >= 12) {
        return segments.join(" ");
      }
    }
  }

  return segments.join(" ");
}

export function buildUnspecifiedExperienceClassification(
  reasons: string[] = [],
  title?: string,
  matchedSignals: ExperienceSignal[] = [],
): ExperienceClassification {
  return {
    confidence: "none",
    source: "unknown",
    reasons,
    isUnspecified: true,
    diagnostics: buildExperienceClassificationDiagnostics({
      title,
      matchedSignals,
      finalSeniority: "unknown",
      rationale: reasons,
    }),
  };
}

export function classifyExperience(input: {
  title?: string;
  explicitExperienceLevel?: ExperienceLevel;
  explicitExperienceSource?: ExperienceClassification["source"];
  explicitExperienceReasons?: string[];
  structuredExperienceHints?: Array<string | undefined>;
  descriptionExperienceHints?: Array<string | undefined>;
  pageFetchExperienceHints?: Array<string | undefined>;
  rawSourceMetadata?: Record<string, unknown>;
}): ExperienceClassification {
  const collectedSignals = collectExperienceSignals(input);
  const explicitExperience = resolveExplicitExperienceClassification(input);
  if (explicitExperience) {
    return explicitExperience;
  }

  const structuredSignal = selectBestExperienceSignal(
    collectedSignals.structuredSignals,
  );
  if (structuredSignal) {
    return classificationFromSignal(structuredSignal, input.title, collectedSignals.allSignals);
  }

  const descriptionSignal = selectBestExperienceSignal(
    collectedSignals.descriptionSignals,
  );
  if (descriptionSignal) {
    return classificationFromSignal(descriptionSignal, input.title, collectedSignals.allSignals);
  }

  const metadataSignal = selectBestExperienceSignal(collectedSignals.metadataSignals);
  if (metadataSignal) {
    return classificationFromSignal(metadataSignal, input.title, collectedSignals.allSignals);
  }

  const pageFetchSignal = selectBestExperienceSignal(
    collectedSignals.pageFetchSignals,
  );
  if (pageFetchSignal) {
    return classificationFromSignal(pageFetchSignal, input.title, collectedSignals.allSignals);
  }

  return buildUnspecifiedExperienceClassification(
    [],
    input.title,
    collectedSignals.allSignals,
  );
}

export function normalizeExperienceClassification(
  classification?: ExperienceClassification,
): ExperienceClassification {
  if (!classification) {
    return buildUnspecifiedExperienceClassification();
  }

  const reasons = classification.reasons.filter(Boolean);
  const explicitLevel = classification.explicitLevel;
  const inferredLevel = classification.inferredLevel;
  const isUnspecified = !explicitLevel && !inferredLevel;
  const diagnostics = normalizeExperienceDiagnostics(
    classification.diagnostics,
    explicitLevel ?? inferredLevel,
    reasons,
  );

  if (isUnspecified) {
    return buildUnspecifiedExperienceClassification(
      reasons,
      diagnostics.originalTitle,
      [],
    );
  }

  return {
    explicitLevel,
    inferredLevel,
    confidence:
      classification.confidence === "none" ? "low" : classification.confidence,
    source: classification.source,
    reasons,
    isUnspecified: false,
    diagnostics,
  };
}

export function resolveJobExperienceClassification(
  job: Pick<
    JobListing,
    "title" | "experienceLevel" | "experienceClassification" | "rawSourceMetadata"
  >,
) {
  const normalizedStoredClassification = normalizeExperienceClassification(
    job.experienceClassification,
  );
  if (!normalizedStoredClassification.isUnspecified) {
    return normalizedStoredClassification;
  }

  if (job.experienceLevel) {
    return {
      explicitLevel: job.experienceLevel,
      confidence: "high",
      source: "unknown",
      reasons: ["Legacy stored experience level."],
      isUnspecified: false,
      diagnostics: buildExperienceClassificationDiagnostics({
        title: job.title,
        finalSeniority: job.experienceLevel,
        rationale: ["Legacy stored experience level."],
      }),
    } satisfies ExperienceClassification;
  }

  return classifyExperience({
    title: job.title,
    rawSourceMetadata: job.rawSourceMetadata,
  });
}

export function resolveJobExperienceLevel(
  job: Pick<
    JobListing,
    "title" | "experienceLevel" | "experienceClassification" | "rawSourceMetadata"
  >,
) {
  return selectExperienceLevel(resolveJobExperienceClassification(job));
}

export function canonicalizeUrl(url: string) {
  const canonicalGreenhouseUrl = canonicalizeGreenhouseUrl(url);
  if (canonicalGreenhouseUrl) {
    return canonicalGreenhouseUrl;
  }

  try {
    const parsed = new URL(url);
    parsed.hash = "";
    parsed.search = "";
    parsed.pathname = parsed.pathname.replace(/\/+$/, "") || "/";
    return parsed.toString();
  } catch {
    return undefined;
  }
}

export function buildContentFingerprint(input: {
  company: string;
  title: string;
  location: string;
}) {
  return createHash("sha256")
    .update(
      [
        normalizeComparableText(input.company),
        normalizeComparableText(input.title),
        normalizeComparableText(input.location),
      ].join("|"),
    )
    .digest("hex");
}

export function buildSourceLookupKey(platform: ProviderPlatform, sourceJobId: string) {
  return `${platform}:${normalizeComparableText(sourceJobId)}`;
}

export function matchesFilters(
  job: ExperienceFilterableJob,
  filters: SearchFilters,
) {
  return evaluateSearchFilters(job, filters, { includeExperience: true }).matches;
}

export function matchesFiltersWithoutExperience(
  job: ExperienceFilterableJob,
  filters: SearchFilters,
) {
  return evaluateSearchFilters(job, filters, { includeExperience: false }).matches;
}

export function evaluateSearchFilters(
  job: ExperienceFilterableJob,
  filters: SearchFilters,
  options: { includeExperience: boolean; titleMatchMode?: TitleMatchMode },
) : FilterEvaluation {
  const resolvedLocation = resolveFilterableLocation(job);
  const titleMatch = getTitleMatchResult(job.title, filters.title, {
    mode: options.titleMatchMode,
  });

  if (!titleMatch.matches) {
    return {
      matches: false,
      reason: "title",
      titleMatch,
    };
  }

  const locationMatch = getLocationMatchResult(job, filters, resolvedLocation);
  if (locationMatch && !locationMatch.matches) {
    return {
      matches: false,
      reason: "location",
      locationMatch,
      experienceMatch: undefined,
    };
  }

  if (options.includeExperience && filters.experienceLevels?.length) {
    const experienceMatch = getExperienceMatchResult(job, filters);
    if (!experienceMatch.matches) {
      return {
        matches: false,
        reason: "experience",
        experienceMatch,
      };
    }

    return {
      matches: true,
      titleMatch,
      ...(locationMatch ? { locationMatch } : {}),
      experienceMatch,
    };
  }

  return {
    matches: true,
    titleMatch,
    ...(locationMatch ? { locationMatch } : {}),
  };
}

function getExperienceMatchResult(
  job: ExperienceFilterableJob,
  filters: SearchFilters,
): ExperienceFilterResult {
  const selectedLevels = filters.experienceLevels;
  if (!selectedLevels?.length) {
    return {
      matches: true,
      classification: resolveJobExperienceClassification(job),
      selectedLevels: [],
      mode: resolveExperienceMatchMode(filters),
      includeUnspecified: filters.includeUnspecifiedExperience === true,
      explanation: "No experience filter is active for this search.",
    };
  }

  const classification = resolveJobExperienceClassification(job);
  const mode = resolveExperienceMatchMode(filters);
  const includeUnspecified =
    filters.includeUnspecifiedExperience === true || mode === "broad";

  if (
    classification.explicitLevel &&
    selectedLevels.includes(classification.explicitLevel)
  ) {
    return {
      matches: true,
      classification,
      selectedLevels,
      mode,
      includeUnspecified,
      matchedLevel: classification.explicitLevel,
      explanation: `Matched explicit experience level "${classification.explicitLevel}".`,
    };
  }

  if (
    mode !== "strict" &&
    classification.inferredLevel &&
    selectedLevels.includes(classification.inferredLevel)
  ) {
    if (mode === "broad") {
      return {
        matches: true,
        classification,
        selectedLevels,
        mode,
        includeUnspecified,
        matchedLevel: classification.inferredLevel,
        explanation: `Matched inferred experience level "${classification.inferredLevel}" in broad mode.`,
      };
    }

    if (
      classification.confidence === "high" ||
      classification.confidence === "medium"
    ) {
      return {
        matches: true,
        classification,
        selectedLevels,
        mode,
        includeUnspecified,
        matchedLevel: classification.inferredLevel,
        explanation: `Matched inferred experience level "${classification.inferredLevel}" with ${classification.confidence} confidence.`,
      };
    }
  }

  if (includeUnspecified && classification.isUnspecified) {
    return {
      matches: true,
      classification,
      selectedLevels,
      mode,
      includeUnspecified,
      explanation: "Allowed an unspecified experience level for this search.",
    };
  }

  const resolvedLevel = classification.explicitLevel ?? classification.inferredLevel;
  const reasons = classification.reasons.filter(Boolean).join(" ");

  return {
    matches: false,
    classification,
    selectedLevels,
    mode,
    includeUnspecified,
    matchedLevel: resolvedLevel,
    explanation: classification.isUnspecified
      ? "Rejected because the role did not provide a usable experience level and unspecified levels are not allowed."
      : `Rejected experience level "${resolvedLevel}" for selected levels ${selectedLevels.join(", ")}.${reasons ? ` ${reasons}` : ""}`,
  };
}

function resolveExperienceMatchMode(filters: SearchFilters): ExperienceMatchMode {
  return filters.experienceMatchMode ?? "balanced";
}

export function normalizeTitleToCanonicalForm(value?: string) {
  return normalizeTitleRetrievalCanonicalForm(value);
}

export function buildDiscoveryRoleQueries(value?: string) {
  return buildTitleRetrievalDiscoveryRoleQueries(value ?? "");
}

export function getTitleMatchResult(
  jobTitle: string,
  queryTitle: string,
  options?: { mode?: TitleMatchMode },
): TitleMatchResult {
  return getTitleRetrievalMatchResult(jobTitle, queryTitle, options);
}

function matchesCountryFilter(
  job: Pick<JobListing, "country" | "state" | "city" | "locationText">,
  filterCountry: string,
  resolvedLocation = resolveFilterableLocation(job),
) {
  const wantedConcept = resolveCountryConcept(filterCountry);
  if (!wantedConcept) {
    return false;
  }

  if (wantedConcept === "united states") {
    return resolvedLocation.isUnitedStates;
  }

  const inferredCountryConcept =
    resolvedLocation.isUnitedStates
      ? "united states"
      : resolveCountryConcept(resolvedLocation.country ?? job.country);
  if (inferredCountryConcept === wantedConcept) {
    return true;
  }

  const haystack = normalizeComparableText(
    `${resolvedLocation.country ?? ""} ${job.country ?? ""} ${job.locationText}`,
  );
  if (!haystack) {
    return false;
  }

  const aliases = countryAliasesByConcept.get(wantedConcept) ?? [wantedConcept];
  return aliases.some((alias) => containsNormalizedTerm(haystack, alias));
}

function matchesStateFilter(
  job: Pick<JobListing, "state" | "locationText">,
  filterState: string,
  resolvedLocation = resolveFilterableLocation(job),
) {
  const wantedUsState = resolveUsState(filterState);
  if (wantedUsState) {
    return resolvedLocation.state === wantedUsState;
  }

  const wanted = normalizeComparableText(filterState);
  const haystack = normalizeComparableText(
    `${resolvedLocation.state ?? ""} ${job.state ?? ""} ${job.locationText}`,
  );
  return Boolean(wanted) && haystack.includes(wanted);
}

function resolveCountryConcept(value?: string) {
  const normalized = normalizeComparableText(value);
  if (!normalized) {
    return undefined;
  }

  return countryConceptByAlias.get(normalized) ?? normalized;
}

function containsNormalizedTerm(haystack: string, term: string) {
  return (
    haystack === term ||
    haystack.startsWith(`${term} `) ||
    haystack.endsWith(` ${term}`) ||
    haystack.includes(` ${term} `)
  );
}

function inferJobCountryConcept(
  job: Pick<JobListing, "country" | "state" | "city" | "locationText">,
) {
  const resolvedLocation = resolveFilterableLocation(job);
  const countryConcept = resolveCountryConcept(resolvedLocation.country ?? job.country);
  if (countryConcept) {
    return countryConcept;
  }

  if (resolvedLocation.isUnitedStates) {
    return "united states";
  }

  return inferCountryConceptFromLocationText(job.locationText);
}

function inferCountryConceptFromLocationText(locationText?: string) {
  const analyzedLocation = analyzeUsLocation(locationText);
  if (analyzedLocation.isUnitedStates) {
    return "united states";
  }

  const normalizedLocationText = normalizeComparableText(locationText);
  if (!normalizedLocationText) {
    return undefined;
  }

  const parsedLocation = parseLocationText(locationText);
  const parsedCountryConcept = resolveCountryConcept(parsedLocation.country);
  if (parsedCountryConcept) {
    return parsedCountryConcept;
  }

  if (resolveUsState(parsedLocation.state) || isRecognizedUsCity(parsedLocation.city)) {
    return "united states";
  }

  for (const part of splitLocationTextParts(locationText)) {
    if (resolveUsState(part)) {
      return "united states";
    }
  }

  if (isRecognizedUsCity(locationText)) {
    return "united states";
  }

  for (const [concept, aliases] of countryAliasesByConcept.entries()) {
    if (aliases.some((alias) => containsNormalizedTerm(normalizedLocationText, alias))) {
      return concept;
    }
  }

  return undefined;
}

function inferJobUsState(
  job: Pick<JobListing, "state" | "locationText">,
) {
  const resolvedLocation = resolveFilterableLocation(job);
  if (resolvedLocation.state) {
    return resolvedLocation.state;
  }

  const directState = resolveUsState(job.state);
  if (directState) {
    return directState;
  }

  const parsedState = resolveUsState(parseLocationText(job.locationText).state);
  if (parsedState) {
    return parsedState;
  }

  for (const part of splitLocationTextParts(job.locationText)) {
    const state = resolveUsState(part);
    if (state) {
      return state;
    }
  }

  return undefined;
}

function analyzeJobLocation(
  job: Pick<JobListing, "country" | "state" | "city" | "locationText">,
) {
  return resolveFilterableLocation(job);
}

function resolveFilterableLocation(
  job: Pick<JobListing, "country" | "state" | "city" | "locationText"> & {
    resolvedLocation?: ResolvedLocation;
    rawSourceMetadata?: Record<string, unknown>;
  },
) {
  return (
    job.resolvedLocation ??
    resolveJobLocation({
      country: job.country,
      state: job.state,
      city: job.city,
      locationText: job.locationText,
      rawSourceMetadata: job.rawSourceMetadata,
    })
  );
}

function splitLocationTextParts(locationText?: string) {
  return (locationText ?? "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function containsAllNormalizedTerms(haystack: string, needle: string) {
  const terms = needle.split(" ").filter(Boolean);
  return terms.length > 0 && terms.every((term) => containsNormalizedTerm(haystack, term));
}

function resolveExplicitExperienceClassification(input: {
  title?: string;
  explicitExperienceLevel?: ExperienceLevel;
  explicitExperienceSource?: ExperienceClassification["source"];
  explicitExperienceReasons?: string[];
}) {
  if (input.explicitExperienceLevel) {
    const reasons =
      input.explicitExperienceReasons?.filter(Boolean) ?? [
        "Provider supplied an explicit experience level.",
      ];
    return {
      explicitLevel: input.explicitExperienceLevel,
      confidence: "high",
      source: input.explicitExperienceSource ?? "structured_metadata",
      reasons,
      isUnspecified: false,
      diagnostics: buildExperienceClassificationDiagnostics({
        title: input.title,
        finalSeniority: input.explicitExperienceLevel,
        rationale: reasons,
      }),
    } satisfies ExperienceClassification;
  }

  const titleSignals = inferExperienceSignalsFromTitle(input.title);
  const titleSignal = selectBestExperienceSignal(titleSignals);
  return titleSignal
    ? {
        explicitLevel: titleSignal.level,
        confidence: titleSignal.confidence,
        source: titleSignal.source,
        reasons: buildClassificationRationale(titleSignal, titleSignals),
        isUnspecified: false,
        diagnostics: buildExperienceClassificationDiagnostics({
          title: input.title,
          matchedSignals: titleSignals,
          finalSeniority: titleSignal.level,
          rationale: buildClassificationRationale(titleSignal, titleSignals),
        }),
      }
    : undefined;
}

function classificationFromSignal(
  signal: ExperienceSignal,
  title?: string,
  allSignals: ExperienceSignal[] = [signal],
): ExperienceClassification {
  const rationale = buildClassificationRationale(signal, allSignals);
  return {
    inferredLevel: signal.level,
    confidence: signal.confidence,
    source: signal.source,
    reasons: rationale,
    isUnspecified: false,
    diagnostics: buildExperienceClassificationDiagnostics({
      title,
      matchedSignals: allSignals,
      finalSeniority: signal.level,
      rationale,
    }),
  };
}

function selectExperienceLevel(classification: ExperienceClassification) {
  return classification.explicitLevel ?? classification.inferredLevel;
}

function inferExperienceSignalsFromTitle(title?: string) {
  if (!title?.trim()) {
    return [];
  }

  const normalizedTitle = normalizeComparableText(title);
  if (!normalizedTitle) {
    return [];
  }

  return collectExperienceSignalsFromRules({
    text: title,
    normalizedText: normalizedTitle,
    rules: prioritizedTitleExperienceRules,
    source: "title",
    defaultSignalType: "title_keyword",
    confidence: "high",
  });
}

function inferExperienceSignalsFromHints(
  values: Array<string | undefined> | undefined,
  source: ExperienceClassification["source"],
) {
  const prompt = buildExperienceInferencePrompt(...(values ?? []));
  if (!prompt) {
    return [];
  }

  return inferExperienceSignalsFromText({
    text: prompt,
    rules: resolveExperienceRulesForSource(source),
    source,
    keywordConfidence: source === "structured_metadata" ? "high" : "medium",
    yearConfidence: source === "page_fetch" ? "low" : "medium",
  });
}

function inferExperienceSignalsFromText(input: {
  text: string;
  rules: ExperienceRule[];
  source: ExperienceClassification["source"];
  keywordConfidence: ExperienceInferenceConfidence;
  yearConfidence: ExperienceInferenceConfidence;
}) {
  const normalized = normalizeComparableText(input.text);
  if (!normalized) {
    return [];
  }

  const keywordSignals = collectExperienceSignalsFromRules({
    text: input.text,
    normalizedText: normalized,
    rules: input.rules,
    source: input.source,
    defaultSignalType:
      input.source === "structured_metadata"
        ? "structured_hint"
        : input.source === "description"
          ? "description_hint"
          : "metadata_hint",
    confidence: input.keywordConfidence,
  });

  const yearsLevel = inferExperienceLevelFromYears(input.text);
  if (!yearsLevel) {
    return keywordSignals;
  }

  return [
    ...keywordSignals,
    {
      ruleId: "years_of_experience",
      signalType: "years_of_experience",
      level: yearsLevel,
      confidence: input.yearConfidence,
      source: input.source,
      reason: buildExperienceYearsReason(input.source, yearsLevel, input.text),
      matchedText: summarizeExperienceText(input.text),
    } satisfies ExperienceSignal,
  ];
}

function inferExperienceSignalsFromMetadata(rawSourceMetadata?: Record<string, unknown>) {
  const hints = collectExperienceMetadataHints(rawSourceMetadata);
  if (hints.length === 0) {
    return [];
  }

  return inferExperienceSignalsFromHints(hints, "structured_metadata");
}

function collectExperienceSignals(input: {
  title?: string;
  structuredExperienceHints?: Array<string | undefined>;
  descriptionExperienceHints?: Array<string | undefined>;
  pageFetchExperienceHints?: Array<string | undefined>;
  rawSourceMetadata?: Record<string, unknown>;
}) {
  const titleSignals = inferExperienceSignalsFromTitle(input.title);
  const structuredSignals = inferExperienceSignalsFromHints(
    input.structuredExperienceHints,
    "structured_metadata",
  );
  const descriptionSignals = inferExperienceSignalsFromHints(
    input.descriptionExperienceHints,
    "description",
  );
  const metadataSignals = inferExperienceSignalsFromMetadata(input.rawSourceMetadata);
  const pageFetchSignals = inferExperienceSignalsFromHints(
    input.pageFetchExperienceHints,
    "page_fetch",
  );

  return {
    titleSignals,
    structuredSignals,
    descriptionSignals,
    metadataSignals,
    pageFetchSignals,
    allSignals: dedupeExperienceSignals([
      ...titleSignals,
      ...structuredSignals,
      ...descriptionSignals,
      ...metadataSignals,
      ...pageFetchSignals,
    ]),
  };
}

function resolveExperienceRulesForSource(source: ExperienceClassification["source"]) {
  if (source === "structured_metadata") {
    return prioritizedStructuredHintRules;
  }

  if (source === "description" || source === "page_fetch") {
    return prioritizedDescriptionHintRules;
  }

  return prioritizedMetadataHintRules;
}

function collectExperienceSignalsFromRules(input: {
  text: string;
  normalizedText: string;
  rules: ExperienceRule[];
  source: ExperienceClassification["source"];
  defaultSignalType:
    | "title_keyword"
    | "structured_hint"
    | "description_hint"
    | "metadata_hint";
  confidence: ExperienceInferenceConfidence;
}) {
  const signals: ExperienceSignal[] = [];

  for (const rule of input.rules) {
    const matchedPattern = rule.patterns.find((pattern) => pattern.test(input.normalizedText));
    if (!matchedPattern) {
      continue;
    }

    signals.push({
      ruleId: rule.id,
      signalType:
        rule.signalType === "title_keyword" && input.defaultSignalType !== "title_keyword"
          ? input.defaultSignalType
          : rule.signalType === "acronym" && input.defaultSignalType !== "title_keyword"
            ? input.defaultSignalType
            : rule.signalType,
      level: rule.level,
      confidence: input.confidence,
      source: input.source,
      reason: buildExperienceReason(input.source, rule.level, input.text, rule.rationale),
      matchedText: summarizeExperienceText(input.text),
    });
  }

  return dedupeExperienceSignals(signals);
}

function dedupeExperienceSignals(signals: ExperienceSignal[]) {
  const seen = new Set<string>();
  return signals.filter((signal) => {
    const key = `${signal.ruleId}:${signal.source}:${signal.level}:${signal.matchedText}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function selectBestExperienceSignal(signals: ExperienceSignal[]) {
  return [...signals].sort(compareExperienceSignals)[0];
}

function compareExperienceSignals(left: ExperienceSignal, right: ExperienceSignal) {
  const confidenceDelta =
    experienceConfidencePriority[right.confidence] -
    experienceConfidencePriority[left.confidence];
  if (confidenceDelta !== 0) {
    return confidenceDelta;
  }

  const levelDelta =
    experienceLevelPriority[right.level] - experienceLevelPriority[left.level];
  if (levelDelta !== 0) {
    return levelDelta;
  }

  return experienceSourcePriority[right.source] - experienceSourcePriority[left.source];
}

function buildClassificationRationale(
  selectedSignal: ExperienceSignal,
  allSignals: ExperienceSignal[],
) {
  const rationale = [selectedSignal.reason];
  const corroborating = allSignals
    .filter(
      (signal) =>
        signal !== selectedSignal &&
        signal.level === selectedSignal.level &&
        signal.reason !== selectedSignal.reason,
    )
    .slice(0, 2)
    .map((signal) => signal.reason);

  return Array.from(new Set([...rationale, ...corroborating]));
}

function buildExperienceClassificationDiagnostics(input: {
  title?: string;
  matchedSignals?: ExperienceSignal[];
  finalSeniority: ExperienceLevel | "unknown";
  rationale?: string[];
}): NonNullable<ExperienceClassification["diagnostics"]> {
  return {
    originalTitle: input.title?.trim() ?? "",
    normalizedTitle: normalizeComparableText(input.title),
    finalSeniority: input.finalSeniority,
    matchedSignals: (input.matchedSignals ?? []).map((signal) => ({
      ruleId: signal.ruleId,
      signalType: signal.signalType,
      source: signal.source,
      level: signal.level,
      confidence: signal.confidence,
      matchedText: signal.matchedText,
      rationale: signal.reason,
    })),
    rationale: input.rationale?.filter(Boolean) ?? [],
  };
}

function normalizeExperienceDiagnostics(
  diagnostics: ExperienceClassification["diagnostics"] | undefined,
  resolvedLevel: ExperienceLevel | undefined,
  fallbackRationale: string[],
): NonNullable<ExperienceClassification["diagnostics"]> {
  if (diagnostics) {
    return {
      ...diagnostics,
      finalSeniority: resolvedLevel ?? "unknown",
      rationale: diagnostics.rationale?.filter(Boolean) ?? fallbackRationale,
    };
  }

  return buildExperienceClassificationDiagnostics({
    finalSeniority: resolvedLevel ?? "unknown",
    rationale: fallbackRationale,
  });
}

function collectExperienceMetadataHints(
  value: unknown,
  results: string[] = [],
  seen = new Set<unknown>(),
) {
  if (!value || results.length >= 48) {
    return results;
  }

  if (typeof value === "string") {
    for (const hint of extractExperienceMetadataHints(value)) {
      if (results.length >= 48) {
        break;
      }

      results.push(hint);
    }
    return results;
  }

  if (typeof value !== "object" || seen.has(value)) {
    return results;
  }

  seen.add(value);

  if (Array.isArray(value)) {
    for (const entry of value) {
      collectExperienceMetadataHints(entry, results, seen);
      if (results.length >= 48) {
        break;
      }
    }
    return results;
  }

  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (experienceMetadataIgnoredKeys.has(key)) {
      continue;
    }

    collectExperienceMetadataHints(entry, results, seen);
    if (results.length >= 48) {
      break;
    }
  }

  return results;
}

function looksLikeExperienceMetadataHint(value: string) {
  const normalized = normalizeComparableText(value);
  if (!normalized) {
    return false;
  }

  return (
    experienceMetadataHintPattern.test(normalized) ||
    /\b\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe)\b/i.test(value)
  );
}

function extractExperienceMetadataHints(value: string) {
  return extractExperiencePromptSegments(value).slice(0, 6);
}

function looksLikeNegativeExperienceMetadataHint(value: string) {
  const normalized = normalizeComparableText(value);
  return Boolean(normalized) && negativeExperienceMetadataHintPatterns.some((pattern) => pattern.test(normalized));
}

function extractExperiencePromptSegments(value?: string) {
  const cleaned = stripExperiencePromptMarkup(value);
  if (!cleaned) {
    return [];
  }

  if (
    cleaned.length <= 220 &&
    looksLikeExperienceMetadataHint(cleaned) &&
    !looksLikeNegativeExperienceMetadataHint(cleaned)
  ) {
    return [cleaned];
  }

  return cleaned
    .split(/(?<=[.!?])\s+|\n+|(?<=[;|])\s+/)
    .map((segment) => segment.trim())
    .filter(Boolean)
    .filter(
      (segment) =>
        looksLikeExperiencePromptSegment(segment) &&
        !looksLikeNegativeExperienceMetadataHint(segment),
    )
    .slice(0, 8);
}

function stripExperiencePromptMarkup(value?: string) {
  return (value ?? "")
    .replace(/<(?:br|\/p|\/li|\/div|\/h\d|\/tr)>/gi, "\n")
    .replace(/<li[^>]*>/gi, "\n")
    .replace(/<\/?(?:p|div|ul|ol|h\d|table|tbody|thead|tr|td|th)[^>]*>/gi, " ")
    .replace(/&lt;\/?[^&]+&gt;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&(nbsp|amp|quot|apos|#39|#x27);/gi, " ")
    .replace(/\u2022/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeExperiencePromptSegment(value: string) {
  const normalized = normalizeComparableText(value);
  if (!normalized) {
    return false;
  }

  return (
    experiencePromptYearPattern.test(value) ||
    experiencePromptSignalPattern.test(normalized) ||
    looksLikeExperienceMetadataHint(value)
  );
}

function buildExperienceReason(
  source: ExperienceClassification["source"],
  level: ExperienceLevel,
  text: string,
  rationale?: string,
) {
  return `${rationale ?? `Detected ${level.replace("_", " ")} markers`} in ${source.replace("_", " ")}: "${summarizeExperienceText(text)}".`;
}

function buildExperienceYearsReason(
  source: ExperienceClassification["source"],
  level: ExperienceLevel,
  text: string,
) {
  return `Mapped years-of-experience guidance in ${source.replace("_", " ")} to ${level.replace("_", " ")}: "${summarizeExperienceText(text)}".`;
}

function summarizeExperienceText(value: string) {
  const cleaned = stripExperiencePromptMarkup(value);
  if (cleaned.length <= 140) {
    return cleaned;
  }

  return `${cleaned.slice(0, 137).trimEnd()}...`;
}

function inferExperienceLevelFromYears(text: string) {
  const signals = extractExperienceYearSignals(text);
  if (signals.length === 0) {
    return undefined;
  }

  const representativeYears = Math.max(
    ...signals.map((signal) =>
      signal.max === undefined ? signal.min : (signal.min + signal.max) / 2,
    ),
  );

  if (representativeYears >= 12) {
    return "principal";
  }

  if (representativeYears >= 8) {
    return "staff";
  }

  if (representativeYears >= 5) {
    return "senior";
  }

  if (representativeYears >= 2) {
    return "mid";
  }

  return "junior";
}

function extractExperienceYearSignals(text: string) {
  const signals: Array<{ min: number; max?: number }> = [];
  let remaining = text.toLowerCase();

  const rangePattern =
    /\b(\d+(?:\.\d+)?)\s*(?:\+)?\s*(?:-|to|–|—)\s*(\d+(?:\.\d+)?)\s*(?:\+)?\s*(?:years?|yrs?|yoe)\b/g;

  for (const match of remaining.matchAll(rangePattern)) {
    const min = Number(match[1]);
    const max = Number(match[2]);
    if (!Number.isNaN(min) && !Number.isNaN(max)) {
      signals.push({ min, max });
    }
  }

  remaining = remaining.replace(rangePattern, " ");

  const minimumPattern =
    /\b(?:at least|minimum of|minimum|more than|over)?\s*(\d+(?:\.\d+)?)\s*(?:\+|plus)?\s*(?:years?|yrs?|yoe)\b/g;

  for (const match of remaining.matchAll(minimumPattern)) {
    const min = Number(match[1]);
    if (!Number.isNaN(min)) {
      signals.push({ min });
    }
  }

  return signals;
}

function sortExperienceMatchersByPriority<T extends { level: ExperienceLevel }>(matchers: T[]) {
  return [...matchers].sort(
    (left, right) =>
      experienceLevelPriority[right.level] - experienceLevelPriority[left.level],
  );
}

export function isValidationStale(lastValidatedAt: string | undefined, ttlMinutes: number, now = new Date()) {
  if (!lastValidatedAt) {
    return true;
  }

  const parsed = new Date(lastValidatedAt);
  if (Number.isNaN(parsed.getTime())) {
    return true;
  }

  return now.getTime() - parsed.getTime() > ttlMinutes * 60_000;
}

export async function runWithConcurrency<T, TResult>(
  items: readonly T[],
  worker: (item: T) => Promise<TResult>,
  concurrency = 4,
) {
  const results: TResult[] = [];
  let currentIndex = 0;

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (currentIndex < items.length) {
        const index = currentIndex;
        currentIndex += 1;
        results[index] = await worker(items[index]);
      }
    }),
  );

  return results;
}
