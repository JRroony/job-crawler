import "server-only";

import { createHash, randomUUID } from "node:crypto";

import {
  analyzeUsLocation,
  isRecognizedUsCity,
  resolveUsState,
} from "@/lib/server/locations/us";
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
  SearchFilters,
} from "@/lib/types";

type CountryAliasGroup = {
  concept: string;
  aliases: string[];
};

export type FilterExclusionReason = "title" | "location" | "experience";

export type FilterEvaluation =
  | { matches: true; titleMatch: TitleMatchResult }
  | { matches: false; reason: FilterExclusionReason; titleMatch?: TitleMatchResult };

type ExperienceFilterableJob = Pick<
  JobListing,
  | "title"
  | "company"
  | "country"
  | "state"
  | "city"
  | "locationText"
  | "experienceLevel"
  | "experienceClassification"
  | "rawSourceMetadata"
>;

type ExperienceSignal = {
  level: ExperienceLevel;
  confidence: ExperienceInferenceConfidence;
  source: ExperienceClassification["source"];
  reason: string;
};

const titleExperienceMatchers: Array<{ level: ExperienceLevel; patterns: RegExp[] }> = [
  {
    level: "intern",
    patterns: [
      /\bintern\b/,
      /\binternship\b/,
      /\bco op\b/,
      /\bworking student\b/,
    ],
  },
  {
    level: "new_grad",
    patterns: [
      /\bnew grad\b/,
      /\bnew graduate\b/,
      /\brecent grad\b/,
      /\brecent graduate\b/,
      /\bentry level\b/,
      /\bearly career\b/,
    ],
  },
  {
    level: "staff",
    patterns: [
      /\bstaff\b/,
      /\bprincipal\b/,
      /\bdistinguished\b/,
      /\bfellow\b/,
      /\bmember of technical staff\b/,
      /\bmts\b/,
      /\blevel 4\b/,
      /\blevel 5\b/,
      /\biv\b/,
      /\bv\b/,
    ],
  },
  {
    level: "senior",
    patterns: [
      /\bsenior\b/,
      /\bsr\b/,
      /\blead\b/,
      /\barchitect\b/,
      /\bmanager\b/,
      /\bdirector\b/,
      /\blevel 3\b/,
      /\biii\b/,
    ],
  },
  {
    level: "junior",
    patterns: [/\bjunior\b/, /\bjr\b/, /\bassociate\b/],
  },
  {
    level: "mid",
    patterns: [/\bmid\b/, /\bmid level\b/, /\blevel 2\b/, /\bii\b/, /\bexperienced\b/],
  },
];

const experienceMatchers: Array<{ level: ExperienceLevel; patterns: RegExp[] }> = [
  {
    level: "intern",
    patterns: [
      /\bintern\b/,
      /\binternship\b/,
      /\bco op\b/,
      /\bcooperative education\b/,
      /\bapprentice(ship)?\b/,
      /\bworking student\b/,
      /\bstudent program\b/,
      /\bstudent opportunity\b/,
      /\bstudent role\b/,
      /\bstudent position\b/,
      /\bfor students\b/,
    ],
  },
  {
    level: "new_grad",
    patterns: [
      /\bnew grad\b/,
      /\bnew graduate\b/,
      /\brecent grad\b/,
      /\brecent graduate\b/,
      /\bgraduate\b/,
      /\bentry level\b/,
      /\bearly career\b/,
    ],
  },
  {
    level: "staff",
    patterns: [
      /\bstaff\b/,
      /\bprincipal\b/,
      /\bdistinguished\b/,
      /\bfellow\b/,
      /\bmember of technical staff\b/,
      /\bmts\b/,
      /\blevel 4\b/,
      /\blevel 5\b/,
      /\biv\b/,
      /\bv\b/,
    ],
  },
  {
    level: "senior",
    patterns: [
      /\bsenior\b/,
      /\bsr\b/,
      /\blead\b/,
      /\barchitect\b/,
      /\bmanager\b/,
      /\bdirector\b/,
      /\blevel 3\b/,
      /\biii\b/,
    ],
  },
  {
    level: "junior",
    patterns: [/\bjunior\b/, /\bassociate\b/, /\bentry associate\b/],
  },
  {
    level: "mid",
    patterns: [/\bmid\b/, /\bii\b/, /\blevel 2\b/, /\bexperienced\b/],
  },
];

const metadataExperienceMatchers: Array<{ level: ExperienceLevel; patterns: RegExp[] }> = [
  {
    level: "intern",
    patterns: [
      /\bintern\b/,
      /\binternship\b/,
      /\bco op\b/,
      /\bcooperative education\b/,
      /\bapprentice(ship)?\b/,
      /\bworking student\b/,
      /\bstudent program\b/,
      /\bstudent opportunity\b/,
      /\bstudent role\b/,
      /\bstudent position\b/,
      /\bfor students\b/,
    ],
  },
  {
    level: "new_grad",
    patterns: [
      /\bnew grad\b/,
      /\bnew graduate\b/,
      /\brecent grad\b/,
      /\brecent graduate\b/,
      /\bentry level\b/,
      /\bearly career\b/,
    ],
  },
  {
    level: "staff",
    patterns: [
      /\bstaff\b/,
      /\bprincipal\b/,
      /\bdistinguished\b/,
      /\bfellow\b/,
      /\bmember of technical staff\b/,
      /\bmts\b/,
      /\blevel 4\b/,
      /\blevel 5\b/,
      /\biv\b/,
      /\bv\b/,
    ],
  },
  {
    level: "senior",
    patterns: [
      /\bsenior\b/,
      /\bsr\b/,
      /\blead\b/,
      /\barchitect\b/,
      /\bmanager\b/,
      /\bdirector\b/,
      /\blevel 3\b/,
      /\biii\b/,
    ],
  },
  {
    level: "junior",
    patterns: [/\bjunior\b/, /\bassociate\b/],
  },
  {
    level: "mid",
    patterns: [/\bmid\b/, /\bmid level\b/, /\blevel 2\b/, /\bexperienced\b/],
  },
];

const experienceLevelPriority: Record<ExperienceLevel, number> = {
  mid: 1,
  junior: 2,
  senior: 3,
  staff: 4,
  new_grad: 5,
  intern: 6,
};

const prioritizedTitleExperienceMatchers = sortExperienceMatchersByPriority(
  titleExperienceMatchers,
);
const prioritizedExperienceMatchers = sortExperienceMatchersByPriority(experienceMatchers);
const prioritizedMetadataExperienceMatchers = sortExperienceMatchersByPriority(
  metadataExperienceMatchers,
);

const experienceMetadataHintPattern =
  /\b(intern|internship|co op|cooperative education|apprentice|working student|student program|student opportunity|student role|student position|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|fellow|member of technical staff|mts|lead|architect|manager|director|level [2-5]|ii|iii|iv|v|experienced|years|year|yrs|yoe|experience)\b/;

const negativeExperienceMetadataHintPatterns = [
  /\bif you are an? (?:intern|new grad|new graduate|staff|junior|senior|associate|working student|apprentice)\b/,
  /\bplease do not apply\b/,
  /\bdo not apply using this link\b/,
  /\bdoes not include internships?\b/,
  /\bnot for interns?\b/,
  /\bexcluding internships?\b/,
];

const experiencePromptSignalPattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|campus|university recruiting|new grads?|new graduates?|recent grads?|recent graduates?|graduate program|entry(?: |-)?level|early career|early talent|junior|jr|associate|mid(?: |-)?level|senior|sr|staff|principal|distinguished|fellow|member of technical staff|mts|lead|architect|manager|director|experienced|career level|seniority|level [2-5]|ii|iii|iv|v|requirements?|qualifications?|minimum qualifications?|preferred qualifications?|years?|yrs?|yoe)\b/i;

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
  const analyzedUsLocation = analyzeUsLocation(cleaned);
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
      city: analyzedUsLocation.isRemote ? undefined : analyzedUsLocation.city,
      state: analyzedUsLocation.stateName,
      country: analyzedUsLocation.isUnitedStates ? "United States" : undefined,
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
        analyzedUsLocation.isRemote || isRemote
          ? undefined
          : analyzedUsLocation.city ?? parts[0],
      state: analyzedUsLocation.stateName ?? usState,
      country:
        analyzedUsLocation.isUnitedStates || usState || countryConcept === "united states"
          ? "United States"
          : parts[1],
      locationText: cleaned,
    };
  }

  return {
    city: analyzedUsLocation.isRemote ? undefined : analyzedUsLocation.city ?? parts[0],
    state: analyzedUsLocation.stateName ?? resolveUsState(parts[1]) ?? parts[1],
    country: analyzedUsLocation.isUnitedStates ? "United States" : parts[2],
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
): ExperienceClassification {
  return {
    confidence: "none",
    source: "unknown",
    reasons,
    isUnspecified: true,
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
  const explicitExperience = resolveExplicitExperienceClassification(input);
  if (explicitExperience) {
    return explicitExperience;
  }

  const structuredSignal = inferExperienceSignalFromHints(
    input.structuredExperienceHints,
    "structured_metadata",
  );
  if (structuredSignal) {
    return classificationFromSignal(structuredSignal);
  }

  const descriptionSignal = inferExperienceSignalFromHints(
    input.descriptionExperienceHints,
    "description",
  );
  if (descriptionSignal) {
    return classificationFromSignal(descriptionSignal);
  }

  const metadataSignal = inferExperienceSignalFromMetadata(input.rawSourceMetadata);
  if (metadataSignal) {
    return classificationFromSignal(metadataSignal);
  }

  const pageFetchSignal = inferExperienceSignalFromHints(
    input.pageFetchExperienceHints,
    "page_fetch",
  );
  if (pageFetchSignal) {
    return classificationFromSignal(pageFetchSignal);
  }

  return buildUnspecifiedExperienceClassification();
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

  if (isUnspecified) {
    return buildUnspecifiedExperienceClassification(reasons);
  }

  return {
    explicitLevel,
    inferredLevel,
    confidence:
      classification.confidence === "none" ? "low" : classification.confidence,
    source: classification.source,
    reasons,
    isUnspecified: false,
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

  if (filters.country) {
    if (!matchesCountryFilter(job, filters.country)) {
      return {
        matches: false,
        reason: "location",
      };
    }
  }

  if (filters.state) {
    if (!matchesStateFilter(job, filters.state)) {
      return {
        matches: false,
        reason: "location",
      };
    }
  }

  if (filters.city) {
    const wanted = normalizeComparableText(filters.city);
    const haystack = normalizeComparableText(`${job.city ?? ""} ${job.locationText}`);
    if (!haystack.includes(wanted)) {
      return {
        matches: false,
        reason: "location",
      };
    }
  }

  if (options.includeExperience && filters.experienceLevels?.length) {
    if (!matchesExperienceFilters(job, filters)) {
      return {
        matches: false,
        reason: "experience",
      };
    }
  }

  return {
    matches: true,
    titleMatch,
  };
}

function matchesExperienceFilters(job: ExperienceFilterableJob, filters: SearchFilters) {
  const selectedLevels = filters.experienceLevels;
  if (!selectedLevels?.length) {
    return true;
  }

  const classification = resolveJobExperienceClassification(job);
  const mode = resolveExperienceMatchMode(filters);
  const includeUnspecified =
    filters.includeUnspecifiedExperience === true || mode === "broad";

  if (
    classification.explicitLevel &&
    selectedLevels.includes(classification.explicitLevel)
  ) {
    return true;
  }

  if (
    mode !== "strict" &&
    classification.inferredLevel &&
    selectedLevels.includes(classification.inferredLevel)
  ) {
    if (mode === "broad") {
      return true;
    }

    if (
      classification.confidence === "high" ||
      classification.confidence === "medium"
    ) {
      return true;
    }
  }

  return includeUnspecified && classification.isUnspecified;
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
) {
  const wantedConcept = resolveCountryConcept(filterCountry);
  if (!wantedConcept) {
    return false;
  }

  if (wantedConcept === "united states") {
    return inferJobCountryConcept(job) === wantedConcept;
  }

  const inferredCountryConcept = inferJobCountryConcept(job);
  if (inferredCountryConcept === wantedConcept) {
    return true;
  }

  const haystack = normalizeComparableText(`${job.country ?? ""} ${job.locationText}`);
  if (!haystack) {
    return false;
  }

  const aliases = countryAliasesByConcept.get(wantedConcept) ?? [wantedConcept];
  return aliases.some((alias) => containsNormalizedTerm(haystack, alias));
}

function matchesStateFilter(
  job: Pick<JobListing, "state" | "locationText">,
  filterState: string,
) {
  const wantedUsState = resolveUsState(filterState);
  if (wantedUsState) {
    return inferJobUsState(job) === wantedUsState;
  }

  const wanted = normalizeComparableText(filterState);
  const haystack = normalizeComparableText(`${job.state ?? ""} ${job.locationText}`);
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
  const countryConcept = resolveCountryConcept(job.country);
  if (countryConcept) {
    return countryConcept;
  }

  const analyzedLocation = analyzeJobLocation(job);
  if (analyzedLocation.isUnitedStates) {
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
  const analyzedLocation = analyzeUsLocation(job.locationText);
  if (analyzedLocation.stateName) {
    return analyzedLocation.stateName;
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
  const analyzedLocation = analyzeUsLocation(
    [job.city, job.state, job.country].filter(Boolean).join(", "),
  );

  if (analyzedLocation.isUnitedStates) {
    return analyzedLocation;
  }

  return analyzeUsLocation(job.locationText);
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
    return {
      explicitLevel: input.explicitExperienceLevel,
      confidence: "high",
      source: input.explicitExperienceSource ?? "structured_metadata",
      reasons:
        input.explicitExperienceReasons?.filter(Boolean) ?? [
          "Provider supplied an explicit experience level.",
        ],
      isUnspecified: false,
    } satisfies ExperienceClassification;
  }

  const titleSignal = inferExperienceSignalFromTitle(input.title);
  return titleSignal
    ? {
        explicitLevel: titleSignal.level,
        confidence: titleSignal.confidence,
        source: titleSignal.source,
        reasons: [titleSignal.reason],
        isUnspecified: false,
      }
    : undefined;
}

function classificationFromSignal(signal: ExperienceSignal): ExperienceClassification {
  return {
    inferredLevel: signal.level,
    confidence: signal.confidence,
    source: signal.source,
    reasons: [signal.reason],
    isUnspecified: false,
  };
}

function selectExperienceLevel(classification: ExperienceClassification) {
  return classification.explicitLevel ?? classification.inferredLevel;
}

function inferExperienceSignalFromTitle(title?: string) {
  if (!title?.trim()) {
    return undefined;
  }

  const normalizedTitle = normalizeComparableText(title);
  if (!normalizedTitle) {
    return undefined;
  }

  for (const matcher of prioritizedTitleExperienceMatchers) {
    if (matcher.patterns.some((pattern) => pattern.test(normalizedTitle))) {
      return {
        level: matcher.level,
        confidence: "high",
        source: "title",
        reason: buildExperienceReason("title", matcher.level, title),
      } satisfies ExperienceSignal;
    }
  }

  return undefined;
}

function inferExperienceSignalFromHints(
  values: Array<string | undefined> | undefined,
  source: ExperienceClassification["source"],
) {
  const prompt = buildExperienceInferencePrompt(...(values ?? []));
  if (!prompt) {
    return undefined;
  }

  return inferExperienceSignalFromText({
    text: prompt,
    matchers:
      source === "structured_metadata"
        ? prioritizedMetadataExperienceMatchers
        : prioritizedExperienceMatchers,
    source,
    keywordConfidence: source === "structured_metadata" ? "high" : "medium",
    yearConfidence: source === "page_fetch" ? "low" : "medium",
  });
}

function inferExperienceSignalFromText(input: {
  text: string;
  matchers: Array<{ level: ExperienceLevel; patterns: RegExp[] }>;
  source: ExperienceClassification["source"];
  keywordConfidence: ExperienceInferenceConfidence;
  yearConfidence: ExperienceInferenceConfidence;
}) {
  const normalized = normalizeComparableText(input.text);
  if (!normalized) {
    return undefined;
  }

  for (const matcher of input.matchers) {
    if (matcher.patterns.some((pattern) => pattern.test(normalized))) {
      return {
        level: matcher.level,
        confidence: input.keywordConfidence,
        source: input.source,
        reason: buildExperienceReason(input.source, matcher.level, input.text),
      } satisfies ExperienceSignal;
    }
  }

  const yearsLevel = inferExperienceLevelFromYears(input.text);
  if (!yearsLevel) {
    return undefined;
  }

  return {
    level: yearsLevel,
    confidence: input.yearConfidence,
    source: input.source,
    reason: buildExperienceYearsReason(input.source, yearsLevel, input.text),
  } satisfies ExperienceSignal;
}

function inferExperienceSignalFromMetadata(rawSourceMetadata?: Record<string, unknown>) {
  const hints = collectExperienceMetadataHints(rawSourceMetadata);
  if (hints.length === 0) {
    return undefined;
  }

  return inferExperienceSignalFromHints(hints, "structured_metadata");
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

  for (const entry of Object.values(value as Record<string, unknown>)) {
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
) {
  return `Detected ${level.replace("_", " ")} markers in ${source.replace("_", " ")}: "${summarizeExperienceText(text)}".`;
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

  if (representativeYears >= 10) {
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
