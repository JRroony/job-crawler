import "server-only";

import { createHash, randomUUID } from "node:crypto";

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

type TitleMatchTier =
  | "exact"
  | "variant"
  | "synonym"
  | "abbreviation"
  | "related"
  | "generic"
  | "none";

type TitleAliasKind = "canonical" | "synonym" | "abbreviation";

type TitleRoleFamily =
  | "software"
  | "data"
  | "qa"
  | "support"
  | "sales"
  | "design";

type TitleRoleConcept =
  | "software_engineer"
  | "backend_engineer"
  | "frontend_engineer"
  | "full_stack_engineer"
  | "platform_engineer"
  | "data_engineer"
  | "qa_engineer"
  | "software_engineer_in_test"
  | "support_engineer"
  | "sales_engineer"
  | "product_designer"
  | "data_scientist";

type TitleRoleDefinition = {
  concept: TitleRoleConcept;
  canonical: string;
  family: TitleRoleFamily;
  synonyms?: string[];
  abbreviations?: string[];
  relevantToBroadSoftwareQueries?: boolean;
};

type AnalyzedTitle = {
  normalized: string;
  baseNormalized: string;
  canonical: string;
  concept?: TitleRoleConcept;
  family?: TitleRoleFamily;
  aliasKind?: TitleAliasKind;
  matchedPhrase?: string;
  relevantToBroadSoftwareQueries: boolean;
  remainderNormalized: string;
};

export type TitleMatchResult = {
  matches: boolean;
  tier: TitleMatchTier;
  score: number;
  canonicalQueryTitle: string;
  canonicalJobTitle: string;
};

export type FilterExclusionReason = "title" | "location" | "experience";

export type FilterEvaluation =
  | { matches: true }
  | { matches: false; reason: FilterExclusionReason };

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
    level: "junior",
    patterns: [/\bjunior\b/, /\bassociate\b/, /\bentry associate\b/],
  },
  {
    level: "staff",
    patterns: [/\bstaff\b/, /\bprincipal\b/, /\bdistinguished\b/],
  },
  {
    level: "senior",
    patterns: [/\bsenior\b/, /\bsr\b/, /\blead\b/, /\bmanager\b/],
  },
  {
    level: "mid",
    patterns: [/\bmid\b/, /\bii\b/, /\biii\b/, /\blevel 2\b/, /\blevel 3\b/, /\bexperienced\b/],
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
    level: "junior",
    patterns: [/\bjunior\b/, /\bassociate\b/],
  },
  {
    level: "staff",
    patterns: [/\bstaff\b/, /\bprincipal\b/, /\bdistinguished\b/],
  },
  {
    level: "senior",
    patterns: [/\bsenior\b/, /\bsr\b/],
  },
  {
    level: "mid",
    patterns: [/\bmid\b/, /\bmid level\b/, /\blevel 2\b/, /\blevel 3\b/, /\bexperienced\b/],
  },
];

const experienceMetadataHintPattern =
  /\b(intern|internship|co op|cooperative education|apprentice|working student|student program|student opportunity|student role|student position|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|level 2|level 3|experienced|years|year|yrs|yoe|experience)\b/;

const negativeExperienceMetadataHintPatterns = [
  /\bif you are an? (?:intern|new grad|new graduate|staff|junior|senior|associate|working student|apprentice)\b/,
  /\bplease do not apply\b/,
  /\bdo not apply using this link\b/,
  /\bdoes not include internships?\b/,
  /\bnot for interns?\b/,
  /\bexcluding internships?\b/,
];

const experiencePromptSignalPattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|campus|university recruiting|new grads?|new graduates?|recent grads?|recent graduates?|graduate program|entry(?: |-)?level|early career|early talent|junior|jr|associate|mid(?: |-)?level|senior|sr|staff|principal|distinguished|lead|manager|experienced|career level|seniority|level [2-5]|ii|iii|iv|v|requirements?|qualifications?|minimum qualifications?|preferred qualifications?|years?|yrs?|yoe)\b/i;

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

const usStatePairs = [
  ["AL", "Alabama"],
  ["AK", "Alaska"],
  ["AZ", "Arizona"],
  ["AR", "Arkansas"],
  ["CA", "California"],
  ["CO", "Colorado"],
  ["CT", "Connecticut"],
  ["DE", "Delaware"],
  ["FL", "Florida"],
  ["GA", "Georgia"],
  ["HI", "Hawaii"],
  ["ID", "Idaho"],
  ["IL", "Illinois"],
  ["IN", "Indiana"],
  ["IA", "Iowa"],
  ["KS", "Kansas"],
  ["KY", "Kentucky"],
  ["LA", "Louisiana"],
  ["ME", "Maine"],
  ["MD", "Maryland"],
  ["MA", "Massachusetts"],
  ["MI", "Michigan"],
  ["MN", "Minnesota"],
  ["MS", "Mississippi"],
  ["MO", "Missouri"],
  ["MT", "Montana"],
  ["NE", "Nebraska"],
  ["NV", "Nevada"],
  ["NH", "New Hampshire"],
  ["NJ", "New Jersey"],
  ["NM", "New Mexico"],
  ["NY", "New York"],
  ["NC", "North Carolina"],
  ["ND", "North Dakota"],
  ["OH", "Ohio"],
  ["OK", "Oklahoma"],
  ["OR", "Oregon"],
  ["PA", "Pennsylvania"],
  ["RI", "Rhode Island"],
  ["SC", "South Carolina"],
  ["SD", "South Dakota"],
  ["TN", "Tennessee"],
  ["TX", "Texas"],
  ["UT", "Utah"],
  ["VT", "Vermont"],
  ["VA", "Virginia"],
  ["WA", "Washington"],
  ["WV", "West Virginia"],
  ["WI", "Wisconsin"],
  ["WY", "Wyoming"],
  ["DC", "District of Columbia"],
] as const;

const usStateByAlias = new Map(
  usStatePairs.flatMap(([abbreviation, name]) => [
    [normalizeComparableText(abbreviation), name],
    [normalizeComparableText(name), name],
  ] as const),
);

const knownUsCityAliases = new Set(
  [
    "Atlanta",
    "Austin",
    "Bellevue",
    "Boston",
    "Chicago",
    "Dallas",
    "Denver",
    "Houston",
    "Irvine",
    "Jersey City",
    "Las Vegas",
    "Los Angeles",
    "Miami",
    "Mountain View",
    "Nashville",
    "New York",
    "Oakland",
    "Philadelphia",
    "Phoenix",
    "Pittsburgh",
    "Portland",
    "Raleigh",
    "Redwood City",
    "Salt Lake City",
    "San Diego",
    "San Francisco",
    "San Jose",
    "Santa Clara",
    "Seattle",
    "Sunnyvale",
    "Washington",
    "Washington DC",
  ].map((city) => normalizeComparableText(city)),
);

const seniorityPhrases = ["new grad", "entry level"];

const seniorityTokens = new Set([
  "associate",
  "distinguished",
  "graduate",
  "jr",
  "junior",
  "lead",
  "mid",
  "principal",
  "senior",
  "sr",
  "staff",
  "i",
  "ii",
  "iii",
  "iv",
  "v",
]);

const broadSoftwarePositivePhrases = [
  "software",
  "developer",
  "full stack",
  "fullstack",
  "front end",
  "frontend",
  "back end",
  "backend",
  "platform",
  "mobile",
  "ios",
  "android",
  "web",
  "application",
  "app",
  "devops",
  "site reliability",
  "sre",
  "infrastructure",
] as const;

const broadSoftwareNegativePhrases = [
  "sales",
  "support",
  "customer",
  "designer",
  "design",
  "quality",
  "qa",
  "test",
  "data",
  "scientist",
  "recruit",
  "marketing",
  "finance",
  "account",
  "legal",
] as const;

const titleRoleDefinitions: TitleRoleDefinition[] = [
  {
    concept: "software_engineer_in_test",
    canonical: "software engineer in test",
    family: "qa",
    synonyms: ["software engineer in test", "software developer in test"],
    abbreviations: ["sdet"],
  },
  {
    concept: "qa_engineer",
    canonical: "qa engineer",
    family: "qa",
    synonyms: ["quality assurance engineer", "quality engineering", "test engineer"],
  },
  {
    concept: "data_engineer",
    canonical: "data engineer",
    family: "data",
    synonyms: ["data engineering", "data platform engineer"],
  },
  {
    concept: "support_engineer",
    canonical: "support engineer",
    family: "support",
  },
  {
    concept: "sales_engineer",
    canonical: "sales engineer",
    family: "sales",
  },
  {
    concept: "product_designer",
    canonical: "product designer",
    family: "design",
    synonyms: ["ux designer"],
  },
  {
    concept: "data_scientist",
    canonical: "data scientist",
    family: "data",
  },
  {
    concept: "software_engineer",
    canonical: "software engineer",
    family: "software",
    synonyms: ["software developer", "software development engineer", "software engineering"],
    abbreviations: ["swe", "sde"],
  },
  {
    concept: "backend_engineer",
    canonical: "backend engineer",
    family: "software",
    synonyms: [
      "back end engineer",
      "backend developer",
      "back end developer",
      "backend engineering",
      "back end engineering",
    ],
    relevantToBroadSoftwareQueries: true,
  },
  {
    concept: "frontend_engineer",
    canonical: "frontend engineer",
    family: "software",
    synonyms: [
      "front end engineer",
      "frontend developer",
      "front end developer",
      "frontend engineering",
      "front end engineering",
    ],
    relevantToBroadSoftwareQueries: true,
  },
  {
    concept: "full_stack_engineer",
    canonical: "full stack engineer",
    family: "software",
    synonyms: [
      "fullstack engineer",
      "full stack developer",
      "fullstack developer",
      "full stack engineering",
      "fullstack engineering",
    ],
    relevantToBroadSoftwareQueries: true,
  },
  {
    concept: "platform_engineer",
    canonical: "platform engineer",
    family: "software",
    synonyms: ["platform developer", "platform engineering"],
    relevantToBroadSoftwareQueries: true,
  },
];

const titleRoleAliases = titleRoleDefinitions
  .flatMap((definition) => [
    {
      concept: definition.concept,
      canonical: definition.canonical,
      family: definition.family,
      kind: "canonical" as const,
      phrase: normalizeComparableText(definition.canonical),
      relevantToBroadSoftwareQueries: definition.relevantToBroadSoftwareQueries ?? false,
    },
    ...(definition.synonyms ?? []).map((phrase) => ({
      concept: definition.concept,
      canonical: definition.canonical,
      family: definition.family,
      kind: "synonym" as const,
      phrase: normalizeComparableText(phrase),
      relevantToBroadSoftwareQueries: definition.relevantToBroadSoftwareQueries ?? false,
    })),
    ...(definition.abbreviations ?? []).map((phrase) => ({
      concept: definition.concept,
      canonical: definition.canonical,
      family: definition.family,
      kind: "abbreviation" as const,
      phrase: normalizeComparableText(phrase),
      relevantToBroadSoftwareQueries: definition.relevantToBroadSoftwareQueries ?? false,
    })),
  ])
  .sort(
    (left, right) =>
      right.phrase.split(" ").length - left.phrase.split(" ").length ||
      right.phrase.length - left.phrase.length,
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
      city: undefined,
      state: undefined,
      country: undefined,
      locationText: parts[0],
    };
  }

  if (parts.length === 2) {
    const normalizedSecondPart = normalizeComparableText(parts[1]);
    const usState = usStateByAlias.get(normalizedSecondPart);
    const countryConcept = resolveCountryConcept(parts[1]);
    const isRemote = normalizeComparableText(parts[0]) === "remote";

    return {
      city: isRemote ? undefined : parts[0],
      state: usState,
      country:
        usState || countryConcept === "united states"
          ? "United States"
          : parts[1],
      locationText: cleaned,
    };
  }

  return {
    city: parts[0],
    state: parts[1],
    country: parts[2],
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
  options: { includeExperience: boolean },
) : FilterEvaluation {
  if (!getTitleMatchResult(job.title, filters.title).matches) {
    return {
      matches: false,
      reason: "title",
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
  return analyzeTitle(value).canonical;
}

export function getTitleMatchResult(jobTitle: string, queryTitle: string): TitleMatchResult {
  const query = analyzeTitle(queryTitle);
  const title = analyzeTitle(jobTitle);

  if (!query.normalized || !title.normalized) {
    return buildTitleMatchResult("none", query, title);
  }

  if (query.normalized === title.normalized) {
    return buildTitleMatchResult("exact", query, title);
  }

  if (query.baseNormalized === title.baseNormalized) {
    return buildTitleMatchResult("variant", query, title, 460);
  }

  if (query.concept && query.concept === title.concept) {
    if (query.aliasKind === "abbreviation" || title.aliasKind === "abbreviation") {
      return buildTitleMatchResult("abbreviation", query, title);
    }

    if (query.aliasKind === "synonym" || title.aliasKind === "synonym") {
      return buildTitleMatchResult("synonym", query, title);
    }

    return buildTitleMatchResult("variant", query, title, 430);
  }

  if (
    isBroadSoftwareQuery(query) &&
    title.family === "software" &&
    title.relevantToBroadSoftwareQueries
  ) {
    return buildTitleMatchResult("related", query, title);
  }

  if (query.concept && title.concept && query.concept !== title.concept) {
    return buildTitleMatchResult("none", query, title);
  }

  if (
    containsAllNormalizedTerms(title.baseNormalized, query.baseNormalized) ||
    containsAllNormalizedTerms(title.normalized, query.normalized)
  ) {
    return buildTitleMatchResult("generic", query, title);
  }

  return buildTitleMatchResult("none", query, title);
}

function matchesCountryFilter(
  job: Pick<JobListing, "country" | "state" | "city" | "locationText">,
  filterCountry: string,
) {
  const wantedConcept = resolveCountryConcept(filterCountry);
  if (!wantedConcept) {
    return false;
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

  if (inferJobUsState(job)) {
    return "united states";
  }

  if (isRecognizedUsCity(job.city)) {
    return "united states";
  }

  return inferCountryConceptFromLocationText(job.locationText);
}

function inferCountryConceptFromLocationText(locationText?: string) {
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

function resolveUsState(value?: string) {
  const normalized = normalizeComparableText(value);
  if (!normalized) {
    return undefined;
  }

  return usStateByAlias.get(normalized);
}

function isRecognizedUsCity(value?: string) {
  const normalized = normalizeComparableText(value);
  return Boolean(normalized) && knownUsCityAliases.has(normalized);
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

  return inferExperienceSignalFromText({
    text: title,
    matchers: experienceMatchers,
    source: "title",
    keywordConfidence: "high",
    yearConfidence: "medium",
  });
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
      source === "structured_metadata" ? metadataExperienceMatchers : experienceMatchers,
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

function analyzeTitle(value?: string): AnalyzedTitle {
  const normalized = normalizeComparableText(value);
  const baseNormalized = stripSeniorityModifiers(normalized);
  const alias = findBestTitleAlias(baseNormalized) ?? findBestTitleAlias(normalized);
  const inferredFamily = alias ? undefined : inferBroadTitleFamily(baseNormalized);
  const canonical = alias?.canonical ?? baseNormalized;
  const remainderNormalized = alias ? removeNormalizedPhrase(baseNormalized, alias.phrase) : "";

  return {
    normalized,
    baseNormalized,
    canonical,
    concept: alias?.concept,
    family: alias?.family ?? inferredFamily?.family,
    aliasKind: alias?.kind,
    matchedPhrase: alias?.phrase,
    relevantToBroadSoftwareQueries:
      alias?.relevantToBroadSoftwareQueries ?? inferredFamily?.relevantToBroadSoftwareQueries ?? false,
    remainderNormalized,
  };
}

function findBestTitleAlias(value: string) {
  if (!value) {
    return undefined;
  }

  return titleRoleAliases.find((alias) => containsNormalizedTerm(value, alias.phrase));
}

function stripSeniorityModifiers(value: string) {
  if (!value) {
    return "";
  }

  let stripped = value;
  for (const phrase of seniorityPhrases) {
    stripped = removeNormalizedPhrase(stripped, phrase);
  }

  return stripped
    .split(" ")
    .filter((token) => token && !seniorityTokens.has(token))
    .join(" ");
}

function removeNormalizedPhrase(value: string, phrase: string) {
  if (!value || !phrase) {
    return value;
  }

  return value
    .replace(new RegExp(`(^| )${escapeRegExp(phrase)}(?= |$)`), " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isBroadSoftwareQuery(title: AnalyzedTitle) {
  return title.concept === "software_engineer" && title.remainderNormalized.length === 0;
}

function inferBroadTitleFamily(value: string) {
  if (!value) {
    return undefined;
  }

  if (broadSoftwareNegativePhrases.some((phrase) => containsNormalizedTerm(value, phrase))) {
    return undefined;
  }

  const hasEngineeringCore =
    containsNormalizedTerm(value, "engineer") ||
    containsNormalizedTerm(value, "developer");
  if (!hasEngineeringCore) {
    return undefined;
  }

  const hasSoftwareSignal = broadSoftwarePositivePhrases.some((phrase) =>
    containsNormalizedTerm(value, phrase),
  );
  if (!hasSoftwareSignal) {
    return undefined;
  }

  return {
    family: "software" as const,
    relevantToBroadSoftwareQueries: true,
  };
}

function buildTitleMatchResult(
  tier: TitleMatchTier,
  query: AnalyzedTitle,
  title: AnalyzedTitle,
  overrideScore?: number,
): TitleMatchResult {
  const score =
    overrideScore ??
    {
      exact: 500,
      variant: 420,
      synonym: 320,
      abbreviation: 220,
      related: 120,
      generic: 40,
      none: 0,
    }[tier];

  return {
    matches: score > 0,
    tier,
    score,
    canonicalQueryTitle: query.canonical,
    canonicalJobTitle: title.canonical,
  };
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
