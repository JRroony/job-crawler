import "server-only";

import {
  findMatchingTitleAliases,
  getTitleConcept,
  getTitleFamily,
  getTitleFamilyRoleGroup,
  listTitleConcepts,
  listTitleFamilies,
} from "@/lib/server/title-retrieval/catalog";
import {
  extractHeadWord,
  extractMeaningfulTokens,
  normalizeTitleText,
  stripTitleSeniority,
  tokenizeTitle,
} from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAnalysis,
  TitleRoleFamily,
  TitleRoleGroup,
} from "@/lib/server/title-retrieval/types";

const familyInferenceOrder: TitleRoleFamily[] = [
  "recruiting",
  "product",
  "program_management",
  "quality_assurance",
  "writing_documentation",
  "support",
  "sales",
  "operations",
  "data_engineering",
  "data_analytics",
  "software_engineering",
];

const roleGroupByHeadWord = new Map<string, TitleRoleGroup>([
  ["architect", "engineering"],
  ["developer", "engineering"],
  ["engineer", "engineering"],
  ["analyst", "analysis"],
  ["manager", "management"],
  ["recruiter", "recruiting"],
  ["sourcer", "recruiting"],
  ["writer", "writing"],
  ["specialist", "operations"],
  ["coordinator", "operations"],
  ["consultant", "sales"],
  ["designer", "writing"],
  ["tester", "quality"],
]);

export function analyzeTitle(value?: string): TitleAnalysis {
  const input = (value ?? "").trim();
  const normalized = normalizeTitleText(input);
  const strippedNormalized = stripTitleSeniority(normalized);
  const normalizedMatches = findMatchingTitleAliases(normalized);
  const strippedMatches =
    strippedNormalized && strippedNormalized !== normalized
      ? findMatchingTitleAliases(strippedNormalized)
      : [];
  const primaryAlias = normalizedMatches[0] ?? strippedMatches[0];
  const canonicalSignalInput = strippedNormalized || normalized;
  const canonicalMeaningfulTokens = extractMeaningfulTokens(canonicalSignalInput);
  const canonicalHeadWord = extractHeadWord(canonicalSignalInput);
  const primaryAliasConcept = getTitleConcept(primaryAlias?.conceptId);
  const inferredFamilyFromKeywords = inferRoleFamily(canonicalSignalInput);
  const signalConcept = inferTitleConceptFromSignals(canonicalSignalInput, inferredFamilyFromKeywords);
  const aliasConceptIds = [...normalizedMatches, ...strippedMatches].map((alias) => alias.conceptId);
  const primaryConcept = resolvePrimaryConcept(
    primaryAliasConcept,
    signalConcept,
    canonicalSignalInput,
    canonicalMeaningfulTokens,
    canonicalHeadWord,
    inferredFamilyFromKeywords,
  );
  const matchedConceptIds = Array.from(
    new Set([
      ...aliasConceptIds,
      ...(signalConcept ? [signalConcept.id] : []),
    ]),
  );
  const inferredFamily =
    primaryConcept?.family ??
    inferredFamilyFromKeywords;
  const roleGroup =
    primaryAlias?.roleGroup ??
    getTitleFamilyRoleGroup(primaryConcept?.family) ??
    getTitleFamilyRoleGroup(inferredFamily) ??
    inferRoleGroupFromHeadWord(strippedNormalized || normalized);
  const canonicalTitle =
    primaryConcept?.canonicalTitle ??
    strippedNormalized ??
    normalized;

  return {
    input,
    normalized,
    strippedNormalized,
    canonicalTitle,
    family: primaryConcept?.family ?? inferredFamily,
    roleGroup,
    primaryConceptId: primaryConcept?.id,
    matchedConceptIds,
    aliasKind: primaryAlias?.kind,
    matchedPhrase: primaryAlias?.phrase,
    headWord: extractHeadWord(strippedNormalized || normalized),
    tokens: tokenizeTitle(strippedNormalized || normalized),
    meaningfulTokens: extractMeaningfulTokens(strippedNormalized || normalized),
    inferenceSource: primaryAliasConcept && primaryConcept?.id === primaryAliasConcept.id
      ? "catalog"
      : primaryConcept
        ? "concept_signals"
        : inferredFamily
          ? "family"
          : canonicalTitle
            ? "fallback"
            : "none",
    confidence: primaryAliasConcept && primaryConcept?.id === primaryAliasConcept.id
      ? "high"
      : primaryConcept
        ? "medium"
        : inferredFamily
          ? "medium"
          : canonicalTitle
            ? "low"
            : "none",
  };
}

function resolvePrimaryConcept(
  primaryAliasConcept: ReturnType<typeof getTitleConcept>,
  signalConcept: ReturnType<typeof inferTitleConceptFromSignals>,
  normalizedTitle: string,
  meaningfulTokens: string[],
  headWord: string | undefined,
  inferredFamily?: TitleRoleFamily,
) {
  if (!primaryAliasConcept) {
    return signalConcept;
  }

  if (!signalConcept || signalConcept.id === primaryAliasConcept.id) {
    return primaryAliasConcept;
  }

  const primaryScore = scoreConceptSignals(
    primaryAliasConcept,
    normalizedTitle,
    meaningfulTokens,
    headWord,
    inferredFamily,
  );

  const signalScore = scoreConceptSignals(
    signalConcept,
    normalizedTitle,
    meaningfulTokens,
    headWord,
    inferredFamily,
  );

  if (
    signalScore >= primaryScore + 140 &&
    extractMeaningfulTokens(signalConcept.canonicalTitle).length >=
      extractMeaningfulTokens(primaryAliasConcept.canonicalTitle).length
  ) {
    return signalConcept;
  }

  return primaryAliasConcept;
}

function inferTitleConceptFromSignals(value: string, inferredFamily?: TitleRoleFamily) {
  if (!value) {
    return undefined;
  }

  const normalized = normalizeTitleText(value);
  const meaningfulTokens = extractMeaningfulTokens(normalized);
  const headWord = extractHeadWord(normalized);
  const scoredConcepts = listTitleConcepts()
    .map((concept) => ({
      concept,
      score: scoreConceptSignals(concept, normalized, meaningfulTokens, headWord, inferredFamily),
    }))
    .filter((candidate) => candidate.score >= 260)
    .sort((left, right) => right.score - left.score || left.concept.canonicalTitle.localeCompare(right.concept.canonicalTitle));

  return scoredConcepts[0]?.concept;
}

function scoreConceptSignals(
  concept: ReturnType<typeof listTitleConcepts>[number],
  normalizedTitle: string,
  meaningfulTokens: string[],
  headWord: string | undefined,
  inferredFamily?: TitleRoleFamily,
) {
  const signalTokens = buildConceptSignalTokens(concept);
  const canonicalHeadWord = extractHeadWord(concept.canonicalTitle);
  const sharedTokenCount = signalTokens.filter((token) => meaningfulTokens.includes(token)).length;
  const containsCanonical = normalizedTitle.includes(normalizeTitleText(concept.canonicalTitle));
  const containsAlias = (concept.aliases ?? []).some((alias) =>
    normalizedTitle.includes(normalizeTitleText(alias)),
  );
  const allCoreSignalsPresent =
    signalTokens.length > 0 && signalTokens.every((token) => meaningfulTokens.includes(token));
  const headCompatible =
    !headWord ||
    !canonicalHeadWord ||
    headWord === canonicalHeadWord ||
    (headWord === "developer" && canonicalHeadWord === "engineer") ||
    (headWord === "engineer" && canonicalHeadWord === "developer");
  const negativeConflict = (concept.negativeKeywords ?? []).some((keyword) =>
    normalizedTitle.includes(normalizeTitleText(keyword)),
  );

  return (
    (containsCanonical ? 420 : 0) +
    (containsAlias ? 280 : 0) +
    sharedTokenCount * 90 +
    (allCoreSignalsPresent ? 120 : 0) +
    (headCompatible ? 40 : -120) +
    (inferredFamily && concept.family === inferredFamily ? 80 : 0) -
    (negativeConflict ? 260 : 0)
  );
}

function buildConceptSignalTokens(concept: ReturnType<typeof listTitleConcepts>[number]) {
  const phrases = [
    concept.canonicalTitle,
    ...(concept.aliases ?? []),
    ...(concept.broadDiscoveryQueries ?? []),
  ];

  return Array.from(
    new Set(
      phrases.flatMap((phrase) => extractMeaningfulTokens(phrase)).filter(Boolean),
    ),
  );
}

export function normalizeTitleToCanonicalForm(value?: string) {
  return analyzeTitle(value).canonicalTitle;
}

function inferRoleFamily(value: string) {
  if (!value) {
    return undefined;
  }

  const normalized = normalizeTitleText(value);
  const headWord = extractHeadWord(normalized);
  const matchedFamily = familyInferenceOrder.find((familyId) => {
    const family = getTitleFamily(familyId);
    if (!family) {
      return false;
    }

    if (
      family.negativeKeywords?.some((keyword) => normalized.includes(normalizeTitleText(keyword)))
    ) {
      return false;
    }

    return family.positiveKeywords.some((keyword) =>
      normalized.includes(normalizeTitleText(keyword)),
    );
  });

  if (!matchedFamily) {
    return undefined;
  }

  if (matchedFamily === "data_analytics") {
    return headWord === "analyst" ? matchedFamily : undefined;
  }

  if (matchedFamily === "data_engineering") {
    return headWord === "engineer" || headWord === "developer" || headWord === "architect"
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "product") {
    return headWord === "manager" ? matchedFamily : undefined;
  }

  if (matchedFamily === "program_management") {
    return headWord === "manager" || normalized.includes("tpm")
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "operations") {
    return headWord === "analyst" ||
      headWord === "manager" ||
      headWord === "coordinator" ||
      headWord === "specialist"
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "sales") {
    return headWord === "engineer" || headWord === "consultant"
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "quality_assurance") {
    return headWord === "engineer" ||
      headWord === "analyst" ||
      headWord === "tester" ||
      normalized.includes("sdet")
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "support") {
    return headWord === "engineer" || headWord === "specialist"
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "recruiting") {
    return headWord === "recruiter" ||
      headWord === "sourcer" ||
      normalized.includes("talent acquisition")
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "writing_documentation") {
    return headWord === "writer" || headWord === "specialist"
      ? matchedFamily
      : undefined;
  }

  if (matchedFamily === "software_engineering") {
    if (headWord !== "engineer" && headWord !== "developer" && headWord !== "architect") {
      return undefined;
    }

    return matchedFamily;
  }

  return matchedFamily;
}

function inferRoleGroupFromHeadWord(value: string) {
  const headWord = extractHeadWord(value);
  return headWord ? roleGroupByHeadWord.get(headWord) : undefined;
}

export function listSupportedRoleFamilies() {
  return listTitleFamilies().map((family) => family.id);
}
