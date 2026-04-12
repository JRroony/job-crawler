import "server-only";

import {
  findMatchingTitleAliases,
  getTitleConcept,
  getTitleFamilyRoleGroup,
  listTitleConcepts,
  listTitleFamilies,
} from "@/lib/server/title-retrieval/catalog";
import {
  extractHeadWord,
  extractMeaningfulPhrases,
  extractMeaningfulTokens,
  extractTitleSeniorityTokens,
  normalizeTitleText,
  stripTitleSeniority,
  tokenizeTitle,
} from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAnalysis,
  TitleFamilyScore,
  TitleRoleFamily,
  TitleRoleGroup,
} from "@/lib/server/title-retrieval/types";

const primaryConceptThreshold = 260;
const relatedConceptThreshold = 180;

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

const fallbackFamilyScoresByHeadWord: Partial<
  Record<string, Partial<Record<TitleRoleFamily, number>>>
> = {
  architect: {
    software_engineering: 130,
    data_engineering: 110,
  },
  consultant: {
    sales: 90,
  },
  coordinator: {
    operations: 85,
  },
  designer: {
    writing_documentation: 60,
  },
  developer: {
    software_engineering: 125,
    data_engineering: 95,
    quality_assurance: 70,
  },
  engineer: {
    software_engineering: 120,
    data_engineering: 100,
    quality_assurance: 90,
    support: 80,
    sales: 75,
  },
  analyst: {
    data_analytics: 95,
    operations: 90,
    quality_assurance: 70,
  },
  manager: {
    product: 90,
    program_management: 90,
    operations: 80,
  },
  recruiter: {
    recruiting: 170,
  },
  sourcer: {
    recruiting: 170,
  },
  specialist: {
    operations: 75,
    support: 65,
    writing_documentation: 60,
    recruiting: 55,
  },
  tester: {
    quality_assurance: 165,
  },
  writer: {
    writing_documentation: 160,
  },
};

export function analyzeTitle(value?: string): TitleAnalysis {
  const input = (value ?? "").trim();
  const normalized = normalizeTitleText(input);
  const strippedNormalized = stripTitleSeniority(normalized);
  const canonicalSignalInput = strippedNormalized || normalized;
  const seniorityTokens = extractTitleSeniorityTokens(input);
  const canonicalMeaningfulTokens = extractMeaningfulTokens(canonicalSignalInput);
  const canonicalHeadWord = extractHeadWord(canonicalSignalInput);
  const familyScores = scoreRoleFamilies(canonicalSignalInput);
  const inferredFamily = selectInferredFamily(familyScores);
  const normalizedMatches = findMatchingTitleAliases(normalized);
  const strippedMatches =
    strippedNormalized && strippedNormalized !== normalized
      ? findMatchingTitleAliases(strippedNormalized)
      : [];
  const allAliasMatches = [...normalizedMatches, ...strippedMatches];
  const promotableAliasMatches = allAliasMatches.filter((alias) =>
    canPromoteAliasMatch(alias, canonicalSignalInput, canonicalMeaningfulTokens),
  );
  const primaryAlias = promotableAliasMatches[0];
  const primaryAliasConcept = getTitleConcept(primaryAlias?.conceptId);
  const conceptCandidates = inferTitleConceptCandidates(
    canonicalSignalInput,
    inferredFamily,
  );
  const signalConcept = conceptCandidates.find(
    (candidate) =>
      candidate.score >= primaryConceptThreshold &&
      canPromoteConceptCandidate(candidate.concept, canonicalMeaningfulTokens),
  )?.concept;
  const aliasConceptIds = promotableAliasMatches.map((alias) => alias.conceptId);
  const relatedAliasConceptIds = allAliasMatches
    .filter((alias) => !promotableAliasMatches.includes(alias))
    .map((alias) => alias.conceptId);
  const primaryConcept = resolvePrimaryConcept(
    primaryAliasConcept,
    signalConcept,
    canonicalSignalInput,
    canonicalMeaningfulTokens,
    canonicalHeadWord,
    inferredFamily,
  );
  const matchedConceptIds = Array.from(
    new Set([
      ...aliasConceptIds,
      ...(signalConcept ? [signalConcept.id] : []),
    ]),
  );
  const candidateConceptIds = conceptCandidates
    .map((candidate) => candidate.concept.id)
    .concat(relatedAliasConceptIds)
    .filter(
      (conceptId) =>
        conceptId !== primaryConcept?.id && !matchedConceptIds.includes(conceptId),
    );
  const roleGroup =
    primaryAlias?.roleGroup ??
    primaryConcept?.roleGroup ??
    getTitleFamilyRoleGroup(primaryConcept?.family) ??
    getTitleFamilyRoleGroup(inferredFamily) ??
    inferRoleGroupFromHeadWord(canonicalSignalInput);
  const canonicalTitle =
    primaryConcept?.canonicalTitle ?? strippedNormalized ?? normalized;

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
    headWord: canonicalHeadWord,
    seniorityTokens,
    tokens: tokenizeTitle(canonicalSignalInput),
    meaningfulTokens: canonicalMeaningfulTokens,
    modifierTokens: canonicalMeaningfulTokens,
    candidateConceptIds,
    familyScores,
    inferenceSource:
      primaryAliasConcept && primaryConcept?.id === primaryAliasConcept.id
        ? "catalog"
        : primaryConcept
          ? "concept_signals"
          : inferredFamily
            ? "family"
            : canonicalTitle
              ? "fallback"
              : "none",
    confidence:
      primaryAliasConcept && primaryConcept?.id === primaryAliasConcept.id
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

function inferTitleConceptFromSignals(
  value: string,
  inferredFamily?: TitleRoleFamily,
) {
  return inferTitleConceptCandidates(value, inferredFamily).find(
    (candidate) => candidate.score >= primaryConceptThreshold,
  )?.concept;
}

function inferTitleConceptCandidates(
  value: string,
  inferredFamily?: TitleRoleFamily,
) {
  if (!value) {
    return [] as Array<{
      concept: ReturnType<typeof listTitleConcepts>[number];
      score: number;
    }>;
  }

  const normalized = normalizeTitleText(value);
  const meaningfulTokens = extractMeaningfulTokens(normalized);
  const headWord = extractHeadWord(normalized);
  const minimumScore = inferredFamily ? relatedConceptThreshold - 20 : relatedConceptThreshold;

  return listTitleConcepts()
    .map((concept) => ({
      concept,
      score: scoreConceptSignals(
        concept,
        normalized,
        meaningfulTokens,
        headWord,
        inferredFamily,
      ),
    }))
    .filter(
      (candidate) =>
        candidate.score >= minimumScore &&
        (!inferredFamily ||
          candidate.concept.family === inferredFamily ||
          candidate.score >= primaryConceptThreshold),
    )
    .sort(
      (left, right) =>
        right.score - left.score ||
        left.concept.canonicalTitle.localeCompare(right.concept.canonicalTitle),
    )
    .slice(0, 4);
}

function scoreConceptSignals(
  concept: ReturnType<typeof listTitleConcepts>[number],
  normalizedTitle: string,
  meaningfulTokens: string[],
  headWord: string | undefined,
  inferredFamily?: TitleRoleFamily,
) {
  const signalTokens = buildConceptSignalTokens(concept);
  const signalPhrases = buildConceptSignalPhrases(concept);
  const canonicalHeadWord = extractHeadWord(concept.canonicalTitle);
  const sharedTokenCount = signalTokens.filter((token) =>
    meaningfulTokens.includes(token),
  ).length;
  const sharedPhraseCount = signalPhrases.filter((phrase) =>
    normalizedTitle.includes(phrase),
  ).length;
  const containsCanonical = normalizedTitle.includes(
    normalizeTitleText(concept.canonicalTitle),
  );
  const containsAlias = (concept.aliases ?? []).some((alias) =>
    normalizedTitle.includes(normalizeTitleText(alias)),
  );
  const querySignalsCovered =
    meaningfulTokens.length > 0 &&
    meaningfulTokens.every((token) => signalTokens.includes(token));
  const allCoreSignalsPresent =
    signalTokens.length > 0 &&
    signalTokens.every((token) => meaningfulTokens.includes(token));
  const headCompatible =
    !headWord ||
    !canonicalHeadWord ||
    headWord === canonicalHeadWord ||
    (headWord === "developer" && canonicalHeadWord === "engineer") ||
    (headWord === "engineer" && canonicalHeadWord === "developer") ||
    (headWord === "architect" && canonicalHeadWord === "engineer") ||
    (headWord === "tester" && canonicalHeadWord === "engineer");
  const negativeConflict = (concept.negativeKeywords ?? []).some((keyword) =>
    normalizedTitle.includes(normalizeTitleText(keyword)),
  );

  return (
    (containsCanonical ? 420 : 0) +
    (containsAlias ? 280 : 0) +
    sharedTokenCount * 90 +
    sharedPhraseCount * 55 +
    (querySignalsCovered ? 70 : 0) +
    (allCoreSignalsPresent ? 120 : 0) +
    (headCompatible ? 45 : -140) +
    (inferredFamily && concept.family === inferredFamily ? 80 : 0) -
    (inferredFamily && concept.family !== inferredFamily ? 70 : 0) -
    (negativeConflict ? 260 : 0)
  );
}

function buildConceptSignalTokens(
  concept: ReturnType<typeof listTitleConcepts>[number],
) {
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

function canPromoteConceptCandidate(
  concept: ReturnType<typeof listTitleConcepts>[number],
  meaningfulTokens: string[],
) {
  if (meaningfulTokens.length <= 1) {
    return true;
  }

  const signalTokens = buildConceptSignalTokens(concept);
  return meaningfulTokens.every((token) => signalTokens.includes(token));
}

function canPromoteAliasMatch(
  alias: ReturnType<typeof findMatchingTitleAliases>[number],
  normalizedTitle: string,
  meaningfulTokens: string[],
) {
  if (alias.phrase === normalizedTitle) {
    return true;
  }

  const concept = getTitleConcept(alias.conceptId);
  return concept ? canPromoteConceptCandidate(concept, meaningfulTokens) : false;
}

function buildConceptSignalPhrases(
  concept: ReturnType<typeof listTitleConcepts>[number],
) {
  const phrases = [
    concept.canonicalTitle,
    ...(concept.aliases ?? []),
    ...(concept.broadDiscoveryQueries ?? []),
  ];

  return Array.from(
    new Set(
      phrases.flatMap((phrase) =>
        extractMeaningfulPhrases(phrase, { minLength: 2, maxLength: 3 }),
      ),
    ),
  );
}

function scoreRoleFamilies(value: string): TitleFamilyScore[] {
  if (!value) {
    return [];
  }

  const normalized = normalizeTitleText(value);
  const headWord = extractHeadWord(normalized);
  const inferredRoleGroup = inferRoleGroupFromHeadWord(normalized);

  return listTitleFamilies()
    .map((family) => {
      const positiveSignals = (family.positiveKeywords ?? []).filter((keyword) =>
        normalized.includes(normalizeTitleText(keyword)),
      );
      const negativeSignals = (family.negativeKeywords ?? []).filter((keyword) =>
        normalized.includes(normalizeTitleText(keyword)),
      );
      const positiveScore = positiveSignals.reduce(
        (total, keyword) => total + scoreKeywordSignal(keyword),
        0,
      );
      const negativeScore = negativeSignals.reduce(
        (total, keyword) => total + scoreKeywordSignal(keyword) + 20,
        0,
      );
      const headFallbackScore = headWord
        ? fallbackFamilyScoresByHeadWord[headWord]?.[family.id] ?? 0
        : 0;
      const roleGroupBonus =
        inferredRoleGroup && inferredRoleGroup === family.roleGroup ? 35 : 0;

      return {
        family: family.id,
        score:
          positiveScore -
          negativeScore +
          headFallbackScore +
          roleGroupBonus +
          computeFamilyHeadCompatibilityAdjustment(family.id, headWord, normalized),
        positiveSignals,
        negativeSignals,
      } satisfies TitleFamilyScore;
    })
    .sort(
      (left, right) =>
        right.score - left.score ||
        right.positiveSignals.length - left.positiveSignals.length ||
        left.family.localeCompare(right.family),
    );
}

function scoreKeywordSignal(keyword: string) {
  const tokenCount = normalizeTitleText(keyword).split(" ").filter(Boolean).length;
  return 90 + Math.max(0, tokenCount - 1) * 25;
}

function computeFamilyHeadCompatibilityAdjustment(
  family: TitleRoleFamily,
  headWord: string | undefined,
  normalized: string,
) {
  if (!headWord) {
    return family === "recruiting" && normalized.includes("talent acquisition")
      ? 70
      : 0;
  }

  if (family === "data_analytics") {
    return headWord === "analyst" ? 0 : -80;
  }

  if (family === "data_engineering") {
    return headWord === "engineer" ||
      headWord === "developer" ||
      headWord === "architect"
      ? 0
      : -90;
  }

  if (family === "product") {
    return headWord === "manager" ? 0 : -85;
  }

  if (family === "program_management") {
    return headWord === "manager" || normalized.includes("tpm") ? 0 : -85;
  }

  if (family === "operations") {
    return headWord === "analyst" ||
      headWord === "manager" ||
      headWord === "coordinator" ||
      headWord === "specialist"
      ? 0
      : -75;
  }

  if (family === "sales") {
    return headWord === "engineer" || headWord === "consultant" ? 0 : -85;
  }

  if (family === "quality_assurance") {
    return headWord === "engineer" ||
      headWord === "analyst" ||
      headWord === "tester" ||
      normalized.includes("sdet")
      ? 0
      : -80;
  }

  if (family === "support") {
    return headWord === "engineer" || headWord === "specialist" ? 0 : -75;
  }

  if (family === "recruiting") {
    return headWord === "recruiter" ||
      headWord === "sourcer" ||
      normalized.includes("talent acquisition")
      ? 0
      : -80;
  }

  if (family === "writing_documentation") {
    return headWord === "writer" || headWord === "specialist" ? 0 : -80;
  }

  if (family === "software_engineering") {
    return headWord === "engineer" ||
      headWord === "developer" ||
      headWord === "architect"
      ? 0
      : -95;
  }

  return 0;
}

function selectInferredFamily(familyScores: TitleFamilyScore[]) {
  const [best, second] = familyScores;
  if (!best) {
    return undefined;
  }

  const hasPositiveSignals = best.positiveSignals.length > 0;
  const minimumScore =
    hasPositiveSignals || best.family === "software_engineering" ? 120 : 150;
  const minimumLead = hasPositiveSignals ? 20 : 20;

  if (best.score < minimumScore) {
    return undefined;
  }

  if (second && best.score < second.score + minimumLead) {
    return undefined;
  }

  return best.family;
}

export function normalizeTitleToCanonicalForm(value?: string) {
  return analyzeTitle(value).canonicalTitle;
}

function inferRoleGroupFromHeadWord(value: string) {
  const headWord = extractHeadWord(value);
  return headWord ? roleGroupByHeadWord.get(headWord) : undefined;
}

export function listSupportedRoleFamilies() {
  return listTitleFamilies().map((family) => family.id);
}
