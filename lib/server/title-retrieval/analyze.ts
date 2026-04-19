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
const analyzeTitleCache = new Map<string, TitleAnalysis>();
const analyzeTitleCacheLimit = 512;

const roleGroupByHeadWord = new Map<string, TitleRoleGroup>([
  ["architect", "solutions"],
  ["administrator", "support"],
  ["consultant", "solutions"],
  ["developer", "engineering"],
  ["designer", "design"],
  ["engineer", "engineering"],
  ["analyst", "analysis"],
  ["manager", "management"],
  ["owner", "management"],
  ["researcher", "design"],
  ["recruiter", "recruiting"],
  ["scientist", "engineering"],
  ["sourcer", "recruiting"],
  ["staff", "engineering"],
  ["writer", "writing"],
  ["specialist", "operations"],
  ["coordinator", "operations"],
  ["tester", "quality"],
]);

const fallbackFamilyScoresByHeadWord: Partial<
  Record<string, Partial<Record<TitleRoleFamily, number>>>
> = {
  architect: {
    architecture_solutions: 175,
    software_engineering: 95,
    cloud_devops_security: 90,
  },
  administrator: {
    cloud_devops_security: 140,
    qa_support_it: 85,
  },
  consultant: {
    architecture_solutions: 110,
    business_operations_people: 70,
  },
  coordinator: {
    business_operations_people: 85,
  },
  designer: {
    design_content_marketing: 150,
  },
  developer: {
    software_engineering: 125,
    data_platform: 100,
    qa_support_it: 75,
    ai_ml_science: 70,
  },
  engineer: {
    software_engineering: 120,
    data_platform: 105,
    ai_ml_science: 110,
    cloud_devops_security: 100,
    qa_support_it: 95,
    architecture_solutions: 90,
  },
  analyst: {
    data_analytics: 95,
    business_operations_people: 90,
    qa_support_it: 70,
  },
  manager: {
    product: 110,
    business_operations_people: 95,
    design_content_marketing: 70,
  },
  owner: {
    product: 105,
  },
  researcher: {
    design_content_marketing: 120,
    ai_ml_science: 115,
  },
  recruiter: {
    business_operations_people: 170,
  },
  scientist: {
    ai_ml_science: 180,
    data_analytics: 80,
  },
  sourcer: {
    business_operations_people: 170,
  },
  staff: {
    software_engineering: 115,
    qa_support_it: 55,
  },
  specialist: {
    business_operations_people: 75,
    qa_support_it: 70,
    design_content_marketing: 65,
  },
  tester: {
    qa_support_it: 165,
  },
  writer: {
    design_content_marketing: 160,
  },
};

export function analyzeTitle(value?: string): TitleAnalysis {
  const cacheKey = value ?? "";
  const cached = analyzeTitleCache.get(cacheKey);
  if (cached) {
    return cached;
  }

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

  const analysis = {
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
  } satisfies TitleAnalysis;

  if (analyzeTitleCache.size >= analyzeTitleCacheLimit) {
    const firstKey = analyzeTitleCache.keys().next().value;
    if (firstKey) {
      analyzeTitleCache.delete(firstKey);
    }
  }

  analyzeTitleCache.set(cacheKey, analysis);
  return analysis;
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
  const synonymTokenMatches = (concept.tokenSynonyms ?? []).reduce((total, synonymGroup) => {
    const normalizedGroup = Array.from(
      new Set(synonymGroup.map((phrase) => normalizeTitleText(phrase)).filter(Boolean)),
    );
    if (normalizedGroup.length === 0) {
      return total;
    }

    const hasTitleSignal = normalizedGroup.some((phrase) => normalizedTitle.includes(phrase));
    const hasQuerySignal = normalizedGroup.some((phrase) =>
      meaningfulTokens.includes(phrase) ||
      phrase.split(" ").every((token) => meaningfulTokens.includes(token)),
    );

    return hasTitleSignal || hasQuerySignal ? total + 1 : total;
  }, 0);
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
    (headWord === "administrator" && canonicalHeadWord === "engineer") ||
    (headWord === "developer" && canonicalHeadWord === "engineer") ||
    (headWord === "engineer" && canonicalHeadWord === "developer") ||
    (headWord === "architect" && canonicalHeadWord === "engineer") ||
    (headWord === "researcher" && canonicalHeadWord === "scientist") ||
    (headWord === "scientist" && canonicalHeadWord === "analyst") ||
    (headWord === "tester" && canonicalHeadWord === "engineer") ||
    (headWord === "staff" && canonicalHeadWord === "engineer") ||
    (headWord === "specialist" && canonicalHeadWord === "manager") ||
    (headWord === "owner" && canonicalHeadWord === "manager");
  const negativeConflict = (concept.negativeKeywords ?? []).some((keyword) =>
    normalizedTitle.includes(normalizeTitleText(keyword)),
  );

  return (
    (containsCanonical ? 420 : 0) +
    (containsAlias ? 280 : 0) +
    synonymTokenMatches * 65 +
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
    ...(concept.tokenSynonyms ?? []).flat(),
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
    ...(concept.tokenSynonyms ?? []).flat(),
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
    return family === "business_operations_people" && normalized.includes("talent acquisition")
      ? 70
      : 0;
  }

  if (family === "data_analytics") {
    return headWord === "analyst" || headWord === "scientist" ? 0 : -80;
  }

  if (family === "data_platform") {
    return headWord === "engineer" ||
      headWord === "developer" ||
      headWord === "architect" ||
      headWord === "administrator"
      ? 0
      : -90;
  }

  if (family === "ai_ml_science") {
    return headWord === "engineer" ||
      headWord === "scientist" ||
      headWord === "researcher" ||
      normalized.includes("mlops")
      ? 0
      : -70;
  }

  if (family === "product") {
    return headWord === "manager" || headWord === "owner" ? 0 : -85;
  }

  if (family === "cloud_devops_security") {
    return headWord === "engineer" ||
      headWord === "architect" ||
      headWord === "administrator" ||
      normalized.includes("sre")
      ? 0
      : -75;
  }

  if (family === "qa_support_it") {
    return headWord === "engineer" ||
      headWord === "analyst" ||
      headWord === "administrator" ||
      headWord === "specialist" ||
      headWord === "tester" ||
      normalized.includes("sdet")
      ? 0
      : -80;
  }

  if (family === "architecture_solutions") {
    return headWord === "architect" ||
      headWord === "engineer" ||
      headWord === "consultant" ||
      normalized.includes("presales") ||
      normalized.includes("pre sales")
      ? 0
      : -70;
  }

  if (family === "design_content_marketing") {
    return headWord === "designer" ||
      headWord === "writer" ||
      headWord === "researcher" ||
      headWord === "manager" ||
      headWord === "specialist"
      ? 0
      : -80;
  }

  if (family === "business_operations_people") {
    return headWord === "analyst" ||
      headWord === "manager" ||
      headWord === "coordinator" ||
      headWord === "specialist" ||
      headWord === "recruiter" ||
      headWord === "sourcer"
      ? 0
      : -75;
  }

  if (family === "software_engineering") {
    return headWord === "engineer" ||
      headWord === "developer" ||
      headWord === "architect" ||
      headWord === "staff"
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
  const minimumLead = hasPositiveSignals
    ? 20
    : best.family === "software_engineering"
      ? 10
      : 20;

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
