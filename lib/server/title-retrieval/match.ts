import "server-only";

import {
  areTitleConceptsAdjacent,
  findMatchingTitleAliases,
  getTitleConcept,
  getTitleFamily,
} from "@/lib/server/title-retrieval/catalog";
import { analyzeTitle } from "@/lib/server/title-retrieval/analyze";
import { buildTitleQueryVariants } from "@/lib/server/title-retrieval/build-queries";
import { extractMeaningfulTokens } from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAnalysis,
  TitleMatchMode,
  TitleMatchResult,
  TitleMatchTier,
} from "@/lib/server/title-retrieval/types";

const titleMatchThresholds: Record<TitleMatchMode, number> = {
  strict: 760,
  balanced: 420,
  broad: 260,
};

type MatchPenalty = {
  reason: string;
  value: number;
  hard?: boolean;
};

export function getTitleMatchResult(
  jobTitle: string,
  queryTitle: string,
  options: {
    mode?: TitleMatchMode;
  } = {},
): TitleMatchResult {
  const mode = options.mode ?? "balanced";
  const threshold = titleMatchThresholds[mode];
  const query = analyzeTitle(queryTitle);
  const job = analyzeTitle(jobTitle);

  if (!query.normalized || !job.normalized) {
    return buildTitleMatchResult({
      mode,
      threshold,
      tier: "none",
      score: 0,
      query,
      job,
      penalties: [],
      explanation: "One of the titles was empty after normalization.",
    });
  }

  const penalties = collectMatchPenalties(query, job);

  if (query.normalized === job.normalized) {
    return buildTitleMatchResult({
      mode,
      threshold,
      tier: "exact",
      score: 1000,
      query,
      job,
      explanation: "The normalized titles are identical.",
      matchedTerms: sharedTerms(query, job),
      penalties,
    });
  }

  if (query.strippedNormalized && query.strippedNormalized === job.strippedNormalized) {
    return buildTitleMatchResult({
      mode,
      threshold,
      tier: "canonical_variant",
      score: 900,
      query,
      job,
      explanation: "The titles match after removing seniority and formatting modifiers.",
      matchedTerms: sharedTerms(query, job),
      matchedConceptId: query.primaryConceptId ?? job.primaryConceptId,
      penalties,
    });
  }

  const sharedConceptId = findSharedConcept(query, job);
  if (sharedConceptId) {
    const concept = getTitleConcept(sharedConceptId);
    const tier =
      query.aliasKind === "abbreviation" || job.aliasKind === "abbreviation"
        ? "abbreviation"
        : query.aliasKind === "alias" || job.aliasKind === "alias"
          ? "synonym"
          : "canonical_variant";
    const score =
      tier === "abbreviation" ? 760 : tier === "synonym" ? 820 : 880;

    return buildTitleMatchResult({
      mode,
      threshold,
      tier,
      score,
      query,
      job,
      matchedConceptId: sharedConceptId,
      matchedTerms: concept ? [concept.canonicalTitle] : sharedTerms(query, job),
      penalties,
      explanation:
        tier === "abbreviation"
          ? "The titles resolve to the same concept through an abbreviation."
          : tier === "synonym"
            ? "The titles resolve to the same concept through a synonym or alias."
            : "The titles resolve to the same canonical concept.",
    });
  }

  const adjacentConceptId = findAdjacentConcept(query, job);
  if (adjacentConceptId) {
    return buildScoredMatchResult({
      mode,
      threshold,
      tier: "adjacent_concept",
      baseScore: 640,
      query,
      job,
      matchedConceptId: adjacentConceptId,
      matchedTerms: sharedTerms(query, job),
      penalties,
      explanation: "The titles map to adjacent concepts in the same role family.",
    });
  }

  const relevantCrossFamilyConceptId = findRelevantCrossFamilyConcept(query, job);
  if (relevantCrossFamilyConceptId) {
    return buildScoredMatchResult({
      mode,
      threshold,
      tier: "adjacent_concept",
      baseScore: 610,
      query,
      job,
      matchedConceptId: relevantCrossFamilyConceptId,
      matchedTerms: sharedTerms(query, job),
      penalties,
      explanation:
        "The titles map to a controlled cross-family analytics relationship with domain-specific modifiers.",
    });
  }

  const sameFamilyScore = computeSameFamilyScore(query, job);
  if (sameFamilyScore > 0) {
    return buildScoredMatchResult({
      mode,
      threshold,
      tier: "same_family_related",
      baseScore: sameFamilyScore,
      query,
      job,
      matchedTerms: sharedTerms(query, job),
      penalties,
      explanation: "The titles share the same inferred family and overlapping role signals.",
    });
  }

  const genericScore = computeGenericTokenOverlapScore(query, job);
  if (genericScore > 0) {
    return buildScoredMatchResult({
      mode,
      threshold,
      tier: "generic_token_overlap",
      baseScore: genericScore,
      query,
      job,
      matchedTerms: sharedTerms(query, job),
      penalties,
      explanation: "The titles share meaningful modifier tokens and compatible role heads.",
    });
  }

  return buildTitleMatchResult({
    mode,
    threshold,
    tier: "none",
    score: scoreAfterPenalties(0, penalties),
    query,
    job,
    penalties,
    explanation:
      penalties.length > 0
        ? `The titles do not clear the conflict penalties: ${penalties.map((penalty) => penalty.reason).join("; ")}.`
        : "The titles do not share a concept, family, or strong token overlap.",
  });
}

function computeSameFamilyScore(query: TitleAnalysis, job: TitleAnalysis) {
  if (!query.family || !job.family || query.family !== job.family) {
    return 0;
  }

  const sharedMeaningful = sharedMeaningfulTerms(query, job);
  const compatibleRoleGroup = Boolean(
    query.roleGroup && job.roleGroup && query.roleGroup === job.roleGroup,
  );
  const familyHeadCompatibility = computeFamilyHeadCompatibilityScore(
    query,
    job,
    compatibleRoleGroup,
    sharedMeaningful.length,
  );
  const catalogRelatedness = computeCatalogFamilyRelatednessScore(
    query,
    job,
    compatibleRoleGroup,
    sharedMeaningful.length,
  );
  if (sharedMeaningful.length === 0) {
    return Math.max(familyHeadCompatibility, catalogRelatedness);
  }

  const queryCovered =
    query.meaningfulTokens.length > 0 &&
    query.meaningfulTokens.every((token) => job.meaningfulTokens.includes(token));

  return Math.min(
    620,
    440 +
      sharedMeaningful.length * 55 +
      (compatibleRoleGroup ? 40 : 0) +
      (queryCovered ? 35 : 0) +
      Math.max(familyHeadCompatibility, catalogRelatedness),
  );
}

function computeCatalogFamilyRelatednessScore(
  query: TitleAnalysis,
  job: TitleAnalysis,
  compatibleRoleGroup: boolean,
  sharedMeaningfulCount: number,
) {
  if (!query.primaryConceptId || !job.primaryConceptId) {
    return 0;
  }

  const queryConcept = getTitleConcept(query.primaryConceptId);
  const jobConcept = getTitleConcept(job.primaryConceptId);
  if (!queryConcept || !jobConcept || queryConcept.family !== jobConcept.family) {
    return 0;
  }

  if (!allowsBroadSameRoleFamily(queryConcept.family)) {
    return 0;
  }

  const sharedAdjacentConcept = (queryConcept.adjacentConceptIds ?? []).some(
    (queryAdjacentConceptId) =>
      (jobConcept.adjacentConceptIds ?? []).includes(queryAdjacentConceptId),
  );
  const conceptAppearsInQueryExpansion =
    query.candidateConceptIds.includes(jobConcept.id) ||
    query.matchedConceptIds.includes(jobConcept.id);
  const conceptAppearsInJobExpansion =
    job.candidateConceptIds.includes(queryConcept.id) ||
    job.matchedConceptIds.includes(queryConcept.id);
  const sameRoleGroup =
    compatibleRoleGroup ||
    Boolean(queryConcept.roleGroup && queryConcept.roleGroup === jobConcept.roleGroup);

  if (
    !sameRoleGroup &&
    !conceptAppearsInQueryExpansion &&
    !conceptAppearsInJobExpansion
  ) {
    return 0;
  }

  return Math.min(
    560,
    430 +
      (sameRoleGroup ? 45 : 0) +
      (sharedAdjacentConcept ? 35 : 0) +
      (conceptAppearsInQueryExpansion || conceptAppearsInJobExpansion ? 30 : 0) +
      sharedMeaningfulCount * 25,
  );
}

function allowsBroadSameRoleFamily(family: string) {
  return (
    family === "software_engineering" ||
    family === "data_platform" ||
    family === "ai_ml_science" ||
    family === "product" ||
    family === "cloud_devops_security" ||
    family === "architecture_solutions"
  );
}

function computeFamilyHeadCompatibilityScore(
  query: TitleAnalysis,
  job: TitleAnalysis,
  compatibleRoleGroup: boolean,
  sharedMeaningfulCount: number,
) {
  if (!compatibleRoleGroup || !query.family || query.family !== job.family) {
    return 0;
  }

  const compatibleHeads = getFamilyCompatibleHeadWords(query.family);
  const queryHead = query.headWord;
  const jobHead = job.headWord;
  if (
    !queryHead ||
    !jobHead ||
    !compatibleHeads.has(queryHead) ||
    !compatibleHeads.has(jobHead)
  ) {
    return 0;
  }

  const queryHasSpecificSpecialization =
    !query.primaryConceptId &&
    query.meaningfulTokens.length > 0 &&
    !isBroadAnchorQuery(query);
  const jobHasSpecificSpecialization = job.meaningfulTokens.some((token) =>
    !query.meaningfulTokens.includes(token),
  );

  if (queryHasSpecificSpecialization) {
    return 0;
  }

  if (sharedMeaningfulCount === 0 && !jobHasSpecificSpecialization) {
    return 430;
  }

  return jobHasSpecificSpecialization ? 0 : 20;
}

function getFamilyCompatibleHeadWords(family: string) {
  switch (family) {
    case "software_engineering":
      return new Set(["engineer", "developer", "architect", "staff"]);
    case "data_platform":
      return new Set(["engineer", "developer", "architect", "administrator"]);
    case "data_analytics":
      return new Set(["analyst", "scientist", "specialist"]);
    case "ai_ml_science":
      return new Set(["engineer", "scientist", "researcher", "developer"]);
    case "product":
      return new Set(["manager", "owner"]);
    case "cloud_devops_security":
      return new Set(["engineer", "architect", "administrator"]);
    case "qa_support_it":
      return new Set(["engineer", "analyst", "administrator", "specialist", "tester"]);
    case "architecture_solutions":
      return new Set(["architect", "engineer", "consultant"]);
    default:
      return new Set<string>();
  }
}

function isBroadAnchorQuery(analysis: TitleAnalysis) {
  const concept = getTitleConcept(analysis.primaryConceptId);
  if (!concept) {
    return false;
  }

  return analysis.canonicalTitle === concept.canonicalTitle;
}

function computeGenericTokenOverlapScore(query: TitleAnalysis, job: TitleAnalysis) {
  if (query.family && job.family && query.family !== job.family) {
    return 0;
  }

  const sharedMeaningful = sharedMeaningfulTerms(query, job);
  if (sharedMeaningful.length === 0) {
    return 0;
  }

  const compatibleRoleGroup =
    Boolean(query.roleGroup) &&
    Boolean(job.roleGroup) &&
    query.roleGroup === job.roleGroup;
  if (!compatibleRoleGroup) {
    return 0;
  }

  const queryCovered =
    query.meaningfulTokens.length > 0 &&
    query.meaningfulTokens.every((token) => job.meaningfulTokens.includes(token));
  const jobCovered =
    job.meaningfulTokens.length > 0 &&
    job.meaningfulTokens.every((token) => query.meaningfulTokens.includes(token));

  return Math.min(
    460,
    250 +
      sharedMeaningful.length * 70 +
      (compatibleRoleGroup ? 50 : 0) +
      (queryCovered ? 60 : 0) +
      (jobCovered ? 20 : 0),
  );
}

function buildScoredMatchResult(input: {
  mode: TitleMatchMode;
  threshold: number;
  tier: Exclude<TitleMatchTier, "none">;
  baseScore: number;
  query: TitleAnalysis;
  job: TitleAnalysis;
  matchedConceptId?: string;
  matchedTerms?: string[];
  penalties: MatchPenalty[];
  explanation: string;
}) {
  const score = scoreAfterPenalties(input.baseScore, input.penalties);
  const hasHardPenalty = input.penalties.some((penalty) => penalty.hard);

  if (score <= 0 || hasHardPenalty) {
    return buildTitleMatchResult({
      ...input,
      tier: "none",
      score,
      explanation: `${input.explanation} Penalized by: ${input.penalties.map((penalty) => penalty.reason).join("; ")}.`,
    });
  }

  return buildTitleMatchResult({
    ...input,
    score,
  });
}

function buildTitleMatchResult(input: {
  mode: TitleMatchMode;
  threshold: number;
  tier: TitleMatchTier;
  score: number;
  query: TitleAnalysis;
  job: TitleAnalysis;
  matchedConceptId?: string;
  matchedTerms?: string[];
  penalties: MatchPenalty[];
  explanation: string;
}) {
  const queryDiagnostics = buildTitleDiagnostics(input.query, true);
  const jobDiagnostics = buildTitleDiagnostics(input.job, false);

  return {
    matches: input.tier !== "none" && input.score >= input.threshold,
    mode: input.mode,
    threshold: input.threshold,
    tier: input.tier,
    score: input.score,
    canonicalQueryTitle: input.query.canonicalTitle,
    canonicalJobTitle: input.job.canonicalTitle,
    queryFamily: input.query.family,
    jobFamily: input.job.family,
    matchedConceptId: input.matchedConceptId,
    matchedTerms: input.matchedTerms ?? [],
    penalties: input.penalties.map((penalty) => penalty.reason),
    explanation: input.explanation,
    queryDiagnostics,
    jobDiagnostics,
  } satisfies TitleMatchResult;
}

function buildTitleDiagnostics(analysis: TitleAnalysis, includeExpansionVariants: boolean) {
  const matchedAliases = findMatchingTitleAliases(analysis.input)
    .map((alias) => alias.phrase)
    .filter(Boolean);
  const expandedAliases = includeExpansionVariants
    ? buildTitleQueryVariants(analysis.input, { maxQueries: 24 }).map((variant) => variant.query)
    : [];

  return {
    original: analysis.input,
    normalized: analysis.normalized,
    canonical: analysis.canonicalTitle,
    family: analysis.family,
    conceptId: analysis.primaryConceptId,
    aliasesUsed: Array.from(new Set([...matchedAliases, ...expandedAliases])),
  };
}

function scoreAfterPenalties(baseScore: number, penalties: MatchPenalty[]) {
  return Math.max(
    0,
    penalties.reduce((score, penalty) => score - penalty.value, baseScore),
  );
}

function collectMatchPenalties(query: TitleAnalysis, job: TitleAnalysis) {
  const penalties: MatchPenalty[] = [];
  const queryConcept = getTitleConcept(query.primaryConceptId);
  const jobConcept = getTitleConcept(job.primaryConceptId);
  const queryFamily = getTitleFamily(query.family);
  const jobFamily = getTitleFamily(job.family);
  const conceptsAreAdjacent = query.matchedConceptIds.some((queryConceptId) =>
    job.matchedConceptIds.some((jobConceptId) =>
      areTitleConceptsAdjacent(queryConceptId, jobConceptId),
    ),
  ) || Boolean(findRelevantCrossFamilyConcept(query, job));
  const sharedPrimaryFamily =
    Boolean(queryConcept?.family) &&
    Boolean(jobConcept?.family) &&
    queryConcept?.family === jobConcept?.family;

  if (query.family && job.family && query.family !== job.family && !conceptsAreAdjacent) {
    penalties.push({
      reason: `conflicting role families (${query.family} vs ${job.family})`,
      value: 520,
      hard: true,
    });
  }

  const queryPrimaryConflict = queryConcept?.negativeConceptIds?.some(
    (conceptId) => conceptId === job.primaryConceptId,
  );
  const querySecondaryConflict =
    !queryPrimaryConflict &&
    !conceptsAreAdjacent &&
    !sharedPrimaryFamily &&
    queryConcept?.negativeConceptIds?.some((conceptId) => job.matchedConceptIds.includes(conceptId));

  if (queryPrimaryConflict || querySecondaryConflict) {
    penalties.push({
      reason: `query concept ${queryConcept?.canonicalTitle ?? query.canonicalTitle} explicitly excludes ${job.canonicalTitle}`,
      value: 420,
      hard: true,
    });
  }

  const jobPrimaryConflict = jobConcept?.negativeConceptIds?.some(
    (conceptId) => conceptId === query.primaryConceptId,
  );
  const jobSecondaryConflict =
    !jobPrimaryConflict &&
    !conceptsAreAdjacent &&
    !sharedPrimaryFamily &&
    jobConcept?.negativeConceptIds?.some((conceptId) => query.matchedConceptIds.includes(conceptId));

  if (jobPrimaryConflict || jobSecondaryConflict) {
    penalties.push({
      reason: `job concept ${jobConcept?.canonicalTitle ?? job.canonicalTitle} explicitly excludes ${query.canonicalTitle}`,
      value: 420,
      hard: true,
    });
  }

  collectKeywordPenalties(query.normalized, job.normalized, queryConcept?.negativeKeywords).forEach(
    (penalty) => penalties.push(penalty),
  );
  collectKeywordPenalties(query.normalized, job.normalized, queryFamily?.negativeKeywords).forEach(
    (penalty) => penalties.push(penalty),
  );
  collectKeywordPenalties(job.normalized, query.normalized, jobConcept?.negativeKeywords).forEach(
    (penalty) => penalties.push(penalty),
  );
  collectKeywordPenalties(job.normalized, query.normalized, jobFamily?.negativeKeywords).forEach(
    (penalty) => penalties.push(penalty),
  );

  return dedupePenalties(penalties);
}

function collectKeywordPenalties(
  ownerTitle: string,
  otherTitle: string,
  keywords?: string[],
) {
  return (keywords ?? [])
    .filter((keyword) => otherTitle.includes(keyword))
    .map((keyword) => ({
      reason: `negative keyword "${keyword}" conflicts with "${ownerTitle}"`,
      value: 180,
    }) satisfies MatchPenalty);
}

function dedupePenalties(penalties: MatchPenalty[]) {
  const seen = new Set<string>();
  return penalties.filter((penalty) => {
    if (seen.has(penalty.reason)) {
      return false;
    }

    seen.add(penalty.reason);
    return true;
  });
}

function findSharedConcept(query: TitleAnalysis, job: TitleAnalysis) {
  if (
    query.primaryConceptId &&
    (job.primaryConceptId === query.primaryConceptId ||
      job.matchedConceptIds.includes(query.primaryConceptId))
  ) {
    return query.primaryConceptId;
  }

  if (
    job.primaryConceptId &&
    (query.primaryConceptId === job.primaryConceptId ||
      query.matchedConceptIds.includes(job.primaryConceptId))
  ) {
    return job.primaryConceptId;
  }

  return query.matchedConceptIds.find((conceptId) => job.matchedConceptIds.includes(conceptId));
}

function findAdjacentConcept(query: TitleAnalysis, job: TitleAnalysis) {
  return query.matchedConceptIds.find((queryConceptId) =>
    job.matchedConceptIds.some((jobConceptId) =>
      areTitleConceptsAdjacent(queryConceptId, jobConceptId),
    ),
  );
}

function findRelevantCrossFamilyConcept(query: TitleAnalysis, job: TitleAnalysis) {
  if (isModifierQualifiedDataScientistMatch(query, job)) {
    return "data_scientist";
  }

  return undefined;
}

function isModifierQualifiedDataScientistMatch(query: TitleAnalysis, job: TitleAnalysis) {
  if (query.primaryConceptId !== "data_analyst" || job.primaryConceptId !== "data_scientist") {
    return false;
  }

  const analystConcept = getTitleConcept("data_analyst");
  if (!analystConcept) {
    return false;
  }

  const analystSignals = new Set(buildConceptSignalTokensForMatch(analystConcept));
  const scientistModifiers = job.meaningfulTokens.filter((token) => token !== "data");
  return (
    scientistModifiers.length > 0 &&
    scientistModifiers.some((token) => analystSignals.has(token))
  );
}

function buildConceptSignalTokensForMatch(
  concept: NonNullable<ReturnType<typeof getTitleConcept>>,
) {
  const phrases = [
    concept.canonicalTitle,
    ...(concept.aliases ?? []),
    ...(concept.tokenSynonyms ?? []).flat(),
    ...(concept.broadDiscoveryQueries ?? []),
  ];

  return Array.from(
    new Set(phrases.flatMap((phrase) => extractMeaningfulTokens(phrase)).filter(Boolean)),
  );
}

function sharedTerms(query: TitleAnalysis, job: TitleAnalysis) {
  return Array.from(new Set(sharedMeaningfulTerms(query, job)));
}

function sharedMeaningfulTerms(query: TitleAnalysis, job: TitleAnalysis) {
  return query.meaningfulTokens.filter((token) => job.meaningfulTokens.includes(token));
}
