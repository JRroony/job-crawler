import "server-only";

import {
  areTitleConceptsAdjacent,
  getTitleConcept,
  getTitleFamily,
} from "@/lib/server/title-retrieval/catalog";
import { analyzeTitle } from "@/lib/server/title-retrieval/analyze";
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
  if (sharedMeaningful.length === 0) {
    return 0;
  }

  const compatibleRoleGroup = query.roleGroup && job.roleGroup && query.roleGroup === job.roleGroup;
  const queryCovered =
    query.meaningfulTokens.length > 0 &&
    query.meaningfulTokens.every((token) => job.meaningfulTokens.includes(token));

  return Math.min(
    620,
    440 +
      sharedMeaningful.length * 55 +
      (compatibleRoleGroup ? 40 : 0) +
      (queryCovered ? 35 : 0),
  );
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
  } satisfies TitleMatchResult;
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
  );

  if (query.family && job.family && query.family !== job.family && !conceptsAreAdjacent) {
    penalties.push({
      reason: `conflicting role families (${query.family} vs ${job.family})`,
      value: 520,
      hard: true,
    });
  }

  if (
    queryConcept?.negativeConceptIds?.some((conceptId) =>
      conceptId === job.primaryConceptId || job.matchedConceptIds.includes(conceptId),
    )
  ) {
    penalties.push({
      reason: `query concept ${queryConcept.canonicalTitle} explicitly excludes ${job.canonicalTitle}`,
      value: 420,
      hard: true,
    });
  }

  if (
    jobConcept?.negativeConceptIds?.some((conceptId) =>
      conceptId === query.primaryConceptId || query.matchedConceptIds.includes(conceptId),
    )
  ) {
    penalties.push({
      reason: `job concept ${jobConcept.canonicalTitle} explicitly excludes ${query.canonicalTitle}`,
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
  if (query.primaryConceptId && job.primaryConceptId && query.primaryConceptId === job.primaryConceptId) {
    return query.primaryConceptId;
  }

  if (query.primaryConceptId || job.primaryConceptId) {
    return undefined;
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

function sharedTerms(query: TitleAnalysis, job: TitleAnalysis) {
  return Array.from(new Set(sharedMeaningfulTerms(query, job)));
}

function sharedMeaningfulTerms(query: TitleAnalysis, job: TitleAnalysis) {
  return query.meaningfulTokens.filter((token) => job.meaningfulTokens.includes(token));
}
