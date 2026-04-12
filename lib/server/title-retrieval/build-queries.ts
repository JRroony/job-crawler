import "server-only";

import {
  getTitleConcept,
  getTitleFamily,
} from "@/lib/server/title-retrieval/catalog";
import { analyzeTitle } from "@/lib/server/title-retrieval/analyze";
import {
  dedupeNormalizedValues,
  extractMeaningfulTokens,
  normalizeTitleText,
  replaceHeadWord,
} from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAnalysis,
  TitleQueryVariant,
  TitleQueryVariantKind,
} from "@/lib/server/title-retrieval/types";

const alternateHeadWords: Partial<Record<string, string[]>> = {
  architect: ["engineer"],
  developer: ["engineer"],
  engineer: ["developer"],
  recruiter: ["sourcer"],
  sourcer: ["recruiter"],
  tester: ["engineer"],
  writer: ["specialist"],
};

const softwareFamilyHeadWords = new Set(["architect", "developer", "engineer"]);

export function buildTitleQueryVariants(
  title: string,
  options: {
    maxQueries?: number;
  } = {},
) {
  const analysis = analyzeTitle(title);
  const variants: TitleQueryVariant[] = [];
  const seen = new Set<string>();

  const push = (
    candidate: string | undefined,
    kind: TitleQueryVariantKind,
    details: Partial<Pick<TitleQueryVariant, "family" | "conceptId">> = {},
  ) => {
    const normalized = normalizeTitleText(candidate);
    if (!normalized || seen.has(normalized)) {
      return;
    }

    seen.add(normalized);
    variants.push({
      query: normalized,
      normalized,
      kind,
      priority: variants.length,
      ...details,
    });
  };

  push(analysis.input, "original", {
    family: analysis.family,
    conceptId: analysis.primaryConceptId,
  });

  if (analysis.strippedNormalized && analysis.strippedNormalized !== analysis.normalized) {
    push(analysis.strippedNormalized, "normalized", {
      family: analysis.family,
      conceptId: analysis.primaryConceptId,
    });
  }

  const concept = getTitleConcept(analysis.primaryConceptId);
  if (concept) {
    push(concept.canonicalTitle, "canonical", {
      family: concept.family,
      conceptId: concept.id,
    });

    dedupeNormalizedValues(concept.broadDiscoveryQueries ?? []).forEach((query) =>
      push(query, classifyConceptQueryKind(concept.id, query), {
        family: concept.family,
        conceptId: concept.id,
      }),
    );
    (concept.aliases ?? []).forEach((alias) =>
      push(alias, "synonym", {
        family: concept.family,
        conceptId: concept.id,
      }),
    );
    (concept.abbreviations ?? []).forEach((abbreviation) =>
      push(abbreviation, "abbreviation", {
        family: concept.family,
        conceptId: concept.id,
      }),
    );
    concept.adjacentConceptIds?.forEach((adjacentConceptId) => {
      const adjacentConcept = getTitleConcept(adjacentConceptId);
      if (!adjacentConcept) {
        return;
      }

      push(adjacentConcept.canonicalTitle, "adjacent_concept", {
        family: adjacentConcept.family,
        conceptId: adjacentConcept.id,
      });
      (adjacentConcept.aliases ?? []).forEach((alias) =>
        push(alias, "adjacent_concept", {
          family: adjacentConcept.family,
          conceptId: adjacentConcept.id,
        }),
      );
      (adjacentConcept.abbreviations ?? []).forEach((abbreviation) =>
        push(abbreviation, "adjacent_concept", {
          family: adjacentConcept.family,
          conceptId: adjacentConcept.id,
        }),
      );
    });

    dedupeNormalizedValues(getTitleFamily(concept.family)?.broadDiscoveryQueries ?? []).forEach(
      (query) =>
        push(query, "family_broadening", {
          family: concept.family,
          conceptId: concept.id,
        }),
    );
  } else {
    buildFallbackVariants(analysis).forEach((query) =>
      push(query, "fallback_variant", {
        family: analysis.family,
      }),
    );
  }

  const maxQueries = options.maxQueries ?? 16;
  const effectiveMaxQueries = Math.max(12, maxQueries);
  return variants.slice(0, effectiveMaxQueries).map((variant, index) => ({
    ...variant,
    priority: index,
  }));
}

function classifyConceptQueryKind(
  conceptId: string,
  query: string,
): TitleQueryVariantKind {
  const concept = getTitleConcept(conceptId);
  if (!concept) {
    return "fallback_variant";
  }

  const normalized = normalizeTitleText(query);
  if (normalized === normalizeTitleText(concept.canonicalTitle)) {
    return "canonical";
  }

  if ((concept.aliases ?? []).some((alias) => normalizeTitleText(alias) === normalized)) {
    return "synonym";
  }

  if (
    (concept.abbreviations ?? []).some(
      (abbreviation) => normalizeTitleText(abbreviation) === normalized,
    )
  ) {
    return "abbreviation";
  }

  const isAdjacentConceptQuery = (concept.adjacentConceptIds ?? []).some(
    (adjacentConceptId) => {
      const adjacentConcept = getTitleConcept(adjacentConceptId);
      return adjacentConcept
        ? normalizeTitleText(adjacentConcept.canonicalTitle) === normalized
        : false;
    },
  );

  return isAdjacentConceptQuery ? "adjacent_concept" : "family_broadening";
}

export function buildDiscoveryRoleQueries(
  title: string,
  options?: {
    maxQueries?: number;
  },
) {
  return buildTitleQueryVariants(title, options).map((variant) => variant.query);
}

function buildFallbackVariants(analysis: TitleAnalysis) {
  const variants = new Set<string>();
  const baseTitle = analysis.strippedNormalized || analysis.normalized;

  if (analysis.strippedNormalized) {
    variants.add(analysis.strippedNormalized);
  }

  buildModifierPreservingVariants(analysis).forEach((query) => variants.add(query));
  buildCandidateConceptVariants(analysis).forEach((query) => variants.add(query));

  const familyAnchor = buildFamilyAnchorVariant(analysis);
  if (familyAnchor) {
    variants.add(familyAnchor);
  }

  variants.delete(baseTitle);
  return Array.from(variants);
}

function buildModifierPreservingVariants(analysis: TitleAnalysis) {
  const baseTitle = analysis.strippedNormalized || analysis.normalized;
  const headWord = analysis.headWord;
  if (!baseTitle || !headWord) {
    return [];
  }

  const variants = new Set<string>();
  const modifierPhrases = buildModifierPhrases(analysis);

  modifierPhrases.forEach((phrase) => {
    const sameHeadVariant = normalizeTitleText(`${phrase} ${headWord}`);
    if (sameHeadVariant && sameHeadVariant !== baseTitle) {
      variants.add(sameHeadVariant);
    }

    getAlternateHeadWords(headWord).forEach((alternateHeadWord) => {
      const swapped = replaceHeadWord(
        normalizeTitleText(`${phrase} ${headWord}`),
        headWord,
        alternateHeadWord,
      );
      if (swapped && swapped !== baseTitle) {
        variants.add(swapped);
      }
    });
  });

  const leadingHeadSwap = buildHeadWordSwapVariant(analysis);
  if (leadingHeadSwap && leadingHeadSwap !== baseTitle) {
    variants.add(leadingHeadSwap);
  }

  if (
    analysis.family === "software_engineering" &&
    softwareFamilyHeadWords.has(headWord) &&
    modifierPhrases[0]
  ) {
    variants.add(normalizeTitleText(`${modifierPhrases[0]} software engineer`));
  }

  return Array.from(variants).filter(Boolean);
}

function buildModifierPhrases(analysis: TitleAnalysis) {
  const modifiers = analysis.modifierTokens.filter(Boolean);
  if (modifiers.length === 0) {
    return [];
  }

  const phrases = [modifiers.join(" ")];
  if (modifiers.length > 1) {
    phrases.push(modifiers.slice(1).join(" "));
  }

  return dedupeNormalizedValues(phrases);
}

function getAlternateHeadWords(headWord: string) {
  return alternateHeadWords[headWord] ?? [];
}

function buildCandidateConceptVariants(analysis: TitleAnalysis) {
  const variants = new Set<string>();
  const normalizedFamilyAnchor = normalizeTitleText(
    getTitleFamily(analysis.family)?.anchorTitle,
  );
  const hasMoreSpecificCandidate = analysis.candidateConceptIds.some((conceptId) => {
    const concept = getTitleConcept(conceptId);
    if (!concept) {
      return false;
    }

    return normalizeTitleText(concept.canonicalTitle) !== normalizedFamilyAnchor;
  });

  analysis.candidateConceptIds.slice(0, 3).forEach((conceptId) => {
    const concept = getTitleConcept(conceptId);
    if (!concept) {
      return;
    }

    const normalizedCanonicalTitle = normalizeTitleText(concept.canonicalTitle);
    if (
      normalizedFamilyAnchor &&
      normalizedCanonicalTitle === normalizedFamilyAnchor &&
      analysis.modifierTokens.length > 1 &&
      hasMoreSpecificCandidate
    ) {
      return;
    }

    variants.add(normalizedCanonicalTitle);

    const relatedQueries = dedupeNormalizedValues([
      ...(concept.aliases ?? []),
      ...(concept.abbreviations ?? []),
      ...(concept.broadDiscoveryQueries ?? []),
    ]).filter((query) => sharesModifierSignals(analysis, query));

    relatedQueries.slice(0, 2).forEach((query) => variants.add(query));
  });

  return Array.from(variants).filter(Boolean);
}

function sharesModifierSignals(analysis: TitleAnalysis, candidate: string) {
  const candidateTokens = extractMeaningfulTokens(candidate);
  if (analysis.modifierTokens.length === 0) {
    return candidateTokens.length === 0;
  }

  return analysis.modifierTokens.some((token) => candidateTokens.includes(token));
}

function buildHeadWordSwapVariant(analysis: TitleAnalysis) {
  if (!analysis.headWord) {
    return undefined;
  }

  return getAlternateHeadWords(analysis.headWord)
    .map((alternateHeadWord) =>
      replaceHeadWord(
        analysis.strippedNormalized || analysis.normalized,
        analysis.headWord!,
        alternateHeadWord,
      ),
    )
    .find(Boolean);
}

function buildFamilyAnchorVariant(analysis: TitleAnalysis) {
  if (!analysis.family || analysis.primaryConceptId || analysis.modifierTokens.length > 0) {
    return undefined;
  }

  const family = getTitleFamily(analysis.family);
  return family?.anchorTitle;
}
