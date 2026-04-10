import "server-only";

import {
  getTitleConcept,
  getTitleFamily,
} from "@/lib/server/title-retrieval/catalog";
import { analyzeTitle } from "@/lib/server/title-retrieval/analyze";
import {
  dedupeNormalizedValues,
  normalizeTitleText,
  replaceHeadWord,
} from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAnalysis,
  TitleQueryVariant,
  TitleQueryVariantKind,
} from "@/lib/server/title-retrieval/types";

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

    dedupeNormalizedValues(getTitleFamily(concept.family)?.broadDiscoveryQueries ?? []).forEach((query) =>
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

  const maxQueries = options.maxQueries ?? 12;
  return variants.slice(0, maxQueries).map((variant, index) => ({
    ...variant,
    priority: index,
  }));
}

function classifyConceptQueryKind(conceptId: string, query: string): TitleQueryVariantKind {
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
    (concept.abbreviations ?? []).some((abbreviation) => normalizeTitleText(abbreviation) === normalized)
  ) {
    return "abbreviation";
  }

  const isAdjacentConceptQuery = (concept.adjacentConceptIds ?? []).some((adjacentConceptId) => {
    const adjacentConcept = getTitleConcept(adjacentConceptId);
    return adjacentConcept
      ? normalizeTitleText(adjacentConcept.canonicalTitle) === normalized
      : false;
  });

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

  if (analysis.strippedNormalized) {
    variants.add(analysis.strippedNormalized);
  }

  const swappedHeadWordVariant = buildHeadWordSwapVariant(analysis);
  if (swappedHeadWordVariant) {
    variants.add(swappedHeadWordVariant);
  }

  const familyAnchor = buildFamilyAnchorVariant(analysis);
  if (familyAnchor) {
    variants.add(familyAnchor);
  }

  return Array.from(variants);
}

function buildHeadWordSwapVariant(analysis: TitleAnalysis) {
  if (!analysis.headWord) {
    return undefined;
  }

  if (analysis.headWord === "engineer") {
    return replaceHeadWord(analysis.strippedNormalized || analysis.normalized, "engineer", "developer");
  }

  if (analysis.headWord === "developer") {
    return replaceHeadWord(analysis.strippedNormalized || analysis.normalized, "developer", "engineer");
  }

  if (analysis.headWord === "recruiter") {
    return replaceHeadWord(analysis.strippedNormalized || analysis.normalized, "recruiter", "sourcer");
  }

  if (analysis.headWord === "writer") {
    return replaceHeadWord(analysis.strippedNormalized || analysis.normalized, "writer", "specialist");
  }

  return undefined;
}

function buildFamilyAnchorVariant(analysis: TitleAnalysis) {
  if (!analysis.family || analysis.primaryConceptId) {
    return undefined;
  }

  const family = getTitleFamily(analysis.family);
  return family?.anchorTitle;
}
