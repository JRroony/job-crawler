import "server-only";

import {
  findMatchingTitleAliases,
  getTitleConcept,
  getTitleFamily,
  getTitleFamilyRoleGroup,
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
  const primaryConcept = getTitleConcept(primaryAlias?.conceptId);
  const matchedConceptIds = Array.from(
    new Set([...normalizedMatches, ...strippedMatches].map((alias) => alias.conceptId)),
  );
  const inferredFamily =
    primaryConcept?.family ??
    inferRoleFamily(strippedNormalized || normalized);
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
    inferenceSource: primaryConcept
      ? "catalog"
      : inferredFamily
        ? "family"
        : canonicalTitle
          ? "fallback"
          : "none",
    confidence: primaryConcept
      ? "high"
      : inferredFamily
        ? "medium"
        : canonicalTitle
          ? "low"
          : "none",
  };
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
