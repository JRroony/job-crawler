import "server-only";

export const titleMatchModes = ["strict", "balanced", "broad"] as const;

export type TitleMatchMode = (typeof titleMatchModes)[number];

export const titleMatchTiers = [
  "exact",
  "canonical_variant",
  "synonym",
  "abbreviation",
  "adjacent_concept",
  "same_family_related",
  "generic_token_overlap",
  "none",
] as const;

export type TitleMatchTier = (typeof titleMatchTiers)[number];

export const titleRoleFamilies = [
  "software_engineering",
  "data_analytics",
  "product",
  "program_management",
  "recruiting",
  "quality_assurance",
  "writing_documentation",
  "support",
  "sales",
  "operations",
] as const;

export type TitleRoleFamily = (typeof titleRoleFamilies)[number];

export const titleRoleGroups = [
  "engineering",
  "analysis",
  "management",
  "recruiting",
  "quality",
  "writing",
  "support",
  "sales",
  "operations",
] as const;

export type TitleRoleGroup = (typeof titleRoleGroups)[number];

export const titleAliasKinds = ["canonical", "alias", "abbreviation"] as const;

export type TitleAliasKind = (typeof titleAliasKinds)[number];

export const titleQueryVariantKinds = [
  "original",
  "normalized",
  "canonical",
  "synonym",
  "abbreviation",
  "adjacent_concept",
  "family_broadening",
  "fallback_variant",
] as const;

export type TitleQueryVariantKind = (typeof titleQueryVariantKinds)[number];

export type TitleConceptId = string;

export type TitleFamilyDefinition = {
  id: TitleRoleFamily;
  label: string;
  roleGroup: TitleRoleGroup;
  anchorTitle: string;
  broadDiscoveryQueries: string[];
  positiveKeywords: string[];
  negativeKeywords?: string[];
};

export type TitleConceptDefinition = {
  id: TitleConceptId;
  family: TitleRoleFamily;
  roleGroup?: TitleRoleGroup;
  canonicalTitle: string;
  aliases?: string[];
  abbreviations?: string[];
  adjacentConceptIds?: TitleConceptId[];
  broadDiscoveryQueries?: string[];
  negativeKeywords?: string[];
};

export type TitleAliasDefinition = {
  conceptId: TitleConceptId;
  family: TitleRoleFamily;
  roleGroup: TitleRoleGroup;
  canonicalTitle: string;
  kind: TitleAliasKind;
  phrase: string;
};

export type TitleAnalysis = {
  input: string;
  normalized: string;
  strippedNormalized: string;
  canonicalTitle: string;
  family?: TitleRoleFamily;
  roleGroup?: TitleRoleGroup;
  primaryConceptId?: TitleConceptId;
  matchedConceptIds: TitleConceptId[];
  aliasKind?: TitleAliasKind;
  matchedPhrase?: string;
  headWord?: string;
  tokens: string[];
  meaningfulTokens: string[];
  inferenceSource: "catalog" | "family" | "fallback" | "none";
  confidence: "high" | "medium" | "low" | "none";
};

export type TitleQueryVariant = {
  query: string;
  normalized: string;
  kind: TitleQueryVariantKind;
  family?: TitleRoleFamily;
  conceptId?: TitleConceptId;
  priority: number;
};

export type TitleMatchResult = {
  matches: boolean;
  mode: TitleMatchMode;
  threshold: number;
  tier: TitleMatchTier;
  score: number;
  canonicalQueryTitle: string;
  canonicalJobTitle: string;
  queryFamily?: TitleRoleFamily;
  jobFamily?: TitleRoleFamily;
  matchedConceptId?: TitleConceptId;
  matchedTerms: string[];
  explanation: string;
};
