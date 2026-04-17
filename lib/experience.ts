import type {
  ExperienceBand,
  ExperienceClassification,
  ExperienceLevel,
  ExperienceMatchMode,
} from "@/lib/types";
import { experienceClassificationVersion } from "@/lib/types";

export const currentExperienceClassificationVersion =
  experienceClassificationVersion;

export function resolveExperienceLevel(
  classification?:
    | Pick<ExperienceClassification, "explicitLevel" | "inferredLevel">
    | null,
) {
  return classification?.explicitLevel ?? classification?.inferredLevel;
}

export function resolveExperienceOutcome(
  classification?:
    | Pick<ExperienceClassification, "explicitLevel" | "inferredLevel">
    | null,
) {
  return resolveExperienceLevel(classification) ?? "unknown";
}

export function resolveExperienceBand(
  level?: ExperienceLevel | "unknown",
): ExperienceBand {
  if (!level || level === "unknown") {
    return "unknown";
  }

  if (level === "mid") {
    return "mid";
  }

  if (level === "senior") {
    return "senior";
  }

  if (level === "lead") {
    return "leadership";
  }

  if (level === "staff" || level === "principal") {
    return "advanced";
  }

  return "entry";
}

export type StoredExperienceMatchInput = {
  classification: Pick<
    ExperienceClassification,
    | "explicitLevel"
    | "inferredLevel"
    | "confidence"
    | "isUnspecified"
    | "reasons"
  >;
  selectedLevels: ExperienceLevel[];
  mode: ExperienceMatchMode;
  includeUnspecified: boolean;
};

export type StoredExperienceMatchResult = {
  matches: boolean;
  matchedLevel?: ExperienceLevel;
  explanation: string;
};

export function evaluateStoredExperienceMatch(
  input: StoredExperienceMatchInput,
): StoredExperienceMatchResult {
  const { classification, selectedLevels, mode, includeUnspecified } = input;

  if (selectedLevels.length === 0) {
    return {
      matches: true,
      explanation: "No experience filter is active for this search.",
    };
  }

  if (
    classification.explicitLevel &&
    selectedLevels.includes(classification.explicitLevel)
  ) {
    return {
      matches: true,
      matchedLevel: classification.explicitLevel,
      explanation: `Matched explicit experience level "${classification.explicitLevel}".`,
    };
  }

  if (
    mode !== "strict" &&
    classification.inferredLevel &&
    selectedLevels.includes(classification.inferredLevel)
  ) {
    if (mode === "broad") {
      return {
        matches: true,
        matchedLevel: classification.inferredLevel,
        explanation: `Matched inferred experience level "${classification.inferredLevel}" in broad mode.`,
      };
    }

    if (
      classification.confidence === "high" ||
      classification.confidence === "medium"
    ) {
      return {
        matches: true,
        matchedLevel: classification.inferredLevel,
        explanation: `Matched inferred experience level "${classification.inferredLevel}" with ${classification.confidence} confidence.`,
      };
    }
  }

  if (includeUnspecified && classification.isUnspecified) {
    return {
      matches: true,
      explanation: "Allowed an unspecified experience level for this search.",
    };
  }

  const resolvedLevel = resolveExperienceLevel(classification);
  const reasons = classification.reasons.filter(Boolean).join(" ");

  return {
    matches: false,
    matchedLevel: resolvedLevel,
    explanation: classification.isUnspecified
      ? "Rejected because the role did not provide a usable experience level and unspecified levels are not allowed."
      : `Rejected experience level "${resolvedLevel}" for selected levels ${selectedLevels.join(", ")}.${reasons ? ` ${reasons}` : ""}`,
  };
}
