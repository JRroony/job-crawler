import { describe, expect, it } from "vitest";

import {
  buildCoverageFilters,
  databaseNameFromMongoUri,
  normalizeDiagnosticText,
  parseDiagnoseSearchArgs,
} from "@/scripts/diagnose-search-coverage";

describe("search coverage diagnostic script helpers", () => {
  it("parses the expected CLI arguments", () => {
    expect(
      parseDiagnoseSearchArgs([
        "--title",
        "machine learning engineer",
        "--location",
        "United States",
      ]),
    ).toEqual({
      title: "machine learning engineer",
      location: "United States",
    });
  });

  it("derives the same default database name convention as the app", () => {
    expect(databaseNameFromMongoUri("mongodb://127.0.0.1:27017/job_crawler")).toBe(
      "job_crawler",
    );
    expect(databaseNameFromMongoUri("mongodb://127.0.0.1:27017")).toBe(
      "job_crawler",
    );
  });

  it("normalizes diagnostic text for raw fallback probes", () => {
    expect(normalizeDiagnosticText("Remote - U.S.A.")).toBe("remote u s a");
  });

  it("matches likely US location evidence without requiring the app search pipeline", () => {
    const filters = buildCoverageFilters();

    expect(
      matchesFilter({ locationText: "Austin, TX" }, filters.rawUsLocationFallbackFilter),
    ).toBe(true);
    expect(
      matchesFilter({ locationText: "Remote - United States" }, filters.likelyUsFilter),
    ).toBe(true);
    expect(matchesFilter({ locationText: "Toronto, Canada" }, filters.likelyUsFilter)).toBe(
      false,
    );
  });

  it("matches AI/ML raw aliases and indexed role-family evidence", () => {
    const filters = buildCoverageFilters();

    expect(matchesFilter({ title: "Senior Applied ML Engineer" }, filters.likelyAimlFilter)).toBe(
      true,
    );
    expect(
      matchesFilter(
        { searchIndex: { titleFamily: "ai_ml_science" } },
        filters.indexedAimlRoleFamilyFilter,
      ),
    ).toBe(true);
    expect(matchesFilter({ title: "Product Manager" }, filters.likelyAimlFilter)).toBe(
      false,
    );
  });

  it("surfaces likely relevant jobs that are missing search index coverage", () => {
    const filters = buildCoverageFilters();
    const likelyRelevantUnindexedJob = {
      title: "LLM Engineer",
      country: "United States",
      locationText: "Remote - United States",
    };

    expect(matchesFilter(likelyRelevantUnindexedJob, filters.likelyUsAndAimlFilter)).toBe(
      true,
    );
    expect(
      matchesFilter(likelyRelevantUnindexedJob, {
        $and: [
          filters.likelyUsAndAimlFilter,
          filters.missingSearchIndexFilter,
          filters.missingLocationSearchKeysFilter,
          filters.missingTitleSearchKeysFilter,
        ],
      }),
    ).toBe(true);
  });
});

function matchesFilter(document: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    if (key === "$or") {
      if (!Array.isArray(condition) || !condition.some((entry) => matchesFilter(document, entry))) {
        return false;
      }
      continue;
    }

    if (key === "$and") {
      if (!Array.isArray(condition) || !condition.every((entry) => matchesFilter(document, entry))) {
        return false;
      }
      continue;
    }

    if (key === "$nor") {
      if (!Array.isArray(condition) || condition.some((entry) => matchesFilter(document, entry))) {
        return false;
      }
      continue;
    }

    const value = getPath(document, key);
    if (!matchesCondition(value, condition)) {
      return false;
    }
  }

  return true;
}

function matchesCondition(value: unknown, condition: unknown): boolean {
  if (isPlainObject(condition)) {
    for (const [operator, expected] of Object.entries(condition)) {
      if (operator === "$regex") {
        if (!(expected instanceof RegExp) || !expected.test(String(value ?? ""))) {
          return false;
        }
        continue;
      }

      if (operator === "$exists") {
        if ((value !== undefined) !== expected) {
          return false;
        }
        continue;
      }

      if (operator === "$ne") {
        if (value === expected) {
          return false;
        }
        continue;
      }

      if (operator === "$in") {
        if (!Array.isArray(expected)) {
          return false;
        }
        if (Array.isArray(value)) {
          return value.some((entry) => expected.includes(entry));
        }
        return expected.includes(value);
      }
    }

    return true;
  }

  if (Array.isArray(value)) {
    return value.includes(condition);
  }

  return value === condition;
}

function getPath(document: Record<string, unknown>, path: string): unknown {
  return path.split(".").reduce<unknown>((value, segment) => {
    if (Array.isArray(value) && /^\d+$/.test(segment)) {
      return value[Number(segment)];
    }

    return isPlainObject(value) ? value[segment] : undefined;
  }, document);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
