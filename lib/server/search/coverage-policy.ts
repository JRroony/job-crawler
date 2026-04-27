import "server-only";

import { analyzeTitle } from "@/lib/server/title-retrieval/analyze";
import type { AppEnv } from "@/lib/server/env";
import type { SearchFilters } from "@/lib/types";

type IndexedCoverageSearchInput = {
  candidateCount: number;
  matchedCount?: number;
  matches?: unknown[];
};

export type IndexedCoveragePolicy = {
  coverageTarget: number;
  reason: string;
  isCoverageSufficient: boolean;
  indexedCandidateCount: number;
  indexedMatchedCount: number;
  latestIndexedJobAgeMs?: number;
  titleBroadness: "broad" | "specific";
  locationBroadness: "country" | "state" | "city" | "unspecified";
  highDemandRole: boolean;
  targetJobCount: number;
};

export function resolveIndexedCoveragePolicy(
  filters: Pick<SearchFilters, "title" | "country" | "state" | "city" | "crawlMode">,
  indexedSearch: IndexedCoverageSearchInput,
  env: Pick<
    AppEnv,
    | "CRAWL_TARGET_JOB_COUNT"
    | "SEARCH_MIN_COVERAGE_FAST"
    | "SEARCH_MIN_COVERAGE_BALANCED"
    | "SEARCH_MIN_COVERAGE_DEEP"
    | "SEARCH_BROAD_COUNTRY_MIN_COVERAGE"
    | "SEARCH_HIGH_DEMAND_ROLE_MIN_COVERAGE"
  >,
  options: {
    latestIndexedJobAgeMs?: number;
  } = {},
): IndexedCoveragePolicy {
  const crawlMode = filters.crawlMode ?? "balanced";
  const titleAnalysis = analyzeTitle(filters.title);
  const country = normalizeCoverageText(filters.country);
  const state = normalizeCoverageText(filters.state);
  const city = normalizeCoverageText(filters.city);
  const isCountryWide = Boolean(country) && !state && !city;
  const isCityLevel = Boolean(city);
  const highDemandRole = isHighDemandRoleFamily(titleAnalysis.family);
  const hasSenioritySpecificity = titleAnalysis.seniorityTokens.length > 0;
  const meaningfulTokenCount = titleAnalysis.meaningfulTokens.length;
  const titleBroadness =
    !hasSenioritySpecificity && meaningfulTokenCount <= 3 ? "broad" : "specific";
  const locationBroadness = isCityLevel
    ? "city"
    : isCountryWide
      ? "country"
      : state
        ? "state"
        : "unspecified";
  const indexedMatchedCount =
    typeof indexedSearch.matchedCount === "number"
      ? indexedSearch.matchedCount
      : indexedSearch.matches?.length ?? 0;
  const targetJobCount = Math.max(1, Math.floor(env.CRAWL_TARGET_JOB_COUNT));
  const modeTarget = resolveModeCoverageTarget(crawlMode, env, targetJobCount);
  const reasons = [`${crawlMode}_mode_${locationBroadness}_search`];
  let coverageTarget = modeTarget;

  if (locationBroadness === "country") {
    coverageTarget = Math.max(
      coverageTarget,
      env.SEARCH_BROAD_COUNTRY_MIN_COVERAGE,
      Math.ceil(targetJobCount * 2.5),
    );
    reasons.push("broad_country_minimum");
  } else if (locationBroadness === "state") {
    coverageTarget = Math.max(coverageTarget, Math.ceil(targetJobCount * 1.4));
    reasons.push("state_scope_dynamic_target");
  } else if (locationBroadness === "city") {
    const cityCoverageRatio =
      crawlMode === "deep"
        ? titleBroadness === "specific"
          ? 0.27
          : 0.3
        : 0.16;
    coverageTarget = Math.min(
      coverageTarget,
      Math.max(1, Math.ceil(targetJobCount * cityCoverageRatio)),
    );
    reasons.push(
      titleBroadness === "specific"
        ? "specific_title_city_dynamic_target"
        : "broad_title_city_dynamic_target",
    );
  }

  if (highDemandRole) {
    if (locationBroadness === "country") {
      coverageTarget = Math.max(
        coverageTarget,
        env.SEARCH_HIGH_DEMAND_ROLE_MIN_COVERAGE,
        Math.ceil(targetJobCount * 4),
      );
      reasons.push("high_demand_role_country_minimum");
    } else if (locationBroadness === "state") {
      coverageTarget = Math.max(coverageTarget, Math.ceil(targetJobCount * 2));
      reasons.push("high_demand_role_state_minimum");
    } else {
      reasons.push("high_demand_role");
    }
  }

  if (titleBroadness === "broad" && locationBroadness !== "city") {
    coverageTarget = Math.max(coverageTarget, Math.ceil(targetJobCount * 1.25));
    reasons.push("broad_title_dynamic_target");
  } else if (titleBroadness === "specific") {
    reasons.push("specific_title");
  }

  if (crawlMode === "deep") {
    coverageTarget = Math.max(coverageTarget, env.SEARCH_MIN_COVERAGE_DEEP);
    reasons.push("deep_mode_highest_target");
  }

  if (indexedMatchedCount === 0) {
    reasons.push("empty_indexed_results_always_insufficient");
  }

  if (typeof options.latestIndexedJobAgeMs === "number") {
    reasons.push(`latest_age_ms:${options.latestIndexedJobAgeMs}`);
  }

  reasons.push(`target_job_count:${targetJobCount}`);
  reasons.push(`candidates:${indexedSearch.candidateCount}`);
  reasons.push(`matches:${indexedMatchedCount}`);

  return {
    coverageTarget,
    reason: reasons.join("|"),
    isCoverageSufficient: indexedMatchedCount > 0 && indexedMatchedCount >= coverageTarget,
    indexedCandidateCount: indexedSearch.candidateCount,
    indexedMatchedCount,
    latestIndexedJobAgeMs: options.latestIndexedJobAgeMs,
    titleBroadness,
    locationBroadness,
    highDemandRole,
    targetJobCount,
  };
}

function resolveModeCoverageTarget(
  crawlMode: SearchFilters["crawlMode"] | undefined,
  env: Pick<
    AppEnv,
    | "SEARCH_MIN_COVERAGE_FAST"
    | "SEARCH_MIN_COVERAGE_BALANCED"
    | "SEARCH_MIN_COVERAGE_DEEP"
  >,
  targetJobCount: number,
) {
  if (crawlMode === "fast") {
    return Math.max(env.SEARCH_MIN_COVERAGE_FAST, Math.ceil(targetJobCount * 0.4));
  }

  if (crawlMode === "deep") {
    return Math.max(env.SEARCH_MIN_COVERAGE_DEEP, Math.ceil(targetJobCount * 2));
  }

  return Math.max(env.SEARCH_MIN_COVERAGE_BALANCED, targetJobCount);
}

function isHighDemandRoleFamily(family: ReturnType<typeof analyzeTitle>["family"]) {
  return (
    family === "ai_ml_science" ||
    family === "software_engineering" ||
    family === "data_platform" ||
    family === "data_analytics" ||
    family === "product"
  );
}

function normalizeCoverageText(value?: string) {
  return value?.trim().toLowerCase() ?? "";
}
