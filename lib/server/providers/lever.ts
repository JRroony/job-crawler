import "server-only";

import {
  inferExperienceLevel,
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  buildCanonicalLeverApiUrl,
  buildCanonicalLeverJobUrl,
} from "@/lib/server/discovery/lever-url";
import {
  type LeverDiscoveredSource,
  isLeverSource,
} from "@/lib/server/discovery/types";
import {
  safeFetchJson,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";
import {
  buildSeed,
  coercePostedAt,
  defaultCompanyName,
  filterProviderSeeds,
  finalizeProviderResult,
  unsupportedProviderResult,
} from "@/lib/server/providers/shared";
import {
  defineProvider,
  type NormalizedJobSeed,
} from "@/lib/server/providers/types";

type LeverPosting = {
  id?: string;
  text?: string;
  description?: string;
  descriptionPlain?: string;
  workplaceType?: string;
  lists?: Array<{
    text?: string;
    content?: string;
  }>;
  country?: string;
  hostedUrl?: string;
  applyUrl?: string;
  createdAt?: number;
  categories?: {
    location?: string;
    allLocations?: string[];
    commitment?: string;
    team?: string;
    department?: string;
  };
};

export function normalizeLeverJob(input: {
  siteToken: string;
  hostedUrl?: string;
  companyName?: string;
  discoveredAt: string;
  job: LeverPosting;
}) {
  const locationText =
    input.job.categories?.location ??
    input.job.categories?.allLocations?.[0] ??
    "Location unavailable";
  const explicitExperienceLevel = resolveLeverExplicitExperienceLevel(input.job);
  const structuredExperienceHints = collectLeverStructuredExperienceHints(input.job);
  const descriptionExperienceHints = collectLeverDescriptionExperienceHints(input.job);

  return buildSeed({
    title: input.job.text ?? "Untitled role",
    companyToken: input.siteToken,
    company: defaultCompanyName(input.siteToken, input.companyName),
    locationText,
    sourcePlatform: "lever",
    sourceJobId:
      input.job.id ??
      input.job.hostedUrl ??
      input.job.applyUrl ??
      input.hostedUrl ??
      input.siteToken,
    sourceUrl:
      input.job.hostedUrl ??
      input.hostedUrl ??
      `https://jobs.lever.co/${input.siteToken}`,
    applyUrl:
      input.job.applyUrl ??
      input.job.hostedUrl ??
      input.hostedUrl ??
      `https://jobs.lever.co/${input.siteToken}`,
    canonicalUrl: input.job.hostedUrl,
    postedAt: coercePostedAt(input.job.createdAt),
    rawSourceMetadata: {
      leverJob: input.job,
      leverStructuredExperienceHints: structuredExperienceHints,
      leverDescriptionExperienceHints: descriptionExperienceHints,
    },
    discoveredAt: input.discoveredAt,
    explicitCountry: input.job.country,
    explicitExperienceLevel,
    explicitExperienceSource: explicitExperienceLevel
      ? "structured_metadata"
      : undefined,
    explicitExperienceReasons: explicitExperienceLevel
      ? [`Lever commitment or workplace metadata explicitly indicates ${explicitExperienceLevel.replace("_", " ")}.`]
      : undefined,
    structuredExperienceHints,
    descriptionExperienceHints,
  });
}

export async function extractLeverJobFromDetailUrl(input: {
  detailUrl: string;
  siteToken: string;
  jobId: string;
  companyHint?: string;
  discoveredAt: string;
  fetchImpl: typeof fetch;
}): Promise<NormalizedJobSeed | undefined> {
  const result = await safeFetchJson<LeverPosting>(
    buildCanonicalLeverApiUrl(input.siteToken, input.jobId),
    {
      fetchImpl: input.fetchImpl,
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      cache: "no-store",
    },
  );

  if (!result.ok || !result.data) {
    return undefined;
  }

  return normalizeLeverJob({
    siteToken: input.siteToken,
    hostedUrl: buildCanonicalLeverJobUrl(input.siteToken, input.jobId),
    companyName: input.companyHint,
    discoveredAt: input.discoveredAt,
    job: result.data,
  });
}

export function createLeverProvider() {
  return defineProvider({
    provider: "lever",
    supportsSource: isLeverSource,
    async crawlSources(context, sources) {
      if (sources.length === 0) {
        return unsupportedProviderResult(
          "lever",
          "No discovered Lever sources are available.",
          sources.length,
        );
      }

      const warnings: string[] = [];
      const discoveredAt = context.now.toISOString();

      const sites = await runWithConcurrency(
        sources,
        async (source) => {
          const apiUrl = resolveLeverApiUrl(source);
          const siteToken = resolveLeverToken(source);
          const hostedUrl = resolveLeverHostedUrl(source);

          if (!apiUrl) {
            warnings.push(
              `Lever source ${describeLeverSource(source)} is missing a usable token or public JSON endpoint.`,
            );
            return {
              fetchedCount: 0,
              jobs: [],
            };
          }

          const result = await safeFetchJson<LeverPosting[]>(apiUrl, {
            fetchImpl: context.fetchImpl,
            method: "GET",
            headers: {
              Accept: "application/json",
            },
            cache: "no-store",
          });

          if (!result.ok) {
            warnings.push(formatLeverFetchWarning(source, result));
            return {
              fetchedCount: 0,
              jobs: [],
              excludedByTitle: 0,
              excludedByLocation: 0,
            };
          }

          const payload = result.data ?? [];
          const normalizedJobs = payload.map((job) =>
            normalizeLeverJob({
              siteToken:
                siteToken ??
                buildProviderCompanyToken(source.companyHint) ??
                "lever",
              hostedUrl,
              companyName: source.companyHint,
              discoveredAt,
              job,
            }),
          );
          const filteredJobs = filterProviderSeeds(normalizedJobs, context.filters);

          return {
            fetchedCount: payload.length,
            jobs: filteredJobs.jobs,
            excludedByTitle: filteredJobs.excludedByTitle,
            excludedByLocation: filteredJobs.excludedByLocation,
          };
        },
        3,
      );

      const fetchedCount = sites.reduce((total, site) => total + site.fetchedCount, 0);
      const jobs = sites.flatMap((site) => site.jobs);
      const excludedByTitle = sites.reduce(
        (total, site) => total + (site.excludedByTitle ?? 0),
        0,
      );
      const excludedByLocation = sites.reduce(
        (total, site) => total + (site.excludedByLocation ?? 0),
        0,
      );

      return finalizeProviderResult({
        provider: "lever",
        jobs,
        sourceCount: sources.length,
        fetchedCount,
        warnings,
        excludedByTitle,
        excludedByLocation,
      });
    },
  });
}

function resolveLeverExplicitExperienceLevel(job: LeverPosting) {
  return inferExperienceLevel(job.categories?.commitment);
}

function collectLeverStructuredExperienceHints(job: LeverPosting) {
  return [
    job.categories?.commitment,
    job.categories?.team,
    job.categories?.department,
    job.workplaceType,
  ].filter((value): value is string => Boolean(value?.trim()));
}

function collectLeverDescriptionExperienceHints(job: LeverPosting) {
  return [
    job.descriptionPlain,
    job.description,
    ...collectLeverListHints(job.lists),
  ].filter((value): value is string => Boolean(value?.trim()));
}

function collectLeverListHints(lists?: LeverPosting["lists"]) {
  return (lists ?? [])
    .flatMap((entry) => [entry.text, entry.content])
    .filter((value): value is string => Boolean(value?.trim()));
}

function resolveLeverApiUrl(source: LeverDiscoveredSource) {
  const token = resolveLeverToken(source);
  if (token) {
    return `https://api.lever.co/v0/postings/${token}?mode=json`;
  }

  return firstLeverUrl([source.apiUrl, source.url]);
}

function resolveLeverHostedUrl(source: LeverDiscoveredSource) {
  const token = resolveLeverToken(source);
  if (token) {
    return `https://jobs.lever.co/${token}`;
  }

  return firstLeverUrl([source.hostedUrl, source.url]) ?? source.url;
}

function resolveLeverToken(source: LeverDiscoveredSource) {
  return (
    cleanString(source.token) ??
    extractLeverTokenFromUrl(source.url) ??
    extractLeverTokenFromUrl(source.hostedUrl) ??
    extractLeverTokenFromUrl(source.apiUrl)
  );
}

function extractLeverTokenFromUrl(value?: string) {
  if (!value?.trim()) {
    return undefined;
  }

  try {
    const url = new URL(value);
    const segments = url.pathname.split("/").filter(Boolean);

    if (url.hostname === "jobs.lever.co") {
      return cleanString(segments[0]);
    }

    if (url.hostname === "api.lever.co") {
      return segments[0] === "v0" && segments[1] === "postings"
        ? cleanString(segments[2])
        : undefined;
    }

    return undefined;
  } catch {
    return undefined;
  }
}

function firstLeverUrl(values: Array<string | undefined>) {
  for (const value of values) {
    const trimmed = cleanString(value);
    if (!trimmed) {
      continue;
    }

    try {
      return new URL(trimmed).toString();
    } catch {
      continue;
    }
  }

  return undefined;
}

function buildProviderCompanyToken(companyHint?: string) {
  const comparable = normalizeComparableText(companyHint ?? "");
  return comparable ? comparable.replace(/\s+/g, "-") : undefined;
}

function formatLeverFetchWarning(
  source: LeverDiscoveredSource,
  result: Extract<SafeFetchResult, { ok: false }>,
) {
  if (
    (result.errorType === "http" || result.errorType === "rate_limit") &&
    result.statusCode !== undefined
  ) {
    return `Lever returned ${result.statusCode} for ${describeLeverSource(source)}.`;
  }

  return `Lever site ${describeLeverSource(source)} failed: ${result.message}`;
}

function describeLeverSource(source: LeverDiscoveredSource) {
  return (
    resolveLeverToken(source) ??
    cleanString(source.companyHint) ??
    cleanString(source.url) ??
    "unknown source"
  );
}

function cleanString(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
