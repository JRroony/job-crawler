import "server-only";

import {
  inferExperienceLevel,
  normalizeComparableText,
} from "@/lib/server/crawler/helpers";
import {
  buildCanonicalLeverApiUrl,
  buildCanonicalLeverJobUrl,
  parseLeverUrl,
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
} from "@/lib/server/providers/shared";
import {
  type NormalizedJobSeed,
  type ProviderExecutionContext,
} from "@/lib/server/providers/types";
import { createAdapterProvider } from "@/lib/server/providers/adapter";
import { getEnv } from "@/lib/server/env";

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
  const locationText = buildLeverLocationText(input.job);
  const explicitExperienceLevel = resolveLeverExplicitExperienceLevel(input.job);
  const structuredExperienceHints = collectLeverStructuredExperienceHints(input.job);
  const descriptionExperienceHints = collectLeverDescriptionExperienceHints(input.job);
  const sourceJobUrl =
    input.job.hostedUrl ??
    input.job.applyUrl ??
    (input.job.id ? buildCanonicalLeverJobUrl(input.siteToken, input.job.id) : undefined);
  const boardUrl = input.hostedUrl ?? `https://jobs.lever.co/${input.siteToken}`;

  return buildSeed({
    title: input.job.text ?? "",
    companyToken: input.siteToken,
    company: defaultCompanyName(input.siteToken, input.companyName),
    locationText,
    sourcePlatform: "lever",
    sourceJobId:
      input.job.id ??
      input.job.hostedUrl ??
      input.job.applyUrl ??
      "",
    sourceUrl: sourceJobUrl ?? boardUrl,
    applyUrl: input.job.applyUrl ?? sourceJobUrl ?? boardUrl,
    canonicalUrl: sourceJobUrl,
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
    explicitEmploymentType: input.job.categories?.commitment,
    structuredExperienceHints,
    descriptionExperienceHints,
    descriptionSnippet: input.job.descriptionPlain ?? input.job.description,
  });
}

function buildLeverLocationText(job: LeverPosting) {
  const locations = [
    job.categories?.location,
    ...(job.categories?.allLocations ?? []),
    job.country,
  ]
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value));
  const uniqueLocations = Array.from(new Set(locations));

  return uniqueLocations.length > 0
    ? uniqueLocations.join(" | ")
    : "Location unavailable";
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
  return createAdapterProvider({
    provider: "lever",
    supportsSource: isLeverSource,
    unsupportedMessage: "No discovered Lever sources are available.",
    concurrency: (context) => {
      const env = getEnv();
      return context.isBackgroundRun
        ? env.BACKGROUND_INGESTION_LEVER_SOURCE_CONCURRENCY
        : env.CRAWL_LEVER_SOURCE_CONCURRENCY;
    },
    async crawlSource(context, source) {
      return crawlLeverSource(context, source);
    },
  });
}

async function crawlLeverSource(
  context: ProviderExecutionContext,
  source: LeverDiscoveredSource,
) {
  await context.throwIfCanceled?.();
  const warnings: string[] = [];
  const dropReasons: string[] = [];
  const discoveredAt = context.now.toISOString();
  const apiUrl = resolveLeverApiUrl(source);
  const siteToken = resolveLeverToken(source);
  const hostedUrl = resolveLeverHostedUrl(source);
  const normalizedSiteToken =
    siteToken ?? buildProviderCompanyToken(source.companyHint) ?? "lever";
  let listFetchSucceeded = false;
  let usableSource = Boolean(apiUrl);

  let payload: LeverPosting[] = [];
  if (apiUrl) {
    await context.throwIfCanceled?.();
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
      dropReasons.push("list_fetch_failed");
    } else {
      listFetchSucceeded = true;
      payload = result.data ?? [];
    }
  } else {
    warnings.push(
      `Lever source ${describeLeverSource(source)} is missing a usable token or public JSON endpoint.`,
    );
    dropReasons.push("missing_api_url");
  }

  const jobs = payload.map((job) =>
    normalizeLeverJob({
      siteToken: normalizedSiteToken,
      hostedUrl,
      companyName: source.companyHint,
      discoveredAt,
      job,
    }),
  );

  const detailFallbackJob =
    jobs.length === 0 && source.jobId && siteToken
      ? await (async () => {
          await context.throwIfCanceled?.();
          return extractLeverJobFromDetailUrl({
          detailUrl:
            parseLeverUrl(source.url)?.canonicalJobUrl ??
            buildCanonicalLeverJobUrl(siteToken, source.jobId!),
          siteToken,
          jobId: source.jobId!,
          companyHint: source.companyHint,
          discoveredAt,
          fetchImpl: context.fetchImpl,
          });
        })()
      : undefined;

  if (payload.length === 0 && !detailFallbackJob) {
    dropReasons.push(source.jobId ? "detail_fallback_failed" : "empty_list_payload");
  }

  const normalizedJobs = detailFallbackJob ? [detailFallbackJob] : jobs;

  console.info("[lever:crawl-source]", {
    siteToken,
    apiUrl,
    hostedUrl,
    detailJobId: source.jobId,
    fetchedCount: payload.length,
    normalizedCount: normalizedJobs.length,
    usedDetailFallback: Boolean(detailFallbackJob),
    dropReasons,
  });

  if (normalizedJobs.length > 0) {
    await context.throwIfCanceled?.();
    await context.onBatch?.({
      provider: "lever",
      jobs: normalizedJobs,
      sourceCount: 1,
      fetchedCount: payload.length || (detailFallbackJob ? 1 : 0),
    });
  }

  return {
    fetchedCount: payload.length || (detailFallbackJob ? 1 : 0),
    fetchCount: (apiUrl ? 1 : 0) + (detailFallbackJob ? 1 : 0),
    jobs: normalizedJobs,
    warnings,
    parseSuccessCount: normalizedJobs.length,
    parseFailureCount: source.jobId && !detailFallbackJob && jobs.length === 0 ? 1 : 0,
    dropReasons,
    sourceSucceeded: listFetchSucceeded || Boolean(detailFallbackJob),
    sourceFailed: usableSource && !listFetchSucceeded && normalizedJobs.length === 0,
    sourceSkipped: !usableSource,
  };
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
