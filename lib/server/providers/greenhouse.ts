import "server-only";

import {
  buildLocationText,
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  buildCanonicalGreenhouseJobApiUrl,
  parseGreenhouseUrl,
} from "@/lib/server/discovery/greenhouse-url";
import {
  type GreenhouseDiscoveredSource,
  isGreenhouseSource,
} from "@/lib/server/discovery/types";
import {
  safeFetchJson,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";
import {
  buildSeed,
  coercePostedAt,
  defaultCompanyName,
  finalizeProviderResult,
  unsupportedProviderResult,
} from "@/lib/server/providers/shared";
import {
  type NormalizedJobSeed,
  type ProviderExecutionContext,
} from "@/lib/server/providers/types";
import { createAdapterProvider } from "@/lib/server/providers/adapter";
import {
  analyzeTitle,
  buildTitleQueryVariants,
  containsNormalizedPhrase,
  normalizeTitleText,
} from "@/lib/server/title-retrieval";

type GreenhouseApiResponse = {
  jobs?: Array<{
    id: number | string;
    title?: unknown;
    absolute_url?: unknown;
    updated_at?: unknown;
    first_published?: unknown;
    company_name?: unknown;
    content?: unknown;
    location?: { name?: unknown };
    offices?: Array<{ name?: unknown; location?: { name?: unknown | null } | null }>;
    departments?: Array<{ name?: unknown }>;
    metadata?: Array<{ name?: unknown; value?: unknown }>;
  }>;
};

type GreenhouseJob = NonNullable<GreenhouseApiResponse["jobs"]>[number];

type GreenhouseBoardFetchOutcome = {
  cacheState: "hit" | "miss" | "inflight_hit";
  result: SafeFetchResult<GreenhouseApiResponse>;
};

type GreenhouseBoardCacheEntry = {
  expiresAt: number;
  promise: Promise<{ result: SafeFetchResult<GreenhouseApiResponse>; expiresAt: number }>;
  value?: { result: SafeFetchResult<GreenhouseApiResponse>; expiresAt: number };
};

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerGreenhouseBoardCache: Map<string, GreenhouseBoardCacheEntry> | undefined;
}

const greenhouseStructuredExperiencePattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|fellow|member of technical staff|mts|lead|architect|manager|director|level [2-5]|ii|iii|iv|v|\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe))\b/i;
const greenhouseBoardCacheTtlMs = 5 * 60_000;
const greenhouseBoardFailureCacheTtlMs = 30_000;

export function normalizeGreenhouseJob(input: {
  companyToken: string;
  boardUrl?: string;
  companyName?: string;
  discoveredAt: string;
  job: GreenhouseJob;
}) {
  const locationText = buildGreenhouseLocationText(input.job);
  const structuredExperienceHints = buildGreenhouseStructuredExperienceHints(input.job);
  const descriptionExperienceHint = stripGreenhouseMarkup(input.job.content);
  const absoluteUrl = readGreenhouseText(input.job.absolute_url);
  const boardUrl =
    input.boardUrl ??
    `https://boards.greenhouse.io/${input.companyToken}`;

  return buildSeed({
    title: readGreenhouseText(input.job.title) ?? "Untitled role",
    companyToken: input.companyToken,
    company: defaultCompanyName(
      input.companyToken,
      readGreenhouseText(input.job.company_name) ?? input.companyName,
    ),
    locationText,
    sourcePlatform: "greenhouse",
    sourceJobId: String(input.job.id),
    sourceUrl: absoluteUrl ?? boardUrl,
    applyUrl: absoluteUrl ?? boardUrl,
    canonicalUrl: absoluteUrl,
    postedAt: coercePostedAt(
      readGreenhouseText(input.job.first_published) ??
      readGreenhouseText(input.job.updated_at),
    ),
    rawSourceMetadata: {
      greenhouseJob: input.job,
      greenhouseBoardToken: input.companyToken,
      greenhouseJobId: String(input.job.id),
      greenhouseStructuredExperienceHints: structuredExperienceHints,
      greenhouseDescriptionExperienceHint: descriptionExperienceHint || undefined,
    },
    discoveredAt: input.discoveredAt,
    structuredExperienceHints,
    descriptionExperienceHints: [descriptionExperienceHint],
    descriptionSnippet: descriptionExperienceHint,
  });
}

export async function extractGreenhouseJobFromDetailUrl(input: {
  detailUrl: string;
  boardSlug: string;
  jobId: string;
  companyHint?: string;
  discoveredAt: string;
  fetchImpl: typeof fetch;
}): Promise<NormalizedJobSeed | undefined> {
  const result = await safeFetchJson<GreenhouseJob | { job?: GreenhouseJob }>(
    buildCanonicalGreenhouseJobApiUrl(input.boardSlug, input.jobId),
    {
      fetchImpl: input.fetchImpl,
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      cache: "no-store",
    },
  );

  if (!result.ok) {
    return undefined;
  }

  const job = normalizeGreenhouseDetailPayload(result.data);
  if (!job) {
    return undefined;
  }

  return normalizeGreenhouseJob({
    companyToken: input.boardSlug,
    boardUrl: `https://boards.greenhouse.io/${input.boardSlug}`,
    companyName: input.companyHint,
    discoveredAt: input.discoveredAt,
    job,
  });
}

export function createGreenhouseProvider() {
  return createAdapterProvider({
    provider: "greenhouse",
    supportsSource: isGreenhouseSource,
    unsupportedMessage: "No discovered Greenhouse sources are available.",
    concurrency: (context) => resolveGreenhouseCrawlStrategy(context.filters.crawlMode).concurrency,
    dedupeSources: dedupeGreenhouseSources,
    async crawlSource(context, source) {
      return crawlGreenhouseSource(context, source);
    },
  });
}

async function crawlGreenhouseSource(
  context: ProviderExecutionContext,
  source: GreenhouseDiscoveredSource,
) {
  await context.throwIfCanceled?.();
  const warnings: string[] = [];
  const dropReasons: string[] = [];
  const discoveredAt = context.now.toISOString();
  const crawlStrategy = resolveGreenhouseCrawlStrategy(context.filters.crawlMode);
  const titlePreselector = buildGreenhouseRawTitlePreselector(
    context.filters.title,
    crawlStrategy.maxTitleVariants,
  );
  const sourceStartedMs = Date.now();
  const apiUrl = resolveGreenhouseApiUrl(source);
  const companyToken = resolveGreenhouseToken(source);
  const boardUrl = resolveGreenhouseBoardUrl(source);
  const sourceDescriptor = {
    token: companyToken,
    boardUrl,
    apiUrl,
    detailJobId: source.jobId,
  };

  if (!apiUrl) {
    const warning = `Greenhouse source ${describeGreenhouseSource(source)} is missing a usable board token or URL.`;
    warnings.push(warning);
    dropReasons.push("missing_api_url");
    console.warn("[greenhouse:crawl-source]", {
      ...sourceDescriptor,
      status: "missing_api_url",
      timingMs: { total: Date.now() - sourceStartedMs },
    });
    return {
      fetchedCount: 0,
      fetchCount: 0,
      jobs: [],
      warnings,
      parseFailureCount: 1,
      dropReasons,
    };
  }

  const fetchStartedMs = Date.now();
  const boardFetch = await fetchGreenhouseBoard(apiUrl, {
    fetchImpl: context.fetchImpl,
    timeoutMs: crawlStrategy.timeoutMs,
    retries: crawlStrategy.retries,
  });
  const result = boardFetch.result;
  const fetchMs = Date.now() - fetchStartedMs;

  if (!result.ok) {
    warnings.push(formatGreenhouseFetchWarning(source, result));
    dropReasons.push("board_fetch_failed");
  }

  const rawJobs = result.ok ? result.data?.jobs ?? [] : [];
  const preselectionStartedMs = Date.now();
  const preselectedRawJobs = rawJobs.filter((job) =>
    titlePreselector.matches(readGreenhouseText(job.title)),
  );
  const preselectionMs = Date.now() - preselectionStartedMs;
  const normalizedCompanyToken =
    companyToken ?? buildProviderCompanyToken(source.companyHint) ?? "greenhouse";
  const normalizationInput = rawJobs;
  const normalizationStartedMs = Date.now();
  const jobs = normalizationInput.map((job) =>
    normalizeGreenhouseJob({
      companyToken: normalizedCompanyToken,
      boardUrl,
      companyName: source.companyHint,
      discoveredAt,
      job,
    }),
  );
  const normalizationMs = Date.now() - normalizationStartedMs;

  const detailFallbackJob =
    jobs.length === 0 && source.jobId && companyToken
      ? await (async () => {
          await context.throwIfCanceled?.();
          return extractGreenhouseJobFromDetailUrl({
          detailUrl:
            parseGreenhouseUrl(source.url)?.canonicalJobUrl ??
            `https://job-boards.greenhouse.io/${companyToken}/jobs/${source.jobId}`,
          boardSlug: companyToken,
          jobId: source.jobId!,
          companyHint: source.companyHint,
          discoveredAt,
          fetchImpl: context.fetchImpl,
          });
        })()
      : undefined;

  if (source.jobId && !detailFallbackJob && jobs.length === 0) {
    dropReasons.push("detail_fallback_failed");
  } else if (rawJobs.length === 0 && !source.jobId) {
    dropReasons.push("empty_board_payload");
  }

  const normalizedJobs = detailFallbackJob ? [detailFallbackJob] : jobs;

  console.info("[greenhouse:crawl-source]", {
    ...sourceDescriptor,
    status: normalizedJobs.length > 0 ? "success" : "empty",
    cacheState: boardFetch.cacheState,
    fetchedCount: rawJobs.length,
    preselectedCount: preselectedRawJobs.length,
    normalizationInputCount: normalizationInput.length,
    normalizedCount: normalizedJobs.length,
    preselectionSkippedCount: Math.max(0, rawJobs.length - preselectedRawJobs.length),
    preselectionWouldHaveDroppedBoard: rawJobs.length > 0 && preselectedRawJobs.length === 0,
    usedDetailFallback: Boolean(detailFallbackJob),
    dropReasons,
    timingMs: {
      fetch: fetchMs,
      preselection: preselectionMs,
      normalization: normalizationMs,
      total: Date.now() - sourceStartedMs,
    },
  });

  if (normalizedJobs.length > 0) {
    await context.throwIfCanceled?.();
    await context.onBatch?.({
      provider: "greenhouse",
      jobs: normalizedJobs,
      sourceCount: 1,
      fetchedCount: rawJobs.length || (detailFallbackJob ? 1 : 0),
    });
  }

  return {
    fetchedCount: rawJobs.length || (detailFallbackJob ? 1 : 0),
    fetchCount: 1 + (detailFallbackJob ? 1 : 0),
    jobs: normalizedJobs,
    warnings,
    parseSuccessCount: normalizedJobs.length,
    parseFailureCount:
      Math.max(0, normalizationInput.length - jobs.length) +
      (source.jobId && !detailFallbackJob && jobs.length === 0 ? 1 : 0),
    dropReasons,
  };
}

function buildGreenhouseRawTitlePreselector(
  title: string,
  maxTitleVariants: number,
) {
  const queryAnalysis = analyzeTitle(title);
  const queryVariants = buildTitleQueryVariants(title, { maxQueries: maxTitleVariants });
  const normalizedPhrases = Array.from(
    new Set(
      queryVariants
        .map((variant) => normalizeTitleText(variant.query))
        .filter((value) => value.length > 0),
    ),
  ).sort((left, right) => right.length - left.length || left.localeCompare(right));
  const modifierTokens = queryAnalysis.modifierTokens.filter(Boolean);
  const headWord = queryAnalysis.headWord;

  return {
    variantCount: normalizedPhrases.length,
    sampleVariants: normalizedPhrases.slice(0, 8),
    matches(rawTitle?: string) {
      const normalizedTitle = normalizeTitleText(rawTitle);
      if (!normalizedTitle) {
        return false;
      }

      if (
        normalizedPhrases.some((phrase) =>
          containsNormalizedPhrase(normalizedTitle, phrase),
        )
      ) {
        return true;
      }

      const titleAnalysis = analyzeTitle(rawTitle ?? normalizedTitle);
      if (
        queryAnalysis.family &&
        titleAnalysis.family &&
        titleAnalysis.family === queryAnalysis.family
      ) {
        const sharedModifierCount = modifierTokens.filter((token) =>
          containsNormalizedPhrase(normalizedTitle, token),
        ).length;
        const isGenericSoftwareHead =
          queryAnalysis.family === "software_engineering" &&
          ["engineer", "developer", "architect", "staff"].includes(titleAnalysis.headWord ?? "");

        if (sharedModifierCount > 0 || isGenericSoftwareHead) {
          return true;
        }
      }

      if (!headWord || modifierTokens.length === 0) {
        return false;
      }

      if (!containsNormalizedPhrase(normalizedTitle, headWord)) {
        return false;
      }

      return modifierTokens.some((token) =>
        containsNormalizedPhrase(normalizedTitle, token),
      );
    },
  };
}

function buildGreenhouseLocationText(job: GreenhouseJob) {
  const candidates = dedupeComparableStrings([
    job.metadata?.find((entry) =>
      readGreenhouseText(entry.name)?.toLowerCase().includes("location"),
    )?.value ?? undefined,
    job.location?.name,
    ...(job.offices ?? []).flatMap((office) => [office.location?.name ?? undefined, office.name]),
  ]);

  return buildLocationText(candidates) || "Location unavailable";
}

function normalizeGreenhouseDetailPayload(
  payload: GreenhouseJob | { job?: GreenhouseJob } | undefined,
) {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }

  if ("id" in payload) {
    return payload as GreenhouseJob;
  }

  return payload.job;
}

function buildGreenhouseStructuredExperienceHints(job: GreenhouseJob) {
  return [
    ...collectStructuredMetadataHints(job.metadata),
    ...collectStructuredNames(job.departments),
    ...collectStructuredNames(job.offices),
  ];
}

function collectStructuredMetadataHints(
  metadata: GreenhouseJob["metadata"],
) {
  return (metadata ?? [])
    .map((entry) =>
      [
        readGreenhouseText(entry.name),
        readGreenhouseText(entry.value),
      ]
        .filter((value): value is string => Boolean(value))
        .join(": "),
    )
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function collectStructuredNames(
  records: Array<{ name?: unknown }> | undefined,
) {
  return (records ?? [])
    .map((record) => readGreenhouseText(record.name))
    .filter((value): value is string => Boolean(value))
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function stripGreenhouseMarkup(value?: unknown) {
  return (typeof value === "string" ? value : "")
    .replace(/&lt;\/?[^&]+&gt;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&(nbsp|amp|quot|apos|#39|#x27);/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeComparableStrings(values: Array<unknown>) {
  const seen = new Set<string>();
  const results: string[] = [];

  for (const value of values) {
    const trimmed = readGreenhouseText(value);
    if (!trimmed) {
      continue;
    }

    const comparable = normalizeComparableText(trimmed);
    if (!comparable || seen.has(comparable)) {
      continue;
    }

    seen.add(comparable);
    results.push(trimmed);
  }

  return results;
}

function readGreenhouseText(value: unknown) {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function resolveGreenhouseApiUrl(source: GreenhouseDiscoveredSource) {
  const token = resolveGreenhouseToken(source);
  if (token) {
    return `https://boards-api.greenhouse.io/v1/boards/${token}/jobs?content=true`;
  }

  return firstGreenhouseUrl([source.apiUrl, source.url, source.boardUrl]);
}

function resolveGreenhouseBoardUrl(source: GreenhouseDiscoveredSource) {
  const token = resolveGreenhouseToken(source);
  if (token) {
    return `https://boards.greenhouse.io/${token}`;
  }

  return firstGreenhouseUrl([source.boardUrl, source.url]) ?? source.url;
}

function resolveGreenhouseToken(source: GreenhouseDiscoveredSource) {
  return (
    cleanString(source.token) ??
    extractGreenhouseTokenFromUrl(source.url) ??
    extractGreenhouseTokenFromUrl(source.boardUrl) ??
    extractGreenhouseTokenFromUrl(source.apiUrl)
  );
}

function extractGreenhouseTokenFromUrl(value?: string) {
  return value ? parseGreenhouseUrl(value)?.boardSlug : undefined;
}

function firstGreenhouseUrl(values: Array<string | undefined>) {
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

function resolveGreenhouseCrawlStrategy(crawlMode?: string) {
  if (crawlMode === "fast") {
    return {
      concurrency: 16,
      timeoutMs: 1_800,
      retries: 0,
      maxTitleVariants: 18,
    };
  }

  if (crawlMode === "deep") {
    return {
      concurrency: 6,
      timeoutMs: 5_500,
      retries: 1,
      maxTitleVariants: 24,
    };
  }

  return {
    concurrency: 10,
    timeoutMs: 2_800,
    retries: 0,
    maxTitleVariants: 22,
  };
}

function dedupeGreenhouseSources(sources: readonly GreenhouseDiscoveredSource[]) {
  const deduped = new Map<string, GreenhouseDiscoveredSource>();

  for (const source of sources) {
    const dedupeKey =
      resolveGreenhouseToken(source) ??
      resolveGreenhouseApiUrl(source) ??
      resolveGreenhouseBoardUrl(source) ??
      source.url;

    if (!deduped.has(dedupeKey)) {
      deduped.set(dedupeKey, source);
    }
  }

  return Array.from(deduped.values());
}

async function fetchGreenhouseBoard(
  apiUrl: string,
  options: {
    fetchImpl: typeof fetch;
    timeoutMs: number;
    retries: number;
  },
): Promise<GreenhouseBoardFetchOutcome> {
  if (options.fetchImpl !== fetch) {
    return {
      cacheState: "miss",
      result: await safeFetchJson<GreenhouseApiResponse>(apiUrl, {
        fetchImpl: options.fetchImpl,
        method: "GET",
        headers: {
          Accept: "application/json",
        },
        cache: "no-store",
        timeoutMs: options.timeoutMs,
        retries: options.retries,
      }),
    };
  }

  const cache = getGreenhouseBoardCache();
  const now = Date.now();
  const cached = cache.get(apiUrl);

  if (cached?.value && cached.expiresAt > now) {
    return {
      cacheState: "hit",
      result: cached.value.result,
    };
  }

  if (cached?.promise && cached.expiresAt > now) {
    const settled = await cached.promise;
    return {
      cacheState: "inflight_hit",
      result: settled.result,
    };
  }

  const requestPromise = safeFetchJson<GreenhouseApiResponse>(apiUrl, {
    fetchImpl: options.fetchImpl,
    method: "GET",
    headers: {
      Accept: "application/json",
    },
    cache: "no-store",
    timeoutMs: options.timeoutMs,
    retries: options.retries,
  })
    .then((result) => {
      const expiresAt =
        Date.now() + (result.ok ? greenhouseBoardCacheTtlMs : greenhouseBoardFailureCacheTtlMs);
      const settled = {
        result,
        expiresAt,
      };

      cache.set(apiUrl, {
        expiresAt,
        promise: Promise.resolve(settled),
        value: settled,
      });

      return settled;
    })
    .catch((error) => {
      cache.delete(apiUrl);
      throw error;
    });

  cache.set(apiUrl, {
    expiresAt: now + greenhouseBoardFailureCacheTtlMs,
    promise: requestPromise,
  });

  const settled = await requestPromise;
  return {
    cacheState: "miss",
    result: settled.result,
  };
}

function getGreenhouseBoardCache() {
  if (!globalThis.__jobCrawlerGreenhouseBoardCache) {
    globalThis.__jobCrawlerGreenhouseBoardCache = new Map<
      string,
      GreenhouseBoardCacheEntry
    >();
  }

  return globalThis.__jobCrawlerGreenhouseBoardCache;
}

function formatGreenhouseFetchWarning(
  source: GreenhouseDiscoveredSource,
  result: Extract<SafeFetchResult, { ok: false }>,
) {
  if (
    (result.errorType === "http" || result.errorType === "rate_limit") &&
    result.statusCode !== undefined
  ) {
    return `Greenhouse returned ${result.statusCode} for ${describeGreenhouseSource(source)}.`;
  }

  return `Greenhouse board ${describeGreenhouseSource(source)} failed: ${result.message}`;
}

function describeGreenhouseSource(source: GreenhouseDiscoveredSource) {
  return (
    resolveGreenhouseToken(source) ??
    cleanString(source.companyHint) ??
    cleanString(source.url) ??
    "unknown source"
  );
}

function cleanString(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
