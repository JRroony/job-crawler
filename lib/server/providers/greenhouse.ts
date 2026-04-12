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
  defineProvider,
  type NormalizedJobSeed,
} from "@/lib/server/providers/types";
import {
  analyzeTitle,
  buildTitleQueryVariants,
  containsNormalizedPhrase,
  getTitleMatchResult,
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

const greenhouseStructuredExperiencePattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|fellow|member of technical staff|mts|lead|architect|manager|director|level [2-5]|ii|iii|iv|v|\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe))\b/i;
const greenhouseSmallBoardFallbackThreshold = 12;

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
  return defineProvider({
    provider: "greenhouse",
    supportsSource: isGreenhouseSource,
    async crawlSources(context, sources) {
      if (sources.length === 0) {
        return unsupportedProviderResult(
          "greenhouse",
          "No discovered Greenhouse sources are available.",
          sources.length,
        );
      }

      const warnings: string[] = [];
      const discoveredAt = context.now.toISOString();
      const crawlStrategy = resolveGreenhouseCrawlStrategy(context.filters.crawlMode);
      const titlePreselector = buildGreenhouseRawTitlePreselector(
        context.filters.title,
        crawlStrategy.maxTitleVariants,
      );
      const providerStartedMs = Date.now();

      const boards = await runWithConcurrency(
        sources,
        async (source) => {
          const sourceStartedMs = Date.now();
          const apiUrl = resolveGreenhouseApiUrl(source);
          const companyToken = resolveGreenhouseToken(source);
          const boardUrl = resolveGreenhouseBoardUrl(source);
          const sourceDescriptor = {
            token: companyToken,
            boardUrl,
            apiUrl,
          };

          if (!apiUrl) {
            warnings.push(
              `Greenhouse source ${describeGreenhouseSource(source)} is missing a usable board token or URL.`,
            );
            console.warn("[greenhouse:crawl-source]", {
              ...sourceDescriptor,
              status: "missing_api_url",
              timingMs: {
                total: Date.now() - sourceStartedMs,
              },
            });
            return {
              fetchedCount: 0,
              preselectedCount: 0,
              jobs: [],
              fetchMs: 0,
              preselectionMs: 0,
              normalizationMs: 0,
            };
          }

          const fetchStartedMs = Date.now();
          const result = await safeFetchJson<GreenhouseApiResponse>(apiUrl, {
            fetchImpl: context.fetchImpl,
            method: "GET",
            headers: {
              Accept: "application/json",
            },
            cache: "no-store",
            timeoutMs: crawlStrategy.timeoutMs,
            retries: crawlStrategy.retries,
          });
          const fetchMs = Date.now() - fetchStartedMs;

          if (!result.ok) {
            warnings.push(formatGreenhouseFetchWarning(source, result));
            console.warn("[greenhouse:crawl-source]", {
              ...sourceDescriptor,
              status: "fetch_failed",
              errorType: result.errorType,
              statusCode: result.statusCode,
              message: result.message,
              timingMs: {
                fetch: fetchMs,
                total: Date.now() - sourceStartedMs,
              },
            });
            return {
              fetchedCount: 0,
              preselectedCount: 0,
              jobs: [],
              fetchMs,
              preselectionMs: 0,
              normalizationMs: 0,
            };
          }

          const rawJobs = result.data?.jobs ?? [];
          const preselectionStartedMs = Date.now();
          const preselectedRawJobs = rawJobs.filter((job) =>
            titlePreselector.matches(readGreenhouseText(job.title)),
          );
          const preselectionMs = Date.now() - preselectionStartedMs;
          const shouldFallbackToSmallBoard =
            preselectedRawJobs.length === 0 &&
            rawJobs.length > 0 &&
            rawJobs.length <= greenhouseSmallBoardFallbackThreshold;
          const normalizationInput = shouldFallbackToSmallBoard ? rawJobs : preselectedRawJobs;

          const normalizationStartedMs = Date.now();
          const jobs = normalizationInput.map((job) =>
            normalizeGreenhouseJob({
              companyToken:
                companyToken ??
                buildProviderCompanyToken(source.companyHint) ??
                "greenhouse",
              boardUrl,
              companyName: source.companyHint,
              discoveredAt,
              job,
            }),
          );
          const normalizationMs = Date.now() - normalizationStartedMs;

          console.info("[greenhouse:crawl-source]", {
            ...sourceDescriptor,
            status: "success",
            fetchedCount: rawJobs.length,
            preselectedCount: preselectedRawJobs.length,
            normalizationInputCount: normalizationInput.length,
            normalizedCount: jobs.length,
            preselectionSkippedCount: Math.max(0, rawJobs.length - preselectedRawJobs.length),
            usedSmallBoardFallback: shouldFallbackToSmallBoard,
            timingMs: {
              fetch: fetchMs,
              preselection: preselectionMs,
              normalization: normalizationMs,
              total: Date.now() - sourceStartedMs,
            },
          });

          return {
            fetchedCount: rawJobs.length,
            preselectedCount: preselectedRawJobs.length,
            jobs,
            fetchMs,
            preselectionMs,
            normalizationMs,
          };
        },
        crawlStrategy.concurrency,
      );

      const fetchedCount = boards.reduce((total, board) => total + board.fetchedCount, 0);
      const preselectedCount = boards.reduce(
        (total, board) => total + (board.preselectedCount ?? 0),
        0,
      );
      const jobs = boards.flatMap((board) => board.jobs);
      const fetchMs = boards.reduce((total, board) => total + (board.fetchMs ?? 0), 0);
      const preselectionMs = boards.reduce(
        (total, board) => total + (board.preselectionMs ?? 0),
        0,
      );
      const normalizationMs = boards.reduce(
        (total, board) => total + (board.normalizationMs ?? 0),
        0,
      );

      console.info("[greenhouse:crawl-summary]", {
        sourceCount: sources.length,
        fetchedCount,
        preselectedCount,
        normalizedCount: jobs.length,
        preselectionSkippedCount: Math.max(0, fetchedCount - preselectedCount),
        warningCount: warnings.length,
        concurrency: crawlStrategy.concurrency,
        titleVariantCount: titlePreselector.variantCount,
        sampleTitleVariants: titlePreselector.sampleVariants,
        timingMs: {
          fetch: fetchMs,
          preselection: preselectionMs,
          normalization: normalizationMs,
          total: Date.now() - providerStartedMs,
        },
      });

      return finalizeProviderResult({
        provider: "greenhouse",
        jobs,
        sourceCount: sources.length,
        fetchedCount,
        warnings,
      });
    },
  });
}

function buildGreenhouseRawTitlePreselector(
  title: string,
  maxTitleVariants: number,
) {
  const normalizedQueryTitle = normalizeTitleText(title);
  const queryAnalysis = analyzeTitle(title);
  const normalizedPhrases = Array.from(
    new Set(
      buildTitleQueryVariants(title, { maxQueries: maxTitleVariants })
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

      const balancedMatch = getTitleMatchResult(rawTitle ?? normalizedTitle, normalizedQueryTitle, {
        mode: "balanced",
      });
      if (balancedMatch.matches) {
        return true;
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
      concurrency: 8,
      timeoutMs: 4_000,
      retries: 0,
      maxTitleVariants: 14,
    };
  }

  if (crawlMode === "deep") {
    return {
      concurrency: 4,
      timeoutMs: 6_500,
      retries: 1,
      maxTitleVariants: 20,
    };
  }

  return {
    concurrency: 6,
    timeoutMs: 5_000,
    retries: 1,
    maxTitleVariants: 18,
  };
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
