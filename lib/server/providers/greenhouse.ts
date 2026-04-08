import "server-only";

import {
  buildLocationText,
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
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
  filterProviderSeeds,
  finalizeProviderResult,
  unsupportedProviderResult,
} from "@/lib/server/providers/shared";
import { defineProvider } from "@/lib/server/providers/types";

type GreenhouseApiResponse = {
  jobs?: Array<{
    id: number | string;
    title?: string;
    absolute_url?: string;
    updated_at?: string;
    first_published?: string;
    company_name?: string;
    content?: string;
    location?: { name?: string };
    offices?: Array<{ name?: string; location?: { name?: string | null } | null }>;
    departments?: Array<{ name?: string }>;
    metadata?: Array<{ name?: string; value?: string | null }>;
  }>;
};

type GreenhouseJob = NonNullable<GreenhouseApiResponse["jobs"]>[number];

const greenhouseStructuredExperiencePattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|level 2|level 3|\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe))\b/i;

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
  const boardUrl =
    input.boardUrl ??
    `https://boards.greenhouse.io/${input.companyToken}`;

  return buildSeed({
    title: input.job.title ?? "Untitled role",
    companyToken: input.companyToken,
    company: defaultCompanyName(
      input.companyToken,
      input.job.company_name ?? input.companyName,
    ),
    locationText,
    sourcePlatform: "greenhouse",
    sourceJobId: String(input.job.id),
    sourceUrl: input.job.absolute_url ?? boardUrl,
    applyUrl: input.job.absolute_url ?? boardUrl,
    canonicalUrl: input.job.absolute_url,
    postedAt: coercePostedAt(input.job.first_published ?? input.job.updated_at),
    rawSourceMetadata: {
      greenhouseJob: input.job,
      greenhouseStructuredExperienceHints: structuredExperienceHints,
      greenhouseDescriptionExperienceHint: descriptionExperienceHint || undefined,
    },
    discoveredAt: input.discoveredAt,
    structuredExperienceHints,
    descriptionExperienceHints: [descriptionExperienceHint],
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

      const boards = await runWithConcurrency(
        sources,
        async (source) => {
          const apiUrl = resolveGreenhouseApiUrl(source);
          const companyToken = resolveGreenhouseToken(source);
          const boardUrl = resolveGreenhouseBoardUrl(source);

          if (!apiUrl) {
            warnings.push(
              `Greenhouse source ${describeGreenhouseSource(source)} is missing a usable board token or URL.`,
            );
            return {
              fetchedCount: 0,
              jobs: [],
            };
          }

          const result = await safeFetchJson<GreenhouseApiResponse>(apiUrl, {
            fetchImpl: context.fetchImpl,
            method: "GET",
            headers: {
              Accept: "application/json",
            },
            cache: "no-store",
          });

          if (!result.ok) {
            warnings.push(formatGreenhouseFetchWarning(source, result));
            return {
              fetchedCount: 0,
              jobs: [],
              excludedByTitle: 0,
              excludedByLocation: 0,
            };
          }

          const jobs = result.data?.jobs ?? [];
          const normalizedJobs = jobs.map((job) =>
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
          const filteredJobs = filterProviderSeeds(normalizedJobs, context.filters);

          return {
            fetchedCount: jobs.length,
            jobs: filteredJobs.jobs,
            excludedByTitle: filteredJobs.excludedByTitle,
            excludedByLocation: filteredJobs.excludedByLocation,
          };
        },
        3,
      );

      const fetchedCount = boards.reduce((total, board) => total + board.fetchedCount, 0);
      const jobs = boards.flatMap((board) => board.jobs);
      const excludedByTitle = boards.reduce(
        (total, board) => total + (board.excludedByTitle ?? 0),
        0,
      );
      const excludedByLocation = boards.reduce(
        (total, board) => total + (board.excludedByLocation ?? 0),
        0,
      );

      return finalizeProviderResult({
        provider: "greenhouse",
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

function buildGreenhouseLocationText(job: GreenhouseJob) {
  const candidates = dedupeComparableStrings([
    job.metadata?.find((entry) => entry.name?.toLowerCase().includes("location"))?.value ?? undefined,
    job.location?.name,
    ...(job.offices ?? []).flatMap((office) => [office.location?.name ?? undefined, office.name]),
  ]);

  return buildLocationText(candidates) || "Location unavailable";
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
    .map((entry) => [entry.name?.trim(), entry.value?.trim()].filter(Boolean).join(": "))
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function collectStructuredNames(
  records: Array<{ name?: string }> | undefined,
) {
  return (records ?? [])
    .map((record) => record.name?.trim())
    .filter((value): value is string => Boolean(value))
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function stripGreenhouseMarkup(value?: string) {
  return (value ?? "")
    .replace(/&lt;\/?[^&]+&gt;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&(nbsp|amp|quot|apos|#39|#x27);/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeComparableStrings(values: Array<string | undefined | null>) {
  const seen = new Set<string>();
  const results: string[] = [];

  for (const value of values) {
    const trimmed = value?.trim();
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
  if (!value?.trim()) {
    return undefined;
  }

  try {
    const url = new URL(value);
    const segments = url.pathname.split("/").filter(Boolean);

    if (url.hostname === "boards.greenhouse.io") {
      return cleanString(segments[0]);
    }

    if (url.hostname === "boards-api.greenhouse.io") {
      return segments[0] === "v1" && segments[1] === "boards"
        ? cleanString(segments[2])
        : undefined;
    }

    return undefined;
  } catch {
    return undefined;
  }
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
