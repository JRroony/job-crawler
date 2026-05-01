import "server-only";

import {
  inferExperienceLevel,
  normalizeComparableText,
} from "@/lib/server/crawler/helpers";
import { buildCanonicalAshbyJobUrl, parseAshbyUrl } from "@/lib/server/discovery/ashby-url";
import {
  type AshbyDiscoveredSource,
  isAshbySource,
} from "@/lib/server/discovery/types";
import {
  safeFetchText,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";
import {
  buildSeed,
  coercePostedAt,
  deepCollect,
  extractWindowAppData,
  extractNextData,
  firstString,
} from "@/lib/server/providers/shared";
import { type ProviderExecutionContext } from "@/lib/server/providers/types";
import { createAdapterProvider } from "@/lib/server/providers/adapter";

type AshbyCandidate = {
  id?: string;
  title?: string;
  location?: string;
  locationName?: string;
  jobUrl?: string;
  absoluteUrl?: string;
  url?: string;
  employmentType?: string;
  departmentName?: string;
  teamName?: string;
  seniority?: string;
  descriptionHtml?: string;
  descriptionPlain?: string;
  jobDescription?: string;
  requirements?: string;
  summary?: string;
  createdAt?: string;
  publishedAt?: string;
  updatedAt?: string;
};

export function normalizeAshbyCandidate(input: {
  companyToken: string;
  boardUrl?: string;
  companyName?: string;
  discoveredAt: string;
  candidate: AshbyCandidate;
}) {
  const boardUrl =
    input.boardUrl ??
    `https://jobs.ashbyhq.com/${input.companyToken}`;
  const explicitExperienceLevel = resolveAshbyExplicitExperienceLevel(input.candidate);
  const structuredExperienceHints = collectAshbyStructuredExperienceHints(input.candidate);
  const descriptionExperienceHints = collectAshbyDescriptionExperienceHints(input.candidate);
  const sourceUrl =
    input.candidate.jobUrl ??
    input.candidate.absoluteUrl ??
    input.candidate.url ??
    buildAshbyCandidateUrl(input.companyToken, input.candidate.id) ??
    "";

  return buildSeed({
    title: input.candidate.title ?? "",
    companyToken: input.companyToken,
    company: input.companyName,
    locationText:
      input.candidate.locationName ??
      input.candidate.location ??
      "Location unavailable",
    sourcePlatform: "ashby",
    sourceJobId: input.candidate.id ?? sourceUrl,
    sourceUrl,
    applyUrl: sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(
      input.candidate.publishedAt ??
        input.candidate.createdAt ??
        input.candidate.updatedAt,
    ),
    rawSourceMetadata: {
      ashbyJob: input.candidate,
      ashbyStructuredExperienceHints: structuredExperienceHints,
      ashbyDescriptionExperienceHints: descriptionExperienceHints,
    },
    discoveredAt: input.discoveredAt,
    explicitExperienceLevel,
    explicitExperienceSource: explicitExperienceLevel
      ? "structured_metadata"
      : undefined,
    explicitExperienceReasons: explicitExperienceLevel
      ? [`Ashby metadata explicitly indicates ${explicitExperienceLevel.replace("_", " ")}.`]
      : undefined,
    explicitEmploymentType: input.candidate.employmentType,
    explicitSeniority: input.candidate.seniority,
    structuredExperienceHints,
    descriptionExperienceHints,
    descriptionSnippet:
      input.candidate.summary ??
      input.candidate.descriptionPlain ??
      input.candidate.jobDescription ??
      input.candidate.descriptionHtml,
  });
}

export async function extractAshbyJobFromDetailUrl(input: {
  detailUrl: string;
  companyToken: string;
  companyHint?: string;
  discoveredAt: string;
  fetchImpl: typeof fetch;
}) {
  const result = await safeFetchText(input.detailUrl, {
    fetchImpl: input.fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
    cache: "no-store",
    retries: 1,
  });

  if (!result.ok || !result.data) {
    return undefined;
  }

  const candidates = extractAshbyCandidates(result.data, input.companyToken);
  const normalizedDetailUrl = normalizeComparableText(input.detailUrl);
  const matchedCandidate =
    candidates.find((candidate) => {
      const candidateUrl = candidate.jobUrl ?? candidate.absoluteUrl ?? candidate.url;
      return candidateUrl
        ? normalizeComparableText(candidateUrl) === normalizedDetailUrl
        : false;
    }) ??
    candidates.find((candidate) =>
      candidate.id
        ? normalizedDetailUrl.includes(normalizeComparableText(candidate.id))
        : false,
    ) ??
    candidates[0];

  if (!matchedCandidate) {
    return undefined;
  }

  return normalizeAshbyCandidate({
    companyToken: input.companyToken,
    boardUrl: `https://jobs.ashbyhq.com/${input.companyToken}`,
    companyName: input.companyHint,
    discoveredAt: input.discoveredAt,
    candidate: matchedCandidate,
  });
}

export function createAshbyProvider() {
  return createAdapterProvider({
    provider: "ashby",
    supportsSource: isAshbySource,
    unsupportedMessage: "No discovered Ashby sources are available.",
    concurrency: 2,
    async crawlSource(context, source) {
      return crawlAshbySource(context, source);
    },
  });
}

async function crawlAshbySource(
  context: ProviderExecutionContext,
  source: AshbyDiscoveredSource,
) {
  await context.throwIfCanceled?.();
  const warnings: string[] = [];
  const dropReasons: string[] = [];
  const discoveredAt = context.now.toISOString();
  const boardUrl = resolveAshbyBoardUrl(source);
  const companyToken = resolveAshbyToken(source);
  const normalizedCompanyToken =
    companyToken ?? buildProviderCompanyToken(source.companyHint) ?? "ashby";

  if (!boardUrl) {
    warnings.push(
      `Ashby source ${describeAshbySource(source)} is missing a usable board URL.`,
    );
    dropReasons.push("missing_board_url");
    return {
      fetchedCount: 0,
      fetchCount: 0,
      jobs: [],
      warnings,
      parseFailureCount: 1,
      dropReasons,
    };
  }

  const result = await safeFetchText(boardUrl, {
    fetchImpl: context.fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
    cache: "no-store",
  });

  if (!result.ok) {
    warnings.push(formatAshbyFetchWarning(source, result));
    dropReasons.push("board_fetch_failed");
  }

  const extracted = result.ok
    ? extractAshbyCandidates(result.data ?? "", normalizedCompanyToken)
    : [];
  const jobs = extracted.map((candidate) =>
    normalizeAshbyCandidate({
      companyToken: normalizedCompanyToken,
      boardUrl,
      companyName: source.companyHint,
      discoveredAt,
      candidate,
    }),
  );

  const detailFallbackJob =
    jobs.length === 0 && companyToken && source.jobId
      ? await (async () => {
          await context.throwIfCanceled?.();
          return extractAshbyJobFromDetailUrl({
          detailUrl:
            parseAshbyUrl(source.url)?.canonicalJobUrl ??
            buildCanonicalAshbyJobUrl(companyToken, source.jobId!),
          companyToken,
          companyHint: source.companyHint,
          discoveredAt,
          fetchImpl: context.fetchImpl,
          });
        })()
      : undefined;

  if (extracted.length === 0 && !detailFallbackJob) {
    dropReasons.push(source.jobId ? "detail_fallback_failed" : "empty_board_payload");
  }

  const normalizedJobs = detailFallbackJob ? [detailFallbackJob] : jobs;

  console.info("[ashby:crawl-source]", {
    companyToken,
    boardUrl,
    detailJobId: source.jobId,
    fetchedCount: extracted.length,
    normalizedCount: normalizedJobs.length,
    usedDetailFallback: Boolean(detailFallbackJob),
    dropReasons,
  });

  if (normalizedJobs.length > 0) {
    await context.throwIfCanceled?.();
    await context.onBatch?.({
      provider: "ashby",
      jobs: normalizedJobs,
      sourceCount: 1,
      fetchedCount: extracted.length || (detailFallbackJob ? 1 : 0),
    });
  }

  return {
    fetchedCount: extracted.length || (detailFallbackJob ? 1 : 0),
    fetchCount: 1 + (detailFallbackJob ? 1 : 0),
    jobs: normalizedJobs,
    warnings,
    parseSuccessCount: normalizedJobs.length,
    parseFailureCount: source.jobId && !detailFallbackJob && jobs.length === 0 ? 1 : 0,
    dropReasons,
  };
}

function extractAshbyCandidates(html: string, companyToken: string): AshbyCandidate[] {
  const appDataCandidates = extractAshbyCandidatesFromAppData(html, companyToken);
  if (appDataCandidates) {
    return appDataCandidates;
  }

  const nextData = extractNextData(html);
  const collected = nextData
    ? deepCollect(nextData, (record) => {
        const id = firstString(record, ["id", "jobId", "slug"]);
        const title = firstString(record, ["title", "jobTitle", "name"]);
        const url = firstString(record, ["jobUrl", "absoluteUrl", "url"]);
        return Boolean(title && (id || (url && String(url).includes(companyToken))));
      }).map(
        (record) =>
          ({
            id: firstString(record, ["id", "jobId", "slug"]),
            title: firstString(record, ["title", "jobTitle", "name"]),
            location:
              firstString(record, ["locationName", "location", "jobLocationText"]) ??
              "Location unavailable",
            employmentType: firstString(record, ["employmentType", "jobType"]),
            departmentName: firstString(record, ["departmentName", "department"]),
            teamName: firstString(record, ["teamName", "team"]),
            seniority: firstString(record, ["seniority", "level"]),
            descriptionHtml: firstString(record, ["descriptionHtml", "description"]),
            descriptionPlain: firstString(record, ["descriptionPlain"]),
            jobDescription: firstString(record, ["jobDescription", "overview"]),
            requirements: firstString(record, ["requirements", "qualifications"]),
            summary: firstString(record, ["summary", "aboutRole"]),
            createdAt: firstString(record, ["createdAt", "datePosted"]),
            publishedAt: firstString(record, ["publishedAt"]),
            updatedAt: firstString(record, ["updatedAt"]),
            jobUrl:
              firstString(record, ["jobUrl", "absoluteUrl", "url"]) ??
              buildAshbyCandidateUrl(companyToken, firstString(record, ["id", "jobId", "slug"])),
          }) satisfies AshbyCandidate,
      )
    : [];

  if (collected.length > 0) {
    return collected;
  }

  const links = Array.from(
    html.matchAll(
      new RegExp(
        `href=["'](https:\\/\\/jobs\\.ashbyhq\\.com\\/${companyToken}[^"']+)["']`,
        "gi",
      ),
    ),
  )
    .map((match) => match[1])
    .filter(Boolean);

  return links.map(
    (link) =>
      ({
        id: firstAshbyJobPathSegment(link),
        location: "Location unavailable",
        jobUrl: link,
      }) satisfies AshbyCandidate,
  );
}

function extractAshbyCandidatesFromAppData(
  html: string,
  companyToken: string,
): AshbyCandidate[] | undefined {
  const appData = extractWindowAppData(html) as
    | {
        jobBoard?: {
          jobPostings?: Array<Record<string, unknown>>;
        };
      }
    | undefined;

  const postings = appData?.jobBoard?.jobPostings;
  const postingCandidates = Array.isArray(postings)
    ? postings
    : deepCollect(appData, (record) =>
        Boolean(
          firstString(record, ["id", "jobId", "slug", "jobUrl", "absoluteUrl", "url"]) &&
            (firstString(record, ["title", "jobTitle", "name"]) ||
              firstString(record, ["locationName", "location", "employmentType"])),
        ),
      );

  if (postingCandidates.length === 0) {
    return undefined;
  }

  return postingCandidates
    .filter((posting) =>
      Boolean(firstString(posting, ["id", "jobId", "slug", "jobUrl", "absoluteUrl", "url"])),
    )
    .map((posting) => {
      const postingId = firstString(posting, ["id", "jobId", "slug"]);
      const sourceUrl =
        firstString(posting, ["jobUrl", "absoluteUrl", "url"]) ??
        buildAshbyCandidateUrl(companyToken, postingId);

      return {
        id: postingId,
        title: firstString(posting, ["title", "jobTitle", "name"]),
        locationName: firstString(posting, ["locationName", "location"]),
        employmentType: firstString(posting, ["employmentType"]),
        departmentName: firstString(posting, ["departmentName", "department"]),
        teamName: firstString(posting, ["teamName", "team"]),
        seniority: firstString(posting, ["seniority", "level"]),
        descriptionHtml: firstString(posting, ["descriptionHtml", "description"]),
        descriptionPlain: firstString(posting, ["descriptionPlain"]),
        jobDescription: firstString(posting, ["jobDescription", "overview"]),
        requirements: firstString(posting, ["requirements", "qualifications"]),
        summary: firstString(posting, ["summary", "aboutRole"]),
        publishedAt: firstString(posting, ["publishedDate", "publishedAt"]),
        updatedAt: firstString(posting, ["updatedAt"]),
        jobUrl: sourceUrl,
      } satisfies AshbyCandidate;
    });
}

function buildAshbyCandidateUrl(companyToken: string, jobPath?: string) {
  return jobPath ? buildCanonicalAshbyJobUrl(companyToken, jobPath) : undefined;
}

function firstAshbyJobPathSegment(value: string) {
  try {
    const url = new URL(value);
    return cleanString(url.pathname.split("/").filter(Boolean)[1]);
  } catch {
    return undefined;
  }
}

function resolveAshbyExplicitExperienceLevel(candidate: AshbyCandidate) {
  return inferExperienceLevel(candidate.seniority, candidate.employmentType);
}

function collectAshbyStructuredExperienceHints(candidate: AshbyCandidate) {
  return [
    candidate.employmentType,
    candidate.departmentName,
    candidate.teamName,
    candidate.seniority,
  ].filter((value): value is string => Boolean(value?.trim()));
}

function collectAshbyDescriptionExperienceHints(candidate: AshbyCandidate) {
  return [
    candidate.descriptionPlain,
    candidate.descriptionHtml,
    candidate.jobDescription,
    candidate.requirements,
    candidate.summary,
  ].filter((value): value is string => Boolean(value?.trim()));
}

function resolveAshbyBoardUrl(source: AshbyDiscoveredSource) {
  const token = resolveAshbyToken(source);
  if (token) {
    return `https://jobs.ashbyhq.com/${token}`;
  }

  return firstAshbyUrl([source.boardUrl, source.url]) ?? source.url;
}

function resolveAshbyToken(source: AshbyDiscoveredSource) {
  return (
    cleanString(source.token) ??
    extractAshbyTokenFromUrl(source.url) ??
    extractAshbyTokenFromUrl(source.boardUrl)
  );
}

function extractAshbyTokenFromUrl(value?: string) {
  if (!value?.trim()) {
    return undefined;
  }

  try {
    const url = new URL(value);
    if (url.hostname !== "jobs.ashbyhq.com") {
      return undefined;
    }

    return cleanString(url.pathname.split("/").filter(Boolean)[0]);
  } catch {
    return undefined;
  }
}

function firstAshbyUrl(values: Array<string | undefined>) {
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

function formatAshbyFetchWarning(
  source: AshbyDiscoveredSource,
  result: Extract<SafeFetchResult, { ok: false }>,
) {
  if (
    (result.errorType === "http" || result.errorType === "rate_limit") &&
    result.statusCode !== undefined
  ) {
    return `Ashby returned ${result.statusCode} for ${describeAshbySource(source)}.`;
  }

  return `Ashby board ${describeAshbySource(source)} failed: ${result.message}`;
}

function describeAshbySource(source: AshbyDiscoveredSource) {
  return (
    resolveAshbyToken(source) ??
    cleanString(source.companyHint) ??
    cleanString(source.url) ??
    "unknown source"
  );
}

function cleanString(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
