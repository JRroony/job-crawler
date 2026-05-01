import "server-only";

import { normalizeComparableText, runWithConcurrency } from "@/lib/server/crawler/helpers";
import {
  buildCanonicalSmartRecruitersJobUrl,
  parseSmartRecruitersUrl,
} from "@/lib/server/discovery/smartrecruiters-url";
import {
  type SmartRecruitersDiscoveredSource,
  isSmartRecruitersSource,
} from "@/lib/server/discovery/types";
import { safeFetchText, type SafeFetchResult } from "@/lib/server/net/fetcher";
import {
  buildSeed,
  collectJsonLdJobPostings,
  collectJsonScriptPayloads,
  companyFromJsonLd,
  coercePostedAt,
  deepCollect,
  firstString,
  locationFromJsonLd,
  resolveUrl,
  stripHtml,
} from "@/lib/server/providers/shared";
import { createAdapterProvider } from "@/lib/server/providers/adapter";
import { type ProviderExecutionContext } from "@/lib/server/providers/types";

type SmartRecruitersCandidate = {
  id?: string;
  title?: string;
  location?: string;
  locationText?: string;
  jobAd?: { sections?: Array<{ title?: string; text?: string }> };
  department?: string;
  function?: string;
  typeOfEmployment?: string;
  releasedDate?: string;
  postedAt?: string;
  createdOn?: string;
  updatedOn?: string;
  jobUrl?: string;
  applyUrl?: string;
  description?: string;
  html?: string;
  company?: string;
  companyName?: string;
};

export function normalizeSmartRecruitersJob(input: {
  companyToken: string;
  boardUrl?: string;
  companyName?: string;
  discoveredAt: string;
  candidate: SmartRecruitersCandidate;
}) {
  const boardUrl = input.boardUrl ?? `https://careers.smartrecruiters.com/${input.companyToken}`;
  const sourceUrl =
    input.candidate.jobUrl ??
    (input.candidate.id
      ? buildCanonicalSmartRecruitersJobUrl(input.companyToken, input.candidate.id)
      : "");
  const descriptionSections = input.candidate.jobAd?.sections
    ?.map((section) => stripHtml(section.text))
    .filter(Boolean);
  const descriptionText =
    input.candidate.description ??
    input.candidate.html ??
    descriptionSections?.join("\n");

  return buildSeed({
    title: input.candidate.title ?? "",
    companyToken: input.companyToken,
    company:
      input.candidate.company ??
      input.candidate.companyName ??
      input.companyName,
    locationText:
      input.candidate.locationText ??
      input.candidate.location ??
      "Location unavailable",
    sourcePlatform: "smartrecruiters",
    sourceJobId: input.candidate.id ?? sourceUrl,
    sourceUrl,
    applyUrl: input.candidate.applyUrl ?? sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(
      input.candidate.releasedDate ??
        input.candidate.postedAt ??
        input.candidate.createdOn ??
        input.candidate.updatedOn,
    ),
    rawSourceMetadata: {
      smartRecruitersJob: input.candidate,
    },
    discoveredAt: input.discoveredAt,
    explicitEmploymentType: input.candidate.typeOfEmployment,
    structuredExperienceHints: [
      input.candidate.department,
      input.candidate.function,
      input.candidate.typeOfEmployment,
    ],
    descriptionExperienceHints: [descriptionText],
    descriptionSnippet: descriptionText,
  });
}

export async function extractSmartRecruitersJobFromDetailUrl(input: {
  detailUrl: string;
  companyToken?: string;
  jobId?: string;
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

  const parsed = parseSmartRecruitersUrl(input.detailUrl);
  const candidate =
    extractSmartRecruitersJsonLdCandidate(result.data, input.detailUrl) ??
    extractSmartRecruitersStructuredCandidate(result.data, input.detailUrl);
  if (!candidate) {
    return undefined;
  }

  return normalizeSmartRecruitersJob({
    companyToken: input.companyToken ?? parsed?.companyToken ?? "smartrecruiters",
    boardUrl:
      parsed?.canonicalBoardUrl ??
      (input.companyToken ? `https://careers.smartrecruiters.com/${input.companyToken}` : undefined),
    companyName: input.companyHint,
    discoveredAt: input.discoveredAt,
    candidate: {
      ...candidate,
      id: candidate.id ?? input.jobId ?? parsed?.jobId,
      jobUrl: candidate.jobUrl ?? parsed?.canonicalJobUrl ?? input.detailUrl,
      applyUrl: candidate.applyUrl ?? parsed?.canonicalJobUrl ?? input.detailUrl,
    },
  });
}

export function createSmartRecruitersProvider() {
  return createAdapterProvider({
    provider: "smartrecruiters",
    supportsSource: isSmartRecruitersSource,
    unsupportedMessage: "No discovered SmartRecruiters sources are available.",
    concurrency: 2,
    async crawlSource(context, source) {
      return crawlSmartRecruitersSource(context, source);
    },
  });
}

async function crawlSmartRecruitersSource(
  context: ProviderExecutionContext,
  source: SmartRecruitersDiscoveredSource,
) {
  await context.throwIfCanceled?.();
  const warnings: string[] = [];
  const dropReasons: string[] = [];
  const discoveredAt = context.now.toISOString();
  const boardUrl = source.boardUrl ?? source.url;
  const companyToken =
    source.token ??
    parseSmartRecruitersUrl(boardUrl)?.companyToken ??
    normalizeComparableText(source.companyHint ?? "") ??
    "smartrecruiters";

  const boardResult = await safeFetchText(boardUrl, {
    fetchImpl: context.fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
    cache: "no-store",
  });

  if (!boardResult.ok) {
    warnings.push(formatSmartRecruitersFetchWarning(source, boardResult));
    dropReasons.push("board_fetch_failed");
  }

  const detailUrls = boardResult.ok
    ? extractSmartRecruitersDetailUrls(boardResult.data ?? "", boardUrl, companyToken)
    : [];
  const seededDetailUrls = dedupeStrings([
    ...detailUrls,
    source.jobUrl,
    source.jobId ? buildCanonicalSmartRecruitersJobUrl(companyToken, source.jobId) : undefined,
  ]);

  const detailJobs = await runWithConcurrency(
    seededDetailUrls,
    async (detailUrl) => {
      await context.throwIfCanceled?.();
      return extractSmartRecruitersJobFromDetailUrl({
        detailUrl,
        companyToken,
        companyHint: source.companyHint,
        discoveredAt,
        fetchImpl: context.fetchImpl,
      });
    },
    Math.min(4, Math.max(1, seededDetailUrls.length || 1)),
  );

  const jobs = detailJobs.filter((job): job is NonNullable<typeof job> => Boolean(job));
  if (jobs.length === 0 && source.jobUrl) {
    dropReasons.push("no_jobs_extracted");
  }

  return {
    fetchedCount: jobs.length,
    fetchCount: (boardResult.ok ? 1 : 0) + seededDetailUrls.length,
    jobs,
    warnings,
    parseSuccessCount: jobs.length,
    parseFailureCount: Math.max(0, seededDetailUrls.length - jobs.length),
    dropReasons,
  };
}

function extractSmartRecruitersDetailUrls(
  html: string,
  baseUrl: string,
  companyToken: string,
) {
  const urls = new Set<string>();
  const patterns = [
    /href=["']([^"']+)["']/gi,
    /https?:\\\/\\\/(?:jobs|careers)\.smartrecruiters\.com\\\/[^"'\\<\s)]+/gi,
  ];

  for (const pattern of patterns) {
    for (const match of html.matchAll(pattern)) {
      const raw = (match[1] ?? match[0])?.replace(/\\\//g, "/");
      const resolved = resolveUrl(raw, baseUrl);
      const parsed = resolved ? parseSmartRecruitersUrl(resolved) : undefined;
      if (!parsed?.companyToken || !parsed.jobPath) {
        continue;
      }

      if (normalizeComparableText(parsed.companyToken) !== normalizeComparableText(companyToken)) {
        continue;
      }

      if (parsed.canonicalJobUrl) {
        urls.add(parsed.canonicalJobUrl);
      }
    }
  }

  return Array.from(urls);
}

function extractSmartRecruitersJsonLdCandidate(html: string, detailUrl: string) {
  const jobPosting = collectJsonLdJobPostings(html)[0];
  if (!jobPosting) {
    return undefined;
  }

  return {
    id: firstString(jobPosting, ["identifier", "jobId"]) ?? parseSmartRecruitersUrl(detailUrl)?.jobId,
    title: firstString(jobPosting, ["title", "name"]),
    locationText: locationFromJsonLd(jobPosting),
    jobUrl: resolveUrl(firstString(jobPosting, ["url"]), detailUrl) ?? detailUrl,
    applyUrl: resolveUrl(firstString(jobPosting, ["url"]), detailUrl) ?? detailUrl,
    postedAt: firstString(jobPosting, ["datePosted"]),
    description: stripHtml(firstString(jobPosting, ["description"])),
    company: companyFromJsonLd(jobPosting, parseSmartRecruitersUrl(detailUrl)?.companyToken ?? "SmartRecruiters"),
    typeOfEmployment: firstString(jobPosting, ["employmentType"]),
  } satisfies SmartRecruitersCandidate;
}

function extractSmartRecruitersStructuredCandidate(html: string, detailUrl: string) {
  const payloads = collectJsonScriptPayloads(html);
  const candidates = payloads.flatMap((payload) =>
    deepCollect(payload, (record) =>
      Boolean(
        firstString(record, ["title", "name"]) &&
        (record.jobAd || firstString(record, ["jobUrl", "applyUrl", "location", "locationText"])),
      ),
    ),
  );

  const matched = candidates.find((candidate) => {
    const jobUrl = resolveUrl(firstString(candidate, ["jobUrl", "applyUrl", "url"]), detailUrl);
    return jobUrl ? normalizeComparableText(jobUrl) === normalizeComparableText(detailUrl) : false;
  }) ?? candidates[0];

  if (!matched) {
    return undefined;
  }

  return {
    id: firstString(matched, ["id", "ref", "jobId"]),
    title: firstString(matched, ["title", "name"]),
    location: firstString(matched, ["location", "locationText"]),
    locationText: firstString(matched, ["locationText", "location"]),
    jobUrl: resolveUrl(firstString(matched, ["jobUrl", "applyUrl", "url"]), detailUrl) ?? detailUrl,
    applyUrl: resolveUrl(firstString(matched, ["applyUrl", "jobUrl", "url"]), detailUrl) ?? detailUrl,
    department: firstString(matched, ["department", "departmentName"]),
    function: firstString(matched, ["function", "jobFunction"]),
    typeOfEmployment: firstString(matched, ["typeOfEmployment", "employmentType"]),
    company: firstString(matched, ["company", "companyName"]),
    description:
      stripHtml(firstString(matched, ["jobDescription", "description", "summary"])) || undefined,
    jobAd:
      matched.jobAd && typeof matched.jobAd === "object"
        ? (matched.jobAd as SmartRecruitersCandidate["jobAd"])
        : undefined,
    postedAt: firstString(matched, ["releasedDate", "postedAt", "createdOn", "updatedOn"]),
  } satisfies SmartRecruitersCandidate;
}

function formatSmartRecruitersFetchWarning(
  source: SmartRecruitersDiscoveredSource,
  result: Extract<SafeFetchResult<string>, { ok: false }>,
) {
  const descriptor = source.companyHint ?? source.token ?? source.url;
  if (
    (result.errorType === "http" || result.errorType === "rate_limit") &&
    result.statusCode !== undefined
  ) {
    return `SmartRecruiters returned ${result.statusCode} for ${descriptor}.`;
  }

  return `SmartRecruiters source ${descriptor} failed with ${result.errorType}.`;
}

function dedupeStrings(values: Array<string | undefined>) {
  return Array.from(new Set(values.filter((value): value is string => Boolean(value?.trim()))));
}
