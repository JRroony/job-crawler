import "server-only";

import {
  buildLocationText,
  inferExperienceLevel,
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  type CompanyPageDiscoveredSource,
  isCompanyPageSource,
} from "@/lib/server/discovery/types";
import {
  safeFetchJson,
  safeFetchText,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";
import {
  buildSeed,
  collectJsonLdJobPostings,
  collectJsonScriptPayloads,
  companyFromJsonLd,
  coercePostedAt,
  deepCollect,
  finalizeProviderResult,
  firstString,
  locationFromJsonLd,
  resolveUrl,
  stripHtml,
  unsupportedProviderResult,
} from "@/lib/server/providers/shared";
import {
  defineProvider,
  type NormalizedJobSeed,
} from "@/lib/server/providers/types";
import type { CompanyPageSourceConfig } from "@/lib/types";

type CompanyPageHtmlSource = Extract<
  CompanyPageSourceConfig,
  { type: "json_ld_page" | "html_page" }
>;

const jobTitlePattern =
  /\b(engineer|developer|designer|manager|scientist|analyst|architect|administrator|product|marketing|sales|support|security|recruit(?:er|ing)?|operations?|finance|account(?:ant| executive|ing)?|specialist|consultant|director|lead|qa|quality|data|platform|devops|intern|graduate|technician|writer|editor|counsel|officer|coordinator|partner|attorney|researcher|swe)\b/i;

const negativeJobTitlePattern =
  /\b(view all jobs?|all jobs?|open roles?|search jobs?|learn more|join our team|life at|benefits|culture|our values|candidate privacy|equal opportunity|read more|meet the team|our offices?|locations?|departments?)\b/i;

const locationPattern =
  /\b(remote|hybrid|on site|onsite|united states|usa|us|canada|united kingdom|uk)\b/i;

const jobPathPattern =
  /\b(job|jobs|career|careers|opening|openings|position|positions|role|roles|requisition|posting|opportunit|vacanc)\b/i;

const publicJobHostPattern =
  /(greenhouse|lever|ashbyhq|myworkdayjobs|smartrecruiters|jobvite|bamboohr|workable|recruitee|icims|dayforce|paylocity|oraclecloud|successfactors|adp)/i;

function isDefined<T>(value: T | undefined): value is T {
  return value !== undefined;
}

export function createCompanyPageProvider() {
  return defineProvider({
    provider: "company_page",
    supportsSource: isCompanyPageSource,
    async crawlSources(context, sources) {
      if (sources.length === 0) {
        return unsupportedProviderResult(
          "company_page",
          "Company-page crawling is supported, but no JSON feed, JSON-LD, or public HTML career pages are configured.",
          sources.length,
        );
      }

      const warnings: string[] = [];
      const sourceJobs = await runWithConcurrency(
        sources,
        async (source) => {
          try {
            const config = toCompanyPageSourceConfig(source);

            if (config.type === "json_feed") {
              const jobs = await crawlJsonFeed(
                config,
                context.fetchImpl,
                context.now.toISOString(),
              );
              return {
                fetchedCount: jobs.length,
                jobs,
              };
            }

            const jobs = await crawlHtmlPage(
              config,
              context.fetchImpl,
              context.now.toISOString(),
            );
            return {
              fetchedCount: jobs.length,
              jobs,
            };
          } catch (error) {
            warnings.push(
              error instanceof Error
                ? error.message
                : `Company page source ${source.companyHint} failed unexpectedly.`,
            );
            return {
              fetchedCount: 0,
              jobs: [],
            };
          }
        },
        2,
      );

      const fetchedCount = sourceJobs.reduce((total, source) => total + source.fetchedCount, 0);
      const jobs = sourceJobs.flatMap((source) => source.jobs);

      return finalizeProviderResult({
        provider: "company_page",
        jobs,
        sourceCount: sources.length,
        fetchedCount,
        warnings,
      });
    },
  });
}

function toCompanyPageSourceConfig(
  source: CompanyPageDiscoveredSource,
): CompanyPageSourceConfig {
  return {
    type: source.pageType,
    company: source.companyHint,
    url: source.url,
  };
}

async function crawlJsonFeed(
  source: Extract<CompanyPageSourceConfig, { type: "json_feed" }>,
  fetchImpl: typeof fetch,
  discoveredAt: string,
) {
  const result = await safeFetchJson(source.url, {
    fetchImpl,
    method: "GET",
    headers: {
      Accept: "application/json",
    },
    cache: "no-store",
  });

  if (!result.ok) {
    throw new Error(formatCompanyPageFetchError("JSON feed", source.company, result));
  }

  const records = extractFeedRecords(result.data);

  return records
    .map((record, index) => normalizeCompanyFeedJob(source.company, discoveredAt, record, index))
    .filter(isDefined);
}

async function crawlHtmlPage(
  source: CompanyPageHtmlSource,
  fetchImpl: typeof fetch,
  discoveredAt: string,
) {
  const result = await safeFetchText(source.url, {
    fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
    cache: "no-store",
  });

  if (!result.ok) {
    throw new Error(formatCompanyPageFetchError("page", source.company, result));
  }

  return extractCompanyHtmlJobs({
    company: source.company,
    sourcePageUrl: source.url,
    html: result.data ?? "",
    discoveredAt,
  });
}

function extractCompanyHtmlJobs(input: {
  company: string;
  sourcePageUrl: string;
  html: string;
  discoveredAt: string;
}) {
  const jsonLdJobs = collectJsonLdJobPostings(input.html)
    .map((record, index) => normalizeJsonLdJob(input.company, input.discoveredAt, record, index))
    .filter(isDefined);

  const embeddedJsonJobs = extractEmbeddedJsonJobs(input);
  const anchorJobs = extractAnchorJobs(input);

  return dedupeJobsBySource([...jsonLdJobs, ...embeddedJsonJobs, ...anchorJobs]);
}

function extractEmbeddedJsonJobs(input: {
  company: string;
  sourcePageUrl: string;
  html: string;
  discoveredAt: string;
}) {
  const payloads = collectJsonScriptPayloads(input.html);
  const records = payloads.flatMap((payload) =>
    deepCollect(payload, (record) => looksLikeStructuredJobRecord(record, input.sourcePageUrl)),
  );

  return records
    .map((record, index) =>
      normalizeStructuredHtmlJob(
        input.company,
        input.sourcePageUrl,
        input.discoveredAt,
        record,
        index,
      ),
    )
    .filter(isDefined);
}

function extractAnchorJobs(input: {
  company: string;
  sourcePageUrl: string;
  html: string;
  discoveredAt: string;
}) {
  const matches = Array.from(
    input.html.matchAll(/<a\b([^>]*?)href=["']([^"']+)["']([^>]*)>([\s\S]*?)<\/a>/gi),
  );

  return matches
    .map((match, index) =>
      normalizeAnchorJob({
        company: input.company,
        sourcePageUrl: input.sourcePageUrl,
        discoveredAt: input.discoveredAt,
        href: match[2],
        attributes: `${match[1]} ${match[3]}`,
        innerHtml: match[4],
        nearbyHtml: input.html.slice(
          Math.max(0, (match.index ?? 0) - 240),
          Math.min(input.html.length, (match.index ?? 0) + match[0].length + 240),
        ),
        index,
      }),
    )
    .filter(isDefined);
}

function normalizeStructuredHtmlJob(
  company: string,
  sourcePageUrl: string,
  discoveredAt: string,
  record: Record<string, unknown>,
  index: number,
) {
  const title = firstString(record, ["title", "name", "jobTitle", "postingTitle", "positionTitle"]);
  const sourceUrl = resolveStructuredJobUrl(record, sourcePageUrl);

  if (!title || !sourceUrl || !looksLikeJobTitle(title) || !isLikelyJobUrl(sourceUrl, sourcePageUrl)) {
    return undefined;
  }

  const description = firstString(record, [
    "description",
    "descriptionPlain",
    "descriptionHtml",
    "jobDescription",
    "summary",
    "overview",
    "qualifications",
    "requirements",
  ]);

  return buildSeed({
    title,
    companyToken: company,
    company:
      firstString(record, ["company", "companyName", "hiringOrganizationName"]) ??
      companyFromJsonLd(record, company),
    locationText: resolveStructuredLocationText(record),
    sourcePlatform: "company_page",
    sourceJobId:
      resolveStructuredJobId(record) ??
      buildCompanyPageFallbackId(sourceUrl, company, index),
    sourceUrl,
    applyUrl: resolveStructuredApplyUrl(record, sourcePageUrl) ?? sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(
      firstValue(record, [
        "postedAt",
        "datePosted",
        "publishedAt",
        "createdAt",
        "updatedAt",
        "listedAt",
      ]),
    ),
    rawSourceMetadata: {
      companyPageRecord: record,
      companyPageExtraction: "embedded_json",
    },
    discoveredAt,
    explicitExperienceLevel: resolveStructuredExplicitExperienceLevel(record),
    explicitExperienceSource: resolveStructuredExplicitExperienceLevel(record)
      ? "structured_metadata"
      : undefined,
    explicitExperienceReasons: resolveStructuredExplicitExperienceLevel(record)
      ? ["Structured company-page metadata explicitly indicates the experience level."]
      : undefined,
    structuredExperienceHints: collectStructuredExperienceHints(record),
    descriptionExperienceHints: [description],
  });
}

function normalizeAnchorJob(input: {
  company: string;
  sourcePageUrl: string;
  discoveredAt: string;
  href: string;
  attributes: string;
  innerHtml: string;
  nearbyHtml: string;
  index: number;
}) {
  const sourceUrl = resolveUrl(input.href, input.sourcePageUrl);
  if (!sourceUrl || !isLikelyJobUrl(sourceUrl, input.sourcePageUrl)) {
    return undefined;
  }

  const segments = dedupeStrings([
    ...extractTextSegments(input.innerHtml),
    extractAttributeValue(input.attributes, "aria-label"),
    extractAttributeValue(input.attributes, "title"),
    ...extractTextSegments(input.nearbyHtml),
  ]);
  const title = segments.find((segment) => looksLikeJobTitle(segment));

  if (!title) {
    return undefined;
  }

  const locationText =
    segments.find((segment) => segment !== title && looksLikeLocationText(segment)) ??
    "Location unavailable";

  return buildSeed({
    title,
    companyToken: input.company,
    company: input.company,
    locationText,
    sourcePlatform: "company_page",
    sourceJobId: buildCompanyPageFallbackId(sourceUrl, input.company, input.index),
    sourceUrl,
    applyUrl: sourceUrl,
    canonicalUrl: sourceUrl,
    rawSourceMetadata: {
      companyPageHtmlAnchor: {
        href: sourceUrl,
        text: stripHtml(input.innerHtml),
      },
      companyPageExtraction: "html_anchor",
    },
    discoveredAt: input.discoveredAt,
    descriptionExperienceHints: segments.slice(0, 8),
  });
}

function extractFeedRecords(payload: unknown): Record<string, unknown>[] {
  if (Array.isArray(payload)) {
    return payload.filter(isRecord);
  }

  if (!payload || typeof payload !== "object") {
    return [];
  }

  const record = payload as Record<string, unknown>;
  for (const value of Object.values(record)) {
    if (Array.isArray(value) && value.every(isRecord)) {
      return value;
    }
  }

  return [];
}

function normalizeCompanyFeedJob(
  company: string,
  discoveredAt: string,
  record: Record<string, unknown>,
  index: number,
) {
  const title = firstString(record, ["title", "name", "jobTitle"]);
  const sourceUrl = firstString(record, ["url", "applyUrl", "absolute_url", "hostedUrl"]);

  if (!title || !sourceUrl) {
    return undefined;
  }

  return buildSeed({
    title,
    companyToken: company,
    company,
    locationText:
      firstString(record, ["location", "locationText", "jobLocationText"]) ??
      "Location unavailable",
    sourcePlatform: "company_page",
    sourceJobId: firstString(record, ["id", "jobId", "slug"]) ?? `${company}-${index}`,
    sourceUrl,
    applyUrl: firstString(record, ["applyUrl", "url"]) ?? sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(
      firstString(record, ["postedAt", "datePosted", "createdAt", "publishedAt"]),
    ),
    rawSourceMetadata: {
      companyFeedJob: record,
    },
    discoveredAt,
    explicitExperienceLevel: resolveStructuredExplicitExperienceLevel(record),
    explicitExperienceSource: resolveStructuredExplicitExperienceLevel(record)
      ? "structured_metadata"
      : undefined,
    explicitExperienceReasons: resolveStructuredExplicitExperienceLevel(record)
      ? ["Structured company feed metadata explicitly indicates the experience level."]
      : undefined,
    structuredExperienceHints: collectStructuredExperienceHints(record),
    descriptionExperienceHints: collectDescriptionExperienceHints(record),
  });
}

function normalizeJsonLdJob(
  company: string,
  discoveredAt: string,
  record: Record<string, unknown>,
  index: number,
) {
  const title = firstString(record, ["title", "name"]);
  const sourceUrl = firstString(record, ["url"]);

  if (!title || !sourceUrl) {
    return undefined;
  }

  return buildSeed({
    title,
    companyToken: company,
    company: companyFromJsonLd(record, company),
    locationText: locationFromJsonLd(record),
    sourcePlatform: "company_page",
    sourceJobId: resolveStructuredJobId(record) ?? `${company}-${index}`,
    sourceUrl,
    applyUrl: sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(firstValue(record, ["datePosted", "validFrom"])),
    rawSourceMetadata: {
      jsonLdJob: record,
      companyPageExtraction: "json_ld",
    },
    discoveredAt,
    explicitExperienceLevel: resolveStructuredExplicitExperienceLevel(record),
    explicitExperienceSource: resolveStructuredExplicitExperienceLevel(record)
      ? "structured_metadata"
      : undefined,
    explicitExperienceReasons: resolveStructuredExplicitExperienceLevel(record)
      ? ["Structured JSON-LD metadata explicitly indicates the experience level."]
      : undefined,
    structuredExperienceHints: collectStructuredExperienceHints(record),
    descriptionExperienceHints: collectDescriptionExperienceHints(record),
  });
}

function resolveStructuredExplicitExperienceLevel(record: Record<string, unknown>) {
  return inferExperienceLevel(
    firstString(record, ["level", "experienceLevel", "seniority", "careerLevel"]),
    firstString(record, ["employmentType"]),
  );
}

function collectStructuredExperienceHints(record: Record<string, unknown>) {
  return [
    firstString(record, ["level", "experienceLevel", "seniority", "careerLevel"]),
    firstString(record, ["employmentType", "department", "team"]),
  ].filter((value): value is string => Boolean(value?.trim()));
}

function collectDescriptionExperienceHints(record: Record<string, unknown>) {
  return [
    firstString(record, ["experienceRequirements", "requirements", "qualifications"]),
    firstString(record, ["description", "descriptionPlain", "descriptionHtml", "jobDescription", "summary", "overview"]),
  ].filter((value): value is string => Boolean(value?.trim()));
}

function dedupeJobsBySource(jobs: NormalizedJobSeed[]) {
  const seen = new Set<string>();

  return jobs.filter((job) => {
    const key =
      job.canonicalUrl ??
      job.sourceUrl ??
      `${normalizeComparableText(job.company)}|${normalizeComparableText(job.title)}|${normalizeComparableText(job.locationText)}`;

    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function looksLikeStructuredJobRecord(record: Record<string, unknown>, sourcePageUrl: string) {
  const title = firstString(record, ["title", "name", "jobTitle", "postingTitle", "positionTitle"]);
  const url = resolveStructuredJobUrl(record, sourcePageUrl);

  return Boolean(title && url && looksLikeJobTitle(title) && isLikelyJobUrl(url, sourcePageUrl));
}

function resolveStructuredJobUrl(record: Record<string, unknown>, sourcePageUrl: string) {
  return firstResolvedUrl(
    [
      record.url,
      record.jobUrl,
      record.absoluteUrl,
      record.applyUrl,
      record.hostedUrl,
      record.canonicalUrl,
      record.detailsUrl,
      record.jobPostingUrl,
      record.jobPath,
      record.relativePath,
      record.path,
    ],
    sourcePageUrl,
  );
}

function resolveStructuredApplyUrl(record: Record<string, unknown>, sourcePageUrl: string) {
  return firstResolvedUrl(
    [record.applyUrl, record.applicationUrl, record.url, record.jobUrl, record.hostedUrl],
    sourcePageUrl,
  );
}

function resolveStructuredLocationText(record: Record<string, unknown>) {
  const direct = firstString(record, [
    "location",
    "locationName",
    "locationText",
    "jobLocationText",
  ]);
  if (direct) {
    return direct;
  }

  if (record.jobLocation) {
    return locationFromJsonLd(record);
  }

  const location = buildLocationText([
    firstString(record, ["city", "addressLocality"]),
    firstString(record, ["state", "region", "addressRegion"]),
    firstString(record, ["country", "addressCountry"]),
  ]);

  return location || "Location unavailable";
}

function resolveStructuredJobId(record: Record<string, unknown>) {
  const direct = firstString(record, ["id", "jobId", "slug", "requisitionId", "externalJobId"]);
  if (direct) {
    return direct;
  }

  const identifier = record.identifier;
  if (typeof identifier === "number") {
    return String(identifier);
  }

  if (typeof identifier === "string" && identifier.trim()) {
    return identifier.trim();
  }

  if (isRecord(identifier)) {
    return (
      firstString(identifier, ["value", "name", "id"]) ??
      (typeof identifier["@id"] === "string" ? identifier["@id"].trim() : undefined)
    );
  }

  const numericId = firstValue(record, ["id", "jobId", "requisitionId"]);
  return typeof numericId === "number" ? String(numericId) : undefined;
}

function firstResolvedUrl(values: unknown[], sourcePageUrl: string) {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }

    const trimmed = value.trim();
    if (!trimmed || (!trimmed.startsWith("/") && !trimmed.includes("/") && !trimmed.startsWith("http"))) {
      continue;
    }

    const resolved = resolveUrl(trimmed, sourcePageUrl);
    if (resolved) {
      return resolved;
    }
  }

  return undefined;
}

function buildCompanyPageFallbackId(sourceUrl: string, company: string, index: number) {
  const parsed = new URL(sourceUrl);
  const path = parsed.pathname.replace(/\/+$/, "") || "/";
  const normalizedPath = normalizeComparableText(path);

  return normalizedPath ? `${company}-${normalizedPath}` : `${company}-${index}`;
}

function looksLikeJobTitle(value: string) {
  const cleaned = value.trim().replace(/\s+/g, " ");
  if (!cleaned || cleaned.length < 4 || cleaned.length > 120) {
    return false;
  }

  const normalized = normalizeComparableText(cleaned);
  if (!normalized || negativeJobTitlePattern.test(normalized)) {
    return false;
  }

  const wordCount = normalized.split(" ").filter(Boolean).length;
  if (wordCount === 0 || wordCount > 12) {
    return false;
  }

  return jobTitlePattern.test(normalized);
}

function looksLikeLocationText(value: string) {
  const cleaned = value.trim().replace(/\s+/g, " ");
  if (!cleaned || cleaned.length > 80) {
    return false;
  }

  const normalized = normalizeComparableText(cleaned);
  if (!normalized) {
    return false;
  }

  if (locationPattern.test(normalized)) {
    return true;
  }

  if (jobTitlePattern.test(normalized)) {
    return false;
  }

  return cleaned.includes(",") || /\b[A-Z][a-z]+,\s?[A-Z]{2}\b/.test(cleaned);
}

function isLikelyJobUrl(url: string, sourcePageUrl: string) {
  try {
    const candidate = new URL(url);
    if (!/^https?:$/.test(candidate.protocol)) {
      return false;
    }

    const baseHost = new URL(sourcePageUrl).host;
    const comparable = `${candidate.host}${candidate.pathname}${candidate.search}`.toLowerCase();

    return (
      candidate.host === baseHost ||
      publicJobHostPattern.test(candidate.host) ||
      jobPathPattern.test(comparable)
    );
  } catch {
    return false;
  }
}

function extractTextSegments(htmlFragment: string) {
  const text = stripHtml(htmlFragment.replace(/></g, ">\n<"));

  return dedupeStrings(
    text
      .split(/\n+/)
      .flatMap((segment) => segment.split(/\s*[|•]\s*|\s+[-–—]\s+/g))
      .map((segment) => segment.trim())
      .filter(Boolean),
  );
}

function dedupeStrings(values: Array<string | undefined>) {
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

function formatCompanyPageFetchError(
  target: "JSON feed" | "page",
  company: string,
  result: Extract<SafeFetchResult, { ok: false }>,
) {
  if (
    (result.errorType === "http" || result.errorType === "rate_limit") &&
    result.statusCode !== undefined
  ) {
    return `Company ${target} ${company} returned ${result.statusCode}.`;
  }

  return `Company ${target} ${company} failed: ${result.message}`;
}

function extractAttributeValue(attributes: string, name: string) {
  const match = attributes.match(new RegExp(`${name}=["']([^"']+)["']`, "i"));
  return match?.[1] ? stripHtml(match[1]) : undefined;
}

function firstValue(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
