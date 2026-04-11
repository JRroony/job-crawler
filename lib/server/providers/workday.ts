import "server-only";

import {
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import {
  buildCanonicalWorkdayApiJobUrl,
  buildCanonicalWorkdayApiListUrl,
  buildCanonicalWorkdayJobUrl,
  parseWorkdayUrl,
} from "@/lib/server/discovery/workday-url";
import {
  type WorkdayDiscoveredSource,
  isWorkdaySource,
} from "@/lib/server/discovery/types";
import {
  safeFetchJson,
  safeFetchText,
  type SafeFetchResult,
} from "@/lib/server/net/fetcher";
import {
  buildSeed,
  coercePostedAt,
  collectJsonLdJobPostings,
  collectJsonScriptPayloads,
  companyFromJsonLd,
  deepCollect,
  extractNextData,
  filterProviderSeeds,
  finalizeProviderResult,
  firstString,
  locationFromJsonLd,
  resolveUrl,
  unsupportedProviderResult,
} from "@/lib/server/providers/shared";
import {
  defineProvider,
  type NormalizedJobSeed,
} from "@/lib/server/providers/types";

type WorkdayRecord = Record<string, unknown>;

type WorkdayFetchAttempt = {
  url: string;
  result: SafeFetchResult<unknown>;
};

const workdayDirectLocationKeys = [
  "locationText",
  "jobLocationText",
  "locationsText",
  "location",
  "primaryLocation",
  "locationName",
  "formattedLocation",
  "countryRegion",
  "primaryLocationText",
  "primaryLocationName",
] as const;

const workdayDescriptionKeys = [
  "description",
  "jobDescription",
  "summary",
  "overview",
  "qualifications",
  "requirements",
  "responsibilities",
  "jobPostingDescription",
  "externalDescription",
  "shortDescription",
  "longDescription",
  "additionalInformation",
  "basicQualifications",
  "preferredQualifications",
] as const;

const workdayStructuredExperienceKeys = [
  "timeType",
  "workerSubType",
  "employmentType",
  "jobType",
  "jobFamily",
  "jobProfile",
  "careerLevel",
  "seniority",
  "bulletFields",
  "additionalLocations",
  "secondaryLocations",
] as const;

export function normalizeWorkdayJob(input: {
  source: Pick<
    WorkdayDiscoveredSource,
    "url" | "token" | "sitePath" | "careerSitePath" | "companyHint"
  >;
  discoveredAt: string;
  candidate: WorkdayRecord;
  detailUrl?: string;
}) {
  const locationText = resolveWorkdayLocationText(input.candidate);
  const sourceUrl =
    resolveWorkdayJobUrl(input.source, input.candidate, input.detailUrl) ??
    input.detailUrl ??
    input.source.url;
  const company =
    resolveWorkdayCompanyName(input.candidate) ??
    input.source.companyHint ??
    "Workday";
  const structuredExperienceHints = collectWorkdayStructuredExperienceHints(input.candidate);
  const descriptionExperienceHints = collectWorkdayDescriptionHints(input.candidate);

  return buildSeed({
    title: resolveWorkdayTitle(input.candidate) ?? "Untitled role",
    companyToken:
      input.source.careerSitePath ??
      input.source.token ??
      input.source.companyHint ??
      "workday",
    company,
    locationText,
    sourcePlatform: "workday",
    sourceJobId: resolveWorkdayJobId(input.candidate, sourceUrl),
    sourceUrl,
    applyUrl: resolveWorkdayApplyUrl(input.candidate, sourceUrl),
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(resolveWorkdayPostedAt(input.candidate)),
    rawSourceMetadata: {
      workdayJob: input.candidate,
      workdayToken: input.source.token,
      workdaySitePath: input.source.sitePath,
      workdayCareerSitePath: input.source.careerSitePath,
      workdayStructuredExperienceHints: structuredExperienceHints,
      workdayDescriptionExperienceHints: descriptionExperienceHints,
    },
    discoveredAt: input.discoveredAt,
    explicitCountry: firstString(input.candidate, ["country", "countryName", "countryRegion"]),
    explicitState: firstString(input.candidate, ["state", "stateName", "region"]),
    explicitCity: firstString(input.candidate, ["city", "locality"]),
    structuredExperienceHints,
    descriptionExperienceHints,
  });
}

export async function extractWorkdayJobFromDetailUrl(input: {
  detailUrl: string;
  source: Pick<
    WorkdayDiscoveredSource,
    "url" | "token" | "sitePath" | "careerSitePath" | "companyHint" | "apiUrl"
  >;
  discoveredAt: string;
  fetchImpl: typeof fetch;
}): Promise<NormalizedJobSeed | undefined> {
  const parsedDetail = parseWorkdayUrl(input.detailUrl);
  const apiJobUrls = dedupeStrings([
    parsedDetail?.canonicalApiJobUrl,
    buildDerivedWorkdayApiJobUrl(input.source, parsedDetail),
  ]);

  for (const apiUrl of apiJobUrls) {
    const result = await safeFetchJson<unknown>(apiUrl, {
      fetchImpl: input.fetchImpl,
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      cache: "no-store",
      retries: 1,
    });

    if (!result.ok || !result.data) {
      continue;
    }

    const candidate = extractWorkdayDetailCandidate(result.data, input.detailUrl);
    if (!candidate) {
      continue;
    }

    return normalizeWorkdayJob({
      source: {
        ...input.source,
        sitePath: parsedDetail?.sitePath ?? input.source.sitePath,
        careerSitePath: parsedDetail?.careerSitePath ?? input.source.careerSitePath,
      },
      discoveredAt: input.discoveredAt,
      candidate,
      detailUrl: input.detailUrl,
    });
  }

  const htmlResult = await safeFetchText(input.detailUrl, {
    fetchImpl: input.fetchImpl,
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
    },
    cache: "no-store",
    retries: 1,
  });

  if (!htmlResult.ok || !htmlResult.data) {
    return undefined;
  }

  const candidate = extractWorkdayDetailCandidateFromHtml(htmlResult.data, input.detailUrl);
  if (!candidate) {
    return undefined;
  }

  return normalizeWorkdayJob({
    source: {
      ...input.source,
      sitePath: parsedDetail?.sitePath ?? input.source.sitePath,
      careerSitePath: parsedDetail?.careerSitePath ?? input.source.careerSitePath,
    },
    discoveredAt: input.discoveredAt,
    candidate,
    detailUrl: input.detailUrl,
  });
}

export function createWorkdayProvider() {
  return defineProvider({
    provider: "workday",
    supportsSource: isWorkdaySource,
    async crawlSources(context, sources) {
      if (sources.length === 0) {
        return unsupportedProviderResult(
          "workday",
          "No discovered Workday sources are available.",
          sources.length,
        );
      }

      const warnings: string[] = [];
      const discoveredAt = context.now.toISOString();

      const sites = await runWithConcurrency(
        sources,
        async (source) => {
          const apiAttempt = await fetchWorkdaySourceFromApi(source, context.fetchImpl);
          const jobsFromApi = apiAttempt.jobs.map((candidate) =>
            normalizeWorkdayJob({
              source,
              discoveredAt,
              candidate,
            }),
          );

          const fallback =
            apiAttempt.jobs.length === 0
              ? await fetchWorkdaySourceFromHtml(source, context.fetchImpl, discoveredAt)
              : {
                  fetchedCount: 0,
                  jobs: [] as NormalizedJobSeed[],
                  warning: undefined,
                };
          const jobsFromHtml = fallback.jobs;

          const normalizedJobs = dedupeWorkdaySeeds([
            ...jobsFromApi,
            ...jobsFromHtml,
          ]);
          const filteredJobs = filterProviderSeeds(normalizedJobs, context.filters);

          if (apiAttempt.warning) {
            warnings.push(apiAttempt.warning);
          }

          if (fallback.warning) {
            warnings.push(fallback.warning);
          }

          return {
            fetchedCount: apiAttempt.fetchedCount + fallback.fetchedCount,
            jobs: filteredJobs.jobs,
            excludedByTitle: filteredJobs.excludedByTitle,
            excludedByLocation: filteredJobs.excludedByLocation,
          };
        },
        2,
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
        provider: "workday",
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

async function fetchWorkdaySourceFromApi(
  source: WorkdayDiscoveredSource,
  fetchImpl: typeof fetch,
) {
  const attempts: WorkdayFetchAttempt[] = [];
  const apiUrls = resolveWorkdayApiUrls(source);

  for (const apiUrl of apiUrls) {
    const result = await safeFetchJson<unknown>(apiUrl, {
      fetchImpl,
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      cache: "no-store",
      retries: 1,
    });
    attempts.push({
      url: apiUrl,
      result,
    });

    if (!result.ok || !result.data) {
      continue;
    }

    const jobs = extractWorkdayListCandidates(result.data);
    if (jobs.length > 0) {
      return {
        fetchedCount: jobs.length,
        jobs,
        warning: undefined,
      };
    }
  }

  return {
    fetchedCount: 0,
    jobs: [] as WorkdayRecord[],
    warning: buildWorkdayFetchWarning(source, attempts),
  };
}

async function fetchWorkdaySourceFromHtml(
  source: WorkdayDiscoveredSource,
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
    retries: 1,
  });

  if (!result.ok || !result.data) {
    return {
      fetchedCount: 0,
      jobs: [] as NormalizedJobSeed[],
      warning: `Workday source ${describeWorkdaySource(source)} could not be fetched from ${source.url}: ${result.ok ? "Response body was empty." : result.message}`,
    };
  }

  const structuredCandidates = extractWorkdayHtmlCandidates(result.data, source.url);
  const structuredJobs = structuredCandidates.map((candidate) =>
    normalizeWorkdayJob({
      source,
      discoveredAt,
      candidate,
    }),
  );

  const detailUrls = extractWorkdayDetailUrlsFromHtml(result.data, source.url);
  const detailJobs = await runWithConcurrency(
    detailUrls,
    async (detailUrl) =>
      extractWorkdayJobFromDetailUrl({
        detailUrl,
        source,
        discoveredAt,
        fetchImpl,
      }),
    Math.min(3, Math.max(1, detailUrls.length)),
  );

  return {
    fetchedCount: structuredCandidates.length + detailUrls.length,
    jobs: dedupeWorkdaySeeds([
      ...structuredJobs,
      ...detailJobs.filter((job): job is NormalizedJobSeed => Boolean(job)),
    ]),
    warning: undefined,
  };
}

function extractWorkdayListCandidates(data: unknown) {
  const directArrays = [
    data,
    isRecord(data) ? data.jobPostings : undefined,
    isRecord(data) ? data.jobs : undefined,
    isRecord(data) ? data.postings : undefined,
    isRecord(data) ? data.jobSearchResults : undefined,
    isRecord(data) ? data.results : undefined,
  ];

  for (const candidate of directArrays) {
    const jobs = collectWorkdayRecords(candidate);
    if (jobs.length > 0) {
      return jobs;
    }
  }

  return deepCollect(data, (record) => isLikelyWorkdayJobRecord(record));
}

function extractWorkdayDetailCandidate(data: unknown, detailUrl: string) {
  if (isRecord(data) && isRecord(data.jobPostingInfo)) {
    return data.jobPostingInfo;
  }

  if (isRecord(data) && isRecord(data.jobPosting)) {
    return data.jobPosting;
  }

  return (
    deepCollect(data, (record) => {
      if (!isLikelyWorkdayJobRecord(record)) {
        return false;
      }

      const candidateUrl = resolveWorkdayCandidateUrl(record, detailUrl);
      return candidateUrl
        ? normalizeComparableText(candidateUrl) === normalizeComparableText(detailUrl)
        : true;
    })[0] ?? undefined
  );
}

function extractWorkdayDetailCandidateFromHtml(html: string, detailUrl: string) {
  const jsonLdCandidate = collectJsonLdJobPostings(html)[0];
  if (jsonLdCandidate) {
    return jsonLdCandidate;
  }

  const exactStructuredMatch = [
    extractNextData(html),
    ...collectJsonScriptPayloads(html),
  ]
    .flatMap((payload) =>
      deepCollect(payload, (record) => {
        if (!isLikelyWorkdayJobRecord(record)) {
          return false;
        }

        const candidateUrl = resolveWorkdayCandidateUrl(record, detailUrl);
        return candidateUrl
          ? normalizeComparableText(candidateUrl) === normalizeComparableText(detailUrl)
          : true;
      }),
    )[0];

  if (exactStructuredMatch) {
    return exactStructuredMatch;
  }

  return extractWorkdayHtmlCandidates(html, detailUrl)[0];
}

function extractWorkdayHtmlCandidates(html: string, baseUrl: string) {
  const jsonLdJobs = collectJsonLdJobPostings(html);
  const structuredJobs = [
    extractNextData(html),
    ...collectJsonScriptPayloads(html),
  ].flatMap((payload) => deepCollect(payload, (record) => isLikelyWorkdayJobRecord(record)));

  const linkDerivedJobs = extractWorkdayDetailUrlsFromHtml(html, baseUrl).map((detailUrl) => ({
    url: detailUrl,
    externalUrl: detailUrl,
    externalPath: detailUrl,
    title: undefined,
  }));

  return dedupeWorkdayCandidates([
    ...jsonLdJobs,
    ...structuredJobs,
    ...linkDerivedJobs,
  ]);
}

function extractWorkdayDetailUrlsFromHtml(html: string, baseUrl: string) {
  const discovered = new Set<string>();

  Array.from(html.matchAll(/href=["']([^"']+)["']/gi)).forEach((match) => {
    const resolved = resolveUrl(match[1]?.trim(), baseUrl);
    if (resolved && isWorkdayDetailUrl(resolved)) {
      discovered.add(resolved);
    }
  });

  Array.from(
    html.matchAll(
      /https?:\\\/\\\/(?:www\\\/\.)?[^"'\\<\s)]+myworkdayjobs\.com[^"'\\<\s)]*/gi,
    ),
  ).forEach((match) => {
    const resolved = match[0].replace(/\\\//g, "/");
    if (isWorkdayDetailUrl(resolved)) {
      discovered.add(resolved);
    }
  });

  return Array.from(discovered);
}

function resolveWorkdayTitle(record: WorkdayRecord) {
  return firstString(record, [
    "title",
    "jobTitle",
    "bulletinTitle",
    "name",
    "postingTitle",
  ]);
}

function resolveWorkdayLocationText(record: WorkdayRecord) {
  const direct = firstString(record, [...workdayDirectLocationKeys]);
  if (direct) {
    return direct;
  }

  const arrayLocation = firstStringArray(
    record,
    ["locations", "additionalLocations", "secondaryLocations", "bulletFields"],
  );
  if (arrayLocation) {
    return arrayLocation;
  }

  const locationRecord = firstObject(record, [
    "locationData",
    "jobLocation",
    "primaryLocation",
    "location",
  ]);
  if (locationRecord) {
    const jsonLdLocation = locationFromJsonLd(locationRecord);
    if (jsonLdLocation && jsonLdLocation !== "Location unavailable") {
      return jsonLdLocation;
    }
  }

  return "Location unavailable";
}

function resolveWorkdayCompanyName(record: WorkdayRecord) {
  const direct = firstString(record, [
    "company",
    "companyName",
    "businessTitle",
    "organizationName",
  ]);
  if (direct) {
    return direct;
  }

  const hiringOrganization = record.hiringOrganization;
  if (isRecord(hiringOrganization)) {
    return companyFromJsonLd({ hiringOrganization }, "Workday");
  }

  return undefined;
}

function resolveWorkdayPostedAt(record: WorkdayRecord) {
  return firstString(record, [
    "postedOn",
    "postedDate",
    "publishedAt",
    "createdAt",
    "updatedAt",
    "startDate",
    "datePosted",
  ]);
}

function resolveWorkdayJobId(record: WorkdayRecord, sourceUrl: string) {
  return (
    firstString(record, [
      "id",
      "jobId",
      "jobPostingId",
      "externalJobPostingId",
      "bulletinId",
      "requisitionId",
      "jobReqId",
      "questionId",
      "externalPath",
      "url",
    ]) ?? sourceUrl
  );
}

function resolveWorkdayApplyUrl(record: WorkdayRecord, fallbackUrl: string) {
  return (
    resolveWorkdayCandidateUrl(record, fallbackUrl) ??
    firstString(record, ["applyUrl", "externalUrl", "url"]) ??
    fallbackUrl
  );
}

function resolveWorkdayJobUrl(
  source: Pick<WorkdayDiscoveredSource, "url" | "sitePath">,
  record: WorkdayRecord,
  fallbackUrl?: string,
) {
  const candidateUrl = resolveWorkdayCandidateUrl(record, source.url);
  if (candidateUrl) {
    return candidateUrl;
  }

  const parsedSource = parseWorkdayUrl(source.url);
  const jobPath = firstString(record, [
    "externalPath",
    "jobPath",
    "jobUrlPath",
    "questionId",
  ]);

  if (jobPath && parsedSource?.sitePath) {
    const normalizedJobPath = jobPath.replace(/^\/?job\/+/i, "").replace(/^\/+/, "");
    return buildCanonicalWorkdayJobUrl(
      new URL(source.url).origin,
      parsedSource.sitePath,
      normalizedJobPath,
    );
  }

  return fallbackUrl;
}

function resolveWorkdayCandidateUrl(record: WorkdayRecord, baseUrl: string) {
  const direct = firstString(record, [
    "externalUrl",
    "externalURL",
    "jobUrl",
    "absoluteUrl",
    "url",
    "applyUrl",
  ]);
  if (direct) {
    return resolveUrl(direct, baseUrl);
  }

  const path = firstString(record, ["externalPath", "jobPath", "jobUrlPath", "questionId"]);
  if (!path) {
    return undefined;
  }

  const parsedBase = parseWorkdayUrl(baseUrl);
  if (!parsedBase?.sitePath) {
    return resolveUrl(path, baseUrl);
  }

  const normalizedPath = path.replace(/^\/?job\/+/i, "").replace(/^\/+/, "");
  return buildCanonicalWorkdayJobUrl(
    new URL(baseUrl).origin,
    parsedBase.sitePath,
    normalizedPath,
  );
}

function collectWorkdayStructuredExperienceHints(record: WorkdayRecord) {
  return workdayStructuredExperienceKeys.flatMap((key) =>
    readStringValues(record[key as keyof WorkdayRecord]),
  );
}

function collectWorkdayDescriptionHints(record: WorkdayRecord) {
  return workdayDescriptionKeys.flatMap((key) =>
    readStringValues(record[key as keyof WorkdayRecord]),
  );
}

function resolveWorkdayApiUrls(source: WorkdayDiscoveredSource) {
  const parsed = parseWorkdayUrl(source.url);
  const tenant = parsed?.tenant ?? source.token?.split(":")[0];
  const careerSitePath =
    parsed?.careerSitePath ??
    source.careerSitePath ??
    source.token?.split(":").slice(1).join(":").replace(/:/g, "/");

  return dedupeStrings([
    source.apiUrl,
    parsed?.canonicalApiUrl,
    tenant && careerSitePath
      ? buildCanonicalWorkdayApiListUrl(new URL(source.url).origin, tenant, careerSitePath)
      : undefined,
  ]);
}

function buildDerivedWorkdayApiJobUrl(
  source: Pick<WorkdayDiscoveredSource, "url" | "token" | "careerSitePath">,
  parsedDetail?: ReturnType<typeof parseWorkdayUrl>,
) {
  const sourceParse = parseWorkdayUrl(source.url);
  const tenant =
    parsedDetail?.tenant ??
    sourceParse?.tenant ??
    source.token?.split(":")[0];
  const careerSitePath =
    parsedDetail?.careerSitePath ??
    sourceParse?.careerSitePath ??
    source.careerSitePath ??
    source.token?.split(":").slice(1).join(":").replace(/:/g, "/");
  const jobPath = parsedDetail?.jobPath;

  if (!tenant || !careerSitePath || !jobPath) {
    return undefined;
  }

  return buildCanonicalWorkdayApiJobUrl(
    new URL(source.url).origin,
    tenant,
    careerSitePath,
    jobPath,
  );
}

function buildWorkdayFetchWarning(
  source: WorkdayDiscoveredSource,
  attempts: WorkdayFetchAttempt[],
) {
  if (attempts.length === 0) {
    return undefined;
  }

  const failure = attempts[attempts.length - 1];
  if (failure.result.ok) {
    return undefined;
  }

  return `Workday source ${describeWorkdaySource(source)} could not be crawled from ${failure.url}: ${failure.result.message}`;
}

function describeWorkdaySource(source: WorkdayDiscoveredSource) {
  return source.companyHint ?? source.careerSitePath ?? source.token ?? source.url;
}

function collectWorkdayRecords(value: unknown) {
  if (!Array.isArray(value)) {
    return [] as WorkdayRecord[];
  }

  return value.filter((entry): entry is WorkdayRecord => isRecord(entry));
}

function isLikelyWorkdayJobRecord(record: WorkdayRecord) {
  const title = resolveWorkdayTitle(record);
  if (!title) {
    return false;
  }

  return Boolean(
    firstString(record, [
      "externalPath",
      "externalUrl",
      "jobUrl",
      "url",
      "jobId",
      "id",
      "jobPostingId",
      "questionId",
    ]) || resolveWorkdayLocationText(record) !== "Location unavailable",
  );
}

function isWorkdayDetailUrl(value: string) {
  const parsed = parseWorkdayUrl(value);
  return parsed?.kind === "job";
}

function readStringValues(value: unknown): string[] {
  if (!value) {
    return [];
  }

  if (typeof value === "string" && value.trim()) {
    return [value.trim()];
  }

  if (Array.isArray(value)) {
    return value.flatMap((entry) => readStringValues(entry));
  }

  if (typeof value !== "object") {
    return [];
  }

  const record = value as Record<string, unknown>;
  return Object.values(record).flatMap((entry) => readStringValues(entry));
}

function firstStringArray(record: WorkdayRecord, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (!Array.isArray(value)) {
      continue;
    }

    const strings = value.flatMap((entry) => readStringValues(entry)).filter(Boolean);
    if (strings.length > 0) {
      return strings.join(" | ");
    }
  }

  return undefined;
}

function firstObject(record: WorkdayRecord, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (isRecord(value)) {
      return value;
    }

    if (Array.isArray(value)) {
      const first = value.find((entry): entry is Record<string, unknown> => isRecord(entry));
      if (first) {
        return first;
      }
    }
  }

  return undefined;
}

function dedupeStrings(values: Array<string | undefined>) {
  const seen = new Set<string>();
  const deduped: string[] = [];

  values.forEach((value) => {
    const trimmed = value?.trim();
    if (!trimmed || seen.has(trimmed)) {
      return;
    }

    seen.add(trimmed);
    deduped.push(trimmed);
  });

  return deduped;
}

function dedupeWorkdaySeeds(seeds: NormalizedJobSeed[]) {
  const seen = new Set<string>();
  return seeds.filter((seed) => {
    const key = [
      seed.sourceJobId,
      seed.canonicalUrl ?? seed.sourceUrl,
      seed.title,
    ]
      .map((value) => normalizeComparableText(value))
      .join("::");

    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function dedupeWorkdayCandidates(candidates: WorkdayRecord[]) {
  const seen = new Set<string>();
  return candidates.filter((candidate) => {
    const key = normalizeComparableText(
      `${resolveWorkdayTitle(candidate) ?? ""} ${firstString(candidate, ["externalPath", "externalUrl", "url", "jobId", "id"]) ?? ""}`,
    );
    if (!key || seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
