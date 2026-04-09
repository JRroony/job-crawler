import { z } from "zod";

export function normalizeOptionalSearchString(value: unknown) {
  if (value == null) {
    return undefined;
  }

  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

const canonicalSearchFilterKeys = [
  "title",
  "country",
  "state",
  "city",
  "platforms",
  "crawlMode",
  "experienceLevel",
  "experienceLevels",
  "experienceMatchMode",
  "includeUnspecifiedExperience",
] as const;

type CanonicalSearchFilterKey = (typeof canonicalSearchFilterKeys)[number];

function isCanonicalSearchFilterKey(value: string): value is CanonicalSearchFilterKey {
  return canonicalSearchFilterKeys.includes(value as CanonicalSearchFilterKey);
}

export function sanitizeSearchFiltersInput(rawFilters: unknown) {
  if (!rawFilters || typeof rawFilters !== "object" || Array.isArray(rawFilters)) {
    return rawFilters;
  }

  const source = rawFilters as Record<string, unknown>;
  const candidate: Record<string, unknown> = {};

  for (const key of Object.keys(source)) {
    if (!isCanonicalSearchFilterKey(key)) {
      continue;
    }

    if (key === "country" || key === "state" || key === "city") {
      const normalized = normalizeOptionalSearchString(source[key]);

      if (typeof normalized !== "undefined") {
        candidate[key] = normalized;
      }

      continue;
    }

    if (source[key] == null) {
      continue;
    }

    candidate[key] = source[key];
  }

  return candidate;
}

const optionalTrimmedString = z.preprocess(
  normalizeOptionalSearchString,
  z.string().min(1).max(160).optional(),
);

export const experienceLevels = [
  "intern",
  "new_grad",
  "junior",
  "mid",
  "senior",
  "staff",
] as const;

export const experienceLevelSchema = z.enum(experienceLevels);

export const experienceInferenceConfidences = [
  "high",
  "medium",
  "low",
  "none",
] as const;

export const experienceInferenceConfidenceSchema = z.enum(
  experienceInferenceConfidences,
);

export const experienceClassificationSources = [
  "title",
  "structured_metadata",
  "description",
  "page_fetch",
  "unknown",
] as const;

export const experienceClassificationSourceSchema = z.enum(
  experienceClassificationSources,
);

export const experienceMatchModes = ["strict", "balanced", "broad"] as const;

export const experienceMatchModeSchema = z.enum(experienceMatchModes);

export const crawlModes = ["fast", "balanced", "deep"] as const;

export const crawlModeSchema = z.enum(crawlModes);

export const crawlValidationModes = [
  "deferred",
  "inline_top_n",
  "full_inline",
] as const;

export const crawlValidationModeSchema = z.enum(crawlValidationModes);

export const crawlerPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "company_page",
  "workday",
] as const;

export const crawlerPlatformSchema = z.enum(crawlerPlatforms);

export function normalizeExperienceLevels(
  value?: ExperienceLevel | ExperienceLevel[] | null,
) {
  const candidates = Array.isArray(value)
    ? value
    : value
      ? [value]
      : [];
  const normalized = experienceLevels.filter((level) => candidates.includes(level));

  return normalized.length > 0 ? normalized : undefined;
}

export function normalizeCrawlerPlatforms(
  value?: CrawlerPlatform[] | null,
) {
  const candidates = Array.isArray(value) ? value : [];
  const normalized = crawlerPlatforms.filter((platform) =>
    candidates.includes(platform),
  );

  if (normalized.length === 0) {
    return undefined;
  }

  const matchesDefaultImplementedScope =
    normalized.length === activeCrawlerPlatforms.length &&
    activeCrawlerPlatforms.every((platform, index) => normalized[index] === platform);

  return matchesDefaultImplementedScope ? undefined : normalized;
}

export function resolveOperationalCrawlerPlatforms(
  value?: CrawlerPlatform[] | null,
) {
  if (!value) {
    return [...activeCrawlerPlatforms];
  }

  return activeCrawlerPlatforms.filter((platform) => value.includes(platform));
}

export const experienceClassificationSchema = z.object({
  explicitLevel: experienceLevelSchema.optional(),
  inferredLevel: experienceLevelSchema.optional(),
  confidence: experienceInferenceConfidenceSchema,
  source: experienceClassificationSourceSchema,
  reasons: z.array(z.string().min(1)).default([]),
  isUnspecified: z.boolean(),
});

export const linkStatuses = ["valid", "invalid", "stale", "unknown"] as const;

export const linkStatusSchema = z.enum(linkStatuses);

export const providerPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "company_page",
  "linkedin_limited",
  "indeed_limited",
] as const;

export const providerPlatformSchema = z.enum(providerPlatforms);

// Only these platform families are runnable in the current discovery-first pipeline.
// The wider provider platform enum is kept so stored provenance and UI labels can
// still represent limited historical or future-only platform values honestly.
export const activeCrawlerPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "company_page",
] as const;

export const activeCrawlerPlatformSchema = z.enum(activeCrawlerPlatforms);

export const crawlRunStatuses = [
  "running",
  "completed",
  "partial",
  "failed",
] as const;

export const crawlRunStatusSchema = z.enum(crawlRunStatuses);

export const crawlSourceStatuses = [
  "success",
  "partial",
  "failed",
  "unsupported",
] as const;

export const crawlSourceStatusSchema = z.enum(crawlSourceStatuses);

export const publicSearchDiscoveryDiagnosticsSchema = z.object({
  generatedQueries: z.number().int().nonnegative().default(0),
  executedQueries: z.number().int().nonnegative().default(0),
  skippedQueries: z.number().int().nonnegative().default(0),
  maxQueries: z.number().int().nonnegative().default(0),
  maxSources: z.number().int().nonnegative().default(0),
  maxResultsPerQuery: z.number().int().nonnegative().default(0),
  roleQueryCount: z.number().int().nonnegative().default(0),
  locationClauseCount: z.number().int().nonnegative().default(0),
  rawResultsHarvested: z.number().int().nonnegative().default(0),
  normalizedUrlsHarvested: z.number().int().nonnegative().default(0),
  platformMatchedUrls: z.number().int().nonnegative().default(0),
  sourcesAdded: z.number().int().nonnegative().default(0),
  engineRequestCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  engineResultCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  dropReasonCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  sampleGeneratedQueries: z.array(z.string().min(1)).max(12).default([]),
  sampleExecutedQueries: z.array(z.string().min(1)).max(12).default([]),
});

export const discoveryStageDiagnosticsSchema = z.object({
  configuredSources: z.number().int().nonnegative().default(0),
  curatedSources: z.number().int().nonnegative().default(0),
  publicSources: z.number().int().nonnegative().default(0),
  discoveredBeforeFiltering: z.number().int().nonnegative().default(0),
  discoveredAfterFiltering: z.number().int().nonnegative().default(0),
  publicSearch: publicSearchDiscoveryDiagnosticsSchema.optional(),
});

export const crawlDiagnosticsSchema = z.object({
  discoveredSources: z.number().int().nonnegative().default(0),
  crawledSources: z.number().int().nonnegative().default(0),
  providerFailures: z.number().int().nonnegative().default(0),
  excludedByTitle: z.number().int().nonnegative().default(0),
  excludedByLocation: z.number().int().nonnegative().default(0),
  excludedByExperience: z.number().int().nonnegative().default(0),
  dedupedOut: z.number().int().nonnegative().default(0),
  validationDeferred: z.number().int().nonnegative().default(0),
  discovery: discoveryStageDiagnosticsSchema.optional(),
});

export const crawlProviderSummarySchema = z.object({
  provider: providerPlatformSchema,
  status: crawlSourceStatusSchema,
  sourceCount: z.number().int().nonnegative().default(0),
  fetchedCount: z.number().int().nonnegative().default(0),
  matchedCount: z.number().int().nonnegative().default(0),
  savedCount: z.number().int().nonnegative().default(0),
  warningCount: z.number().int().nonnegative().default(0),
  errorMessage: z.string().optional(),
});

export const searchFiltersSchema = z
  .object({
    title: z.string().trim().min(2).max(160),
    country: optionalTrimmedString,
    state: optionalTrimmedString,
    city: optionalTrimmedString,
    platforms: z.array(crawlerPlatformSchema).max(crawlerPlatforms.length).optional(),
    crawlMode: crawlModeSchema.optional(),
    experienceLevel: experienceLevelSchema.optional(),
    experienceLevels: z.array(experienceLevelSchema).max(experienceLevels.length).optional(),
    experienceMatchMode: experienceMatchModeSchema.optional(),
    includeUnspecifiedExperience: z.boolean().optional(),
  })
  .strip()
  .transform(
    ({
      platforms,
      crawlMode,
      experienceLevel,
      experienceLevels,
      experienceMatchMode,
      includeUnspecifiedExperience,
      ...filters
    }) => {
    const normalizedExperienceLevels = normalizeExperienceLevels([
      ...(experienceLevels ?? []),
      ...(experienceLevel ? [experienceLevel] : []),
    ]);
    const normalizedPlatforms = normalizeCrawlerPlatforms(platforms);
    const normalizedIncludeUnspecified =
      includeUnspecifiedExperience || experienceMatchMode === "broad"
        ? true
        : undefined;

    return {
      ...filters,
      ...(normalizedExperienceLevels
        ? {
            experienceLevels: normalizedExperienceLevels,
          }
        : {}),
      ...(normalizedPlatforms
        ? {
            platforms: normalizedPlatforms,
          }
        : {}),
      ...(crawlMode
        ? {
            crawlMode,
          }
        : {}),
      ...(experienceMatchMode
        ? {
            experienceMatchMode,
          }
        : {}),
      ...(normalizedIncludeUnspecified
        ? {
            includeUnspecifiedExperience: true,
          }
        : {}),
    };
    },
  );

export const sourceProvenanceSchema = z.object({
  sourcePlatform: providerPlatformSchema,
  sourceJobId: z.string().min(1),
  sourceUrl: z.string().url(),
  applyUrl: z.string().url(),
  resolvedUrl: z.string().url().optional(),
  canonicalUrl: z.string().url().optional(),
  discoveredAt: z.string().datetime(),
  rawSourceMetadata: z.record(z.string(), z.unknown()).default({}),
});

export const jobListingSchema = z.object({
  _id: z.string().min(1),
  title: z.string().min(1),
  company: z.string().min(1),
  country: z.string().optional(),
  state: z.string().optional(),
  city: z.string().optional(),
  locationText: z.string().min(1),
  experienceLevel: experienceLevelSchema.optional(),
  experienceClassification: experienceClassificationSchema.optional(),
  sourcePlatform: providerPlatformSchema,
  sourceJobId: z.string().min(1),
  sourceUrl: z.string().url(),
  applyUrl: z.string().url(),
  resolvedUrl: z.string().url().optional(),
  canonicalUrl: z.string().url().optional(),
  postedAt: z.string().datetime().optional(),
  discoveredAt: z.string().datetime(),
  linkStatus: linkStatusSchema.default("unknown"),
  lastValidatedAt: z.string().datetime().optional(),
  rawSourceMetadata: z.record(z.string(), z.unknown()).default({}),
  sourceProvenance: z.array(sourceProvenanceSchema).default([]),
  sourceLookupKeys: z.array(z.string().min(1)).default([]),
  crawlRunIds: z.array(z.string().min(1)).default([]),
  companyNormalized: z.string().min(1),
  titleNormalized: z.string().min(1),
  locationNormalized: z.string().min(1),
  contentFingerprint: z.string().min(1),
});

export const persistableJobSchema = jobListingSchema.omit({
  _id: true,
  crawlRunIds: true,
});

export const searchDocumentSchema = z.object({
  _id: z.string().min(1),
  filters: searchFiltersSchema,
  latestCrawlRunId: z.string().optional(),
  createdAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
  lastStatus: crawlRunStatusSchema.optional(),
});

export const crawlRunDocumentSchema = z.object({
  _id: z.string().min(1),
  searchId: z.string().min(1),
  startedAt: z.string().datetime(),
  finishedAt: z.string().datetime().optional(),
  status: crawlRunStatusSchema,
  discoveredSourcesCount: z.number().int().nonnegative().default(0),
  crawledSourcesCount: z.number().int().nonnegative().default(0),
  totalFetchedJobs: z.number().int().nonnegative(),
  totalMatchedJobs: z.number().int().nonnegative(),
  dedupedJobs: z.number().int().nonnegative(),
  validationMode: crawlValidationModeSchema.default("deferred"),
  providerSummary: z.array(crawlProviderSummarySchema).default([]),
  errorMessage: z.string().optional(),
  diagnostics: crawlDiagnosticsSchema.default({}),
});

export const crawlSourceResultSchema = z.object({
  _id: z.string().min(1),
  crawlRunId: z.string().min(1),
  searchId: z.string().min(1),
  provider: providerPlatformSchema,
  status: crawlSourceStatusSchema,
  sourceCount: z.number().int().nonnegative().default(0),
  fetchedCount: z.number().int().nonnegative(),
  matchedCount: z.number().int().nonnegative(),
  savedCount: z.number().int().nonnegative(),
  warningCount: z.number().int().nonnegative().default(0),
  errorMessage: z.string().optional(),
  startedAt: z.string().datetime(),
  finishedAt: z.string().datetime(),
});

export const linkValidationResultSchema = z.object({
  _id: z.string().min(1),
  jobId: z.string().min(1),
  applyUrl: z.string().url(),
  resolvedUrl: z.string().url().optional(),
  canonicalUrl: z.string().url().optional(),
  status: linkStatusSchema,
  method: z.enum(["HEAD", "GET", "CACHE"]),
  httpStatus: z.number().int().min(100).max(599).optional(),
  checkedAt: z.string().datetime(),
  errorMessage: z.string().optional(),
  staleMarkers: z.array(z.string()).optional(),
});

export const companyPageSourceConfigSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("json_feed"),
    company: z.string().min(1),
    url: z.string().url(),
  }),
  z.object({
    type: z.literal("json_ld_page"),
    company: z.string().min(1),
    url: z.string().url(),
  }),
  z.object({
    type: z.literal("html_page"),
    company: z.string().min(1),
    url: z.string().url(),
  }),
]);

export const crawlResponseSchema = z.object({
  search: searchDocumentSchema,
  crawlRun: crawlRunDocumentSchema,
  sourceResults: z.array(crawlSourceResultSchema),
  jobs: z.array(jobListingSchema),
  diagnostics: crawlRunDocumentSchema.shape.diagnostics.default({}),
});

export type ExperienceLevel = z.infer<typeof experienceLevelSchema>;
export type ExperienceInferenceConfidence = z.infer<
  typeof experienceInferenceConfidenceSchema
>;
export type ExperienceClassification = z.infer<
  typeof experienceClassificationSchema
>;
export type ExperienceMatchMode = z.infer<typeof experienceMatchModeSchema>;
export type CrawlerPlatform = z.infer<typeof crawlerPlatformSchema>;
export type ActiveCrawlerPlatform = z.infer<typeof activeCrawlerPlatformSchema>;
export type CrawlMode = z.infer<typeof crawlModeSchema>;
export type CrawlValidationMode = z.infer<typeof crawlValidationModeSchema>;
export type SearchFilters = z.infer<typeof searchFiltersSchema>;
export type JobListing = z.infer<typeof jobListingSchema>;
export type PersistableJobDocument = z.infer<typeof persistableJobSchema>;
export type SearchDocument = z.infer<typeof searchDocumentSchema>;
export type CrawlRun = z.infer<typeof crawlRunDocumentSchema>;
export type CrawlSourceResult = z.infer<typeof crawlSourceResultSchema>;
export type CrawlProviderSummary = z.infer<typeof crawlProviderSummarySchema>;
export type LinkValidationResult = z.infer<typeof linkValidationResultSchema>;
export type SourceProvenance = z.infer<typeof sourceProvenanceSchema>;
export type ProviderPlatform = z.infer<typeof providerPlatformSchema>;
export type CrawlRunStatus = z.infer<typeof crawlRunStatusSchema>;
export type CrawlSourceStatus = z.infer<typeof crawlSourceStatusSchema>;
export type LinkStatus = z.infer<typeof linkStatusSchema>;
export type CompanyPageSourceConfig = z.infer<typeof companyPageSourceConfigSchema>;
export type CrawlResponse = z.infer<typeof crawlResponseSchema>;
export type CrawlDiagnostics = CrawlRun["diagnostics"];
export type PublicSearchDiscoveryDiagnostics = z.infer<
  typeof publicSearchDiscoveryDiagnosticsSchema
>;
export type DiscoveryStageDiagnostics = z.infer<typeof discoveryStageDiagnosticsSchema>;
