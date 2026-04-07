import { z } from "zod";

const optionalTrimmedString = z.preprocess((value) => {
  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}, z.string().min(1).max(160).optional());

export const experienceLevels = [
  "intern",
  "new_grad",
  "junior",
  "mid",
  "senior",
  "staff",
] as const;

export const experienceLevelSchema = z.enum(experienceLevels);

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

export const searchFiltersSchema = z
  .object({
    title: z.string().trim().min(2).max(160),
    country: optionalTrimmedString,
    state: optionalTrimmedString,
    city: optionalTrimmedString,
    experienceLevel: experienceLevelSchema.optional(),
    experienceLevels: z.array(experienceLevelSchema).max(experienceLevels.length).optional(),
  })
  .transform(({ experienceLevel, experienceLevels, ...filters }) => {
    const normalizedExperienceLevels = normalizeExperienceLevels([
      ...(experienceLevels ?? []),
      ...(experienceLevel ? [experienceLevel] : []),
    ]);

    return {
      ...filters,
      ...(normalizedExperienceLevels
        ? {
            experienceLevels: normalizedExperienceLevels,
          }
        : {}),
    };
  });

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
  sourcePlatform: providerPlatformSchema,
  sourceJobId: z.string().min(1),
  sourceUrl: z.string().url(),
  applyUrl: z.string().url(),
  resolvedUrl: z.string().url().optional(),
  canonicalUrl: z.string().url().optional(),
  postedAt: z.string().datetime().optional(),
  discoveredAt: z.string().datetime(),
  linkStatus: linkStatusSchema,
  lastValidatedAt: z.string().datetime().optional(),
  rawSourceMetadata: z.record(z.string(), z.unknown()),
  sourceProvenance: z.array(sourceProvenanceSchema),
  sourceLookupKeys: z.array(z.string().min(1)),
  crawlRunIds: z.array(z.string().min(1)),
  companyNormalized: z.string().min(1),
  titleNormalized: z.string().min(1),
  locationNormalized: z.string().min(1),
  contentFingerprint: z.string().min(1),
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
  totalFetchedJobs: z.number().int().nonnegative(),
  totalMatchedJobs: z.number().int().nonnegative(),
  dedupedJobs: z.number().int().nonnegative(),
  errorMessage: z.string().optional(),
});

export const crawlSourceResultSchema = z.object({
  _id: z.string().min(1),
  crawlRunId: z.string().min(1),
  searchId: z.string().min(1),
  provider: providerPlatformSchema,
  status: crawlSourceStatusSchema,
  fetchedCount: z.number().int().nonnegative(),
  matchedCount: z.number().int().nonnegative(),
  savedCount: z.number().int().nonnegative(),
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
});

export type ExperienceLevel = z.infer<typeof experienceLevelSchema>;
export type SearchFilters = z.infer<typeof searchFiltersSchema>;
export type JobListing = z.infer<typeof jobListingSchema>;
export type SearchDocument = z.infer<typeof searchDocumentSchema>;
export type CrawlRun = z.infer<typeof crawlRunDocumentSchema>;
export type CrawlSourceResult = z.infer<typeof crawlSourceResultSchema>;
export type LinkValidationResult = z.infer<typeof linkValidationResultSchema>;
export type SourceProvenance = z.infer<typeof sourceProvenanceSchema>;
export type ProviderPlatform = z.infer<typeof providerPlatformSchema>;
export type CrawlRunStatus = z.infer<typeof crawlRunStatusSchema>;
export type CrawlSourceStatus = z.infer<typeof crawlSourceStatusSchema>;
export type LinkStatus = z.infer<typeof linkStatusSchema>;
export type CompanyPageSourceConfig = z.infer<typeof companyPageSourceConfigSchema>;
export type CrawlResponse = z.infer<typeof crawlResponseSchema>;
