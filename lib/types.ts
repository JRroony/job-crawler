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

export function normalizeOptionalSchemaInput(value: unknown) {
  return value == null ? undefined : value;
}

function nullableOptional<TSchema extends z.ZodTypeAny>(schema: TSchema) {
  return z.preprocess(normalizeOptionalSchemaInput, schema.optional());
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
  "lead",
  "staff",
  "principal",
] as const;

export const experienceLevelSchema = z.enum(experienceLevels);

export const experienceClassifierOutcomes = [...experienceLevels, "unknown"] as const;

export const experienceClassifierOutcomeSchema = z.enum(
  experienceClassifierOutcomes,
);

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

export const experienceClassificationSignalSchema = z.object({
  ruleId: z.string().min(1),
  signalType: z.enum([
    "title_keyword",
    "acronym",
    "level_code",
    "leadership_context",
    "years_of_experience",
    "structured_hint",
    "description_hint",
    "metadata_hint",
  ]),
  source: experienceClassificationSourceSchema,
  level: experienceLevelSchema,
  confidence: experienceInferenceConfidenceSchema,
  matchedText: z.string().min(1),
  rationale: z.string().min(1),
});

export const experienceClassificationDiagnosticsSchema = z.object({
  originalTitle: z.string().default(""),
  normalizedTitle: z.string().default(""),
  finalSeniority: experienceClassifierOutcomeSchema,
  matchedSignals: z.array(experienceClassificationSignalSchema).default([]),
  rationale: z.array(z.string().min(1)).default([]),
});

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

export const remoteTypes = ["remote", "hybrid", "onsite", "unknown"] as const;

export const remoteTypeSchema = z.enum(remoteTypes);

export const employmentTypes = [
  "full_time",
  "part_time",
  "contract",
  "temporary",
  "internship",
  "apprenticeship",
  "seasonal",
  "freelance",
  "unknown",
] as const;

export const employmentTypeSchema = z.enum(employmentTypes);

export const sponsorshipHints = ["supported", "not_supported", "unknown"] as const;

export const sponsorshipHintSchema = z.enum(sponsorshipHints);

export const salaryIntervals = [
  "hour",
  "day",
  "week",
  "month",
  "year",
  "unknown",
] as const;

export const salaryIntervalSchema = z.enum(salaryIntervals);

export const salaryInfoSchema = z.object({
  minAmount: z.number().nonnegative().optional(),
  maxAmount: z.number().nonnegative().optional(),
  currency: z.string().min(1).optional(),
  interval: salaryIntervalSchema.default("unknown"),
  rawText: z.string().min(1).optional(),
});

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
  diagnostics: experienceClassificationDiagnosticsSchema.optional(),
});

export const linkStatuses = ["valid", "invalid", "stale", "unknown"] as const;

export const linkStatusSchema = z.enum(linkStatuses);

export const providerPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "workday",
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
  "workday",
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

export const crawlRunStages = [
  "queued",
  "discovering",
  "crawling",
  "validating",
  "finalizing",
] as const;

export const crawlRunStageSchema = z.enum(crawlRunStages);

export const crawlSourceStatuses = [
  "running",
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
  candidateUrlsHarvested: z.number().int().nonnegative().default(0),
  detailUrlsHarvested: z.number().int().nonnegative().default(0),
  sourceUrlsHarvested: z.number().int().nonnegative().default(0),
  recoveredSourcesFromDetailUrls: z.number().int().nonnegative().default(0),
  directJobsExtracted: z.number().int().nonnegative().default(0),
  sourcesAdded: z.number().int().nonnegative().default(0),
  engineRequestCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  engineResultCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  dropReasonCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  sampleGeneratedRoleQueries: z.array(z.string().min(1)).max(12).default([]),
  sampleGeneratedQueries: z.array(z.string().min(1)).max(12).default([]),
  sampleExecutedRoleQueries: z.array(z.string().min(1)).max(12).default([]),
  sampleExecutedQueries: z.array(z.string().min(1)).max(12).default([]),
  sampleHarvestedCandidateUrls: z.array(z.string().url()).max(12).default([]),
  sampleHarvestedDetailUrls: z.array(z.string().url()).max(12).default([]),
  sampleHarvestedSourceUrls: z.array(z.string().url()).max(12).default([]),
  sampleRecoveredSourceUrls: z.array(z.string().url()).max(12).default([]),
  coverageNotes: z.array(z.string().min(1)).max(12).default([]),
});

export const discoveryStageDiagnosticsSchema = z.object({
  configuredSources: z.number().int().nonnegative().default(0),
  curatedSources: z.number().int().nonnegative().default(0),
  publicSources: z.number().int().nonnegative().default(0),
  publicJobs: z.number().int().nonnegative().default(0),
  discoveredBeforeFiltering: z.number().int().nonnegative().default(0),
  discoveredAfterFiltering: z.number().int().nonnegative().default(0),
  platformCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  publicJobPlatformCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  zeroCoverageReason: nullableOptional(z.string()),
  publicSearchSkippedReason: nullableOptional(z.string()),
  publicSearch: nullableOptional(publicSearchDiscoveryDiagnosticsSchema),
});

export const crawlDiagnosticsSchema = z.object({
  discoveredSources: z.number().int().nonnegative().default(0),
  crawledSources: z.number().int().nonnegative().default(0),
  providersEnqueued: z.number().int().nonnegative().default(0),
  providerFailures: z.number().int().nonnegative().default(0),
  directJobsHarvested: z.number().int().nonnegative().default(0),
  jobsBeforeDedupe: z.number().int().nonnegative().default(0),
  jobsAfterDedupe: z.number().int().nonnegative().default(0),
  excludedByTitle: z.number().int().nonnegative().default(0),
  excludedByLocation: z.number().int().nonnegative().default(0),
  excludedByExperience: z.number().int().nonnegative().default(0),
  dedupedOut: z.number().int().nonnegative().default(0),
  validationDeferred: z.number().int().nonnegative().default(0),
  performance: z
    .object({
      timeToFirstVisibleResultMs: nullableOptional(z.number().nonnegative()),
      stageTimingsMs: z
        .object({
          discovery: z.number().nonnegative().default(0),
          providerExecution: z.number().nonnegative().default(0),
          filtering: z.number().nonnegative().default(0),
          dedupe: z.number().nonnegative().default(0),
          persistence: z.number().nonnegative().default(0),
          validation: z.number().nonnegative().default(0),
          responseAssembly: z.number().nonnegative().default(0),
          total: z.number().nonnegative().default(0),
        })
        .default({}),
      providerTimingsMs: z
        .array(
          z.object({
            provider: providerPlatformSchema,
            duration: z.number().nonnegative(),
            sourceCount: z.number().int().nonnegative(),
            timedOut: z.boolean().default(false),
          }),
        )
        .default([]),
      progressUpdateCount: z.number().int().nonnegative().default(0),
      persistenceBatchCount: z.number().int().nonnegative().default(0),
    })
    .default({}),
  dropReasonCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
  filterDecisionTraces: z
    .array(
      z.object({
        traceId: z.string().min(1),
        sourcePlatform: providerPlatformSchema,
        sourceJobId: z.string().min(1),
        sourceUrl: z.string().url(),
        applyUrl: z.string().url(),
        canonicalUrl: nullableOptional(z.string().url()),
        company: z.string().min(1),
        title: z.string().min(1),
        locationText: z.string().min(1),
        filterStage: z.enum(["title", "location", "experience"]),
        outcome: z.enum(["passed", "dropped"]),
        dropReason: nullableOptional(z.string()),
        titleDiagnostics: z.object({
          original: z.string(),
          normalized: z.string(),
          canonical: nullableOptional(z.string()),
          family: nullableOptional(z.string()),
          tier: nullableOptional(z.string()),
          score: nullableOptional(z.number()),
          threshold: nullableOptional(z.number()),
          explanation: nullableOptional(z.string()),
          matchedTerms: z.array(z.string()).default([]),
          penalties: z.array(z.string()).default([]),
          passed: z.boolean(),
        }),
        locationDiagnostics: z.object({
          raw: z.string(),
          normalized: z.string(),
          country: nullableOptional(z.string()),
          state: nullableOptional(z.string()),
          stateCode: nullableOptional(z.string()),
          city: nullableOptional(z.string()),
          isRemote: z.boolean(),
          isUnitedStates: z.boolean(),
          explanation: nullableOptional(z.string()),
          matchedTerms: z.array(z.string()).default([]),
          passed: z.boolean(),
        }),
        experienceDiagnostics: z.object({
          level: nullableOptional(experienceLevelSchema),
          finalSeniority: experienceClassifierOutcomeSchema,
          normalizedTitle: z.string(),
          source: z.string(),
          confidence: experienceInferenceConfidenceSchema,
          selectedLevels: z.array(experienceLevelSchema).default([]),
          mode: experienceMatchModeSchema,
          includeUnspecified: z.boolean(),
          explanation: z.string(),
          passed: z.boolean(),
          reasons: z.array(z.string()).default([]),
          matchedSignals: z.array(experienceClassificationSignalSchema).default([]),
        }),
      }),
    )
    .default([]),
  dedupeDecisionTraces: z
    .array(
      z.object({
        traceId: z.string().min(1),
        keptTraceId: z.string().min(1),
        originalIdentifiers: z.object({
          databaseId: nullableOptional(z.string()),
          sourcePlatform: providerPlatformSchema,
          sourceJobId: z.string().min(1),
          sourceUrl: z.string().url(),
          applyUrl: z.string().url(),
          resolvedUrl: nullableOptional(z.string().url()),
          canonicalUrl: nullableOptional(z.string().url()),
          sourceLookupKeys: z.array(z.string()).default([]),
        }),
        normalizedIdentity: z.object({
          company: z.string().min(1),
          title: z.string().min(1),
          location: z.string().min(1),
          platformJobKeys: z.array(z.string()).default([]),
          sourceUrl: z.string().min(1),
          applyUrl: z.string().min(1),
          resolvedUrl: nullableOptional(z.string()),
          canonicalUrl: nullableOptional(z.string()),
          fallbackFingerprint: z.string().min(1),
        }),
        sourcePlatform: providerPlatformSchema,
        sourceJobId: z.string().min(1),
        sourceUrl: z.string().url(),
        canonicalUrl: nullableOptional(z.string().url()),
        applyUrl: z.string().url(),
        title: z.string().min(1),
        company: z.string().min(1),
        locationText: z.string().min(1),
        outcome: z.enum(["kept", "deduped"]),
        dropReason: nullableOptional(z.string()),
        decisionReason: z.string().min(1),
        matchedKeys: z.array(z.string()).default([]),
      }),
    )
    .default([]),
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
  errorMessage: nullableOptional(z.string()),
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

export const resolvedLocationConfidenceLevels = [
  "high",
  "medium",
  "low",
  "none",
] as const;

export const resolvedLocationConfidenceSchema = z.enum(
  resolvedLocationConfidenceLevels,
);

export const resolvedLocationEvidenceSources = [
  "structured_fields",
  "location_text",
  "metadata",
  "office_metadata",
  "description",
  "remote_hint",
] as const;

export const resolvedLocationEvidenceSourceSchema = z.enum(
  resolvedLocationEvidenceSources,
);

export const resolvedLocationEvidenceSchema = z.object({
  source: resolvedLocationEvidenceSourceSchema,
  value: z.string().min(1),
});

export const resolvedLocationSchema = z.object({
  country: z.string().optional(),
  state: z.string().optional(),
  stateCode: z.string().optional(),
  city: z.string().optional(),
  isRemote: z.boolean(),
  isUnitedStates: z.boolean(),
  confidence: resolvedLocationConfidenceSchema,
  evidence: z.array(resolvedLocationEvidenceSchema).default([]),
});

// Canonical job-search entity. Keep this aligned with docs/normalized-job-model.md.
export const jobListingSchema = z.object({
  _id: z.string().min(1),
  title: z.string().min(1),
  company: z.string().min(1),
  normalizedCompany: z.string().min(1),
  normalizedTitle: z.string().min(1),
  country: z.string().optional(),
  state: z.string().optional(),
  city: z.string().optional(),
  locationRaw: z.string().min(1),
  normalizedLocation: z.string().min(1),
  locationText: z.string().min(1),
  resolvedLocation: resolvedLocationSchema.optional(),
  remoteType: remoteTypeSchema.default("unknown"),
  employmentType: employmentTypeSchema.optional(),
  seniority: experienceLevelSchema.optional(),
  experienceLevel: experienceLevelSchema.optional(),
  experienceClassification: experienceClassificationSchema.optional(),
  sourcePlatform: providerPlatformSchema,
  sourceCompanySlug: z.string().min(1).optional(),
  sourceJobId: z.string().min(1),
  sourceUrl: z.string().url(),
  applyUrl: z.string().url(),
  resolvedUrl: z.string().url().optional(),
  canonicalUrl: z.string().url().optional(),
  postingDate: z.string().datetime().optional(),
  postedAt: z.string().datetime().optional(),
  discoveredAt: z.string().datetime(),
  crawledAt: z.string().datetime(),
  descriptionSnippet: z.string().min(1).optional(),
  salaryInfo: salaryInfoSchema.optional(),
  sponsorshipHint: sponsorshipHintSchema.default("unknown"),
  linkStatus: linkStatusSchema.default("unknown"),
  lastValidatedAt: z.string().datetime().optional(),
  rawSourceMetadata: z.record(z.string(), z.unknown()).default({}),
  sourceProvenance: z.array(sourceProvenanceSchema).default([]),
  sourceLookupKeys: z.array(z.string().min(1)).default([]),
  crawlRunIds: z.array(z.string().min(1)).default([]),
  dedupeFingerprint: z.string().min(1),
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
  latestCrawlRunId: nullableOptional(z.string()),
  createdAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
  lastStatus: nullableOptional(crawlRunStatusSchema),
});

export const crawlRunDocumentSchema = z.object({
  _id: z.string().min(1),
  searchId: z.string().min(1),
  startedAt: z.string().datetime(),
  finishedAt: nullableOptional(z.string().datetime()),
  status: crawlRunStatusSchema,
  stage: nullableOptional(crawlRunStageSchema),
  discoveredSourcesCount: z.number().int().nonnegative().default(0),
  crawledSourcesCount: z.number().int().nonnegative().default(0),
  totalFetchedJobs: z.number().int().nonnegative(),
  totalMatchedJobs: z.number().int().nonnegative(),
  dedupedJobs: z.number().int().nonnegative(),
  validationMode: crawlValidationModeSchema.default("deferred"),
  providerSummary: z.array(crawlProviderSummarySchema).default([]),
  errorMessage: nullableOptional(z.string()),
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
  errorMessage: nullableOptional(z.string()),
  startedAt: z.string().datetime(),
  finishedAt: z.string().datetime(),
});

export const linkValidationResultSchema = z.object({
  _id: z.string().min(1),
  jobId: z.string().min(1),
  applyUrl: z.string().url(),
  resolvedUrl: nullableOptional(z.string().url()),
  canonicalUrl: nullableOptional(z.string().url()),
  status: linkStatusSchema,
  method: z.enum(["HEAD", "GET", "CACHE"]),
  httpStatus: nullableOptional(z.number().int().min(100).max(599)),
  checkedAt: z.string().datetime(),
  errorMessage: nullableOptional(z.string()),
  staleMarkers: nullableOptional(z.array(z.string())),
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
export type RemoteType = z.infer<typeof remoteTypeSchema>;
export type EmploymentType = z.infer<typeof employmentTypeSchema>;
export type SponsorshipHint = z.infer<typeof sponsorshipHintSchema>;
export type SalaryInfo = z.infer<typeof salaryInfoSchema>;
export type SearchFilters = z.infer<typeof searchFiltersSchema>;
export type ResolvedLocationConfidence = z.infer<
  typeof resolvedLocationConfidenceSchema
>;
export type ResolvedLocationEvidenceSource = z.infer<
  typeof resolvedLocationEvidenceSourceSchema
>;
export type ResolvedLocationEvidence = z.infer<
  typeof resolvedLocationEvidenceSchema
>;
export type ResolvedLocation = z.infer<typeof resolvedLocationSchema>;
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
export type CrawlRunStage = z.infer<typeof crawlRunStageSchema>;
export type CrawlSourceStatus = z.infer<typeof crawlSourceStatusSchema>;
export type LinkStatus = z.infer<typeof linkStatusSchema>;
export type CompanyPageSourceConfig = z.infer<typeof companyPageSourceConfigSchema>;
export type CrawlResponse = z.infer<typeof crawlResponseSchema>;
export type CrawlDiagnostics = CrawlRun["diagnostics"];
export type PublicSearchDiscoveryDiagnostics = z.infer<
  typeof publicSearchDiscoveryDiagnosticsSchema
>;
export type DiscoveryStageDiagnostics = z.infer<typeof discoveryStageDiagnosticsSchema>;
