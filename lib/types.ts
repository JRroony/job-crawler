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
  "manager",
  "director",
  "executive",
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

export const experienceBands = [
  "unknown",
  "entry",
  "mid",
  "senior",
  "leadership",
  "advanced",
] as const;

export const experienceBandSchema = z.enum(experienceBands);

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

export const experienceClassificationVersion = 2;

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
  "smartrecruiters",
  "workday",
  "company_page",
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
  const normalized = activeCrawlerPlatforms.filter((platform) =>
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
  experienceVersion: z.number().int().positive().default(
    experienceClassificationVersion,
  ),
  experienceBand: experienceBandSchema.default("unknown"),
  experienceSource: experienceClassificationSourceSchema.default("unknown"),
  experienceConfidence: experienceInferenceConfidenceSchema.default("none"),
  experienceSignals: z.array(experienceClassificationSignalSchema).default([]),
  explicitLevel: experienceLevelSchema.optional(),
  inferredLevel: experienceLevelSchema.optional(),
  confidence: experienceInferenceConfidenceSchema.default("none"),
  source: experienceClassificationSourceSchema.default("unknown"),
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
  "smartrecruiters",
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
  "smartrecruiters",
  "workday",
  "company_page",
] as const;

export const activeCrawlerPlatformSchema = z.enum(activeCrawlerPlatforms);

export const crawlRunStatuses = [
  "running",
  "completed",
  "partial",
  "failed",
  "aborted",
] as const;

export const crawlRunStatusSchema = z.enum(crawlRunStatuses);

export const crawlQueueStatuses = [
  "queued",
  ...crawlRunStatuses,
] as const;

export const crawlQueueStatusSchema = z.enum(crawlQueueStatuses);

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
  "timed_out",
  "failed",
  "aborted",
  "unsupported",
] as const;

export const crawlSourceStatusSchema = z.enum(crawlSourceStatuses);

export const publicSearchDiscoveryDiagnosticsSchema = z.object({
  executionStrategy: nullableOptional(
    z.object({
      requestedMode: nullableOptional(z.string()),
      effectiveMode: z.string().min(1),
      reason: z.string().min(1),
      title: nullableOptional(z.string()),
      country: nullableOptional(z.string()),
      state: nullableOptional(z.string()),
      city: nullableOptional(z.string()),
      titleFamily: nullableOptional(z.string()),
      titleConcept: nullableOptional(z.string()),
      canadaHighDemandRole: z.boolean().default(false),
    }),
  ),
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
  stopReason: nullableOptional(
    z.enum([
      "no_queries",
      "completed_query_plan",
      "query_budget_exhausted",
      "source_budget_exhausted",
      "stagnant_query_plateau",
    ]),
  ),
});

export const discoveryStageDiagnosticsSchema = z.object({
  inventorySources: z.number().int().nonnegative().default(0),
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
  sourcesTruncated: z.boolean().optional(),
  sourcesTruncatedCount: z.number().int().nonnegative().optional(),
  sourcesBeforeTruncation: z.number().int().nonnegative().optional(),
  sourcesAfterTruncation: z.number().int().nonnegative().optional(),
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
  backgroundCycle: z
    .object({
      selectedProfiles: z.number().int().nonnegative().default(0),
      selectedProfileIds: z.array(z.string().min(1)).max(24).default([]),
      selectedProfileLabels: z.array(z.string().min(1)).max(24).default([]),
      startedRuns: z.number().int().nonnegative().default(0),
      skippedActiveRuns: z.number().int().nonnegative().default(0),
      sourceBudgetPerCycle: z.number().int().nonnegative().default(0),
      sourceBudgetPerProfile: z.number().int().nonnegative().default(0),
      schedulingIntervalMs: z.number().int().nonnegative().default(0),
      providerTimeoutMs: z.number().int().nonnegative().default(0),
      sourceTimeoutMs: z.number().int().nonnegative().default(0),
      maxSourcesPerProvider: z.number().int().nonnegative().default(0),
      providerConcurrency: z.number().int().nonnegative().default(0),
      runTimeoutMs: z.number().int().nonnegative().default(0),
    })
    .optional(),
  backgroundHealth: z
    .object({
      activeQueueEntries: z.number().int().nonnegative().default(0),
      staleActiveQueueEntries: z.number().int().nonnegative().default(0),
      runningCrawlRuns: z.number().int().nonnegative().default(0),
      staleRunningCrawlRuns: z.number().int().nonnegative().default(0),
      latestQueueHeartbeatAt: nullableOptional(z.string().datetime()),
      latestRunHeartbeatAt: nullableOptional(z.string().datetime()),
      inventorySources: z.number().int().nonnegative().default(0),
      eligibleInventorySources: z.number().int().nonnegative().default(0),
      inventoryByPlatform: z.record(z.string(), z.number().int().nonnegative()).default({}),
      eligibleInventoryByPlatform: z.record(z.string(), z.number().int().nonnegative()).default({}),
      providerSavedCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
      providerFetchedCounts: z.record(z.string(), z.number().int().nonnegative()).default({}),
      jobsUpdatedInLast24Hours: z.number().int().nonnegative().default(0),
      indexedEventsInLast24Hours: z.number().int().nonnegative().default(0),
      recoveredStaleQueueEntries: z.number().int().nonnegative().default(0),
      recoveredStaleCrawlRuns: z.number().int().nonnegative().default(0),
    })
    .optional(),
  inventoryScheduling: z
    .object({
      inventorySources: z.number().int().nonnegative().default(0),
      crawlableSources: z.number().int().nonnegative().default(0),
      eligibleSources: z.number().int().nonnegative().default(0),
      selectedSources: z.number().int().nonnegative().default(0),
      inventoryByPlatform: z.record(z.string(), z.number().int().nonnegative()).default({}),
      eligibleByPlatform: z.record(z.string(), z.number().int().nonnegative()).default({}),
      skippedByReason: z.record(z.string(), z.number().int().nonnegative()).default({}),
      skippedByPlatformReason: z.record(z.string(), z.number().int().nonnegative()).default({}),
      freshnessBuckets: z.record(z.string(), z.number().int().nonnegative()).default({}),
      selectedByPlatform: z.record(z.string(), z.number().int().nonnegative()).default({}),
      platformSelectionBudgets: z.record(z.string(), z.number().int().nonnegative()).default({}),
      selectedByProvider: z.record(z.string(), z.number().int().nonnegative()).default({}),
      selectedByHealth: z.record(z.string(), z.number().int().nonnegative()).default({}),
      selectedSourceIds: z.array(z.string().min(1)).max(12).default([]),
      skippedSourceSamples: z.array(z.string().min(1)).max(12).default([]),
    })
    .optional(),
  systemProfile: z
    .object({
      id: z.string().min(1),
      label: z.string().min(1),
      canonicalJobFamily: z.string().min(1).optional(),
      queryTitleVariant: z.string().min(1).optional(),
      titleVariantTier: z.number().int().nonnegative().optional(),
      geography: z
        .object({
          id: z.string().min(1),
          label: z.string().min(1),
          scope: z.enum(["country", "state", "province", "city"]),
          country: z.string().min(1),
          state: nullableOptional(z.string()),
          city: nullableOptional(z.string()),
          priorityOffset: z.number().int().nonnegative(),
          variantTiers: z.array(z.number().int().nonnegative()).default([]),
        })
        .optional(),
      platformPreference: z
        .object({
          mode: z.enum(["preference", "restriction"]),
          platforms: z.array(z.string().min(1)).default([]),
        })
        .optional(),
      priority: z.number().int().nonnegative().optional(),
      enabled: z.boolean().optional(),
      cadenceMs: z.number().int().nonnegative().optional(),
      cooldownMs: z.number().int().nonnegative().optional(),
      lastRunAt: nullableOptional(z.string().datetime()),
      nextEligibleAt: nullableOptional(z.string().datetime()),
      successCount: z.number().int().nonnegative().optional(),
      failureCount: z.number().int().nonnegative().optional(),
      consecutiveFailureCount: z.number().int().nonnegative().optional(),
      filters: z.object({
        title: z.string().min(1),
        country: nullableOptional(z.string()),
        state: nullableOptional(z.string()),
        city: nullableOptional(z.string()),
        platforms: z.array(z.string().min(1)).optional(),
        crawlMode: nullableOptional(z.string()),
      }),
    })
    .optional(),
  inventoryExpansion: z
    .object({
      beforeCount: z.number().int().nonnegative().default(0),
      afterRefreshCount: z.number().int().nonnegative().default(0),
      afterExpansionCount: z.number().int().nonnegative().default(0),
      selectedSearches: z.number().int().nonnegative().default(0),
      candidateSources: z.number().int().nonnegative().default(0),
      newSourcesAdded: z.number().int().nonnegative().default(0),
      selectedSearchTitles: z.array(z.string().min(1)).max(12).default([]),
      selectedSearchFilters: z
        .array(
          z.object({
            title: z.string().min(1),
            country: nullableOptional(z.string()),
            state: nullableOptional(z.string()),
            city: nullableOptional(z.string()),
            platforms: z.array(z.string().min(1)).optional(),
            crawlMode: nullableOptional(z.string()),
          }),
        )
        .max(12)
        .default([]),
      selectedSourceIds: z.array(z.string().min(1)).max(12).default([]),
      newSourceIds: z.array(z.string().min(1)).max(12).default([]),
      platformCountsBefore: z.record(z.string(), z.number().int().nonnegative()).default({}),
      platformCountsAfter: z.record(z.string(), z.number().int().nonnegative()).default({}),
      skippedReason: nullableOptional(z.string()),
      searchDiagnostics: z
        .array(
          z.object({
            title: z.string().min(1),
            country: nullableOptional(z.string()),
            state: nullableOptional(z.string()),
            city: nullableOptional(z.string()),
            discoveredSources: z.number().int().nonnegative().default(0),
            publicSources: z.number().int().nonnegative().default(0),
            publicJobs: z.number().int().nonnegative().default(0),
            publicSearch: nullableOptional(publicSearchDiscoveryDiagnosticsSchema),
          }),
        )
        .max(12)
        .default([]),
    })
    .optional(),
  backgroundPersistence: z
    .object({
      jobsInserted: z.number().int().nonnegative().default(0),
      jobsUpdated: z.number().int().nonnegative().default(0),
      jobsLinkedToRun: z.number().int().nonnegative().default(0),
      indexedEventsEmitted: z.number().int().nonnegative().default(0),
      failedBatches: z.number().int().nonnegative().default(0),
      failureSamples: z.array(z.string().min(1)).max(8).default([]),
      skippedReason: nullableOptional(z.string()),
      providerStats: z
        .array(
          z.object({
            provider: providerPlatformSchema,
            sourceCount: z.number().int().nonnegative().default(0),
            fetchedCount: z.number().int().nonnegative().default(0),
            matchedCount: z.number().int().nonnegative().default(0),
            seedCount: z.number().int().nonnegative().default(0),
            savedCount: z.number().int().nonnegative().default(0),
            insertedCount: z.number().int().nonnegative().default(0),
            updatedCount: z.number().int().nonnegative().default(0),
            linkedToRunCount: z.number().int().nonnegative().default(0),
            indexedEventCount: z.number().int().nonnegative().default(0),
            warningCount: z.number().int().nonnegative().default(0),
            failedBatches: z.number().int().nonnegative().default(0),
          }),
        )
        .default([]),
    })
    .optional(),
  backgroundBootstrap: z
    .object({
      blocked: z.boolean().default(false),
      reason: nullableOptional(
        z.enum(["mongo_unavailable", "mongo_transient", "bootstrap_running", "bootstrap_failed"]),
      ),
      phase: nullableOptional(z.enum(["repository_resolution", "index_initialization"])),
      message: nullableOptional(z.string()),
    })
    .optional(),
  searchResponse: z
    .object({
      requestedFilters: z.record(z.string(), z.unknown()),
      parsedFilters: z.record(z.string(), z.unknown()),
      searchId: z.string().min(1),
      sessionId: nullableOptional(z.string()),
      candidateCount: z.number().int().nonnegative().default(0),
      matchedCount: z.number().int().nonnegative().default(0),
      finalMatchedCount: z.number().int().nonnegative().optional(),
      totalMatchedCount: z.number().int().nonnegative().optional(),
      returnedCount: z.number().int().nonnegative().optional(),
      pageSize: z.number().int().positive().optional(),
      nextCursor: z.number().int().nonnegative().nullable().optional(),
      hasMore: z.boolean().optional(),
      excludedByTitleCount: z.number().int().nonnegative().default(0),
      excludedByLocationCount: z.number().int().nonnegative().default(0),
      excludedByExperienceCount: z.number().int().nonnegative().default(0),
    })
    .optional(),
  searchTrace: z.record(z.string(), z.unknown()).optional(),
  session: z
    .object({
      indexedResultsCount: z.number().int().nonnegative().default(0),
      initialIndexedResultsCount: z.number().int().nonnegative().optional(),
      supplementalResultsCount: z.number().int().nonnegative().default(0),
      totalVisibleResultsCount: z.number().int().nonnegative().default(0),
      indexedCandidateCount: z.number().int().nonnegative().default(0),
      indexedRequestTimeEvaluationCount: z.number().int().nonnegative().default(0),
      indexedRequestTimeExcludedCount: z.number().int().nonnegative().default(0),
      indexedSearchTimingsMs: z
        .object({
          candidateQuery: z.number().nonnegative().default(0),
          requestTimeRefinement: z.number().nonnegative().default(0),
          total: z.number().nonnegative().default(0),
        })
        .optional(),
      minimumIndexedCoverage: z.number().int().nonnegative().default(0),
      coverageTarget: z.number().int().nonnegative().default(0),
      coveragePolicyReason: nullableOptional(z.string()),
      targetJobCount: z.number().int().nonnegative().default(0),
      supplementalQueued: z.boolean().default(false),
      supplementalRunning: z.boolean().default(false),
      targetedReplenishmentQueued: z.boolean().default(false),
      targetedReplenishmentActive: z.boolean().default(false),
      activeQueueAlreadyExists: z.boolean().default(false),
      backgroundRefreshSuggested: z.boolean().default(false),
      backgroundRefreshQueued: z.boolean().default(false),
      triggerReason: z
        .enum([
          "indexed_coverage_sufficient",
          "reused_completed_coverage",
          "insufficient_indexed_coverage",
          "insufficient_indexed_coverage_targeted_replenishment",
          "insufficient_indexed_coverage_background_requested",
          "indexed_empty_background_requested",
          "background_ingestion_already_active",
          "background_ingestion_unavailable",
          "explicit_request_time_recovery",
          "freshness_recovery",
          "stale_indexed_coverage_background_requested",
          "retry_incomplete_previous_run",
          "incomplete_previous_run_background_requested",
        ])
        .optional(),
      triggerExplanation: nullableOptional(z.string()),
      reusedExistingSearch: z.boolean().optional(),
      previousVisibleJobCount: z.number().int().nonnegative().optional(),
      previousRunStatus: nullableOptional(crawlRunStatusSchema),
      previousFinishedAt: nullableOptional(z.string().datetime()),
      latestIndexedJobAgeMs: nullableOptional(z.number().int().nonnegative()),
      backgroundIngestion: z
        .object({
          status: z.enum([
            "not_requested",
            "started",
            "already_active",
            "disabled",
            "mongo_unavailable",
            "mongo_transient",
            "bootstrap_running",
            "bootstrap_failed",
            "failed",
          ]),
          searchId: nullableOptional(z.string()),
          crawlRunId: nullableOptional(z.string()),
          systemProfileId: nullableOptional(z.string()),
          reason: nullableOptional(z.string()),
          message: nullableOptional(z.string()),
        })
        .optional(),
    })
    .optional(),
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
  stoppedReason: nullableOptional(z.enum(["timeout", "target_met", "completed"])),
  budgetExhausted: z.boolean().optional(),
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

export const resolvedLocationPointSchema = z.object({
  country: z.string().min(1),
  state: z.string().min(1).optional(),
  stateCode: z.string().min(1).optional(),
  city: z.string().min(1).optional(),
  confidence: resolvedLocationConfidenceSchema,
  evidence: z.array(resolvedLocationEvidenceSchema).default([]),
});

export const resolvedLocationConflictSchema = z.object({
  kind: z.enum(["country_conflict", "physical_remote_conflict"]),
  countries: z.array(z.string().min(1)).default([]),
  evidence: z.array(resolvedLocationEvidenceSchema).default([]),
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
  physicalLocations: z.array(resolvedLocationPointSchema).optional(),
  eligibilityCountries: z.array(z.string().min(1)).optional(),
  conflicts: z.array(resolvedLocationConflictSchema).optional(),
});

export const geoLocationPointSchema = z.object({
  country: z.string().min(1).optional(),
  countryCode: z.string().min(1).optional(),
  region: z.string().min(1).optional(),
  regionCode: z.string().min(1).optional(),
  city: z.string().min(1).optional(),
  searchKeys: z.array(z.string().min(1)).default([]),
  confidence: z.enum(["high", "medium", "low"]),
  evidence: z.array(z.string().min(1)).default([]),
});

export const geoLocationSchema = z.object({
  rawText: z.string().min(1),
  normalizedText: z.string(),
  physicalLocations: z.array(geoLocationPointSchema).default([]),
  remoteEligibility: z.array(geoLocationPointSchema).default([]),
  workplaceType: z.enum(["remote", "hybrid", "onsite", "unknown"]),
  isGlobalRemote: z.boolean(),
  unresolvedTerms: z.array(z.string()).default([]),
  conflicts: z.array(z.string()).default([]),
  searchKeys: z.array(z.string().min(1)).default([]),
});

export const jobSearchIndexSchema = z.object({
  titleNormalized: z.string().min(1),
  titleStrippedNormalized: z.string().min(1),
  titleFamily: z.string().min(1).optional(),
  titleRoleGroup: z.string().min(1).optional(),
  titleConceptIds: z.array(z.string().min(1)).default([]),
  titleSearchTerms: z.array(z.string().min(1)).default([]),
  titleSearchKeys: z.array(z.string().min(1)).default([]),
  locationCountryKeys: z.array(z.string().min(1)).default([]),
  locationRegionKeys: z.array(z.string().min(1)).default([]),
  locationCityKeys: z.array(z.string().min(1)).default([]),
  locationSearchKeys: z.array(z.string().min(1)).default([]),
  experienceSearchKeys: z.array(z.string().min(1)).default([]),
  statusSearchKeys: z.array(z.string().min(1)).default([]),
  rankingTimestamps: z
    .object({
      postingDate: z.string().datetime().optional(),
      postedAt: z.string().datetime().optional(),
      lastSeenAt: z.string().datetime().optional(),
      crawledAt: z.string().datetime().optional(),
      discoveredAt: z.string().datetime().optional(),
      indexedAt: z.string().datetime().optional(),
    })
    .default({}),
});

// Canonical job-search entity. Keep this aligned with docs/normalized-job-model.md.
export const jobListingSchema = z.object({
  _id: z.string().min(1),
  canonicalJobKey: z.string().min(1),
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
  geoLocation: geoLocationSchema.optional(),
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
  firstSeenAt: z.string().datetime(),
  lastSeenAt: z.string().datetime(),
  indexedAt: z.string().datetime(),
  isActive: z.boolean().default(true),
  closedAt: z.string().datetime().optional(),
  searchIndex: jobSearchIndexSchema.optional(),
  dedupeFingerprint: z.string().min(1),
  companyNormalized: z.string().min(1),
  titleNormalized: z.string().min(1),
  locationNormalized: z.string().min(1),
  contentFingerprint: z.string().min(1),
  contentHash: z.string().min(1),
});

export const persistableJobSchema = jobListingSchema.omit({
  _id: true,
  crawlRunIds: true,
});

export const searchDocumentSchema = z.object({
  _id: z.string().min(1),
  filters: searchFiltersSchema,
  systemProfileId: nullableOptional(z.string()),
  systemProfileLabel: nullableOptional(z.string()),
  latestCrawlRunId: nullableOptional(z.string()),
  latestSearchSessionId: nullableOptional(z.string()),
  createdAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
  lastStatus: nullableOptional(crawlRunStatusSchema),
});

export const searchSessionDocumentSchema = z.object({
  _id: z.string().min(1),
  searchId: z.string().min(1),
  latestCrawlRunId: nullableOptional(z.string()),
  status: crawlRunStatusSchema,
  createdAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
  finishedAt: nullableOptional(z.string().datetime()),
  lastEventSequence: z.number().int().nonnegative().default(0),
  lastEventAt: nullableOptional(z.string().datetime()),
});

export const crawlRunDocumentSchema = z.object({
  _id: z.string().min(1),
  searchId: z.string().min(1),
  searchSessionId: nullableOptional(z.string()),
  startedAt: z.string().datetime(),
  finishedAt: nullableOptional(z.string().datetime()),
  status: crawlRunStatusSchema,
  stage: nullableOptional(crawlRunStageSchema),
  cancelRequestedAt: nullableOptional(z.string().datetime()),
  cancelReason: nullableOptional(z.string()),
  lastHeartbeatAt: nullableOptional(z.string().datetime()),
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

export const crawlControlDocumentSchema = z.object({
  _id: z.string().min(1),
  crawlRunId: z.string().min(1),
  searchId: z.string().min(1),
  searchSessionId: nullableOptional(z.string()),
  ownerKey: nullableOptional(z.string()),
  status: crawlRunStatusSchema,
  startedAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
  finishedAt: nullableOptional(z.string().datetime()),
  cancelRequestedAt: nullableOptional(z.string().datetime()),
  cancelReason: nullableOptional(z.string()),
  lastHeartbeatAt: nullableOptional(z.string().datetime()),
  workerId: nullableOptional(z.string()),
});

export const crawlQueueDocumentSchema = z.object({
  _id: z.string().min(1),
  crawlRunId: z.string().min(1),
  searchId: z.string().min(1),
  searchSessionId: nullableOptional(z.string()),
  ownerKey: nullableOptional(z.string()),
  status: crawlQueueStatusSchema,
  queuedAt: z.string().datetime(),
  startedAt: nullableOptional(z.string().datetime()),
  updatedAt: z.string().datetime(),
  finishedAt: nullableOptional(z.string().datetime()),
  cancelRequestedAt: nullableOptional(z.string().datetime()),
  cancelReason: nullableOptional(z.string()),
  lastHeartbeatAt: nullableOptional(z.string().datetime()),
  workerId: nullableOptional(z.string()),
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

export const searchSessionJobEventSchema = z.object({
  _id: z.string().min(1),
  searchSessionId: z.string().min(1),
  crawlRunId: z.string().min(1),
  jobId: z.string().min(1),
  sequence: z.number().int().nonnegative(),
  createdAt: z.string().datetime(),
});

export const indexedJobEventSchema = z.object({
  _id: z.string().min(1),
  jobId: z.string().min(1),
  crawlRunId: z.string().min(1),
  sequence: z.number().int().nonnegative(),
  createdAt: z.string().datetime(),
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
  searchId: z.string().min(1).optional(),
  searchSessionId: nullableOptional(z.string()),
  candidateCount: z.number().int().nonnegative().optional(),
  finalMatchedCount: z.number().int().nonnegative().optional(),
  totalMatchedCount: z.number().int().nonnegative().optional(),
  returnedCount: z.number().int().nonnegative().optional(),
  pageSize: z.number().int().positive().optional(),
  nextCursor: z.number().int().nonnegative().nullable().optional(),
  hasMore: z.boolean().optional(),
  search: searchDocumentSchema,
  searchSession: searchSessionDocumentSchema.optional(),
  crawlRun: crawlRunDocumentSchema,
  sourceResults: z.array(crawlSourceResultSchema),
  jobs: z.array(jobListingSchema),
  diagnostics: crawlRunDocumentSchema.shape.diagnostics.default({}),
  delivery: z.object({
    mode: z.literal("full"),
    cursor: z.number().int().nonnegative(),
    indexedCursor: z.number().int().nonnegative().optional(),
  }).optional(),
});

export const crawlDeltaResponseSchema = z.object({
  searchId: z.string().min(1).optional(),
  searchSessionId: nullableOptional(z.string()),
  candidateCount: z.number().int().nonnegative().optional(),
  finalMatchedCount: z.number().int().nonnegative().optional(),
  totalMatchedCount: z.number().int().nonnegative().optional(),
  returnedCount: z.number().int().nonnegative().optional(),
  pageSize: z.number().int().positive().optional(),
  nextCursor: z.number().int().nonnegative().nullable().optional(),
  hasMore: z.boolean().optional(),
  search: searchDocumentSchema,
  searchSession: searchSessionDocumentSchema.optional(),
  crawlRun: crawlRunDocumentSchema,
  sourceResults: z.array(crawlSourceResultSchema),
  jobs: z.array(jobListingSchema),
  diagnostics: crawlRunDocumentSchema.shape.diagnostics.default({}),
  delivery: z.object({
    mode: z.literal("delta"),
    cursor: z.number().int().nonnegative(),
    previousCursor: z.number().int().nonnegative(),
    indexedCursor: z.number().int().nonnegative().optional(),
    previousIndexedCursor: z.number().int().nonnegative().optional(),
  }),
});

export type ExperienceLevel = z.infer<typeof experienceLevelSchema>;
export type ExperienceBand = z.infer<typeof experienceBandSchema>;
export type ExperienceInferenceConfidence = z.infer<
  typeof experienceInferenceConfidenceSchema
>;
export type ExperienceClassificationSource = z.infer<
  typeof experienceClassificationSourceSchema
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
export type JobSearchIndex = z.infer<typeof jobSearchIndexSchema>;
export type JobListing = z.infer<typeof jobListingSchema>;
export type PersistableJobDocument = z.infer<typeof persistableJobSchema>;
export type SearchDocument = z.infer<typeof searchDocumentSchema>;
export type SearchSessionDocument = z.infer<typeof searchSessionDocumentSchema>;
export type CrawlRun = z.infer<typeof crawlRunDocumentSchema>;
export type CrawlControlDocument = z.infer<typeof crawlControlDocumentSchema>;
export type CrawlQueueDocument = z.infer<typeof crawlQueueDocumentSchema>;
export type CrawlSourceResult = z.infer<typeof crawlSourceResultSchema>;
export type SearchSessionJobEvent = z.infer<typeof searchSessionJobEventSchema>;
export type IndexedJobEvent = z.infer<typeof indexedJobEventSchema>;
export type CrawlProviderSummary = z.infer<typeof crawlProviderSummarySchema>;
export type LinkValidationResult = z.infer<typeof linkValidationResultSchema>;
export type SourceProvenance = z.infer<typeof sourceProvenanceSchema>;
export type ProviderPlatform = z.infer<typeof providerPlatformSchema>;
export type CrawlRunStatus = z.infer<typeof crawlRunStatusSchema>;
export type CrawlQueueStatus = z.infer<typeof crawlQueueStatusSchema>;
export type CrawlRunStage = z.infer<typeof crawlRunStageSchema>;
export type CrawlSourceStatus = z.infer<typeof crawlSourceStatusSchema>;
export type LinkStatus = z.infer<typeof linkStatusSchema>;
export type CompanyPageSourceConfig = z.infer<typeof companyPageSourceConfigSchema>;
export type CrawlResponse = z.infer<typeof crawlResponseSchema>;
export type CrawlDeltaResponse = z.infer<typeof crawlDeltaResponseSchema>;
export type CrawlDiagnostics = CrawlRun["diagnostics"];
export type PublicSearchDiscoveryDiagnostics = z.infer<
  typeof publicSearchDiscoveryDiagnosticsSchema
>;
export type DiscoveryStageDiagnostics = z.infer<typeof discoveryStageDiagnosticsSchema>;
