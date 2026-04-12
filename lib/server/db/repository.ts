import "server-only";

import type { Db } from "mongodb";

import { dedupeJobs, dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { buildSourceLookupKey, createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/indexes";
import { getMemoryDb } from "@/lib/server/db/memory";
import { getMongoDb } from "@/lib/server/mongodb";
import type {
  CrawlDiagnostics,
  CrawlProviderSummary,
  CrawlRun,
  CrawlRunStatus,
  CrawlSourceResult,
  CrawlValidationMode,
  JobListing,
  LinkValidationResult,
  PersistableJobDocument,
  SearchDocument,
  SearchFilters,
  SourceProvenance,
} from "@/lib/types";
import {
  crawlDiagnosticsSchema,
  experienceClassificationSchema,
  crawlProviderSummarySchema,
  crawlRunDocumentSchema,
  crawlSourceResultSchema,
  jobListingSchema,
  linkValidationResultSchema,
  persistableJobSchema,
  resolvedLocationSchema,
  sanitizeSearchFiltersInput,
  searchDocumentSchema,
  sourceProvenanceSchema,
} from "@/lib/types";

type SortSpec = Record<string, 1 | -1>;

export type CollectionAdapter<TDocument extends Record<string, unknown>> = {
  findOne(
    filter: Record<string, unknown>,
    options?: { sort?: SortSpec },
  ): Promise<TDocument | null>;
  insertOne(document: TDocument): Promise<unknown>;
  updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ): Promise<unknown>;
  find(
    filter?: Record<string, unknown>,
    options?: { sort?: SortSpec; limit?: number },
  ): { toArray(): Promise<TDocument[]> };
};

export type DatabaseAdapter = {
  collection<TDocument extends Record<string, unknown>>(name: string): CollectionAdapter<TDocument>;
};

export type PersistableJob = PersistableJobDocument;

let hasWarnedMemoryFallback = false;

export class JobCrawlerRepository {
  constructor(private readonly db: DatabaseAdapter) {}

  async createSearch(filters: SearchFilters, now = new Date().toISOString()) {
    const document = searchDocumentSchema.parse({
      _id: createId(),
      filters,
      createdAt: now,
      updatedAt: now,
    });

    await this.searches().insertOne(document);
    return document;
  }

  async listRecentSearches(limit = 6) {
    const documents = await this.searches()
      .find({}, { sort: { createdAt: -1 }, limit })
      .toArray();

    return documents.map((document) => parseStoredSearch(document));
  }

  async getSearch(searchId: string) {
    const document = await this.searches().findOne({ _id: searchId });
    return document ? parseStoredSearch(document) : null;
  }

  async updateSearchLatestRun(
    searchId: string,
    crawlRunId: string,
    status: CrawlRunStatus,
    now = new Date().toISOString(),
  ) {
    await this.searches().updateOne(
      { _id: searchId },
      {
        $set: {
          latestCrawlRunId: crawlRunId,
          lastStatus: status,
          updatedAt: now,
        },
      },
    );
  }

  async createCrawlRun(
    searchId: string,
    now = new Date().toISOString(),
    options: {
      validationMode?: CrawlValidationMode;
    } = {},
  ) {
    const document = createStoredCrawlRunDocument(searchId, now, options.validationMode);

    await this.crawlRuns().insertOne(document);
    return document;
  }

  async getCrawlRun(crawlRunId: string) {
    const document = await this.crawlRuns().findOne({ _id: crawlRunId });
    return document ? parseStoredCrawlRun(document) : null;
  }

  async finalizeCrawlRun(
    crawlRunId: string,
    payload: {
      status: CrawlRunStatus;
      totalFetchedJobs: number;
      totalMatchedJobs: number;
      dedupedJobs: number;
      diagnostics?: CrawlDiagnostics;
      validationMode?: CrawlValidationMode;
      providerSummary?: CrawlProviderSummary[];
      errorMessage?: string;
      finishedAt?: string;
    },
  ) {
    const diagnostics = normalizeCrawlDiagnostics(payload.diagnostics);

    await this.crawlRuns().updateOne(
      { _id: crawlRunId },
      {
        $set: {
          ...payload,
          diagnostics,
          discoveredSourcesCount: diagnostics.discoveredSources,
          crawledSourcesCount: diagnostics.crawledSources,
          validationMode: payload.validationMode ?? "deferred",
          providerSummary: normalizeProviderSummary(payload.providerSummary),
          finishedAt: payload.finishedAt ?? new Date().toISOString(),
        },
      },
    );
  }

  async saveCrawlSourceResults(sourceResults: CrawlSourceResult[]) {
    for (const sourceResult of sourceResults) {
      await this.crawlSourceResults().insertOne(crawlSourceResultSchema.parse(sourceResult));
    }
  }

  async getCrawlSourceResults(crawlRunId: string) {
    const documents = await this.crawlSourceResults()
      .find({ crawlRunId }, { sort: { provider: 1 } })
      .toArray();

    return documents.map((document) => parseStoredCrawlSourceResult(document));
  }

  async getJobsByCrawlRun(crawlRunId: string) {
    const documents = await this.jobs()
      .find(
        { crawlRunIds: crawlRunId },
        { sort: { postedAt: -1, sourcePlatform: 1, title: 1 } },
      )
      .toArray();

    return dedupeStoredJobs(documents.map((document) => parseStoredJob(document)));
  }

  async getJob(jobId: string) {
    const document = await this.jobs().findOne({ _id: jobId });
    return document ? parseStoredJob(document) : null;
  }

  async persistJobs(crawlRunId: string, jobs: PersistableJob[]) {
    const savedJobsById = new Map<string, JobListing>();
    const sanitizedJobs = dedupeJobs(jobs.map((job) => sanitizePersistableJob(job)));

    for (const job of sanitizedJobs) {
      const existing = await this.findExistingJob(job);
      if (!existing) {
        const document = jobListingSchema.parse({
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        });
        await this.jobs().insertOne(document);
        savedJobsById.set(document._id, document);
        continue;
      }

      const merged = mergeJobRecords(existing, job, crawlRunId);
      const { _id: _ignoredId, ...updateFields } = merged;
      await this.jobs().updateOne({ _id: existing._id }, { $set: updateFields });
      savedJobsById.set(merged._id, merged);
    }

    return dedupeStoredJobs(Array.from(savedJobsById.values()));
  }

  async saveLinkValidation(result: LinkValidationResult) {
    await this.linkValidations().insertOne(linkValidationResultSchema.parse(result));
  }

  async getFreshValidation(applyUrl: string, checkedAfter: string) {
    const validations = await this.linkValidations()
      .find({ applyUrl }, { sort: { checkedAt: -1 } })
      .toArray();

    const normalized = validations.map((validation) => parseStoredLinkValidation(validation));

    return normalized.find((validation) => validation.checkedAt >= checkedAfter) ?? null;
  }

  private async findExistingJob(job: PersistableJob) {
    if (job.canonicalUrl) {
      const byCanonical = await this.jobs().findOne({ canonicalUrl: job.canonicalUrl });
      if (byCanonical) {
        return parseStoredJob(byCanonical);
      }
    }

    if (job.resolvedUrl) {
      const byResolved = await this.jobs().findOne({ resolvedUrl: job.resolvedUrl });
      if (byResolved) {
        return parseStoredJob(byResolved);
      }
    }

    const byApplyUrl = await this.jobs().findOne({ applyUrl: job.applyUrl });
    if (byApplyUrl) {
      return parseStoredJob(byApplyUrl);
    }

    for (const lookupKey of job.sourceLookupKeys) {
      const bySourceLookup = await this.jobs().findOne({ sourceLookupKeys: lookupKey });
      if (bySourceLookup) {
        return parseStoredJob(bySourceLookup);
      }
    }

    return null;
  }

  private searches() {
    return this.db.collection<SearchDocument>(collectionNames.searches);
  }

  private jobs() {
    return this.db.collection<JobListing>(collectionNames.jobs);
  }

  private crawlRuns() {
    return this.db.collection<CrawlRun>(collectionNames.crawlRuns);
  }

  private crawlSourceResults() {
    return this.db.collection<CrawlSourceResult>(collectionNames.crawlSourceResults);
  }

  private linkValidations() {
    return this.db.collection<LinkValidationResult>(collectionNames.linkValidations);
  }
}

export async function getRepository(db?: DatabaseAdapter | Db) {
  if (db) {
    return new JobCrawlerRepository(db as DatabaseAdapter);
  }

  try {
    return new JobCrawlerRepository((await getMongoDb()) as DatabaseAdapter);
  } catch (error) {
    if (!hasWarnedMemoryFallback) {
      hasWarnedMemoryFallback = true;
      console.warn(
        "[db:fallback] MongoDB is unavailable; using in-memory persistence for this process.",
        error instanceof Error ? { message: error.message } : { error },
      );
    }

    return new JobCrawlerRepository(getMemoryDb());
  }
}

function mergeJobRecords(
  existing: JobListing,
  incoming: PersistableJob,
  crawlRunId: string,
): JobListing {
  const mergedProvenance = dedupeStrings(
    [...existing.sourceLookupKeys, ...incoming.sourceLookupKeys],
  );
  const crawlRunIds = dedupeStrings([...existing.crawlRunIds, crawlRunId]);
  const sourceProvenance = dedupeProvenance([
    ...existing.sourceProvenance,
    ...incoming.sourceProvenance,
  ]);

  return {
    ...existing,
    ...selectPrimaryRecord(existing, incoming),
    sourceLookupKeys: mergedProvenance,
    crawlRunIds,
    sourceProvenance,
    discoveredAt:
      existing.discoveredAt < incoming.discoveredAt
        ? existing.discoveredAt
        : incoming.discoveredAt,
    postedAt: latestDate(existing.postedAt, incoming.postedAt),
  };
}

function selectPrimaryRecord(existing: JobListing, incoming: PersistableJob) {
  const incomingScore = recordScore(incoming);
  const existingScore = recordScore(existing);
  return incomingScore >= existingScore ? incoming : existing;
}

function recordScore(job: Pick<JobListing, "linkStatus" | "resolvedUrl" | "canonicalUrl" | "lastValidatedAt">) {
  return (
    linkScore(job.linkStatus) +
    (job.resolvedUrl ? 1 : 0) +
    (job.canonicalUrl ? 1 : 0) +
    (job.lastValidatedAt ? 1 : 0)
  );
}

function linkScore(status: JobListing["linkStatus"]) {
  if (status === "valid") {
    return 3;
  }

  if (status === "unknown") {
    return 2;
  }

  if (status === "stale") {
    return 1;
  }

  return 0;
}

function latestDate(left?: string, right?: string) {
  if (!left) {
    return right;
  }

  if (!right) {
    return left;
  }

  return left > right ? left : right;
}

function dedupeStrings(values: string[]) {
  return Array.from(new Set(values));
}

function sanitizePersistableJob(job: PersistableJob): PersistableJob {
  const normalized = normalizeStoredJobFields(job as Record<string, unknown>);
  const {
    _id: _ignoredId,
    crawlRunIds: _ignoredCrawlRunIds,
    ...persistable
  } = normalized as PersistableJob & { _id?: string; crawlRunIds?: string[] };

  return persistableJobSchema.parse(persistable);
}

function dedupeProvenance(records: JobListing["sourceProvenance"]) {
  const map = new Map<string, JobListing["sourceProvenance"][number]>();

  for (const record of records) {
    const key = `${record.sourcePlatform}:${record.sourceJobId}:${record.applyUrl}`;
    map.set(key, record);
  }

  return Array.from(map.values());
}

function parseStoredSearch(document: Record<string, unknown>) {
  return searchDocumentSchema.parse({
    ...document,
    filters: sanitizeSearchFiltersInput(document.filters),
    latestCrawlRunId: normalizeOptionalDocumentString(document.latestCrawlRunId),
    lastStatus: document.lastStatus == null ? undefined : document.lastStatus,
  });
}

function parseStoredCrawlRun(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);
  const diagnostics = normalizeCrawlDiagnostics(normalizedDocument.diagnostics);

  return crawlRunDocumentSchema.parse({
    ...normalizedDocument,
    finishedAt: normalizeOptionalDocumentString(normalizedDocument.finishedAt),
    errorMessage: normalizeOptionalDocumentString(normalizedDocument.errorMessage),
    diagnostics,
    discoveredSourcesCount:
      typeof normalizedDocument.discoveredSourcesCount === "number"
        ? normalizedDocument.discoveredSourcesCount
        : diagnostics.discoveredSources,
    crawledSourcesCount:
      typeof normalizedDocument.crawledSourcesCount === "number"
        ? normalizedDocument.crawledSourcesCount
        : diagnostics.crawledSources,
    validationMode:
      typeof normalizedDocument.validationMode === "string"
        ? normalizedDocument.validationMode
        : "deferred",
    providerSummary: normalizeProviderSummary(normalizedDocument.providerSummary),
  });
}

function parseStoredCrawlSourceResult(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return crawlSourceResultSchema.parse({
    ...normalizedDocument,
    errorMessage: normalizeOptionalDocumentString(normalizedDocument.errorMessage),
  });
}

function parseStoredJob(document: Record<string, unknown>) {
  return jobListingSchema.parse(normalizeStoredJobFields(document));
}

function parseStoredLinkValidation(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return linkValidationResultSchema.parse({
    ...normalizedDocument,
    resolvedUrl: normalizeOptionalDocumentString(normalizedDocument.resolvedUrl),
    canonicalUrl: normalizeOptionalDocumentString(normalizedDocument.canonicalUrl),
    httpStatus:
      typeof normalizedDocument.httpStatus === "number"
        ? normalizedDocument.httpStatus
        : undefined,
    errorMessage: normalizeOptionalDocumentString(normalizedDocument.errorMessage),
    staleMarkers: Array.isArray(normalizedDocument.staleMarkers)
      ? normalizedDocument.staleMarkers.filter(isNonEmptyString)
      : undefined,
  });
}

function createStoredCrawlRunDocument(
  searchId: string,
  now: string,
  validationMode: CrawlValidationMode | undefined,
) {
  const diagnostics = normalizeCrawlDiagnostics();

  return crawlRunDocumentSchema.parse({
    _id: createId(),
    searchId,
    startedAt: now,
    status: "running",
    discoveredSourcesCount: diagnostics.discoveredSources,
    crawledSourcesCount: diagnostics.crawledSources,
    totalFetchedJobs: 0,
    totalMatchedJobs: 0,
    dedupedJobs: 0,
    validationMode: validationMode ?? "deferred",
    providerSummary: [],
    diagnostics,
  });
}

function normalizeCrawlDiagnostics(diagnostics?: unknown) {
  return crawlDiagnosticsSchema.parse(normalizeLegacyStoredValue(diagnostics) ?? {});
}

function normalizeProviderSummary(summary?: unknown) {
  const normalizedSummary = normalizeLegacyStoredValue(summary);
  if (!Array.isArray(normalizedSummary)) {
    return [];
  }

  return normalizedSummary.map((entry) =>
    crawlSourceResultToProviderSummary(entry as Record<string, unknown>),
  );
}

function crawlSourceResultToProviderSummary(sourceResult: Record<string, unknown>) {
  const normalizedSourceResult = normalizeLegacyStoredRecord(sourceResult);

  return crawlProviderSummarySchema.parse({
    provider: normalizedSourceResult.provider,
    status: normalizedSourceResult.status,
    sourceCount:
      typeof normalizedSourceResult.sourceCount === "number"
        ? normalizedSourceResult.sourceCount
        : 0,
    fetchedCount:
      typeof normalizedSourceResult.fetchedCount === "number"
        ? normalizedSourceResult.fetchedCount
        : 0,
    matchedCount:
      typeof normalizedSourceResult.matchedCount === "number"
        ? normalizedSourceResult.matchedCount
        : 0,
    savedCount:
      typeof normalizedSourceResult.savedCount === "number"
        ? normalizedSourceResult.savedCount
        : 0,
    warningCount:
      typeof normalizedSourceResult.warningCount === "number"
        ? normalizedSourceResult.warningCount
        : 0,
    errorMessage:
      normalizeOptionalDocumentString(normalizedSourceResult.errorMessage),
  });
}

function normalizeSourceProvenance(
  document: Record<string, unknown>,
  rawSourceMetadata: Record<string, unknown>,
) {
  if (Array.isArray(document.sourceProvenance) && document.sourceProvenance.length > 0) {
    const normalizedRecords = document.sourceProvenance
      .map((record) => normalizeStoredProvenanceRecord(record, rawSourceMetadata))
      .filter((record): record is SourceProvenance => Boolean(record));

    if (normalizedRecords.length > 0) {
      return normalizedRecords;
    }
  }

  const fallback = buildFallbackSourceProvenance(document, rawSourceMetadata);
  return fallback ? [fallback] : [];
}

function normalizeSourceLookupKeys(
  document: Record<string, unknown>,
  sourceProvenance: SourceProvenance[],
) {
  const explicitLookupKeys = normalizeStringArray(document.sourceLookupKeys);
  if (explicitLookupKeys.length > 0) {
    return explicitLookupKeys;
  }

  return dedupeStrings(
    sourceProvenance.map((record) =>
      buildSourceLookupKey(record.sourcePlatform, record.sourceJobId),
    ),
  );
}

function buildFallbackSourceProvenance(
  document: Record<string, unknown>,
  rawSourceMetadata: Record<string, unknown>,
) {
  const sourceUrl = normalizeOptionalDocumentString(document.sourceUrl);
  const applyUrl = normalizeOptionalDocumentString(document.applyUrl);
  const discoveredAt = normalizeOptionalDocumentString(document.discoveredAt);

  if (
    !isNonEmptyString(document.sourcePlatform) ||
    !isNonEmptyString(document.sourceJobId) ||
    !sourceUrl ||
    !applyUrl ||
    !discoveredAt
  ) {
    return undefined;
  }

  return sourceProvenanceSchema.parse({
    sourcePlatform: document.sourcePlatform,
    sourceJobId: document.sourceJobId,
    sourceUrl,
    applyUrl,
    resolvedUrl: normalizeOptionalDocumentString(document.resolvedUrl),
    canonicalUrl: normalizeOptionalDocumentString(document.canonicalUrl),
    discoveredAt,
    rawSourceMetadata,
  });
}

function normalizeStringArray(value: unknown) {
  return Array.isArray(value) ? value.filter(isNonEmptyString) : [];
}

function normalizeStoredJobFields(document: Record<string, unknown>) {
  const rawSourceMetadata = isRecord(document.rawSourceMetadata) ? document.rawSourceMetadata : {};
  const sourceProvenance = normalizeSourceProvenance(document, rawSourceMetadata);
  const sourceLookupKeys = normalizeSourceLookupKeys(document, sourceProvenance);
  const crawlRunIds = normalizeStringArray(document.crawlRunIds);
  const resolvedLocation = normalizeResolvedLocation(document.resolvedLocation);

  return {
    ...document,
    country: normalizeOptionalDocumentString(document.country),
    state: normalizeOptionalDocumentString(document.state),
    city: normalizeOptionalDocumentString(document.city),
    ...(resolvedLocation ? { resolvedLocation } : {}),
    experienceLevel: document.experienceLevel == null ? undefined : document.experienceLevel,
    experienceClassification: normalizeExperienceClassification(document.experienceClassification),
    resolvedUrl: normalizeOptionalDocumentString(document.resolvedUrl),
    canonicalUrl: normalizeOptionalDocumentString(document.canonicalUrl),
    postedAt: normalizeOptionalDocumentString(document.postedAt),
    lastValidatedAt: normalizeOptionalDocumentString(document.lastValidatedAt),
    rawSourceMetadata,
    sourceProvenance,
    sourceLookupKeys,
    crawlRunIds,
    linkStatus: typeof document.linkStatus === "string" ? document.linkStatus : "unknown",
  };
}

function normalizeStoredProvenanceRecord(
  record: unknown,
  fallbackRawSourceMetadata: Record<string, unknown>,
) {
  if (!isRecord(record)) {
    return undefined;
  }

  const parsed = sourceProvenanceSchema.safeParse({
    sourcePlatform: record.sourcePlatform,
    sourceJobId: record.sourceJobId,
    sourceUrl: normalizeOptionalDocumentString(record.sourceUrl),
    applyUrl: normalizeOptionalDocumentString(record.applyUrl),
    resolvedUrl: normalizeOptionalDocumentString(record.resolvedUrl),
    canonicalUrl: normalizeOptionalDocumentString(record.canonicalUrl),
    discoveredAt: normalizeOptionalDocumentString(record.discoveredAt),
    rawSourceMetadata: isRecord(record.rawSourceMetadata)
      ? record.rawSourceMetadata
      : fallbackRawSourceMetadata,
  });

  return parsed.success ? parsed.data : undefined;
}

function normalizeExperienceClassification(value: unknown) {
  if (!value) {
    return undefined;
  }

  const parsed = experienceClassificationSchema.safeParse(
    normalizeLegacyStoredValue(value),
  );
  return parsed.success ? parsed.data : undefined;
}

function normalizeResolvedLocation(value: unknown) {
  if (!value) {
    return undefined;
  }

  const record = isRecord(value)
    ? {
        country: normalizeOptionalDocumentString(value.country),
        state: normalizeOptionalDocumentString(value.state),
        stateCode: normalizeOptionalDocumentString(value.stateCode),
        city: normalizeOptionalDocumentString(value.city),
        isRemote: value.isRemote === true,
        isUnitedStates: value.isUnitedStates === true,
        confidence:
          typeof value.confidence === "string" ? value.confidence : "none",
        evidence: Array.isArray(value.evidence)
          ? value.evidence
              .filter(isRecord)
              .map((entry) => ({
                source: entry.source,
                value: normalizeOptionalDocumentString(entry.value),
              }))
              .filter((entry) => typeof entry.source === "string" && entry.value)
          : [],
      }
    : value;

  const parsed = resolvedLocationSchema.safeParse(record);
  return parsed.success ? parsed.data : undefined;
}

function normalizeLegacyStoredValue(value: unknown): unknown {
  if (value == null) {
    return undefined;
  }

  if (Array.isArray(value)) {
    return value
      .map((entry) => normalizeLegacyStoredValue(entry))
      .filter((entry) => typeof entry !== "undefined");
  }

  if (!isRecord(value)) {
    return value;
  }

  return Object.entries(value).reduce<Record<string, unknown>>((record, [key, entry]) => {
    const normalizedEntry = normalizeLegacyStoredValue(entry);
    if (typeof normalizedEntry !== "undefined") {
      record[key] = normalizedEntry;
    }

    return record;
  }, {});
}

function normalizeLegacyStoredRecord(value: Record<string, unknown>) {
  const normalized = normalizeLegacyStoredValue(value);
  return isRecord(normalized) ? normalized : value;
}

function normalizeOptionalDocumentString(value: unknown) {
  if (value == null) {
    return undefined;
  }

  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
