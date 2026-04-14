import "server-only";

import type { Db } from "mongodb";

import { buildCanonicalJobIdentity, normalizeComparableIdentityText } from "@/lib/job-identity";
import { dedupeJobs, dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { buildSourceLookupKey, createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/indexes";
import { getMemoryDb } from "@/lib/server/db/memory";
import { getMongoDb } from "@/lib/server/mongodb";
import type {
  CrawlDiagnostics,
  CrawlProviderSummary,
  CrawlRun,
  CrawlRunStage,
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
  crawlProviderSummarySchema,
  crawlRunDocumentSchema,
  crawlSourceResultSchema,
  employmentTypeSchema,
  experienceClassificationSchema,
  experienceLevelSchema,
  jobListingSchema,
  linkValidationResultSchema,
  persistableJobSchema,
  resolvedLocationSchema,
  salaryInfoSchema,
  sanitizeSearchFiltersInput,
  searchDocumentSchema,
  sourceProvenanceSchema,
  sponsorshipHintSchema,
} from "@/lib/types";

type SortSpec = Record<string, 1 | -1>;

export type CollectionAdapter<TDocument extends Record<string, unknown>> = {
  findOne(
    filter: Record<string, unknown>,
    options?: { sort?: SortSpec },
  ): Promise<TDocument | null>;
  insertOne(document: TDocument): Promise<unknown>;
  bulkWrite?(
    operations: Array<
      | { insertOne: { document: TDocument } }
      | {
          updateOne: {
            filter: Record<string, unknown>;
            update: Record<string, unknown>;
            options?: Record<string, unknown>;
          };
        }
    >,
  ): Promise<unknown>;
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
      stage?: CrawlRunStage;
    } = {},
  ) {
    const document = createStoredCrawlRunDocument(
      searchId,
      now,
      options.validationMode,
      options.stage,
    );

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
      stage?: CrawlRunStage;
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
          stage: payload.stage,
          validationMode: payload.validationMode ?? "deferred",
          providerSummary: normalizeProviderSummary(payload.providerSummary),
          finishedAt: payload.finishedAt ?? new Date().toISOString(),
        },
      },
    );
  }

  async updateCrawlRunProgress(
    crawlRunId: string,
    payload: {
      status?: CrawlRunStatus;
      stage?: CrawlRunStage;
      totalFetchedJobs?: number;
      totalMatchedJobs?: number;
      dedupedJobs?: number;
      diagnostics?: CrawlDiagnostics;
      validationMode?: CrawlValidationMode;
      providerSummary?: CrawlProviderSummary[];
      errorMessage?: string;
      finishedAt?: string;
    },
  ) {
    const updateFields: Record<string, unknown> = {};

    if (typeof payload.status !== "undefined") {
      updateFields.status = payload.status;
    }

    if (typeof payload.stage !== "undefined") {
      updateFields.stage = payload.stage;
    }

    if (typeof payload.totalFetchedJobs === "number") {
      updateFields.totalFetchedJobs = payload.totalFetchedJobs;
    }

    if (typeof payload.totalMatchedJobs === "number") {
      updateFields.totalMatchedJobs = payload.totalMatchedJobs;
    }

    if (typeof payload.dedupedJobs === "number") {
      updateFields.dedupedJobs = payload.dedupedJobs;
    }

    if (typeof payload.validationMode !== "undefined") {
      updateFields.validationMode = payload.validationMode;
    }

    if (typeof payload.errorMessage !== "undefined") {
      updateFields.errorMessage = payload.errorMessage;
    }

    if (typeof payload.finishedAt !== "undefined") {
      updateFields.finishedAt = payload.finishedAt;
    }

    if (typeof payload.diagnostics !== "undefined") {
      const diagnostics = normalizeCrawlDiagnostics(payload.diagnostics);
      updateFields.diagnostics = diagnostics;
      updateFields.discoveredSourcesCount = diagnostics.discoveredSources;
      updateFields.crawledSourcesCount = diagnostics.crawledSources;
    }

    if (typeof payload.providerSummary !== "undefined") {
      updateFields.providerSummary = normalizeProviderSummary(payload.providerSummary);
    }

    if (Object.keys(updateFields).length === 0) {
      return;
    }

    await this.crawlRuns().updateOne(
      { _id: crawlRunId },
      {
        $set: updateFields,
      },
    );
  }

  async saveCrawlSourceResults(sourceResults: CrawlSourceResult[]) {
    for (const sourceResult of sourceResults) {
      await this.crawlSourceResults().insertOne(crawlSourceResultSchema.parse(sourceResult));
    }
  }

  async updateCrawlSourceResult(sourceResult: CrawlSourceResult) {
    const normalized = crawlSourceResultSchema.parse(sourceResult);
    const { _id, ...updateFields } = normalized;
    const updateResult = (await this.crawlSourceResults().updateOne(
      { _id },
      { $set: updateFields },
    )) as { matchedCount?: number };

    if ((updateResult.matchedCount ?? 0) > 0) {
      return;
    }

    await this.crawlSourceResults().insertOne(normalized);
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
        { sort: { postingDate: -1, crawledAt: -1, discoveredAt: -1, title: 1 } },
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
    const existingJobs = await this.findExistingJobsForBatch(sanitizedJobs);
    const inserts: JobListing[] = [];
    const updates = new Map<string, JobListing>();

    for (const job of sanitizedJobs) {
      const existing = resolveExistingJobForPersistable(existingJobs, job);
      if (!existing) {
        const document = jobListingSchema.parse({
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        });
        inserts.push(document);
        savedJobsById.set(document._id, document);
        indexJobForBatchLookup(existingJobs, document);
        continue;
      }

      const merged = mergeJobRecords(existing, job, crawlRunId);
      savedJobsById.set(merged._id, merged);
      updates.set(merged._id, merged);
      indexJobForBatchLookup(existingJobs, merged);
    }

    await persistBatchMutations(this.jobs(), {
      inserts,
      updates: Array.from(updates.values()),
    });

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

  private async findExistingJobsForBatch(jobs: PersistableJob[]) {
    const lookup = createPersistBatchLookup();
    const canonicalUrls = collectUniqueStrings(jobs.map((job) => job.canonicalUrl));
    const resolvedUrls = collectUniqueStrings(jobs.map((job) => job.resolvedUrl));
    const applyUrls = collectUniqueStrings(jobs.map((job) => job.applyUrl));
    const sourceUrls = collectUniqueStrings(jobs.map((job) => job.sourceUrl));
    const sourceLookupKeys = collectUniqueStrings(jobs.flatMap((job) => job.sourceLookupKeys));
    const contentFingerprints = collectUniqueStrings(
      jobs
        .filter((job) => !buildCanonicalJobIdentity(job).hasStrongIdentity)
        .map((job) => job.contentFingerprint),
    );

    const queryResults = await Promise.all([
      fetchJobsByField(this.jobs(), "canonicalUrl", canonicalUrls),
      fetchJobsByField(this.jobs(), "resolvedUrl", resolvedUrls),
      fetchJobsByField(this.jobs(), "applyUrl", applyUrls),
      fetchJobsByField(this.jobs(), "sourceUrl", sourceUrls),
      fetchJobsByField(this.jobs(), "sourceLookupKeys", sourceLookupKeys),
      fetchJobsByField(this.jobs(), "contentFingerprint", contentFingerprints),
    ]);

    queryResults
      .flat()
      .map((document) => parseStoredJob(document))
      .forEach((job) => indexJobForBatchLookup(lookup, job));

    return lookup;
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
    crawledAt: latestDate(existing.crawledAt, incoming.crawledAt) ?? existing.crawledAt,
    postingDate: latestDate(existing.postingDate, incoming.postingDate),
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

type PersistBatchLookup = {
  byCanonicalUrl: Map<string, JobListing>;
  byResolvedUrl: Map<string, JobListing>;
  byApplyUrl: Map<string, JobListing>;
  bySourceUrl: Map<string, JobListing>;
  byPlatformJobKey: Map<string, JobListing>;
  byContentFingerprint: Map<string, JobListing>;
};

function createPersistBatchLookup(): PersistBatchLookup {
  return {
    byCanonicalUrl: new Map<string, JobListing>(),
    byResolvedUrl: new Map<string, JobListing>(),
    byApplyUrl: new Map<string, JobListing>(),
    bySourceUrl: new Map<string, JobListing>(),
    byPlatformJobKey: new Map<string, JobListing>(),
    byContentFingerprint: new Map<string, JobListing>(),
  };
}

function indexJobForBatchLookup(lookup: PersistBatchLookup, job: JobListing) {
  const identity = buildCanonicalJobIdentity(job);

  if (job.canonicalUrl) {
    lookup.byCanonicalUrl.set(job.canonicalUrl, job);
  }

  if (job.resolvedUrl) {
    lookup.byResolvedUrl.set(job.resolvedUrl, job);
  }

  lookup.byApplyUrl.set(job.applyUrl, job);
  lookup.bySourceUrl.set(job.sourceUrl, job);

  identity.normalizedIdentity.platformJobKeys.forEach((key) => {
    lookup.byPlatformJobKey.set(key, job);
  });

  if (!identity.hasStrongIdentity && identity.normalizedIdentity.fallbackFingerprint) {
    lookup.byContentFingerprint.set(identity.normalizedIdentity.fallbackFingerprint, job);
  }
}

function resolveExistingJobForPersistable(
  lookup: PersistBatchLookup,
  job: PersistableJob,
) {
  const identity = buildCanonicalJobIdentity(job);

  if (job.canonicalUrl) {
    const byCanonical = lookup.byCanonicalUrl.get(job.canonicalUrl);
    if (byCanonical) {
      return byCanonical;
    }
  }

  if (job.resolvedUrl) {
    const byResolved = lookup.byResolvedUrl.get(job.resolvedUrl);
    if (byResolved) {
      return byResolved;
    }
  }

  const byApply = lookup.byApplyUrl.get(job.applyUrl);
  if (byApply) {
    return byApply;
  }

  const bySourceUrl = lookup.bySourceUrl.get(job.sourceUrl);
  if (bySourceUrl) {
    return bySourceUrl;
  }

  for (const platformJobKey of identity.normalizedIdentity.platformJobKeys) {
    const byPlatformJobKey = lookup.byPlatformJobKey.get(platformJobKey);
    if (byPlatformJobKey) {
      return byPlatformJobKey;
    }
  }

  if (identity.hasStrongIdentity || !identity.normalizedIdentity.fallbackFingerprint) {
    return null;
  }

  return lookup.byContentFingerprint.get(identity.normalizedIdentity.fallbackFingerprint) ?? null;
}

async function fetchJobsByField(
  collection: CollectionAdapter<JobListing>,
  field:
    | "canonicalUrl"
    | "resolvedUrl"
    | "applyUrl"
    | "sourceUrl"
    | "sourceLookupKeys"
    | "contentFingerprint",
  values: string[],
) {
  if (values.length === 0) {
    return [];
  }

  return collection.find({ [field]: { $in: values } }).toArray();
}

async function persistBatchMutations(
  collection: CollectionAdapter<JobListing>,
  operations: {
    inserts: JobListing[];
    updates: JobListing[];
  },
) {
  const bulkOperations = [
    ...operations.inserts.map(
      (document) =>
        ({
          insertOne: {
            document,
          },
        }) as const,
    ),
    ...operations.updates.map((document) => {
      const { _id, ...updateFields } = document;
      return {
        updateOne: {
          filter: { _id },
          update: { $set: updateFields },
        },
      } as const;
    }),
  ];

  if (bulkOperations.length === 0) {
    return;
  }

  if (collection.bulkWrite) {
    await collection.bulkWrite(bulkOperations);
    return;
  }

  for (const operation of bulkOperations) {
    if ("insertOne" in operation) {
      await collection.insertOne(operation.insertOne.document);
      continue;
    }

    await collection.updateOne(
      operation.updateOne.filter,
      operation.updateOne.update,
    );
  }
}

function collectUniqueStrings(values: Array<string | undefined>) {
  const results: string[] = [];
  const seen = new Set<string>();

  values.forEach((value) => {
    if (!value || seen.has(value)) {
      return;
    }

    seen.add(value);
    results.push(value);
  });

  return results;
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
    stage:
      typeof normalizedDocument.stage === "string"
        ? normalizedDocument.stage
        : undefined,
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
  stage: CrawlRunStage | undefined,
) {
  const diagnostics = normalizeCrawlDiagnostics();

  return crawlRunDocumentSchema.parse({
    _id: createId(),
    searchId,
    startedAt: now,
    status: "running",
    stage: stage ?? "queued",
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
  const company = normalizeOptionalDocumentString(document.company) ?? "";
  const title = normalizeOptionalDocumentString(document.title) ?? "";
  const locationRaw =
    normalizeOptionalDocumentString(document.locationRaw) ??
    normalizeOptionalDocumentString(document.locationText) ??
    buildFallbackLocationRaw(document, resolvedLocation) ??
    "Location unavailable";
  const normalizedCompany =
    normalizeOptionalDocumentString(document.normalizedCompany) ??
    normalizeOptionalDocumentString(document.companyNormalized) ??
    normalizeComparableIdentityText(company);
  const normalizedTitle =
    normalizeOptionalDocumentString(document.normalizedTitle) ??
    normalizeOptionalDocumentString(document.titleNormalized) ??
    normalizeComparableIdentityText(title);
  const normalizedLocation =
    normalizeOptionalDocumentString(document.normalizedLocation) ??
    normalizeOptionalDocumentString(document.locationNormalized) ??
    normalizeComparableIdentityText(locationRaw);
  const postingDate =
    normalizeOptionalDocumentString(document.postingDate) ??
    normalizeOptionalDocumentString(document.postedAt);
  const crawledAt =
    normalizeOptionalDocumentString(document.crawledAt) ??
    normalizeOptionalDocumentString(document.discoveredAt);
  const dedupeFingerprint =
    normalizeOptionalDocumentString(document.dedupeFingerprint) ??
    normalizeOptionalDocumentString(document.contentFingerprint) ??
    [normalizedCompany, normalizedTitle, normalizedLocation].filter(Boolean).join("|");

  return {
    ...document,
    company,
    title,
    normalizedCompany,
    normalizedTitle,
    country: normalizeOptionalDocumentString(document.country),
    state: normalizeOptionalDocumentString(document.state),
    city: normalizeOptionalDocumentString(document.city),
    locationRaw,
    normalizedLocation,
    locationText: normalizeOptionalDocumentString(document.locationText) ?? locationRaw,
    ...(resolvedLocation ? { resolvedLocation } : {}),
    remoteType: normalizeRemoteType(document.remoteType, resolvedLocation, locationRaw),
    employmentType: normalizeEmploymentType(document.employmentType),
    seniority:
      normalizeOptionalExperienceLevel(document.seniority) ??
      normalizeOptionalExperienceLevel(document.experienceLevel),
    experienceLevel: document.experienceLevel == null ? undefined : document.experienceLevel,
    experienceClassification: normalizeExperienceClassification(document.experienceClassification),
    sourceCompanySlug: normalizeOptionalDocumentString(document.sourceCompanySlug),
    resolvedUrl: normalizeOptionalDocumentString(document.resolvedUrl),
    canonicalUrl: normalizeOptionalDocumentString(document.canonicalUrl),
    postingDate,
    postedAt: postingDate,
    lastValidatedAt: normalizeOptionalDocumentString(document.lastValidatedAt),
    discoveredAt: normalizeOptionalDocumentString(document.discoveredAt),
    crawledAt,
    descriptionSnippet: normalizeOptionalDocumentString(document.descriptionSnippet),
    salaryInfo: normalizeSalaryInfo(document.salaryInfo),
    sponsorshipHint: normalizeSponsorshipHint(document.sponsorshipHint),
    rawSourceMetadata,
    sourceProvenance,
    sourceLookupKeys,
    crawlRunIds,
    dedupeFingerprint,
    linkStatus: typeof document.linkStatus === "string" ? document.linkStatus : "unknown",
    companyNormalized: normalizedCompany,
    titleNormalized: normalizedTitle,
    locationNormalized: normalizedLocation,
    contentFingerprint: dedupeFingerprint,
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

function buildFallbackLocationRaw(
  document: Record<string, unknown>,
  resolvedLocation?: JobListing["resolvedLocation"],
) {
  const city = normalizeOptionalDocumentString(document.city) ?? resolvedLocation?.city;
  const state = normalizeOptionalDocumentString(document.state) ?? resolvedLocation?.state;
  const country = normalizeOptionalDocumentString(document.country) ?? resolvedLocation?.country;

  return [city, state, country].filter(Boolean).join(", ") || undefined;
}

function normalizeOptionalExperienceLevel(value: unknown) {
  const parsed = experienceLevelSchema.safeParse(value);
  return parsed.success ? parsed.data : undefined;
}

function normalizeRemoteType(
  value: unknown,
  resolvedLocation?: JobListing["resolvedLocation"],
  locationRaw?: string,
) {
  if (typeof value === "string" && ["remote", "hybrid", "onsite", "unknown"].includes(value)) {
    return value as JobListing["remoteType"];
  }

  if (typeof locationRaw === "string" && /\bhybrid\b/i.test(locationRaw)) {
    return "hybrid";
  }

  if (resolvedLocation?.isRemote || (typeof locationRaw === "string" && /\bremote\b/i.test(locationRaw))) {
    return "remote";
  }

  return locationRaw ? "onsite" : "unknown";
}

function normalizeEmploymentType(value: unknown) {
  const parsed = employmentTypeSchema.safeParse(value);
  return parsed.success ? parsed.data : undefined;
}

function normalizeSalaryInfo(value: unknown) {
  if (!value) {
    return undefined;
  }

  const parsed = salaryInfoSchema.safeParse(normalizeLegacyStoredValue(value));
  return parsed.success ? parsed.data : undefined;
}

function normalizeSponsorshipHint(value: unknown) {
  const parsed = sponsorshipHintSchema.safeParse(value);
  return parsed.success ? parsed.data : "unknown";
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
    return undefined;
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
