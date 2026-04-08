import "server-only";

import type { Db } from "mongodb";

import { buildSourceLookupKey, createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/indexes";
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
  crawlProviderSummarySchema,
  crawlRunDocumentSchema,
  crawlSourceResultSchema,
  jobListingSchema,
  linkValidationResultSchema,
  normalizeOptionalSearchFilterFields,
  persistableJobSchema,
  searchDocumentSchema,
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

    return documents.map((document) => crawlSourceResultSchema.parse(document));
  }

  async getJobsByCrawlRun(crawlRunId: string) {
    const documents = await this.jobs()
      .find(
        { crawlRunIds: crawlRunId },
        { sort: { postedAt: -1, sourcePlatform: 1, title: 1 } },
      )
      .toArray();

    return documents.map((document) => parseStoredJob(document));
  }

  async getJob(jobId: string) {
    const document = await this.jobs().findOne({ _id: jobId });
    return document ? parseStoredJob(document) : null;
  }

  async persistJobs(crawlRunId: string, jobs: PersistableJob[]) {
    const savedJobs: JobListing[] = [];

    for (const rawJob of jobs) {
      const job = sanitizePersistableJob(rawJob);
      const existing = await this.findExistingJob(job);
      if (!existing) {
        const document = jobListingSchema.parse({
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        });
        await this.jobs().insertOne(document);
        savedJobs.push(document);
        continue;
      }

      const merged = mergeJobRecords(existing, job, crawlRunId);
      const { _id: _ignoredId, ...updateFields } = merged;
      await this.jobs().updateOne({ _id: existing._id }, { $set: updateFields });
      savedJobs.push(merged);
    }

    return savedJobs;
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

    const byFingerprint = await this.jobs().findOne({ contentFingerprint: job.contentFingerprint });
    return byFingerprint ? parseStoredJob(byFingerprint) : null;
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
  const database = db ?? (await getMongoDb());
  return new JobCrawlerRepository(database as DatabaseAdapter);
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
  const {
    _id: _ignoredId,
    crawlRunIds: _ignoredCrawlRunIds,
    ...persistable
  } = job as PersistableJob & { _id?: string; crawlRunIds?: string[] };

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
    filters: normalizeOptionalSearchFilterFields(document.filters),
  });
}

function parseStoredCrawlRun(document: Record<string, unknown>) {
  const diagnostics = normalizeCrawlDiagnostics(document.diagnostics);

  return crawlRunDocumentSchema.parse({
    ...document,
    diagnostics,
    discoveredSourcesCount:
      typeof document.discoveredSourcesCount === "number"
        ? document.discoveredSourcesCount
        : diagnostics.discoveredSources,
    crawledSourcesCount:
      typeof document.crawledSourcesCount === "number"
        ? document.crawledSourcesCount
        : diagnostics.crawledSources,
    validationMode:
      typeof document.validationMode === "string" ? document.validationMode : "deferred",
    providerSummary: normalizeProviderSummary(document.providerSummary),
  });
}

function parseStoredJob(document: Record<string, unknown>) {
  const rawSourceMetadata = isRecord(document.rawSourceMetadata) ? document.rawSourceMetadata : {};
  const sourceProvenance = normalizeSourceProvenance(document, rawSourceMetadata);
  const sourceLookupKeys = normalizeSourceLookupKeys(document, sourceProvenance);
  const crawlRunIds = normalizeStringArray(document.crawlRunIds);

  return jobListingSchema.parse({
    ...document,
    rawSourceMetadata,
    sourceProvenance,
    sourceLookupKeys,
    crawlRunIds,
    linkStatus: typeof document.linkStatus === "string" ? document.linkStatus : "unknown",
  });
}

function parseStoredLinkValidation(document: Record<string, unknown>) {
  return linkValidationResultSchema.parse(document);
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
  return crawlDiagnosticsSchema.parse(diagnostics ?? {});
}

function normalizeProviderSummary(summary?: unknown) {
  if (!Array.isArray(summary)) {
    return [];
  }

  return summary.map((entry) =>
    crawlSourceResultToProviderSummary(entry as Record<string, unknown>),
  );
}

function crawlSourceResultToProviderSummary(sourceResult: Record<string, unknown>) {
  return crawlProviderSummarySchema.parse({
    provider: sourceResult.provider,
    status: sourceResult.status,
    sourceCount:
      typeof sourceResult.sourceCount === "number" ? sourceResult.sourceCount : 0,
    fetchedCount:
      typeof sourceResult.fetchedCount === "number" ? sourceResult.fetchedCount : 0,
    matchedCount:
      typeof sourceResult.matchedCount === "number" ? sourceResult.matchedCount : 0,
    savedCount:
      typeof sourceResult.savedCount === "number" ? sourceResult.savedCount : 0,
    warningCount:
      typeof sourceResult.warningCount === "number" ? sourceResult.warningCount : 0,
    errorMessage:
      typeof sourceResult.errorMessage === "string" ? sourceResult.errorMessage : undefined,
  });
}

function normalizeSourceProvenance(
  document: Record<string, unknown>,
  rawSourceMetadata: Record<string, unknown>,
) {
  if (Array.isArray(document.sourceProvenance) && document.sourceProvenance.length > 0) {
    return document.sourceProvenance;
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
  if (
    !isNonEmptyString(document.sourcePlatform) ||
    !isNonEmptyString(document.sourceJobId) ||
    !isNonEmptyString(document.sourceUrl) ||
    !isNonEmptyString(document.applyUrl) ||
    !isNonEmptyString(document.discoveredAt)
  ) {
    return undefined;
  }

  return {
    sourcePlatform: document.sourcePlatform,
    sourceJobId: document.sourceJobId,
    sourceUrl: document.sourceUrl,
    applyUrl: document.applyUrl,
    resolvedUrl: isNonEmptyString(document.resolvedUrl) ? document.resolvedUrl : undefined,
    canonicalUrl: isNonEmptyString(document.canonicalUrl) ? document.canonicalUrl : undefined,
    discoveredAt: document.discoveredAt,
    rawSourceMetadata,
  };
}

function normalizeStringArray(value: unknown) {
  return Array.isArray(value) ? value.filter(isNonEmptyString) : [];
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
