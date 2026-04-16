import "server-only";

import type { Db } from "mongodb";

import { buildCanonicalJobIdentity, normalizeComparableIdentityText } from "@/lib/job-identity";
import { isBackgroundIngestionSearchFilters } from "@/lib/server/background/constants";
import { dedupeJobs, dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { buildSourceLookupKey, createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/indexes";
import { getMemoryDb } from "@/lib/server/db/memory";
import {
  inventoryOriginFromDiscoveryMethod,
  sourceInventoryRecordSchema,
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import { getMongoDb } from "@/lib/server/mongodb";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type {
  CrawlControlDocument,
  CrawlDiagnostics,
  CrawlQueueDocument,
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
  SearchSessionDocument,
  SearchSessionJobEvent,
  SearchFilters,
  SourceProvenance,
} from "@/lib/types";
import {
  crawlControlDocumentSchema,
  crawlDiagnosticsSchema,
  crawlProviderSummarySchema,
  crawlQueueDocumentSchema,
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
  searchSessionDocumentSchema,
  searchSessionJobEventSchema,
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
export type CrawlRunControlState = Pick<
  CrawlControlDocument,
  "_id" | "crawlRunId" | "searchId" | "status" | "cancelRequestedAt" | "cancelReason" | "lastHeartbeatAt" | "finishedAt"
>;

export type CrawlQueueState = Pick<
  CrawlQueueDocument,
  | "_id"
  | "crawlRunId"
  | "searchId"
  | "searchSessionId"
  | "ownerKey"
  | "status"
  | "queuedAt"
  | "startedAt"
  | "updatedAt"
  | "finishedAt"
  | "cancelRequestedAt"
  | "cancelReason"
  | "lastHeartbeatAt"
  | "workerId"
>;

type CrawlRunJobEvent = {
  _id: string;
  crawlRunId: string;
  jobId: string;
  sequence: number;
  savedAt: string;
};

export type SearchSessionControlState = Pick<
  SearchSessionDocument,
  "_id" | "searchId" | "latestCrawlRunId" | "status" | "finishedAt" | "lastEventSequence" | "lastEventAt"
>;

export type SourceInventoryObservation = {
  sourceId: string;
  observedAt: string;
  status?: SourceInventoryRecord["status"];
  health?: SourceInventoryRecord["health"];
  lastFailureReason?: string;
  succeeded?: boolean;
};

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

    return documents
      .map((document) => parseStoredSearch(document))
      .filter((document) => !isBackgroundIngestionSearchFilters(document.filters));
  }

  async getSearch(searchId: string) {
    const document = await this.searches().findOne({ _id: searchId });
    return document ? parseStoredSearch(document) : null;
  }

  async findMostRecentSearchByFilters(filters: SearchFilters) {
    const documents = await this.searches()
      .find({}, { sort: { createdAt: -1 } })
      .toArray();
    const expected = stableSerialize(filters);
    const matched = documents.find((document) => {
      const parsed = parseStoredSearch(document);
      return stableSerialize(parsed.filters) === expected;
    });

    return matched ? parseStoredSearch(matched) : null;
  }

  async createSearchSession(
    searchId: string,
    now = new Date().toISOString(),
    options: {
      status?: CrawlRunStatus;
      latestCrawlRunId?: string;
    } = {},
  ) {
    const document = searchSessionDocumentSchema.parse({
      _id: createId(),
      searchId,
      latestCrawlRunId: options.latestCrawlRunId,
      status: options.status ?? "running",
      createdAt: now,
      updatedAt: now,
      finishedAt: undefined,
      lastEventSequence: 0,
      lastEventAt: undefined,
    });

    await this.searchSessions().insertOne(document);
    return document;
  }

  async getSearchSession(searchSessionId: string) {
    const document = await this.searchSessions().findOne({ _id: searchSessionId });
    return document ? parseStoredSearchSession(document) : null;
  }

  async updateSearchLatestSession(
    searchId: string,
    searchSessionId: string,
    status: CrawlRunStatus,
    now = new Date().toISOString(),
  ) {
    await this.searches().updateOne(
      { _id: searchId },
      {
        $set: {
          latestSearchSessionId: searchSessionId,
          lastStatus: status,
          updatedAt: now,
        },
      },
    );
  }

  async updateSearchSession(
    searchSessionId: string,
    payload: {
      status?: CrawlRunStatus;
      latestCrawlRunId?: string;
      finishedAt?: string;
      updatedAt?: string;
      lastEventSequence?: number;
      lastEventAt?: string;
    },
  ) {
    const updateFields: Record<string, unknown> = {};

    if (typeof payload.status !== "undefined") {
      updateFields.status = payload.status;
    }

    if (typeof payload.latestCrawlRunId !== "undefined") {
      updateFields.latestCrawlRunId = payload.latestCrawlRunId;
    }

    if (typeof payload.finishedAt !== "undefined") {
      updateFields.finishedAt = payload.finishedAt;
    }

    if (typeof payload.lastEventSequence === "number") {
      updateFields.lastEventSequence = payload.lastEventSequence;
    }

    if (typeof payload.lastEventAt !== "undefined") {
      updateFields.lastEventAt = payload.lastEventAt;
    }

    updateFields.updatedAt = payload.updatedAt ?? new Date().toISOString();

    await this.searchSessions().updateOne(
      { _id: searchSessionId },
      {
        $set: updateFields,
      },
    );

    return this.getSearchSession(searchSessionId);
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
      searchSessionId?: string;
    } = {},
  ) {
    const document = createStoredCrawlRunDocument(
      searchId,
      now,
      options.validationMode,
      options.stage,
      options.searchSessionId,
    );

    await this.crawlRuns().insertOne(document);
    await this.crawlControls().insertOne(
      createStoredCrawlControlDocument(document, now),
    );
    return document;
  }

  async getCrawlRun(crawlRunId: string) {
    const document = await this.crawlRuns().findOne({ _id: crawlRunId });
    return document ? parseStoredCrawlRun(document) : null;
  }

  async requestCrawlRunCancellation(
    crawlRunId: string,
    payload: {
      reason?: string;
      requestedAt?: string;
    } = {},
  ) {
    const existing = await this.getCrawlRun(crawlRunId);
    if (!existing) {
      return null;
    }

    if (existing.finishedAt || existing.status !== "running") {
      return existing;
    }

    const requestedAt = payload.requestedAt ?? new Date().toISOString();
    await this.crawlRuns().updateOne(
      { _id: crawlRunId },
      {
        $set: {
          cancelRequestedAt: requestedAt,
          cancelReason: payload.reason ?? existing.cancelReason ?? "The crawl was canceled.",
        },
      },
    );
    await this.crawlControls().updateOne(
      { crawlRunId },
      {
        $set: {
          status: existing.status,
          updatedAt: requestedAt,
          cancelRequestedAt: requestedAt,
          cancelReason: payload.reason ?? existing.cancelReason ?? "The crawl was canceled.",
        },
      },
    );
    await this.crawlQueue().updateOne(
      { crawlRunId },
      {
        $set: {
          updatedAt: requestedAt,
          cancelRequestedAt: requestedAt,
          cancelReason: payload.reason ?? existing.cancelReason ?? "The crawl was canceled.",
        },
      },
    );

    return this.getCrawlRun(crawlRunId);
  }

  async getCrawlRunControlState(crawlRunId: string): Promise<CrawlRunControlState | null> {
    const document = await this.crawlControls().findOne({ crawlRunId });
    if (!document) {
      return null;
    }

    return parseStoredCrawlControl(document);
  }

  async heartbeatCrawlRun(
    crawlRunId: string,
    heartbeatAt = new Date().toISOString(),
  ) {
    const existing = await this.getCrawlRun(crawlRunId);
    if (!existing) {
      return null;
    }

    if (existing.finishedAt || existing.status !== "running") {
      return existing;
    }

    await this.crawlRuns().updateOne(
      { _id: crawlRunId },
      {
        $set: {
          lastHeartbeatAt: heartbeatAt,
        },
      },
    );
    await this.crawlControls().updateOne(
      { crawlRunId },
      {
        $set: {
          status: existing.status,
          updatedAt: heartbeatAt,
          lastHeartbeatAt: heartbeatAt,
        },
      },
    );
    await this.crawlQueue().updateOne(
      { crawlRunId },
      {
        $set: {
          status: existing.status,
          updatedAt: heartbeatAt,
          lastHeartbeatAt: heartbeatAt,
        },
      },
    );

    return this.getCrawlRun(crawlRunId);
  }

  async enqueueCrawlRun(
    payload: {
      crawlRunId: string;
      searchId: string;
      searchSessionId?: string;
      ownerKey?: string;
      queuedAt?: string;
    },
  ) {
    const existing = await this.getCrawlQueueEntryByRunId(payload.crawlRunId);
    if (existing) {
      return existing;
    }

    const queuedAt = payload.queuedAt ?? new Date().toISOString();
    const document = crawlQueueDocumentSchema.parse({
      _id: createId(),
      crawlRunId: payload.crawlRunId,
      searchId: payload.searchId,
      searchSessionId: payload.searchSessionId,
      ownerKey: payload.ownerKey,
      status: "queued",
      queuedAt,
      startedAt: undefined,
      updatedAt: queuedAt,
      finishedAt: undefined,
      cancelRequestedAt: undefined,
      cancelReason: undefined,
      lastHeartbeatAt: undefined,
      workerId: undefined,
    });

    await this.crawlQueue().insertOne(document);
    await this.crawlControls().updateOne(
      { crawlRunId: payload.crawlRunId },
      {
        $set: {
          ownerKey: payload.ownerKey,
          updatedAt: queuedAt,
        },
      },
    );

    return document;
  }

  async markCrawlRunStarted(
    crawlRunId: string,
    payload: {
      startedAt?: string;
      workerId?: string;
      ownerKey?: string;
    } = {},
  ) {
    const existing = await this.getCrawlQueueEntryByRunId(crawlRunId);
    if (!existing) {
      return null;
    }

    const startedAt = payload.startedAt ?? new Date().toISOString();
    await this.crawlQueue().updateOne(
      { crawlRunId },
      {
        $set: {
          status: "running",
          startedAt,
          updatedAt: startedAt,
          lastHeartbeatAt: startedAt,
          workerId: payload.workerId ?? existing.workerId,
          ownerKey: payload.ownerKey ?? existing.ownerKey,
        },
      },
    );
    await this.crawlControls().updateOne(
      { crawlRunId },
      {
        $set: {
          status: "running",
          updatedAt: startedAt,
          lastHeartbeatAt: startedAt,
          workerId: payload.workerId,
          ownerKey: payload.ownerKey ?? existing.ownerKey,
        },
      },
    );

    return this.getCrawlQueueEntryByRunId(crawlRunId);
  }

  async finalizeCrawlQueueEntry(
    crawlRunId: string,
    payload: {
      status: CrawlRunStatus;
      finishedAt?: string;
    },
  ) {
    const existing = await this.getCrawlQueueEntryByRunId(crawlRunId);
    if (!existing) {
      return null;
    }

    const finishedAt = payload.finishedAt ?? new Date().toISOString();
    await this.crawlQueue().updateOne(
      { crawlRunId },
      {
        $set: {
          status: payload.status,
          updatedAt: finishedAt,
          finishedAt,
        },
      },
    );

    return this.getCrawlQueueEntryByRunId(crawlRunId);
  }

  async getCrawlQueueEntryByRunId(crawlRunId: string) {
    const document = await this.crawlQueue().findOne({ crawlRunId });
    return document ? parseStoredCrawlQueue(document) : null;
  }

  async getActiveCrawlQueueEntryForSearch(searchId: string) {
    const document = await this.crawlQueue().findOne(
      { searchId, status: { $in: ["queued", "running"] } },
      { sort: { updatedAt: -1 } },
    );

    return document ? parseStoredCrawlQueue(document) : null;
  }

  async getActiveCrawlQueueEntryForOwner(ownerKey: string) {
    const document = await this.crawlQueue().findOne(
      { ownerKey, status: { $in: ["queued", "running"] } },
      { sort: { updatedAt: -1 } },
    );

    return document ? parseStoredCrawlQueue(document) : null;
  }

  async hasActiveCrawlQueueEntryForSearch(searchId: string) {
    return Boolean(await this.getActiveCrawlQueueEntryForSearch(searchId));
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
    await this.crawlControls().updateOne(
      { crawlRunId },
      {
        $set: {
          status: payload.status,
          updatedAt: payload.finishedAt ?? new Date().toISOString(),
          finishedAt: payload.finishedAt ?? new Date().toISOString(),
          lastHeartbeatAt: payload.finishedAt ?? new Date().toISOString(),
          cancelReason: payload.status === "aborted" ? payload.errorMessage : undefined,
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
    await this.crawlControls().updateOne(
      { crawlRunId },
      {
        $set: {
          ...(typeof payload.status !== "undefined"
            ? { status: payload.status }
            : {}),
          updatedAt: payload.finishedAt ?? new Date().toISOString(),
          ...(typeof payload.finishedAt !== "undefined"
            ? {
                finishedAt: payload.finishedAt,
                lastHeartbeatAt: payload.finishedAt,
              }
            : {}),
        },
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

  async getCrawlRunDeliveryCursor(crawlRunId: string) {
    const events = await this.crawlRunJobEvents().find({ crawlRunId }).toArray();

    return events
      .map((event) => parseStoredCrawlRunJobEvent(event).sequence)
      .reduce((max, sequence) => (sequence > max ? sequence : max), 0);
  }

  async getJobsByCrawlRunAfterSequence(crawlRunId: string, afterSequence = 0) {
    const allEvents = (await this.crawlRunJobEvents()
      .find({ crawlRunId }, { sort: { sequence: 1 } })
      .toArray())
      .map((document) => parseStoredCrawlRunJobEvent(document));
    const nextEvents = allEvents.filter((event) => event.sequence > afterSequence);
    const cursor = allEvents[allEvents.length - 1]?.sequence ?? 0;

    if (nextEvents.length === 0) {
      return {
        cursor,
        jobs: [] as JobListing[],
      };
    }

    const jobDocuments = await this.jobs()
      .find({ _id: { $in: nextEvents.map((event) => event.jobId) } })
      .toArray();
    const jobsById = new Map(
      jobDocuments.map((document) => {
        const job = parseStoredJob(document);
        return [job._id, job] as const;
      }),
    );

    return {
      cursor,
      jobs: dedupeStoredJobs(
        nextEvents
          .map((event) => jobsById.get(event.jobId))
          .filter((job): job is JobListing => Boolean(job)),
      ),
    };
  }

  async getSearchSessionDeliveryCursor(searchSessionId: string) {
    const searchSession = await this.getSearchSession(searchSessionId);
    return searchSession?.lastEventSequence ?? 0;
  }

  async appendExistingJobsToSearchSession(
    searchSessionId: string,
    crawlRunId: string,
    jobIds: string[],
  ) {
    if (jobIds.length === 0) {
      return this.updateSearchSession(searchSessionId, {
        latestCrawlRunId: crawlRunId,
      });
    }

    await this.appendSearchSessionJobEvents(searchSessionId, crawlRunId, jobIds);
    return this.getSearchSession(searchSessionId);
  }

  async getJobsBySearchSession(searchSessionId: string) {
    const events = (await this.searchSessionJobEvents()
      .find({ searchSessionId }, { sort: { sequence: 1 } })
      .toArray())
      .map((document) => parseStoredSearchSessionJobEvent(document));

    if (events.length === 0) {
      return [] as JobListing[];
    }

    const jobDocuments = await this.jobs()
      .find({ _id: { $in: events.map((event) => event.jobId) } })
      .toArray();
    const jobsById = new Map(
      jobDocuments.map((document) => {
        const job = parseStoredJob(document);
        return [job._id, job] as const;
      }),
    );

    return dedupeStoredJobs(
      events
        .map((event) => jobsById.get(event.jobId))
        .filter((job): job is JobListing => Boolean(job)),
    );
  }

  async getJobsBySearchSessionAfterSequence(searchSessionId: string, afterSequence = 0) {
    const allEvents = (await this.searchSessionJobEvents()
      .find({ searchSessionId }, { sort: { sequence: 1 } })
      .toArray())
      .map((document) => parseStoredSearchSessionJobEvent(document));
    const nextEvents = allEvents.filter((event) => event.sequence > afterSequence);
    const cursor = allEvents[allEvents.length - 1]?.sequence ?? 0;

    if (nextEvents.length === 0) {
      return {
        cursor,
        jobs: [] as JobListing[],
      };
    }

    const jobDocuments = await this.jobs()
      .find({ _id: { $in: nextEvents.map((event) => event.jobId) } })
      .toArray();
    const jobsById = new Map(
      jobDocuments.map((document) => {
        const job = parseStoredJob(document);
        return [job._id, job] as const;
      }),
    );

    return {
      cursor,
      jobs: dedupeStoredJobs(
        nextEvents
          .map((event) => jobsById.get(event.jobId))
          .filter((job): job is JobListing => Boolean(job)),
      ),
    };
  }

  async getJob(jobId: string) {
    const document = await this.jobs().findOne({ _id: jobId });
    return document ? parseStoredJob(document) : null;
  }

  async listJobs() {
    const documents = await this.jobs()
      .find(
        {},
        { sort: { postingDate: -1, crawledAt: -1, discoveredAt: -1, title: 1 } },
      )
      .toArray();

    return dedupeStoredJobs(documents.map((document) => parseStoredJob(document)));
  }

  async persistJobs(
    crawlRunId: string,
    jobs: PersistableJob[],
    options: {
      searchSessionId?: string;
    } = {},
  ) {
    const savedJobsById = new Map<string, JobListing>();
    const sanitizedJobs = dedupeJobs(jobs.map((job) => sanitizePersistableJob(job)));
    const existingJobs = await this.findExistingJobsForBatch(sanitizedJobs);
    const inserts: JobListing[] = [];
    const updates = new Map<string, JobListing>();
    const newToRunJobIds = new Set<string>();

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
        newToRunJobIds.add(document._id);
        indexJobForBatchLookup(existingJobs, document);
        continue;
      }

      const merged = mergeJobRecords(existing, job, crawlRunId);
      savedJobsById.set(merged._id, merged);
      updates.set(merged._id, merged);
      if (!existing.crawlRunIds.includes(crawlRunId)) {
        newToRunJobIds.add(merged._id);
      }
      indexJobForBatchLookup(existingJobs, merged);
    }

    await persistBatchMutations(this.jobs(), {
      inserts,
      updates: Array.from(updates.values()),
    });

    if (newToRunJobIds.size > 0) {
      await this.appendCrawlRunJobEvents(crawlRunId, Array.from(newToRunJobIds));
      if (options.searchSessionId) {
        await this.appendSearchSessionJobEvents(
          options.searchSessionId,
          crawlRunId,
          Array.from(newToRunJobIds),
        );
      }
    }

    return dedupeStoredJobs(Array.from(savedJobsById.values()));
  }

  async saveLinkValidation(result: LinkValidationResult) {
    await this.linkValidations().insertOne(linkValidationResultSchema.parse(result));
  }

  async upsertSourceInventory(records: SourceInventoryRecord[]) {
    const normalizedRecords = records.map((record) => sanitizeSourceInventoryRecord(record));

    for (const record of normalizedRecords) {
      const existing = await this.sourceInventory().findOne({ _id: record._id });
      if (existing) {
        const parsedExisting = parseStoredSourceInventory(existing);
        const merged = sourceInventoryRecordSchema.parse({
          ...parsedExisting,
          ...record,
          firstSeenAt: parsedExisting.firstSeenAt,
          status: parsedExisting.status,
          health: parsedExisting.health,
          crawlPriority: Math.min(
            parsedExisting.crawlPriority,
            record.crawlPriority,
          ),
          failureCount: parsedExisting.failureCount,
          consecutiveFailures: parsedExisting.consecutiveFailures,
          lastFailureReason: parsedExisting.lastFailureReason ?? record.lastFailureReason,
          lastSeenAt: record.lastSeenAt,
          lastRefreshedAt: record.lastRefreshedAt,
          lastCrawledAt: parsedExisting.lastCrawledAt,
          lastSucceededAt: parsedExisting.lastSucceededAt,
          lastFailedAt: parsedExisting.lastFailedAt,
        });
        const { _id, ...updateFields } = merged;
        await this.sourceInventory().updateOne({ _id }, { $set: updateFields });
        continue;
      }

      await this.sourceInventory().insertOne(record);
    }

    return this.listSourceInventory();
  }

  async upsertDiscoveredSourcesIntoInventory(
    sources: DiscoveredSource[],
    observedAt: string,
  ) {
    const records = sources.map((source, index) =>
      toSourceInventoryRecord(source, {
        now: observedAt,
        inventoryOrigin: inventoryOriginFromDiscoveryMethod(source.discoveryMethod),
        inventoryRank: index,
      }),
    );

    return this.upsertSourceInventory(records);
  }

  async listSourceInventory(platforms?: SourceInventoryRecord["platform"][]) {
    const documents = await this.sourceInventory()
      .find(
        platforms && platforms.length > 0 ? { platform: { $in: platforms } } : {},
        {
          sort: {
            status: 1,
            crawlPriority: 1,
            inventoryRank: 1,
            platform: 1,
            companyHint: 1,
            url: 1,
          },
        },
      )
      .toArray();

    return documents.map((document) => parseStoredSourceInventory(document));
  }

  async recordSourceInventoryObservations(observations: SourceInventoryObservation[]) {
    for (const observation of observations) {
      const existing = await this.sourceInventory().findOne({ _id: observation.sourceId });
      if (!existing) {
        continue;
      }

      const parsedExisting = parseStoredSourceInventory(existing);
      const hasOutcome = typeof observation.succeeded === "boolean";
      const succeeded = observation.succeeded === true;
      const nextFailureCount = !hasOutcome
        ? parsedExisting.failureCount
        : succeeded
          ? parsedExisting.failureCount
          : parsedExisting.failureCount + 1;
      const nextConsecutiveFailures = !hasOutcome
        ? parsedExisting.consecutiveFailures
        : succeeded
          ? 0
          : parsedExisting.consecutiveFailures + 1;
      const health =
        observation.health ??
        (!hasOutcome
          ? parsedExisting.health
          : succeeded
          ? "healthy"
          : nextConsecutiveFailures >= 3
            ? "failing"
            : "degraded");
      const status = observation.status ?? parsedExisting.status;
      const merged = sourceInventoryRecordSchema.parse({
        ...parsedExisting,
        status,
        health,
        failureCount: nextFailureCount,
        consecutiveFailures: nextConsecutiveFailures,
        lastFailureReason: !hasOutcome || succeeded
          ? undefined
          : observation.lastFailureReason ?? parsedExisting.lastFailureReason,
        lastSeenAt: observation.observedAt,
        lastCrawledAt: observation.observedAt,
        lastSucceededAt: succeeded ? observation.observedAt : parsedExisting.lastSucceededAt,
        lastFailedAt:
          hasOutcome && !succeeded ? observation.observedAt : parsedExisting.lastFailedAt,
      });
      const { _id, ...updateFields } = merged;
      await this.sourceInventory().updateOne({ _id }, { $set: updateFields });
    }

    return this.listSourceInventory();
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

  private searchSessions() {
    return this.db.collection<SearchSessionDocument>(collectionNames.searchSessions);
  }

  private jobs() {
    return this.db.collection<JobListing>(collectionNames.jobs);
  }

  private crawlRuns() {
    return this.db.collection<CrawlRun>(collectionNames.crawlRuns);
  }

  private crawlControls() {
    return this.db.collection<CrawlControlDocument>(collectionNames.crawlControls);
  }

  private crawlQueue() {
    return this.db.collection<CrawlQueueDocument>(collectionNames.crawlQueue);
  }

  private crawlSourceResults() {
    return this.db.collection<CrawlSourceResult>(collectionNames.crawlSourceResults);
  }

  private crawlRunJobEvents() {
    return this.db.collection<CrawlRunJobEvent>(collectionNames.crawlRunJobEvents);
  }

  private searchSessionJobEvents() {
    return this.db.collection<SearchSessionJobEvent>(collectionNames.searchSessionJobEvents);
  }

  private linkValidations() {
    return this.db.collection<LinkValidationResult>(collectionNames.linkValidations);
  }

  private sourceInventory() {
    return this.db.collection<SourceInventoryRecord>(collectionNames.sourceInventory);
  }

  private async appendCrawlRunJobEvents(crawlRunId: string, jobIds: string[]) {
    const existingEvents = await this.crawlRunJobEvents().find({ crawlRunId }).toArray();
    const latestSequence = existingEvents.length;
    const savedAt = new Date().toISOString();

    await persistBatchMutations(this.crawlRunJobEvents(), {
      inserts: jobIds.map((jobId, index) => ({
        _id: createId(),
        crawlRunId,
        jobId,
        sequence: latestSequence + index + 1,
        savedAt,
      })),
      updates: [],
    });
  }

  private async appendSearchSessionJobEvents(
    searchSessionId: string,
    crawlRunId: string,
    jobIds: string[],
  ) {
    const existingEvents = (await this.searchSessionJobEvents()
      .find({ searchSessionId }, { sort: { sequence: 1 } })
      .toArray())
      .map((document) => parseStoredSearchSessionJobEvent(document));
    const existingJobIds = new Set(existingEvents.map((event) => event.jobId));
    const newJobIds = jobIds.filter((jobId) => !existingJobIds.has(jobId));

    if (newJobIds.length === 0) {
      return this.updateSearchSession(searchSessionId, {
        latestCrawlRunId: crawlRunId,
      });
    }

    const latestSequence = existingEvents[existingEvents.length - 1]?.sequence ?? 0;
    const createdAt = new Date().toISOString();
    const inserts = newJobIds.map((jobId, index) =>
      searchSessionJobEventSchema.parse({
        _id: createId(),
        searchSessionId,
        crawlRunId,
        jobId,
        sequence: latestSequence + index + 1,
        createdAt,
      }),
    );

    await persistBatchMutations(this.searchSessionJobEvents(), {
      inserts,
      updates: [],
    });

    await this.updateSearchSession(searchSessionId, {
      latestCrawlRunId: crawlRunId,
      lastEventSequence: inserts[inserts.length - 1]?.sequence ?? latestSequence,
      lastEventAt: createdAt,
    });
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

function stableSerialize(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

function parseStoredSourceInventory(document: Record<string, unknown>) {
  return sourceInventoryRecordSchema.parse(
    normalizeStoredSourceInventoryFields(document),
  );
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

function sanitizeSourceInventoryRecord(
  record: SourceInventoryRecord | Record<string, unknown>,
): SourceInventoryRecord {
  return sourceInventoryRecordSchema.parse(
    normalizeStoredSourceInventoryFields(record),
  );
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

async function persistBatchMutations<TDocument extends { _id: string }>(
  collection: CollectionAdapter<TDocument>,
  operations: {
    inserts: TDocument[];
    updates: TDocument[];
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
    latestSearchSessionId: normalizeOptionalDocumentString(document.latestSearchSessionId),
    lastStatus: document.lastStatus == null ? undefined : document.lastStatus,
  });
}

function parseStoredSearchSession(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return searchSessionDocumentSchema.parse({
    ...normalizedDocument,
    latestCrawlRunId: normalizeOptionalDocumentString(normalizedDocument.latestCrawlRunId),
    finishedAt: normalizeOptionalDocumentString(normalizedDocument.finishedAt),
    lastEventSequence:
      typeof normalizedDocument.lastEventSequence === "number"
        ? normalizedDocument.lastEventSequence
        : 0,
    lastEventAt: normalizeOptionalDocumentString(normalizedDocument.lastEventAt),
  });
}

function parseStoredCrawlRun(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);
  const diagnostics = normalizeCrawlDiagnostics(normalizedDocument.diagnostics);

    return crawlRunDocumentSchema.parse({
    ...normalizedDocument,
    searchSessionId: normalizeOptionalDocumentString(normalizedDocument.searchSessionId),
    finishedAt: normalizeOptionalDocumentString(normalizedDocument.finishedAt),
    cancelRequestedAt: normalizeOptionalDocumentString(normalizedDocument.cancelRequestedAt),
    cancelReason: normalizeOptionalDocumentString(normalizedDocument.cancelReason),
    lastHeartbeatAt: normalizeOptionalDocumentString(normalizedDocument.lastHeartbeatAt),
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

function parseStoredCrawlControl(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return crawlControlDocumentSchema.parse({
    ...normalizedDocument,
    searchSessionId: normalizeOptionalDocumentString(normalizedDocument.searchSessionId),
    ownerKey: normalizeOptionalDocumentString(normalizedDocument.ownerKey),
    finishedAt: normalizeOptionalDocumentString(normalizedDocument.finishedAt),
    cancelRequestedAt: normalizeOptionalDocumentString(normalizedDocument.cancelRequestedAt),
    cancelReason: normalizeOptionalDocumentString(normalizedDocument.cancelReason),
    lastHeartbeatAt: normalizeOptionalDocumentString(normalizedDocument.lastHeartbeatAt),
    workerId: normalizeOptionalDocumentString(normalizedDocument.workerId),
  });
}

function parseStoredCrawlQueue(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return crawlQueueDocumentSchema.parse({
    ...normalizedDocument,
    searchSessionId: normalizeOptionalDocumentString(normalizedDocument.searchSessionId),
    ownerKey: normalizeOptionalDocumentString(normalizedDocument.ownerKey),
    startedAt: normalizeOptionalDocumentString(normalizedDocument.startedAt),
    finishedAt: normalizeOptionalDocumentString(normalizedDocument.finishedAt),
    cancelRequestedAt: normalizeOptionalDocumentString(normalizedDocument.cancelRequestedAt),
    cancelReason: normalizeOptionalDocumentString(normalizedDocument.cancelReason),
    lastHeartbeatAt: normalizeOptionalDocumentString(normalizedDocument.lastHeartbeatAt),
    workerId: normalizeOptionalDocumentString(normalizedDocument.workerId),
  });
}

function parseStoredCrawlSourceResult(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return crawlSourceResultSchema.parse({
    ...normalizedDocument,
    errorMessage: normalizeOptionalDocumentString(normalizedDocument.errorMessage),
  });
}

function parseStoredCrawlRunJobEvent(document: Record<string, unknown>): CrawlRunJobEvent {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return {
    _id: String(normalizedDocument._id),
    crawlRunId: String(normalizedDocument.crawlRunId),
    jobId: String(normalizedDocument.jobId),
    sequence: Number(normalizedDocument.sequence),
    savedAt: String(normalizedDocument.savedAt),
  };
}

function parseStoredSearchSessionJobEvent(document: Record<string, unknown>): SearchSessionJobEvent {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return searchSessionJobEventSchema.parse({
    _id: String(normalizedDocument._id),
    searchSessionId: String(normalizedDocument.searchSessionId),
    crawlRunId: String(normalizedDocument.crawlRunId),
    jobId: String(normalizedDocument.jobId),
    sequence: Number(normalizedDocument.sequence),
    createdAt: String(normalizedDocument.createdAt),
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
  searchSessionId: string | undefined,
) {
  const diagnostics = normalizeCrawlDiagnostics();

  return crawlRunDocumentSchema.parse({
    _id: createId(),
    searchId,
    searchSessionId,
    startedAt: now,
    status: "running",
    stage: stage ?? "queued",
    cancelRequestedAt: undefined,
    cancelReason: undefined,
    lastHeartbeatAt: now,
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

function createStoredCrawlControlDocument(
  crawlRun: CrawlRun,
  now: string,
) {
  return crawlControlDocumentSchema.parse({
    _id: crawlRun._id,
    crawlRunId: crawlRun._id,
    searchId: crawlRun.searchId,
    searchSessionId: crawlRun.searchSessionId,
    ownerKey: undefined,
    status: crawlRun.status,
    startedAt: crawlRun.startedAt,
    updatedAt: now,
    finishedAt: crawlRun.finishedAt,
    cancelRequestedAt: crawlRun.cancelRequestedAt,
    cancelReason: crawlRun.cancelReason,
    lastHeartbeatAt: crawlRun.lastHeartbeatAt,
    workerId: undefined,
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
    resolvedLocation,
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

function normalizeStoredSourceInventoryFields(document: Record<string, unknown>) {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return {
    ...normalizedDocument,
    _id: normalizeOptionalDocumentString(normalizedDocument._id),
    platform: normalizedDocument.platform,
    url: normalizeOptionalDocumentString(normalizedDocument.url),
    sourceType: normalizeOptionalSourceInventoryEnum(normalizedDocument.sourceType),
    sourceKey: normalizeOptionalDocumentString(normalizedDocument.sourceKey),
    token: normalizeOptionalDocumentString(normalizedDocument.token),
    companyHint: normalizeOptionalDocumentString(normalizedDocument.companyHint),
    confidence: normalizedDocument.confidence,
    inventoryOrigin: normalizedDocument.inventoryOrigin,
    originalDiscoveryMethod: normalizedDocument.originalDiscoveryMethod,
    jobId: normalizeOptionalDocumentString(normalizedDocument.jobId),
    boardUrl: normalizeOptionalDocumentString(normalizedDocument.boardUrl),
    hostedUrl: normalizeOptionalDocumentString(normalizedDocument.hostedUrl),
    apiUrl: normalizeOptionalDocumentString(normalizedDocument.apiUrl),
    pageType: normalizeOptionalSourceInventoryEnum(normalizedDocument.pageType),
    sitePath: normalizeOptionalDocumentString(normalizedDocument.sitePath),
    careerSitePath: normalizeOptionalDocumentString(normalizedDocument.careerSitePath),
    jobUrl: normalizeOptionalDocumentString(normalizedDocument.jobUrl),
    status: normalizedDocument.status,
    health: normalizedDocument.health,
    crawlPriority: normalizedDocument.crawlPriority,
    inventoryRank: normalizedDocument.inventoryRank,
    failureCount: normalizedDocument.failureCount,
    consecutiveFailures: normalizedDocument.consecutiveFailures,
    lastFailureReason: normalizeOptionalDocumentString(normalizedDocument.lastFailureReason),
    firstSeenAt: normalizeOptionalDocumentString(normalizedDocument.firstSeenAt),
    lastSeenAt: normalizeOptionalDocumentString(normalizedDocument.lastSeenAt),
    lastRefreshedAt: normalizeOptionalDocumentString(normalizedDocument.lastRefreshedAt),
    lastCrawledAt: normalizeOptionalDocumentString(normalizedDocument.lastCrawledAt),
    lastSucceededAt: normalizeOptionalDocumentString(normalizedDocument.lastSucceededAt),
    lastFailedAt: normalizeOptionalDocumentString(normalizedDocument.lastFailedAt),
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

function normalizeOptionalSourceInventoryEnum(value: unknown) {
  return normalizeOptionalDocumentString(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
