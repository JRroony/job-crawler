import "server-only";

import { createHash } from "node:crypto";
import type { Db } from "mongodb";

import {
  currentExperienceClassificationVersion,
  resolveExperienceBand,
  resolveExperienceLevel,
} from "@/lib/experience";
import { buildCanonicalJobIdentity, normalizeComparableIdentityText } from "@/lib/job-identity";
import {
  isBackgroundIngestionSearchFilters,
  type SystemSearchProfileRunState,
} from "@/lib/server/background/constants";
import { dedupeJobs, dedupeStoredJobs } from "@/lib/server/crawler/dedupe";
import { buildSourceLookupKey, createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/collections";
import { getMemoryDb } from "@/lib/server/db/memory";
import {
  inventoryOriginFromDiscoveryMethod,
  sourceInventoryRecordSchema,
  toSourceInventoryRecord,
  type SourceInventoryRecord,
} from "@/lib/server/discovery/inventory";
import { getEnv } from "@/lib/server/env";
import { resolveObservedSourceNextEligibleAt } from "@/lib/server/inventory/selection";
import {
  buildIndexedJobCandidateQuery,
  buildJobSearchIndex,
  emptyIndexedJobCandidateChannelBreakdown,
  type IndexedJobCandidateChannelBreakdown,
  type IndexedJobCandidateChannelName,
} from "@/lib/server/search/job-search-index";
import { emitSearchTraceStage } from "@/lib/server/search/search-trace";
import { normalizeJobGeoLocation } from "@/lib/server/geo/match";
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
  IndexedJobEvent,
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
  indexedJobEventSchema,
  jobListingSchema,
  linkStatusSchema,
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
      | { deleteOne: { filter: Record<string, unknown> } }
      | {
          updateOne: {
            filter: Record<string, unknown>;
            update: Record<string, unknown>;
            upsert?: boolean;
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
  listIndexes?(): { toArray(): Promise<Array<Record<string, unknown>>> };
  dropIndex?(name: string): Promise<unknown>;
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

type IndexedJobDelta = {
  cursor: number;
  jobs: JobListing[];
};

export type PersistJobsWithStatsResult = {
  jobs: JobListing[];
  insertedCount: number;
  updatedCount: number;
  linkedToRunCount: number;
  indexedEventCount: number;
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
  nextEligibleAt?: string;
};

let hasWarnedMemoryFallback = false;

export class JobCrawlerRepository {
  constructor(private readonly db: DatabaseAdapter) {}

  async createSearch(
    filters: SearchFilters,
    now = new Date().toISOString(),
    options: {
      systemProfileId?: string;
      systemProfileLabel?: string;
    } = {},
  ) {
    const document = searchDocumentSchema.parse({
      _id: createId(),
      filters,
      systemProfileId: options.systemProfileId,
      systemProfileLabel: options.systemProfileLabel,
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
      .filter(
        (document) =>
          !document.systemProfileId &&
          !isBackgroundIngestionSearchFilters(document.filters),
      );
  }

  async getSearch(searchId: string) {
    const document = await this.searches().findOne({ _id: searchId });
    return document ? parseStoredSearch(document) : null;
  }

  async findMostRecentSearchByFilters(
    filters: SearchFilters,
    options: {
      systemProfileId?: string;
      includeSystemProfiles?: boolean;
    } = {},
  ) {
    const documents = await this.searches()
      .find({}, { sort: { createdAt: -1 } })
      .toArray();
    const expected = stableSerialize(filters);
    const matched = documents.find((document) => {
      const parsed = parseStoredSearch(document);
      if (options.systemProfileId && parsed.systemProfileId !== options.systemProfileId) {
        return false;
      }

      if (!options.systemProfileId && !options.includeSystemProfiles && parsed.systemProfileId) {
        return false;
      }

      return stableSerialize(parsed.filters) === expected;
    });

    return matched ? parseStoredSearch(matched) : null;
  }

  async listSystemSearchProfileRunStates(): Promise<SystemSearchProfileRunState[]> {
    const searches = (await this.searches()
      .find({}, { sort: { createdAt: 1 } })
      .toArray())
      .map((document) => parseStoredSearch(document))
      .filter((search) => Boolean(search.systemProfileId));
    if (searches.length === 0) {
      return [];
    }

    const profileSearches = new Map<string, SearchDocument[]>();
    for (const search of searches) {
      if (!search.systemProfileId) {
        continue;
      }

      const existing = profileSearches.get(search.systemProfileId) ?? [];
      existing.push(search);
      profileSearches.set(search.systemProfileId, existing);
    }

    const crawlRunsBySearchId = new Map<string, CrawlRun[]>();
    const runs = (await this.crawlRuns()
      .find({}, { sort: { startedAt: 1 } })
      .toArray())
      .map((document) => parseStoredCrawlRun(document));
    for (const run of runs) {
      const existing = crawlRunsBySearchId.get(run.searchId) ?? [];
      existing.push(run);
      crawlRunsBySearchId.set(run.searchId, existing);
    }

    return Array.from(profileSearches.entries()).map(([profileId, profileSearchGroup]) => {
      const profileRuns = profileSearchGroup.flatMap(
        (search) => crawlRunsBySearchId.get(search._id) ?? [],
      );
      const sortedRuns = profileRuns.sort(compareCrawlRunsByActivity);
      const latestRun = sortedRuns[sortedRuns.length - 1];
      const successCount = sortedRuns.filter((run) => isSuccessfulProfileRunStatus(run.status)).length;
      const failureCount = sortedRuns.filter((run) => isFailedProfileRunStatus(run.status)).length;
      let consecutiveFailureCount = 0;

      for (const run of sortedRuns.slice().reverse()) {
        if (isFailedProfileRunStatus(run.status)) {
          consecutiveFailureCount += 1;
          continue;
        }

        if (isSuccessfulProfileRunStatus(run.status)) {
          break;
        }
      }

      const latestSearch = profileSearchGroup[profileSearchGroup.length - 1];

      return {
        profileId,
        searchId: latestSearch?._id,
        latestCrawlRunId: latestRun?._id ?? latestSearch?.latestCrawlRunId,
        lastRunAt: latestRun?.startedAt ?? latestSearch?.updatedAt,
        lastFinishedAt: latestRun?.finishedAt,
        lastStatus: latestRun?.status ?? latestSearch?.lastStatus,
        successCount,
        failureCount,
        consecutiveFailureCount,
      };
    });
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

  async getIndexedJobDeliveryCursor() {
    const latestEvent = await this.indexedJobEvents().findOne(
      {},
      { sort: { sequence: -1 } },
    );

    return latestEvent ? parseStoredIndexedJobEvent(latestEvent).sequence : 0;
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

  async getIndexedJobsAfterSequence(afterSequence = 0): Promise<IndexedJobDelta> {
    const allEvents = (await this.indexedJobEvents()
      .find({}, { sort: { sequence: 1 } })
      .toArray())
      .map((document) => parseStoredIndexedJobEvent(document));
    const cursor = allEvents[allEvents.length - 1]?.sequence ?? 0;
    const nextEvents = allEvents.filter((event) => event.sequence > afterSequence);

    if (nextEvents.length === 0) {
      return {
        cursor,
        jobs: [],
      };
    }

    const latestEventByJobId = new Map<string, IndexedJobEvent>();
    nextEvents.forEach((event) => {
      latestEventByJobId.set(event.jobId, event);
    });
    const orderedEvents = Array.from(latestEventByJobId.values()).sort(
      (left, right) => left.sequence - right.sequence,
    );
    const jobDocuments = await this.jobs()
      .find({ _id: { $in: orderedEvents.map((event) => event.jobId) } })
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
        orderedEvents
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

  async getIndexedJobCandidatesForSearch(
    filters: SearchFilters,
    options: { traceId?: string } = {},
  ) {
    const candidateQuery = buildIndexedJobCandidateQuery(filters);
    if (options.traceId) {
      emitSearchTraceStage("candidate-query", {
        traceId: options.traceId,
        filter: candidateQuery.filter,
        sort: candidateQuery.sort,
        limit: candidateQuery.limit,
        diagnostics: candidateQuery.diagnostics,
      });
    }
    const channelResults = await Promise.all(
      candidateQuery.channels.map(async (channel) => ({
        channel,
        documents: await this.jobs()
          .find(channel.filter, {
            sort: channel.sort,
            limit: channel.limit,
          })
          .toArray(),
      })),
    );
    const candidateChannelBreakdown = buildIndexedCandidateChannelBreakdown(
      channelResults.map((result) => ({
        channelName: result.channel.name,
        count: result.documents.length,
      })),
    );
    const documents = mergeIndexedCandidateChannelDocuments(
      channelResults.map((result) => result.documents),
      candidateQuery.mergedCandidateLimit,
    );
    candidateChannelBreakdown.mergedCandidateCount = documents.length;
    const diagnostics = {
      ...candidateQuery.diagnostics,
      candidateChannelBreakdown,
    };

    if (options.traceId) {
      console.info("[search:indexed-candidate-channels]", {
        traceId: options.traceId,
        ...candidateChannelBreakdown,
        mergedCandidateLimit: candidateQuery.mergedCandidateLimit,
        channelLimits: candidateQuery.diagnostics.channelLimits,
      });
    }

    return {
      jobs: dedupeStoredJobs(documents.map((document) => parseStoredJob(document))),
      query: {
        ...candidateQuery,
        diagnostics,
      },
      candidateChannelBreakdown,
    };
  }

  async persistJobs(
    crawlRunId: string,
    jobs: PersistableJob[],
    options: {
      searchSessionId?: string;
    } = {},
  ) {
    return (await this.persistJobsWithStats(crawlRunId, jobs, options)).jobs;
  }

  async persistJobsWithStats(
    crawlRunId: string,
    jobs: PersistableJob[],
    options: {
      searchSessionId?: string;
    } = {},
  ): Promise<PersistJobsWithStatsResult> {
    const savedJobsById = new Map<string, JobListing>();
    const sanitizedJobs = coalescePersistableJobs(
      dedupeJobs(jobs.map((job) => sanitizePersistableJob(job))),
      crawlRunId,
    );
    const existingJobs = await this.findExistingJobsForBatch(sanitizedJobs);
    const nextIndexSequenceBase = await this.getIndexedJobDeliveryCursor();
    const newToRunJobIds = new Set<string>();
    const indexedJobIds: string[] = [];
    const upserts = new Map<string, JobListing>();
    const insertedJobIds = new Set<string>();
    const updatedJobIds = new Set<string>();

    for (const job of sanitizedJobs) {
      const existing = resolveExistingJobForPersistable(existingJobs, job);
      if (!existing) {
        const document = jobListingSchema.parse({
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        });
        savedJobsById.set(document._id, document);
        newToRunJobIds.add(document._id);
        indexedJobIds.push(document._id);
        upserts.set(document.canonicalJobKey, document);
        insertedJobIds.add(document._id);
        indexJobForBatchLookup(existingJobs, document);
        continue;
      }

      const merged = mergeJobRecords(existing, job, crawlRunId);
      savedJobsById.set(merged._id, merged);
      upserts.set(merged.canonicalJobKey, merged);
      updatedJobIds.add(merged._id);
      if (!existing.crawlRunIds.includes(crawlRunId)) {
        newToRunJobIds.add(merged._id);
      }
      if (shouldEmitIndexedJobEvent(existing, merged)) {
        indexedJobIds.push(merged._id);
      }
      indexJobForBatchLookup(existingJobs, merged);
    }

    await persistJobUpserts(this.jobs(), {
      upserts: Array.from(upserts.values()),
    });

    if (indexedJobIds.length > 0) {
      await this.appendIndexedJobEvents(crawlRunId, indexedJobIds, nextIndexSequenceBase);
    }

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

    console.info("[repository:persist-jobs]", {
      crawlRunId,
      searchSessionId: options.searchSessionId,
      inputCount: jobs.length,
      sanitizedCount: sanitizedJobs.length,
      upsertCount: upserts.size,
      insertedCount: insertedJobIds.size,
      updatedCount: updatedJobIds.size,
      linkedToRunCount: newToRunJobIds.size,
      indexedEventCount: dedupeStrings(indexedJobIds).length,
    });

    return {
      jobs: dedupeStoredJobs(Array.from(savedJobsById.values())),
      insertedCount: insertedJobIds.size,
      updatedCount: updatedJobIds.size,
      linkedToRunCount: newToRunJobIds.size,
      indexedEventCount: dedupeStrings(indexedJobIds).length,
    };
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
          sourceMetadata: {
            ...parsedExisting.sourceMetadata,
            ...record.sourceMetadata,
          },
          lastSeenAt: record.lastSeenAt,
          lastRefreshedAt: record.lastRefreshedAt,
          lastCrawledAt: parsedExisting.lastCrawledAt,
          lastSucceededAt: parsedExisting.lastSucceededAt,
          lastFailedAt: parsedExisting.lastFailedAt,
          nextEligibleAt: parsedExisting.nextEligibleAt ?? record.nextEligibleAt,
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
      const nextEligibleAt =
        observation.nextEligibleAt ??
        (!hasOutcome
          ? parsedExisting.nextEligibleAt
          : resolveObservedSourceNextEligibleAt({
              record: parsedExisting,
              observedAt: observation.observedAt,
              intervalMs: getEnv().BACKGROUND_INGESTION_INTERVAL_MS,
              health,
              consecutiveFailures: nextConsecutiveFailures,
              succeeded,
            }));
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
        nextEligibleAt,
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
    const canonicalJobKeys = collectUniqueStrings(jobs.map((job) => job.canonicalJobKey));
    const directMatches = await fetchJobsByField(this.jobs(), "canonicalJobKey", canonicalJobKeys);

    directMatches
      .map((document) => parseStoredJob(document))
      .forEach((job) => indexJobForBatchLookup(lookup, job));

    const unresolvedJobs = jobs.filter(
      (job) => !lookup.byCanonicalJobKey.has(job.canonicalJobKey),
    );
    if (unresolvedJobs.length === 0) {
      return lookup;
    }

    const canonicalUrls = collectUniqueStrings(unresolvedJobs.map((job) => job.canonicalUrl));
    const resolvedUrls = collectUniqueStrings(unresolvedJobs.map((job) => job.resolvedUrl));
    const applyUrls = collectUniqueStrings(unresolvedJobs.map((job) => job.applyUrl));
    const sourceUrls = collectUniqueStrings(unresolvedJobs.map((job) => job.sourceUrl));
    const sourceLookupKeys = collectUniqueStrings(
      unresolvedJobs.flatMap((job) => job.sourceLookupKeys),
    );
    const contentFingerprints = collectUniqueStrings(
      unresolvedJobs
        .filter((job) => !buildCanonicalJobIdentity(job).hasStrongIdentity)
        .map((job) => job.contentFingerprint),
    );
    const legacyMatches = await Promise.all([
      fetchJobsByField(this.jobs(), "canonicalUrl", canonicalUrls),
      fetchJobsByField(this.jobs(), "resolvedUrl", resolvedUrls),
      fetchJobsByField(this.jobs(), "applyUrl", applyUrls),
      fetchJobsByField(this.jobs(), "sourceUrl", sourceUrls),
      fetchJobsByField(this.jobs(), "sourceLookupKeys", sourceLookupKeys),
      fetchJobsByField(this.jobs(), "contentFingerprint", contentFingerprints),
    ]);

    legacyMatches
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

  private indexedJobEvents() {
    return this.db.collection<IndexedJobEvent>(collectionNames.indexedJobEvents);
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

  private async appendIndexedJobEvents(
    crawlRunId: string,
    jobIds: string[],
    latestSequence: number,
  ) {
    const createdAt = new Date().toISOString();
    const uniqueJobIds = dedupeStrings(jobIds);
    const inserts = uniqueJobIds.map((jobId, index) =>
      indexedJobEventSchema.parse({
        _id: createId(),
        jobId,
        crawlRunId,
        sequence: latestSequence + index + 1,
        createdAt,
      }),
    );

    await persistBatchMutations(this.indexedJobEvents(), {
      inserts,
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
    const { getMongoDb } = await import("@/lib/server/mongodb");
    return new JobCrawlerRepository((await getMongoDb()) as DatabaseAdapter);
  } catch (error) {
    if (!hasWarnedMemoryFallback) {
      hasWarnedMemoryFallback = true;
      const message =
        error instanceof Error ? error.message : "Unknown MongoDB initialization error.";
      console.warn(
        /bootstrap failed|migration|index initialization|jobs_canonical_job_key/i.test(message)
          ? "[db:fallback] MongoDB bootstrap failed; using in-memory persistence for this process."
          : "[db:fallback] MongoDB is unavailable; using in-memory persistence for this process.",
        error instanceof Error ? { message } : { error },
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
  return mergeNormalizedJobRecord(existing, incoming, {
    additionalCrawlRunIds: [crawlRunId],
    forceCanonicalJobKey: existing.canonicalJobKey,
    preserveId: existing._id,
  });
}

function earliestDate(left?: string, right?: string) {
  if (!left) {
    return right;
  }

  if (!right) {
    return left;
  }

  return left < right ? left : right;
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

function buildContentHash(value: unknown) {
  return createHash("sha256").update(stableSerialize(value)).digest("hex");
}

function buildIndexedCandidateChannelBreakdown(
  counts: Array<{
    channelName: IndexedJobCandidateChannelName;
    count: number;
  }>,
): IndexedJobCandidateChannelBreakdown {
  const breakdown = emptyIndexedJobCandidateChannelBreakdown();

  for (const { channelName, count } of counts) {
    if (channelName === "exactTitleChannel") {
      breakdown.exactTitleCount = count;
    } else if (channelName === "aliasTitleChannel") {
      breakdown.aliasTitleCount = count;
    } else if (channelName === "conceptChannel") {
      breakdown.conceptCount = count;
    } else if (channelName === "familyChannel") {
      breakdown.familyCount = count;
    } else if (channelName === "geoChannel") {
      breakdown.geoCount = count;
    } else if (channelName === "legacyTitleFallbackChannel") {
      breakdown.legacyTitleFallbackCount = count;
    } else if (channelName === "legacyLocationFallbackChannel") {
      breakdown.legacyLocationFallbackCount = count;
    }
  }

  return breakdown;
}

function mergeIndexedCandidateChannelDocuments<TDocument extends Record<string, unknown>>(
  channelDocuments: TDocument[][],
  limit: number,
) {
  const merged: TDocument[] = [];
  const seenIds = new Set<string>();
  const maxChannelLength = Math.max(0, ...channelDocuments.map((documents) => documents.length));

  for (let index = 0; index < maxChannelLength && merged.length < limit; index += 1) {
    for (const documents of channelDocuments) {
      if (merged.length >= limit) {
        break;
      }

      const document = documents[index];
      if (!document) {
        continue;
      }

      const id = typeof document._id === "string" ? document._id : String(document._id ?? "");
      if (!id || seenIds.has(id)) {
        continue;
      }

      seenIds.add(id);
      merged.push(document);
    }
  }

  return merged;
}

function parseStoredSourceInventory(document: Record<string, unknown>) {
  return sourceInventoryRecordSchema.parse(
    normalizeStoredSourceInventoryFields(document),
  );
}

function selectPrimaryRecord<TJob extends Pick<
  JobListing,
  | "_id"
  | "applyUrl"
  | "canonicalUrl"
  | "resolvedUrl"
  | "sourceUrl"
  | "linkStatus"
  | "lastValidatedAt"
> & Partial<Pick<JobListing, "crawlRunIds" | "sourceLookupKeys" | "sourceProvenance" | "firstSeenAt" | "lastSeenAt">>>(
  left: TJob,
  right: TJob,
) {
  return compareJobRecordQuality(left, right) >= 0 ? left : right;
}

function recordScore(job: Pick<JobListing, "linkStatus" | "resolvedUrl" | "canonicalUrl" | "lastValidatedAt">) {
  return (
    linkScore(job.linkStatus) +
    (job.resolvedUrl ? 1 : 0) +
    (job.canonicalUrl ? 1 : 0) +
    (job.lastValidatedAt ? 1 : 0)
  );
}

function compareJobRecordQuality<
  TJob extends Pick<
    JobListing,
    | "_id"
    | "applyUrl"
    | "canonicalUrl"
    | "resolvedUrl"
    | "sourceUrl"
    | "linkStatus"
    | "lastValidatedAt"
  > &
    Partial<
      Pick<
        JobListing,
        "crawlRunIds" | "sourceLookupKeys" | "sourceProvenance" | "firstSeenAt" | "lastSeenAt"
      >
    >,
>(left: TJob, right: TJob) {
  const scoreDelta = recordScore(left) - recordScore(right);
  if (scoreDelta !== 0) {
    return scoreDelta;
  }

  const populatedUrlDelta = countPresentStrings(left) - countPresentStrings(right);
  if (populatedUrlDelta !== 0) {
    return populatedUrlDelta;
  }

  const provenanceDelta =
    (left.sourceProvenance?.length ?? 0) - (right.sourceProvenance?.length ?? 0);
  if (provenanceDelta !== 0) {
    return provenanceDelta;
  }

  const lookupDelta =
    (left.sourceLookupKeys?.length ?? 0) - (right.sourceLookupKeys?.length ?? 0);
  if (lookupDelta !== 0) {
    return lookupDelta;
  }

  const crawlRunDelta = (left.crawlRunIds?.length ?? 0) - (right.crawlRunIds?.length ?? 0);
  if (crawlRunDelta !== 0) {
    return crawlRunDelta;
  }

  if ((left.lastSeenAt ?? "") !== (right.lastSeenAt ?? "")) {
    return (left.lastSeenAt ?? "") > (right.lastSeenAt ?? "") ? 1 : -1;
  }

  if ((left.firstSeenAt ?? "") !== (right.firstSeenAt ?? "")) {
    return (left.firstSeenAt ?? "") < (right.firstSeenAt ?? "") ? 1 : -1;
  }

  return (left._id ?? "") <= (right._id ?? "") ? 1 : -1;
}

function countPresentStrings(
  job: Pick<JobListing, "applyUrl" | "canonicalUrl" | "resolvedUrl" | "sourceUrl">,
) {
  return [job.canonicalUrl, job.resolvedUrl, job.applyUrl, job.sourceUrl].filter(Boolean).length;
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

function pickPreferredValue<T>(preferred: T | undefined, alternate: T | undefined) {
  return preferred ?? alternate;
}

function mergeNormalizedJobRecord(
  existing: JobListing,
  incoming: PersistableJob | JobListing,
  options: {
    additionalCrawlRunIds?: string[];
    forceCanonicalJobKey?: string;
    preserveId?: string;
    preserveAnyActiveState?: boolean;
  } = {},
): JobListing {
  const preferred = selectPrimaryRecord(existing, incoming as JobListing);
  const alternate = preferred === existing ? incoming : existing;
  const sourceLookupKeys = dedupeStrings([
    ...existing.sourceLookupKeys,
    ...incoming.sourceLookupKeys,
  ]);
  const crawlRunIds = dedupeStrings([
    ...existing.crawlRunIds,
    ...("crawlRunIds" in incoming && Array.isArray(incoming.crawlRunIds)
      ? incoming.crawlRunIds
      : []),
    ...(options.additionalCrawlRunIds ?? []),
  ]);
  const sourceProvenance = dedupeProvenance([
    ...existing.sourceProvenance,
    ...incoming.sourceProvenance,
  ]);
  const isActive = options.preserveAnyActiveState
    ? existing.isActive || incoming.isActive
    : incoming.isActive;
  const lastSeenAt = latestDate(existing.lastSeenAt, incoming.lastSeenAt) ?? existing.lastSeenAt;
  const mergedId =
    options.preserveId ??
    pickPreferredValue(preferred._id, "_id" in alternate ? alternate._id : undefined) ??
    existing._id;

  return jobListingSchema.parse({
    ...alternate,
    ...preferred,
    _id: mergedId,
    canonicalJobKey:
      options.forceCanonicalJobKey ??
      pickPreferredValue(preferred.canonicalJobKey, alternate.canonicalJobKey),
    sourceLookupKeys,
    crawlRunIds,
    sourceProvenance,
    canonicalUrl: pickPreferredValue(preferred.canonicalUrl, alternate.canonicalUrl),
    resolvedUrl: pickPreferredValue(preferred.resolvedUrl, alternate.resolvedUrl),
    applyUrl: pickPreferredValue(preferred.applyUrl, alternate.applyUrl),
    sourceUrl: pickPreferredValue(preferred.sourceUrl, alternate.sourceUrl),
    sourceCompanySlug: pickPreferredValue(
      preferred.sourceCompanySlug,
      alternate.sourceCompanySlug,
    ),
    sourceJobId: pickPreferredValue(preferred.sourceJobId, alternate.sourceJobId),
    firstSeenAt: earliestDate(existing.firstSeenAt, incoming.firstSeenAt) ?? existing.firstSeenAt,
    lastSeenAt,
    indexedAt: latestDate(existing.indexedAt, incoming.indexedAt) ?? existing.indexedAt,
    isActive,
    closedAt: isActive
      ? undefined
      : latestDate(existing.closedAt, incoming.closedAt) ?? lastSeenAt,
    discoveredAt:
      earliestDate(existing.discoveredAt, incoming.discoveredAt) ?? existing.discoveredAt,
    crawledAt: latestDate(existing.crawledAt, incoming.crawledAt) ?? existing.crawledAt,
    postingDate: latestDate(existing.postingDate, incoming.postingDate),
    postedAt: latestDate(existing.postedAt, incoming.postedAt),
    contentHash: pickPreferredValue(preferred.contentHash, alternate.contentHash),
  });
}

export function mergeStoredJobs(existing: JobListing, incoming: JobListing) {
  return mergeNormalizedJobRecord(existing, incoming, {
    forceCanonicalJobKey: existing.canonicalJobKey,
    preserveAnyActiveState: true,
  });
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

function coalescePersistableJobs(jobs: PersistableJob[], crawlRunId: string) {
  const byCanonicalJobKey = new Map<string, JobListing>();

  for (const job of jobs) {
    const existing = byCanonicalJobKey.get(job.canonicalJobKey);
    if (!existing) {
      byCanonicalJobKey.set(
        job.canonicalJobKey,
        jobListingSchema.parse({
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        }),
      );
      continue;
    }

    byCanonicalJobKey.set(job.canonicalJobKey, mergeJobRecords(existing, job, crawlRunId));
  }

  return Array.from(byCanonicalJobKey.values()).map(({ _id, crawlRunIds: _ignored, ...job }) =>
    persistableJobSchema.parse(job),
  );
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
    const key = stableSerialize({
      sourcePlatform: record.sourcePlatform,
      sourceJobId: record.sourceJobId,
      sourceUrl: record.sourceUrl,
      applyUrl: record.applyUrl,
    });
    const existing = map.get(key);
    if (!existing) {
      map.set(key, record);
      continue;
    }

    const preferred = selectPreferredProvenanceRecord(existing, record);
    const alternate = preferred === existing ? record : existing;
    map.set(key, {
      ...alternate,
      ...preferred,
      discoveredAt:
        earliestDate(existing.discoveredAt, record.discoveredAt) ?? preferred.discoveredAt,
      resolvedUrl: preferred.resolvedUrl ?? alternate.resolvedUrl,
      canonicalUrl: preferred.canonicalUrl ?? alternate.canonicalUrl,
      rawSourceMetadata: {
        ...existing.rawSourceMetadata,
        ...record.rawSourceMetadata,
      },
    });
  }

  return Array.from(map.values());
}

function selectPreferredProvenanceRecord(
  left: JobListing["sourceProvenance"][number],
  right: JobListing["sourceProvenance"][number],
) {
  const leftScore = Number(Boolean(left.resolvedUrl)) + Number(Boolean(left.canonicalUrl));
  const rightScore = Number(Boolean(right.resolvedUrl)) + Number(Boolean(right.canonicalUrl));
  if (leftScore !== rightScore) {
    return leftScore > rightScore ? left : right;
  }

  return left.discoveredAt <= right.discoveredAt ? left : right;
}

function shouldEmitIndexedJobEvent(existing: JobListing, merged: JobListing) {
  return buildIndexedJobEventFingerprint(existing) !== buildIndexedJobEventFingerprint(merged);
}

function buildIndexedJobEventFingerprint(job: JobListing) {
  return stableSerialize({
    canonicalJobKey: job.canonicalJobKey,
    title: job.title,
    normalizedTitle: job.normalizedTitle,
    company: job.company,
    normalizedCompany: job.normalizedCompany,
    country: job.country,
    state: job.state,
    city: job.city,
    locationRaw: job.locationRaw,
    normalizedLocation: job.normalizedLocation,
    locationText: job.locationText,
    remoteType: job.remoteType,
    employmentType: job.employmentType,
    seniority: job.seniority,
    experienceLevel: job.experienceLevel,
    experienceClassification: job.experienceClassification,
    canonicalUrl: job.canonicalUrl,
    resolvedUrl: job.resolvedUrl,
    applyUrl: job.applyUrl,
    sourceUrl: job.sourceUrl,
    postingDate: job.postingDate,
    postedAt: job.postedAt,
    descriptionSnippet: job.descriptionSnippet,
    salaryInfo: job.salaryInfo,
    sponsorshipHint: job.sponsorshipHint,
    linkStatus: job.linkStatus,
    lastValidatedAt: job.lastValidatedAt,
    isActive: job.isActive,
    closedAt: job.closedAt,
    contentHash: job.contentHash,
  });
}

type PersistBatchLookup = {
  byCanonicalJobKey: Map<string, JobListing>;
  byCanonicalUrl: Map<string, JobListing>;
  byResolvedUrl: Map<string, JobListing>;
  byApplyUrl: Map<string, JobListing>;
  bySourceUrl: Map<string, JobListing>;
  byPlatformJobKey: Map<string, JobListing>;
  byContentFingerprint: Map<string, JobListing>;
};

function createPersistBatchLookup(): PersistBatchLookup {
  return {
    byCanonicalJobKey: new Map<string, JobListing>(),
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
  lookup.byCanonicalJobKey.set(identity.canonicalJobKey, job);

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
  const byCanonicalJobKey = lookup.byCanonicalJobKey.get(identity.canonicalJobKey);
  if (byCanonicalJobKey) {
    return byCanonicalJobKey;
  }

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
    | "canonicalJobKey"
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

async function persistJobUpserts<TDocument extends { _id: string; canonicalJobKey: string }>(
  collection: CollectionAdapter<TDocument>,
  operations: {
    upserts: TDocument[];
  },
) {
  const bulkOperations = operations.upserts.map((document) => {
    const { _id, canonicalJobKey, ...updateFields } = document;
    return {
      updateOne: {
        filter: { canonicalJobKey },
        update: {
          $set: updateFields,
          $setOnInsert: {
            _id,
            canonicalJobKey,
          },
        },
        upsert: true,
      },
    } as const;
  });

  if (bulkOperations.length === 0) {
    return;
  }

  if (collection.bulkWrite) {
    await collection.bulkWrite(bulkOperations);
    return;
  }

  for (const operation of bulkOperations) {
    await collection.updateOne(
      operation.updateOne.filter,
      operation.updateOne.update,
      { upsert: true },
    );
  }
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

function parseStoredIndexedJobEvent(document: Record<string, unknown>): IndexedJobEvent {
  const normalizedDocument = normalizeLegacyStoredRecord(document);

  return indexedJobEventSchema.parse({
    _id: String(normalizedDocument._id),
    jobId: String(normalizedDocument.jobId),
    crawlRunId: String(normalizedDocument.crawlRunId),
    sequence: Number(normalizedDocument.sequence),
    createdAt: String(normalizedDocument.createdAt),
  });
}

export function parseStoredJob(document: Record<string, unknown>) {
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
  const discoveredAt =
    normalizeOptionalDocumentString(document.discoveredAt) ?? new Date(0).toISOString();
  const firstSeenAt =
    normalizeOptionalDocumentString(document.firstSeenAt) ?? discoveredAt;
  const lastSeenAt =
    normalizeOptionalDocumentString(document.lastSeenAt) ?? crawledAt ?? discoveredAt;
  const indexedAt =
    normalizeOptionalDocumentString(document.indexedAt) ?? crawledAt ?? discoveredAt;
  const isActive = typeof document.isActive === "boolean" ? document.isActive : true;
  const closedAt = isActive
    ? undefined
    : normalizeOptionalDocumentString(document.closedAt) ?? lastSeenAt;
  const contentHash =
    normalizeOptionalDocumentString(document.contentHash) ??
    buildContentHash({
      title,
      company,
      locationRaw,
      descriptionSnippet: normalizeOptionalDocumentString(document.descriptionSnippet),
      employmentType: normalizeEmploymentType(document.employmentType),
      remoteType: normalizeRemoteType(document.remoteType, resolvedLocation, locationRaw),
      seniority:
        normalizeOptionalExperienceLevel(document.seniority) ??
        normalizeOptionalExperienceLevel(document.experienceLevel),
      experienceLevel: normalizeOptionalExperienceLevel(document.experienceLevel),
      canonicalUrl: normalizeOptionalDocumentString(document.canonicalUrl),
      applyUrl: normalizeOptionalDocumentString(document.applyUrl),
      salaryInfo: normalizeSalaryInfo(document.salaryInfo),
      postingDate,
    });
  const canonicalJobKey =
    normalizeOptionalDocumentString(document.canonicalJobKey) ??
    buildCanonicalJobIdentity({
      _id: normalizeOptionalDocumentString(document._id),
      sourcePlatform: document.sourcePlatform as JobListing["sourcePlatform"],
      sourceCompanySlug: normalizeOptionalDocumentString(document.sourceCompanySlug),
      sourceJobId: normalizeOptionalDocumentString(document.sourceJobId) ?? "",
      sourceUrl: normalizeOptionalDocumentString(document.sourceUrl) ?? "",
      applyUrl: normalizeOptionalDocumentString(document.applyUrl) ?? "",
      resolvedUrl: normalizeOptionalDocumentString(document.resolvedUrl),
      canonicalUrl: normalizeOptionalDocumentString(document.canonicalUrl),
      sourceLookupKeys,
      company,
      title,
      locationRaw,
      locationText: normalizeOptionalDocumentString(document.locationText) ?? locationRaw,
      normalizedCompany,
      normalizedTitle,
      normalizedLocation,
      dedupeFingerprint,
      companyNormalized: normalizedCompany,
      titleNormalized: normalizedTitle,
      locationNormalized: normalizedLocation,
      contentFingerprint: dedupeFingerprint,
    }).canonicalJobKey;
  const geoLocation = normalizeJobGeoLocation({
    country: normalizeOptionalDocumentString(document.country),
    state: normalizeOptionalDocumentString(document.state),
    city: normalizeOptionalDocumentString(document.city),
    locationText: normalizeOptionalDocumentString(document.locationText) ?? locationRaw,
    locationRaw,
    normalizedLocation,
    locationNormalized: normalizedLocation,
    resolvedLocation,
    rawSourceMetadata,
  });
  const searchIndex = buildJobSearchIndex({
    title,
    normalizedTitle,
    country: normalizeOptionalDocumentString(document.country),
    state: normalizeOptionalDocumentString(document.state),
    city: normalizeOptionalDocumentString(document.city),
    locationText: normalizeOptionalDocumentString(document.locationText) ?? locationRaw,
    normalizedLocation,
    locationNormalized: normalizedLocation,
    resolvedLocation,
    geoLocation,
    experienceLevel: normalizeOptionalExperienceLevel(document.experienceLevel),
    experienceClassification: normalizeExperienceClassification(document.experienceClassification),
    sourcePlatform: document.sourcePlatform as JobListing["sourcePlatform"],
    linkStatus: normalizeLinkStatus(document.linkStatus),
    isActive,
    postingDate,
    postedAt: postingDate,
    lastSeenAt,
    crawledAt,
    discoveredAt,
    indexedAt,
  });

  return {
    ...document,
    canonicalJobKey,
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
    geoLocation,
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
    discoveredAt,
    crawledAt,
    descriptionSnippet: normalizeOptionalDocumentString(document.descriptionSnippet),
    salaryInfo: normalizeSalaryInfo(document.salaryInfo),
    sponsorshipHint: normalizeSponsorshipHint(document.sponsorshipHint),
    rawSourceMetadata,
    sourceProvenance,
    sourceLookupKeys,
    crawlRunIds,
    firstSeenAt,
    lastSeenAt,
    indexedAt,
    isActive,
    closedAt,
    searchIndex,
    dedupeFingerprint,
    linkStatus: typeof document.linkStatus === "string" ? document.linkStatus : "unknown",
    companyNormalized: normalizedCompany,
    titleNormalized: normalizedTitle,
    locationNormalized: normalizedLocation,
    contentFingerprint: dedupeFingerprint,
    contentHash,
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
    sourceMetadata: isRecord(normalizedDocument.sourceMetadata)
      ? normalizedDocument.sourceMetadata
      : {},
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
    nextEligibleAt: normalizeOptionalDocumentString(normalizedDocument.nextEligibleAt),
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

  const normalizedValue = normalizeLegacyStoredValue(value);
  if (!isRecord(normalizedValue)) {
    return undefined;
  }

  const explicitLevel = normalizeOptionalExperienceLevel(normalizedValue.explicitLevel);
  const inferredLevel = normalizeOptionalExperienceLevel(normalizedValue.inferredLevel);
  const resolvedLevel = explicitLevel ?? inferredLevel;
  const diagnostics = isRecord(normalizedValue.diagnostics)
    ? {
        ...normalizedValue.diagnostics,
        matchedSignals: Array.isArray(normalizedValue.diagnostics.matchedSignals)
          ? normalizedValue.diagnostics.matchedSignals
          : [],
      }
    : undefined;
  const parsed = experienceClassificationSchema.safeParse(
    {
      ...normalizedValue,
      experienceVersion:
        typeof normalizedValue.experienceVersion === "number"
          ? normalizedValue.experienceVersion
          : currentExperienceClassificationVersion,
      experienceBand:
        typeof normalizedValue.experienceBand === "string"
          ? normalizedValue.experienceBand
          : resolveExperienceBand(resolvedLevel ?? "unknown"),
      experienceSource:
        typeof normalizedValue.experienceSource === "string"
          ? normalizedValue.experienceSource
          : normalizedValue.source,
      experienceConfidence:
        typeof normalizedValue.experienceConfidence === "string"
          ? normalizedValue.experienceConfidence
          : normalizedValue.confidence,
      experienceSignals: Array.isArray(normalizedValue.experienceSignals)
        ? normalizedValue.experienceSignals
        : diagnostics?.matchedSignals,
      explicitLevel,
      inferredLevel,
    },
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
        ...(Array.isArray(value.physicalLocations)
          ? { physicalLocations: value.physicalLocations }
          : {}),
        ...(Array.isArray(value.eligibilityCountries)
          ? { eligibilityCountries: value.eligibilityCountries }
          : {}),
        ...(Array.isArray(value.conflicts) ? { conflicts: value.conflicts } : {}),
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

function normalizeLinkStatus(value: unknown) {
  const parsed = linkStatusSchema.safeParse(value);
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

function compareCrawlRunsByActivity(left: CrawlRun, right: CrawlRun) {
  return resolveCrawlRunActivityAt(left).localeCompare(resolveCrawlRunActivityAt(right));
}

function resolveCrawlRunActivityAt(run: CrawlRun) {
  return run.finishedAt ?? run.lastHeartbeatAt ?? run.startedAt;
}

function isSuccessfulProfileRunStatus(status: CrawlRunStatus) {
  return status === "completed" || status === "partial";
}

function isFailedProfileRunStatus(status: CrawlRunStatus) {
  return status === "failed" || status === "aborted";
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
