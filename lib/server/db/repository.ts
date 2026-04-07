import "server-only";

import type { Db } from "mongodb";

import { createId } from "@/lib/server/crawler/helpers";
import { collectionNames } from "@/lib/server/db/indexes";
import { getMongoDb } from "@/lib/server/mongodb";
import type {
  CrawlRun,
  CrawlRunStatus,
  CrawlSourceResult,
  JobListing,
  LinkValidationResult,
  SearchDocument,
  SearchFilters,
} from "@/lib/types";
import { searchDocumentSchema } from "@/lib/types";

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

export type PersistableJob = Omit<JobListing, "_id" | "crawlRunIds">;

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

    return documents.map((document) => searchDocumentSchema.parse(document));
  }

  async getSearch(searchId: string) {
    const document = await this.searches().findOne({ _id: searchId });
    return document ? searchDocumentSchema.parse(document) : null;
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

  async createCrawlRun(searchId: string, now = new Date().toISOString()) {
    const document: CrawlRun = {
      _id: createId(),
      searchId,
      startedAt: now,
      status: "running",
      totalFetchedJobs: 0,
      totalMatchedJobs: 0,
      dedupedJobs: 0,
    };

    await this.crawlRuns().insertOne(document);
    return document;
  }

  async getCrawlRun(crawlRunId: string) {
    return this.crawlRuns().findOne({ _id: crawlRunId });
  }

  async finalizeCrawlRun(
    crawlRunId: string,
    payload: {
      status: CrawlRunStatus;
      totalFetchedJobs: number;
      totalMatchedJobs: number;
      dedupedJobs: number;
      errorMessage?: string;
      finishedAt?: string;
    },
  ) {
    await this.crawlRuns().updateOne(
      { _id: crawlRunId },
      {
        $set: {
          ...payload,
          finishedAt: payload.finishedAt ?? new Date().toISOString(),
        },
      },
    );
  }

  async saveCrawlSourceResults(sourceResults: CrawlSourceResult[]) {
    for (const sourceResult of sourceResults) {
      await this.crawlSourceResults().insertOne(sourceResult);
    }
  }

  async getCrawlSourceResults(crawlRunId: string) {
    return this.crawlSourceResults()
      .find({ crawlRunId }, { sort: { provider: 1 } })
      .toArray();
  }

  async getJobsByCrawlRun(crawlRunId: string) {
    return this.jobs()
      .find(
        { crawlRunIds: crawlRunId },
        { sort: { postedAt: -1, sourcePlatform: 1, title: 1 } },
      )
      .toArray();
  }

  async getJob(jobId: string) {
    return this.jobs().findOne({ _id: jobId });
  }

  async persistJobs(crawlRunId: string, jobs: PersistableJob[]) {
    const savedJobs: JobListing[] = [];

    for (const rawJob of jobs) {
      const job = sanitizePersistableJob(rawJob);
      const existing = await this.findExistingJob(job);
      if (!existing) {
        const document: JobListing = {
          _id: createId(),
          ...job,
          crawlRunIds: [crawlRunId],
        };
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
    await this.linkValidations().insertOne(result);
  }

  async getFreshValidation(applyUrl: string, checkedAfter: string) {
    const validations = await this.linkValidations()
      .find({ applyUrl }, { sort: { checkedAt: -1 } })
      .toArray();

    return (
      validations.find((validation) => validation.checkedAt >= checkedAfter) ?? null
    );
  }

  async searchesCollectionNames() {
    return collectionNames;
  }

  private async findExistingJob(job: PersistableJob) {
    if (job.canonicalUrl) {
      const byCanonical = await this.jobs().findOne({ canonicalUrl: job.canonicalUrl });
      if (byCanonical) {
        return byCanonical;
      }
    }

    if (job.resolvedUrl) {
      const byResolved = await this.jobs().findOne({ resolvedUrl: job.resolvedUrl });
      if (byResolved) {
        return byResolved;
      }
    }

    for (const lookupKey of job.sourceLookupKeys) {
      const bySourceLookup = await this.jobs().findOne({ sourceLookupKeys: lookupKey });
      if (bySourceLookup) {
        return bySourceLookup;
      }
    }

    return this.jobs().findOne({ contentFingerprint: job.contentFingerprint });
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
  const incomingScore = linkScore(incoming.linkStatus) + (incoming.resolvedUrl ? 1 : 0);
  const existingScore = linkScore(existing.linkStatus) + (existing.resolvedUrl ? 1 : 0);
  return incomingScore >= existingScore ? incoming : existing;
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

  return persistable;
}

function dedupeProvenance(records: JobListing["sourceProvenance"]) {
  const map = new Map<string, JobListing["sourceProvenance"][number]>();

  for (const record of records) {
    const key = `${record.sourcePlatform}:${record.sourceJobId}:${record.applyUrl}`;
    map.set(key, record);
  }

  return Array.from(map.values());
}
