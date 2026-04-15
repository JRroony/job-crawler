import "server-only";

type IndexSpec = {
  key: Record<string, 1 | -1>;
  name: string;
  unique?: boolean;
  sparse?: boolean;
};

export type CollectionLike = {
  createIndexes(indexes: IndexSpec[]): Promise<unknown>;
};

export type DatabaseLike = {
  collection(name: string): CollectionLike;
};

export const collectionNames = {
  searches: "searches",
  searchSessions: "searchSessions",
  jobs: "jobs",
  crawlRuns: "crawlRuns",
  crawlControls: "crawlControls",
  crawlQueue: "crawlQueue",
  crawlSourceResults: "crawlSourceResults",
  crawlRunJobEvents: "crawlRunJobEvents",
  searchSessionJobEvents: "searchSessionJobEvents",
  linkValidations: "linkValidations",
  sourceInventory: "sourceInventory",
} as const;

let indexesEnsured = false;

export async function ensureDatabaseIndexes(db: DatabaseLike) {
  if (indexesEnsured) {
    return;
  }

  await db.collection(collectionNames.searches).createIndexes([
    {
      key: { createdAt: -1 },
      name: "searches_createdAt_desc",
    },
    {
      key: { latestCrawlRunId: 1 },
      name: "searches_latestCrawlRunId",
      sparse: true,
    },
    {
      key: { latestSearchSessionId: 1 },
      name: "searches_latestSearchSessionId",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.searchSessions).createIndexes([
    {
      key: { searchId: 1, createdAt: -1 },
      name: "searchSessions_searchId_createdAt_desc",
    },
    {
      key: { latestCrawlRunId: 1 },
      name: "searchSessions_latestCrawlRunId",
      sparse: true,
    },
    {
      key: { status: 1, updatedAt: -1 },
      name: "searchSessions_status_updatedAt_desc",
    },
  ]);

  await db.collection(collectionNames.jobs).createIndexes([
    {
      key: { crawlRunIds: 1, postedAt: -1, sourcePlatform: 1, title: 1 },
      name: "jobs_listing_by_run_and_sort",
    },
    {
      key: { sourcePlatform: 1, postedAt: -1, companyNormalized: 1, titleNormalized: 1 },
      name: "jobs_export_by_platform_and_postedAt",
    },
    {
      key: { sourceLookupKeys: 1 },
      name: "jobs_source_lookup_keys",
    },
    {
      key: { canonicalUrl: 1 },
      name: "jobs_canonical_url",
      sparse: true,
    },
    {
      key: { resolvedUrl: 1 },
      name: "jobs_resolved_url",
      sparse: true,
    },
    {
      key: { applyUrl: 1 },
      name: "jobs_apply_url",
    },
    {
      key: { sourceUrl: 1 },
      name: "jobs_source_url",
    },
    {
      key: { contentFingerprint: 1 },
      name: "jobs_content_fingerprint",
    },
    {
      key: { linkStatus: 1, lastValidatedAt: -1 },
      name: "jobs_linkStatus_lastValidatedAt_desc",
    },
  ]);

  await db.collection(collectionNames.crawlRuns).createIndexes([
    {
      key: { searchId: 1, startedAt: -1 },
      name: "crawlRuns_searchId_startedAt_desc",
    },
    {
      key: { searchSessionId: 1, startedAt: -1 },
      name: "crawlRuns_searchSessionId_startedAt_desc",
      sparse: true,
    },
    {
      key: { status: 1, startedAt: -1 },
      name: "crawlRuns_status_startedAt_desc",
    },
    {
      key: { validationMode: 1, startedAt: -1 },
      name: "crawlRuns_validationMode_startedAt_desc",
    },
  ]);

  await db.collection(collectionNames.crawlControls).createIndexes([
    {
      key: { crawlRunId: 1 },
      name: "crawlControls_crawlRunId",
      unique: true,
    },
    {
      key: { searchId: 1, status: 1, updatedAt: -1 },
      name: "crawlControls_searchId_status_updatedAt_desc",
    },
    {
      key: { ownerKey: 1, status: 1, updatedAt: -1 },
      name: "crawlControls_ownerKey_status_updatedAt_desc",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.crawlQueue).createIndexes([
    {
      key: { crawlRunId: 1 },
      name: "crawlQueue_crawlRunId",
      unique: true,
    },
    {
      key: { searchId: 1, status: 1, updatedAt: -1 },
      name: "crawlQueue_searchId_status_updatedAt_desc",
    },
    {
      key: { ownerKey: 1, status: 1, updatedAt: -1 },
      name: "crawlQueue_ownerKey_status_updatedAt_desc",
      sparse: true,
    },
  ]);

  await db.collection(collectionNames.crawlSourceResults).createIndexes([
    {
      key: { crawlRunId: 1, provider: 1 },
      name: "crawlSourceResults_run_provider",
    },
    {
      key: { searchId: 1, finishedAt: -1 },
      name: "crawlSourceResults_searchId_finishedAt_desc",
    },
  ]);

  await db.collection(collectionNames.crawlRunJobEvents).createIndexes([
    {
      key: { crawlRunId: 1, sequence: 1 },
      name: "crawlRunJobEvents_run_sequence",
      unique: true,
    },
    {
      key: { crawlRunId: 1, jobId: 1 },
      name: "crawlRunJobEvents_run_job",
    },
  ]);

  await db.collection(collectionNames.searchSessionJobEvents).createIndexes([
    {
      key: { searchSessionId: 1, sequence: 1 },
      name: "searchSessionJobEvents_session_sequence",
      unique: true,
    },
    {
      key: { searchSessionId: 1, jobId: 1 },
      name: "searchSessionJobEvents_session_job",
    },
    {
      key: { searchSessionId: 1, crawlRunId: 1, sequence: 1 },
      name: "searchSessionJobEvents_session_run_sequence",
    },
  ]);

  await db.collection(collectionNames.linkValidations).createIndexes([
    {
      key: { jobId: 1, checkedAt: -1 },
      name: "linkValidations_job_checkedAt_desc",
    },
    {
      key: { applyUrl: 1, checkedAt: -1 },
      name: "linkValidations_applyUrl_checkedAt_desc",
    },
  ]);

  await db.collection(collectionNames.sourceInventory).createIndexes([
    {
      key: { platform: 1, status: 1, crawlPriority: 1, companyHint: 1 },
      name: "sourceInventory_platform_status_priority_companyHint",
    },
    {
      key: { token: 1, platform: 1 },
      name: "sourceInventory_token_platform",
      sparse: true,
    },
    {
      key: { lastRefreshedAt: -1, platform: 1 },
      name: "sourceInventory_lastRefreshedAt_desc_platform",
    },
    {
      key: { lastCrawledAt: -1, platform: 1, health: 1 },
      name: "sourceInventory_lastCrawledAt_desc_platform_health",
      sparse: true,
    },
  ]);

  indexesEnsured = true;
}
