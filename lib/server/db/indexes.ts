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
  jobs: "jobs",
  crawlRuns: "crawlRuns",
  crawlSourceResults: "crawlSourceResults",
  linkValidations: "linkValidations",
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
  ]);

  await db.collection(collectionNames.jobs).createIndexes([
    {
      key: { crawlRunIds: 1, postedAt: -1, sourcePlatform: 1, title: 1 },
      name: "jobs_listing_by_run_and_sort",
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
      key: { contentFingerprint: 1 },
      name: "jobs_content_fingerprint",
    },
  ]);

  await db.collection(collectionNames.crawlRuns).createIndexes([
    {
      key: { searchId: 1, startedAt: -1 },
      name: "crawlRuns_searchId_startedAt_desc",
    },
    {
      key: { status: 1, startedAt: -1 },
      name: "crawlRuns_status_startedAt_desc",
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

  indexesEnsured = true;
}
