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
  crawlRunJobEvents: "crawlRunJobEvents",
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
      key: { status: 1, startedAt: -1 },
      name: "crawlRuns_status_startedAt_desc",
    },
    {
      key: { validationMode: 1, startedAt: -1 },
      name: "crawlRuns_validationMode_startedAt_desc",
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
