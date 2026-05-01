import "server-only";

import { executeCrawlPipeline } from "@/lib/server/crawler/pipeline";
import { isSearchRunPending, queueSearchRun } from "@/lib/server/crawler/background-runs";
import {
  resolveJobExperienceClassification,
  resolveJobExperienceLevel,
} from "@/lib/server/crawler/helpers";
import {
  toStoredValidation,
  validateJobLink,
} from "@/lib/server/crawler/link-validation";
import { createDiscoveryService } from "@/lib/server/discovery/service";
import { createDefaultProviders } from "@/lib/server/providers";
import { selectProvidersForTieredCrawl } from "@/lib/server/providers/tiers";
import type { CrawlRun, JobListing, SearchDocument, SearchSessionDocument } from "@/lib/types";
import { resolveRepository, type JobCrawlerRuntime } from "@/lib/server/services/runtime";

import { ResourceNotFoundError } from "@/lib/server/search/errors";

type SearchExecutionTarget = {
  search: SearchDocument;
  searchSession: SearchSessionDocument;
  crawlRun: CrawlRun;
};

async function buildSearchIngestionContext(
  runtime: JobCrawlerRuntime = {},
) {
  const repository = await resolveRepository(runtime.repository);

  return {
    repository,
    discovery: runtime.discovery ?? createDiscoveryService({ repository }),
    providers: runtime.providers ?? createDefaultProviders(),
    fetchImpl: runtime.fetchImpl ?? fetch,
    now: runtime.now ?? new Date(),
    deepExperienceInference: runtime.deepExperienceInference ?? false,
    linkValidationMode: runtime.linkValidationMode,
    inlineValidationTopN: runtime.inlineValidationTopN,
    providerTimeoutMs: runtime.providerTimeoutMs,
    sourceTimeoutMs: runtime.sourceTimeoutMs,
    progressUpdateIntervalMs: runtime.progressUpdateIntervalMs,
  };
}

export async function executeSearchIngestion(
  target: SearchExecutionTarget,
  runtime: JobCrawlerRuntime = {},
) {
  const context = await buildSearchIngestionContext(runtime);

  return executeCrawlPipeline({
    search: target.search,
    searchSession: target.searchSession,
    crawlRun: target.crawlRun,
    repository: context.repository,
    discovery: context.discovery,
    providers: context.providers,
    fetchImpl: context.fetchImpl,
    now: context.now,
    deepExperienceInference: context.deepExperienceInference,
    linkValidationMode: context.linkValidationMode,
    inlineValidationTopN: context.inlineValidationTopN,
    providerTimeoutMs: context.providerTimeoutMs,
    sourceTimeoutMs: context.sourceTimeoutMs,
    progressUpdateIntervalMs: context.progressUpdateIntervalMs,
    signal: runtime.signal,
  });
}

export async function queueSearchIngestion(
  target: SearchExecutionTarget,
  runtime: JobCrawlerRuntime = {},
) {
  const context = await buildSearchIngestionContext(runtime);
  const ownerKey = normalizeRequestOwnerKey(runtime.requestOwnerKey);
  const crawlMode = target.search.filters.crawlMode ?? "balanced";
  const providerSelection = selectProvidersForTieredCrawl({
    providers: context.providers,
    selectedPlatforms: target.search.filters.platforms,
    crawlMode,
    includeSlowProviders: crawlMode === "deep",
  });
  const queuedResult = await queueSearchRun(
    target.search._id,
    context.repository,
    async (signal) => {
      await executeCrawlPipeline({
        search: target.search,
        searchSession: target.searchSession,
        crawlRun: target.crawlRun,
        repository: context.repository,
        discovery: context.discovery,
        providers: context.providers,
        fetchImpl: context.fetchImpl,
        now: context.now,
        deepExperienceInference: context.deepExperienceInference,
        linkValidationMode: context.linkValidationMode,
        inlineValidationTopN: context.inlineValidationTopN,
        providerTimeoutMs: context.providerTimeoutMs,
        sourceTimeoutMs: context.sourceTimeoutMs,
        progressUpdateIntervalMs: context.progressUpdateIntervalMs,
        signal,
      });
    },
    {
      ownerKey,
      crawlRunId: target.crawlRun._id,
      searchSessionId: target.searchSession._id,
      queuedAt: context.now.toISOString(),
    },
  );
  const isSearchRunPendingResult = await isSearchRunPending(
    target.search._id,
    context.repository,
  );
  const queued = queuedResult || isSearchRunPendingResult;

  console.info("[ingestion:trace:queue-request]", {
    searchId: target.search._id,
    searchSessionId: target.searchSession._id,
    crawlRunId: target.crawlRun._id,
    ownerKey,
    queuedResult,
    isSearchRunPendingResult,
  });
  console.info("[ingestion:targeted-queue]", {
    searchId: target.search._id,
    searchSessionId: target.searchSession._id,
    crawlRunId: target.crawlRun._id,
    filters: target.search.filters,
    selectedProviders: providerSelection.selectedProviders.map((provider) => provider.provider),
    skippedSlowProviders: providerSelection.skippedSlowProviders,
    ownerKey: ownerKey ?? null,
    queued,
    activeQueueAlreadyExists: !queuedResult && isSearchRunPendingResult,
    reason: runtime.ingestionQueueReason ?? null,
  });

  return queued;
}

export async function runSearchIngestionFromSession(
  target: SearchExecutionTarget,
  runtime: JobCrawlerRuntime = {},
) {
  return executeSearchIngestion(target, runtime);
}

export async function revalidateJob(jobId: string, runtime: JobCrawlerRuntime = {}) {
  const repository = await resolveRepository(runtime.repository);
  const job = await repository.getJob(jobId);

  if (!job) {
    throw new ResourceNotFoundError(`Job ${jobId} was not found.`);
  }

  const validation = await validateJobLink(
    job.applyUrl,
    runtime.fetchImpl ?? fetch,
    runtime.now ?? new Date(),
  );

  const updatedJob: JobListing = {
    ...job,
    experienceLevel: resolveJobExperienceLevel(job),
    experienceClassification: resolveJobExperienceClassification(job),
    resolvedUrl: validation.resolvedUrl ?? job.resolvedUrl,
    canonicalUrl: validation.canonicalUrl ?? job.canonicalUrl,
    linkStatus: validation.status,
    lastValidatedAt: validation.checkedAt,
  };

  const latestRunId = job.crawlRunIds[job.crawlRunIds.length - 1] ?? job.crawlRunIds[0];
  const [savedJob] = await repository.persistJobs(latestRunId, [
    {
      ...updatedJob,
      sourceProvenance: updatedJob.sourceProvenance,
      sourceLookupKeys: updatedJob.sourceLookupKeys,
    },
  ]);

  await repository.saveLinkValidation(toStoredValidation(jobId, validation));

  return savedJob;
}

function normalizeRequestOwnerKey(value?: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
