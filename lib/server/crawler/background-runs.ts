import "server-only";

import { createId } from "@/lib/server/crawler/helpers";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";

type PendingRunRecord = {
  promise: Promise<void>;
  startedAt: string;
  controller: AbortController;
  ownerKey?: string;
  crawlRunId?: string;
};

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerPendingRuns: Map<string, PendingRunRecord> | undefined;
  // eslint-disable-next-line no-var
  var __jobCrawlerPendingRunOwners: Map<string, string> | undefined;
}

function getPendingRuns() {
  if (!globalThis.__jobCrawlerPendingRuns) {
    globalThis.__jobCrawlerPendingRuns = new Map<string, PendingRunRecord>();
  }

  return globalThis.__jobCrawlerPendingRuns;
}

function getPendingRunOwners() {
  if (!globalThis.__jobCrawlerPendingRunOwners) {
    globalThis.__jobCrawlerPendingRunOwners = new Map<string, string>();
  }

  return globalThis.__jobCrawlerPendingRunOwners;
}

export async function isSearchRunPending(
  searchId: string,
  repository: JobCrawlerRepository,
) {
  if (getPendingRuns().has(searchId)) {
    return true;
  }

  return repository.hasActiveCrawlQueueEntryForSearch(searchId);
}

export async function queueSearchRun(
  searchId: string,
  repository: JobCrawlerRepository,
  task: (signal: AbortSignal) => Promise<void>,
  options: {
    ownerKey?: string;
    crawlRunId?: string;
    searchSessionId?: string;
    queuedAt?: string;
    deferStart?: boolean;
  } = {},
) {
  const crawlRunId = options.crawlRunId;
  if (!crawlRunId) {
    throw new Error("queueSearchRun requires a crawlRunId for durable task control.");
  }

  const pendingRuns = getPendingRuns();
  if (pendingRuns.has(searchId)) {
    return false;
  }

  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForSearch(searchId);
  if (activeQueueEntry && activeQueueEntry.crawlRunId !== crawlRunId) {
    return false;
  }

  if (options.ownerKey) {
    const activeOwnerQueueEntry = await repository.getActiveCrawlQueueEntryForOwner(
      options.ownerKey,
    );
    if (activeOwnerQueueEntry && activeOwnerQueueEntry.crawlRunId !== crawlRunId) {
      return false;
    }

    const activeOwnerSearchId = getPendingRunOwners().get(options.ownerKey);
    if (activeOwnerSearchId && activeOwnerSearchId !== searchId) {
      return false;
    }
  }

  await repository.enqueueCrawlRun({
    crawlRunId,
    searchId,
    searchSessionId: options.searchSessionId,
    ownerKey: options.ownerKey,
    queuedAt: options.queuedAt,
  });

  const pendingRunOwners = getPendingRunOwners();
  const controller = new AbortController();
  const startedAt = new Date().toISOString();
  const workerId = `worker:${createId()}`;
  const promise = (async () => {
    if (options.deferStart) {
      await deferBackgroundRunStart();
    }

    await repository.markCrawlRunStarted(crawlRunId, {
      startedAt,
      workerId,
      ownerKey: options.ownerKey,
    });

    try {
      await task(controller.signal);
    } catch (error) {
      if (!isAbortLikeError(error)) {
        console.error("[crawl:background-run]", {
          searchId,
          crawlRunId,
          message:
            error instanceof Error
              ? error.message
              : "Background crawl failed unexpectedly.",
        });
      }
    } finally {
      const controlState = await repository.getCrawlRunControlState(crawlRunId);
      const finalizedStatus =
        controlState && controlState.status !== "running"
          ? controlState.status
          : controller.signal.aborted || controlState?.cancelRequestedAt
            ? "aborted"
            : "completed";
      await repository.finalizeCrawlQueueEntry(crawlRunId, {
        status: finalizedStatus,
      });
      pendingRuns.delete(searchId);
      if (options.ownerKey && pendingRunOwners.get(options.ownerKey) === searchId) {
        pendingRunOwners.delete(options.ownerKey);
      }
    }
  })();

  pendingRuns.set(searchId, {
    promise,
    startedAt,
    controller,
    ownerKey: options.ownerKey,
    crawlRunId,
  });
  if (options.ownerKey) {
    pendingRunOwners.set(options.ownerKey, searchId);
  }

  return true;
}

function deferBackgroundRunStart() {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

export async function abortSearchRun(
  searchId: string,
  repository: JobCrawlerRepository,
  options: {
    reason?: string;
    awaitCompletion?: boolean;
  } = {},
) {
  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForSearch(searchId);
  const record = getPendingRuns().get(searchId);
  const crawlRunId = activeQueueEntry?.crawlRunId ?? record?.crawlRunId;

  if (!crawlRunId) {
    return false;
  }

  await repository.requestCrawlRunCancellation(crawlRunId, {
    reason: options.reason,
  });
  abortPendingRecord(record, options.reason);

  if (options.awaitCompletion) {
    if (record) {
      await record.promise;
    }

    await waitForRunToSettle(crawlRunId, repository);
  }

  return true;
}

export function getPendingSearchRun(searchId: string) {
  return getPendingRuns().get(searchId);
}

export async function abortOwnerSearchRun(
  ownerKey: string,
  repository: JobCrawlerRepository,
  options: {
    reason?: string;
    awaitCompletion?: boolean;
  } = {},
) {
  const activeQueueEntry = await repository.getActiveCrawlQueueEntryForOwner(ownerKey);
  const searchId = activeQueueEntry?.searchId ?? getPendingRunOwners().get(ownerKey);

  if (!searchId) {
    return false;
  }

  return abortSearchRun(searchId, repository, options);
}

async function waitForRunToSettle(
  crawlRunId: string,
  repository: JobCrawlerRepository,
  timeoutMs = 5_000,
) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const queueState = await repository.getCrawlQueueEntryByRunId(crawlRunId);
    if (!queueState || queueState.finishedAt || !["queued", "running"].includes(queueState.status)) {
      return;
    }

    const controlState = await repository.getCrawlRunControlState(crawlRunId);
    if (!controlState || controlState.finishedAt || controlState.status !== "running") {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

function abortPendingRecord(record: PendingRunRecord | undefined, reason?: string) {
  if (!record || record.controller.signal.aborted) {
    return;
  }

  const abortError = new Error(reason ?? "The crawl was aborted.");
  abortError.name = "AbortError";
  record.controller.abort(abortError);
}

function isAbortLikeError(error: unknown) {
  return error instanceof Error && error.name === "AbortError";
}
