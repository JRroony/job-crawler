import "server-only";

type PendingRunRecord = {
  promise: Promise<void>;
  startedAt: string;
  controller: AbortController;
  ownerKey?: string;
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

export function isSearchRunPending(searchId: string) {
  return getPendingRuns().has(searchId);
}

export function queueSearchRun(
  searchId: string,
  task: (signal: AbortSignal) => Promise<void>,
  options: {
    ownerKey?: string;
  } = {},
) {
  const pendingRuns = getPendingRuns();
  if (pendingRuns.has(searchId)) {
    return false;
  }

  const pendingRunOwners = getPendingRunOwners();
  const controller = new AbortController();
  const promise = task(controller.signal)
    .catch((error) => {
      if (isAbortLikeError(error)) {
        return;
      }

      console.error("[crawl:background-run]", {
        searchId,
        message: error instanceof Error ? error.message : "Background crawl failed unexpectedly.",
      });
    })
    .finally(() => {
      pendingRuns.delete(searchId);
      if (options.ownerKey && pendingRunOwners.get(options.ownerKey) === searchId) {
        pendingRunOwners.delete(options.ownerKey);
      }
    });

  pendingRuns.set(searchId, {
    promise,
    startedAt: new Date().toISOString(),
    controller,
    ownerKey: options.ownerKey,
  });
  if (options.ownerKey) {
    pendingRunOwners.set(options.ownerKey, searchId);
  }

  return true;
}

export async function abortSearchRun(
  searchId: string,
  options: {
    reason?: string;
    awaitCompletion?: boolean;
  } = {},
) {
  const record = getPendingRuns().get(searchId);
  if (!record) {
    return false;
  }

  if (!record.controller.signal.aborted) {
    const abortError = new Error(options.reason ?? "The crawl was aborted.");
    abortError.name = "AbortError";
    record.controller.abort(abortError);
  }

  if (options.awaitCompletion) {
    await record.promise;
  }

  return true;
}

export async function abortOwnerSearchRun(
  ownerKey: string,
  options: {
    reason?: string;
    awaitCompletion?: boolean;
  } = {},
) {
  const searchId = getPendingRunOwners().get(ownerKey);
  if (!searchId) {
    return false;
  }

  return abortSearchRun(searchId, options);
}

function isAbortLikeError(error: unknown) {
  return error instanceof Error && error.name === "AbortError";
}
