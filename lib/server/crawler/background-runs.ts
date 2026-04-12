import "server-only";

type PendingRunRecord = {
  promise: Promise<void>;
  startedAt: string;
};

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerPendingRuns: Map<string, PendingRunRecord> | undefined;
}

function getPendingRuns() {
  if (!globalThis.__jobCrawlerPendingRuns) {
    globalThis.__jobCrawlerPendingRuns = new Map<string, PendingRunRecord>();
  }

  return globalThis.__jobCrawlerPendingRuns;
}

export function isSearchRunPending(searchId: string) {
  return getPendingRuns().has(searchId);
}

export function queueSearchRun(searchId: string, task: () => Promise<void>) {
  const pendingRuns = getPendingRuns();
  if (pendingRuns.has(searchId)) {
    return false;
  }

  const promise = task()
    .catch((error) => {
      console.error("[crawl:background-run]", {
        searchId,
        message: error instanceof Error ? error.message : "Background crawl failed unexpectedly.",
      });
    })
    .finally(() => {
      pendingRuns.delete(searchId);
    });

  pendingRuns.set(searchId, {
    promise,
    startedAt: new Date().toISOString(),
  });

  return true;
}
