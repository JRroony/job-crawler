import "server-only";

import { setMaxListeners } from "node:events";

export class ProviderSourceTimeoutError extends Error {
  constructor(input: { provider: string; sourceId: string; timeoutMs: number }) {
    super(
      `Provider ${input.provider} source ${input.sourceId} exceeded the ${input.timeoutMs}ms source crawl budget.`,
    );
    this.name = "ProviderSourceTimeoutError";
  }
}

export async function runProviderSourceWithTimeout<T>(input: {
  provider: string;
  sourceId: string;
  timeoutMs?: number;
  parentSignal?: AbortSignal;
  task: (sourceSignal?: AbortSignal) => Promise<T>;
}): Promise<T> {
  const timeoutMs = Math.max(0, Math.floor(input.timeoutMs ?? 0));
  if (timeoutMs <= 0) {
    return input.task(input.parentSignal);
  }

  const controller = new AbortController();
  const timeoutError = new ProviderSourceTimeoutError({
    provider: input.provider,
    sourceId: input.sourceId,
    timeoutMs,
  });
  let didTimeout = false;
  const cleanup = linkAbortSignals(controller, [input.parentSignal]);
  const timeoutHandle = setTimeout(() => {
    didTimeout = true;
    controller.abort(timeoutError);
  }, timeoutMs);
  timeoutHandle.unref?.();

  try {
    return await Promise.race([
      input.task(controller.signal),
      new Promise<never>((_, reject) => {
        controller.signal.addEventListener(
          "abort",
          () => {
            reject(controller.signal.reason ?? timeoutError);
          },
          { once: true },
        );
      }),
    ]);
  } catch (error) {
    if (didTimeout || error === timeoutError) {
      throw timeoutError;
    }

    throw error;
  } finally {
    clearTimeout(timeoutHandle);
    cleanup();
  }
}

export function createSignalAwareFetch(
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): typeof fetch {
  if (!signal) {
    return fetchImpl;
  }

  return async (input, init) => {
    if (signal.aborted) {
      throw signal.reason instanceof Error ? signal.reason : new Error("Provider source was aborted.");
    }

    const requestSignal = init?.signal ? mergeSignals(signal, init.signal) : signal;
    return fetchImpl(input, {
      ...init,
      signal: requestSignal,
    });
  };
}

export function isProviderSourceTimeoutError(
  error: unknown,
): error is ProviderSourceTimeoutError {
  return error instanceof Error && error.name === "ProviderSourceTimeoutError";
}

function mergeSignals(...signals: AbortSignal[]) {
  const controller = new AbortController();
  const cleanup = linkAbortSignals(controller, signals);

  if (!controller.signal.aborted) {
    controller.signal.addEventListener("abort", cleanup, { once: true });
  } else {
    cleanup();
  }

  return controller.signal;
}

function linkAbortSignals(
  controller: AbortController,
  signals: Array<AbortSignal | undefined>,
) {
  const listeners: Array<{
    signal: AbortSignal;
    onAbort: () => void;
  }> = [];

  for (const signal of signals) {
    if (!signal) {
      continue;
    }

    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }

    allowManyAbortListeners(signal);
    const onAbort = () => {
      controller.abort(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    listeners.push({ signal, onAbort });
  }

  return () => {
    listeners.forEach(({ signal, onAbort }) => {
      signal.removeEventListener("abort", onAbort);
    });
  };
}

function allowManyAbortListeners(signal: AbortSignal) {
  try {
    setMaxListeners(0, signal);
  } catch {
    // Older runtimes may not support EventTarget max listener controls.
  }
}
