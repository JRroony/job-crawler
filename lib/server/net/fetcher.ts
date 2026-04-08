import "server-only";

export type FetchErrorType =
  | "timeout"
  | "network"
  | "http"
  | "parse"
  | "rate_limit"
  | "unknown";

type SafeFetchErrorResult = {
  ok: false;
  errorType: FetchErrorType;
  statusCode?: number;
  message: string;
  response?: Response;
};

export type SafeFetchResult<T = unknown> =
  | { ok: true; response: Response; data?: T }
  | SafeFetchErrorResult;

export type SafeFetchOptions = Omit<RequestInit, "signal"> & {
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
  timeoutMs?: number;
  retries?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
};

export const defaultFetchTimeoutMs = 10_000;
export const defaultFetchRetries = 2;

const defaultRetryBaseDelayMs = 250;
const defaultRetryMaxDelayMs = 2_000;
const retryableNetworkCodes = new Set([
  "ECONNABORTED",
  "ECONNREFUSED",
  "ECONNRESET",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETDOWN",
  "ENETUNREACH",
  "ENOTFOUND",
  "EPIPE",
  "ETIMEDOUT",
  "UND_ERR_BODY_TIMEOUT",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_SOCKET",
]);

const timeoutMessagePattern = /\b(timeout|timed out|time-out)\b/i;
const retryableNetworkMessagePattern =
  /\b(fetch failed|network|socket|connection|connect|reset|refused|econn|eai_again|enotfound|unreach)\b/i;

export async function safeFetch(
  input: RequestInfo | URL,
  options: SafeFetchOptions = {},
): Promise<SafeFetchResult> {
  const {
    fetchImpl = fetch,
    signal,
    timeoutMs = defaultFetchTimeoutMs,
    retries = defaultFetchRetries,
    retryBaseDelayMs = defaultRetryBaseDelayMs,
    retryMaxDelayMs = defaultRetryMaxDelayMs,
    ...init
  } = options;

  const maxAttempts = Math.max(1, retries + 1);

  for (let attemptIndex = 0; attemptIndex < maxAttempts; attemptIndex += 1) {
    const attempt = createAttemptContext(signal, timeoutMs);
    let failure: SafeFetchErrorResult | undefined;

    try {
      const response = await fetchImpl(input, {
        ...init,
        signal: attempt.signal,
      });

      if (response.ok || (response.status >= 200 && response.status < 300)) {
        return {
          ok: true,
          response,
        };
      }

      failure = {
        ok: false,
        errorType: response.status === 429 ? "rate_limit" : "http",
        statusCode: response.status,
        message:
          response.status === 429
            ? "Request was rate limited (HTTP 429)."
            : `Request failed with HTTP ${response.status}.`,
        response,
      };

      if (shouldRetryFailure(failure)) {
        await discardResponseBody(response);
      }
    } catch (error) {
      failure = normalizeThrownError(error, attempt.didTimeout(), timeoutMs);
    } finally {
      attempt.cleanup();
    }

    if (!failure) {
      return {
        ok: false,
        errorType: "unknown",
        message: "Request failed unexpectedly.",
      };
    }

    if (attemptIndex === maxAttempts - 1 || !shouldRetryFailure(failure)) {
      return failure;
    }

    const delayMs = computeBackoffDelay(
      attemptIndex,
      retryBaseDelayMs,
      retryMaxDelayMs,
    );
    const didWait = await waitForRetry(delayMs, signal);

    if (!didWait) {
      return {
        ok: false,
        errorType: "unknown",
        message: "Request was aborted before it could be retried.",
      };
    }
  }

  return {
    ok: false,
    errorType: "unknown",
    message: "Request failed unexpectedly.",
  };
}

export async function safeFetchText(
  input: RequestInfo | URL,
  options: SafeFetchOptions = {},
): Promise<SafeFetchResult<string>> {
  const result = await safeFetch(input, options);

  if (!result.ok) {
    return result;
  }

  try {
    return {
      ok: true,
      response: result.response,
      data: await result.response.text(),
    };
  } catch {
    return {
      ok: false,
      errorType: "parse",
      statusCode: result.response.status,
      message: "Response body could not be read as text.",
      response: result.response,
    };
  }
}

export async function safeFetchJson<T>(
  input: RequestInfo | URL,
  options: SafeFetchOptions = {},
): Promise<SafeFetchResult<T>> {
  const result = await safeFetch(input, options);

  if (!result.ok) {
    return result;
  }

  try {
    return {
      ok: true,
      response: result.response,
      data: (await result.response.json()) as T,
    };
  } catch {
    return {
      ok: false,
      errorType: "parse",
      statusCode: result.response.status,
      message: "Response body could not be parsed as JSON.",
      response: result.response,
    };
  }
}

function shouldRetryFailure(result: SafeFetchErrorResult) {
  return (
    result.errorType === "timeout" ||
    result.errorType === "network" ||
    result.errorType === "rate_limit" ||
    (result.errorType === "http" && (result.statusCode ?? 0) >= 500)
  );
}

function normalizeThrownError(
  error: unknown,
  didTimeout: boolean,
  timeoutMs: number,
): SafeFetchErrorResult {
  if (didTimeout || looksLikeTimeoutError(error)) {
    return {
      ok: false,
      errorType: "timeout",
      message:
        timeoutMs > 0
          ? `Request timed out after ${timeoutMs}ms.`
          : "Request timed out.",
    };
  }

  if (looksLikeNetworkError(error)) {
    return {
      ok: false,
      errorType: "network",
      message: `Network error: ${extractErrorMessage(error)}`,
    };
  }

  return {
    ok: false,
    errorType: "unknown",
    message: extractErrorMessage(error),
  };
}

function looksLikeTimeoutError(error: unknown) {
  const message = extractErrorMessage(error);
  return timeoutMessagePattern.test(message);
}

function looksLikeNetworkError(error: unknown) {
  const code = extractErrorCode(error);
  if (code && retryableNetworkCodes.has(code)) {
    return true;
  }

  const message = extractErrorMessage(error);
  return retryableNetworkMessagePattern.test(message);
}

function extractErrorCode(error: unknown) {
  if (!error || typeof error !== "object") {
    return undefined;
  }

  const directCode = "code" in error ? error.code : undefined;
  if (typeof directCode === "string") {
    return directCode;
  }

  const cause = "cause" in error ? error.cause : undefined;
  if (cause && typeof cause === "object" && "code" in cause && typeof cause.code === "string") {
    return cause.code;
  }

  return undefined;
}

function extractErrorMessage(error: unknown) {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  return "Request failed unexpectedly.";
}

function computeBackoffDelay(
  attemptIndex: number,
  retryBaseDelayMs: number,
  retryMaxDelayMs: number,
) {
  const baseDelay = retryBaseDelayMs * 2 ** attemptIndex;
  const cappedDelay =
    retryMaxDelayMs > 0 ? Math.min(baseDelay, retryMaxDelayMs) : baseDelay;
  const jitterWindow = cappedDelay * 0.2;
  const jitterOffset = (Math.random() * 2 - 1) * jitterWindow;

  return Math.max(0, Math.round(cappedDelay + jitterOffset));
}

function waitForRetry(delayMs: number, signal?: AbortSignal) {
  if (delayMs <= 0) {
    return Promise.resolve(!signal?.aborted);
  }

  if (signal?.aborted) {
    return Promise.resolve(false);
  }

  return new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      cleanup();
      resolve(true);
    }, delayMs);

    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      resolve(false);
    };

    const cleanup = () => {
      signal?.removeEventListener("abort", onAbort);
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function createAttemptContext(signal: AbortSignal | undefined, timeoutMs: number) {
  const controller = new AbortController();
  let didTimeout = false;

  const onAbort = () => {
    controller.abort();
  };

  if (signal) {
    if (signal.aborted) {
      controller.abort();
    } else {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  }

  const timer =
    timeoutMs > 0
      ? setTimeout(() => {
          didTimeout = true;
          controller.abort();
        }, timeoutMs)
      : undefined;

  return {
    signal: controller.signal,
    didTimeout: () => didTimeout,
    cleanup: () => {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      signal?.removeEventListener("abort", onAbort);
    },
  };
}

async function discardResponseBody(response: Response) {
  try {
    await response.body?.cancel();
  } catch {
    // Ignore cleanup failures between retry attempts.
  }
}
