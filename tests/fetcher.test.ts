import { afterEach, describe, expect, it, vi } from "vitest";

import {
  safeFetch,
  safeFetchJson,
} from "@/lib/server/net/fetcher";

afterEach(() => {
  vi.useRealTimers();
});

describe("safeFetch", () => {
  it("retries retryable HTTP failures and returns parsed JSON on success", async () => {
    let attempts = 0;
    const fetchImpl = vi.fn(async () => {
      attempts += 1;

      if (attempts < 3) {
        return new Response("Busy", {
          status: 503,
          headers: {
            "content-type": "text/plain",
          },
        });
      }

      return new Response(JSON.stringify({ jobs: [1, 2, 3] }), {
        status: 200,
        headers: {
          "content-type": "application/json",
        },
      });
    }) as unknown as typeof fetch;

    const result = await safeFetchJson<{ jobs: number[] }>(
      "https://example.com/jobs",
      {
        fetchImpl,
        retries: 2,
        retryBaseDelayMs: 0,
        retryMaxDelayMs: 0,
      },
    );

    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(result).toMatchObject({
      ok: true,
      data: {
        jobs: [1, 2, 3],
      },
    });
  });

  it("does not retry non-retryable HTTP failures", async () => {
    const fetchImpl = vi.fn(async () => {
      return new Response("Missing", {
        status: 404,
        headers: {
          "content-type": "text/plain",
        },
      });
    }) as unknown as typeof fetch;

    const result = await safeFetch("https://example.com/missing", {
      fetchImpl,
      retries: 2,
      retryBaseDelayMs: 0,
      retryMaxDelayMs: 0,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      ok: false,
      errorType: "http",
      statusCode: 404,
    });
  });

  it("classifies invalid JSON payloads as parse errors", async () => {
    const fetchImpl = vi.fn(async () => {
      return new Response("<html>not json</html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await safeFetchJson("https://example.com/html", {
      fetchImpl,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      ok: false,
      errorType: "parse",
    });
  });

  it("times out and retries slow requests", async () => {
    vi.useFakeTimers();

    const fetchImpl = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            const error = new Error("The operation was aborted.");
            error.name = "AbortError";
            reject(error);
          },
          { once: true },
        );
      });
    }) as unknown as typeof fetch;

    const resultPromise = safeFetch("https://example.com/slow", {
      fetchImpl,
      timeoutMs: 50,
      retries: 1,
      retryBaseDelayMs: 0,
      retryMaxDelayMs: 0,
    });

    await vi.advanceTimersByTimeAsync(200);

    const result = await resultPromise;

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(result).toMatchObject({
      ok: false,
      errorType: "timeout",
    });
  });
});
