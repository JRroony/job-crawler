import { describe, expect, it, vi } from "vitest";

import { validateJobLink } from "@/lib/server/crawler/link-validation";

describe("validateJobLink", () => {
  it("uses HEAD before GET and marks successful pages valid", async () => {
    const calls: string[] = [];
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      calls.push(init?.method ?? "GET");

      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-type": "text/html",
          },
        });
      }

      return new Response("<html><body>Open role</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await validateJobLink(
      "https://example.com/jobs/1",
      fetchImpl,
      new Date("2026-03-29T00:00:00.000Z"),
    );

    expect(calls).toEqual(["HEAD", "GET"]);
    expect(result.status).toBe("valid");
  });

  it("marks clear stale pages as stale", async () => {
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 200,
        });
      }

      return new Response("<html><body>This job is no longer available.</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await validateJobLink(
      "https://example.com/jobs/2",
      fetchImpl,
      new Date("2026-03-29T00:00:00.000Z"),
    );

    expect(result.status).toBe("stale");
    expect(result.staleMarkers).toContain("this job is no longer available");
  });

  it("rejects malformed URLs", async () => {
    const result = await validateJobLink("not-a-url", fetch, new Date("2026-03-29T00:00:00.000Z"));
    expect(result.status).toBe("invalid");
  });

  it("falls back to GET when HEAD is not supported", async () => {
    const calls: string[] = [];
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      calls.push(init?.method ?? "GET");

      if (init?.method === "HEAD") {
        return new Response(null, {
          status: 405,
        });
      }

      return new Response("<html><body>Open role</body></html>", {
        status: 200,
        headers: {
          "content-type": "text/html",
        },
      });
    }) as unknown as typeof fetch;

    const result = await validateJobLink(
      "https://example.com/jobs/3",
      fetchImpl,
      new Date("2026-03-29T00:00:00.000Z"),
    );

    expect(calls).toEqual(["HEAD", "GET"]);
    expect(result.status).toBe("valid");
  });
});
