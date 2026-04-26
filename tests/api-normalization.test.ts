import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  abortSearchMock,
  getSearchDetailsMock,
  getSearchJobDeltasMock,
  isInputValidationErrorMock,
  listRecentSearchesMock,
  resourceNotFoundErrorClass,
  startSearchFromFiltersMock,
  startSearchRerunMock,
} = vi.hoisted(() => ({
  abortSearchMock: vi.fn(),
  getSearchDetailsMock: vi.fn(),
  getSearchJobDeltasMock: vi.fn(),
  isInputValidationErrorMock: vi.fn(() => false),
  listRecentSearchesMock: vi.fn(),
  resourceNotFoundErrorClass: class ResourceNotFoundError extends Error {},
  startSearchFromFiltersMock: vi.fn(),
  startSearchRerunMock: vi.fn(),
}));

vi.mock("@/lib/server/search/service", () => ({
  isInputValidationError: isInputValidationErrorMock,
  listRecentSearches: listRecentSearchesMock,
  startSearchFromFilters: startSearchFromFiltersMock,
  startSearchRerun: startSearchRerunMock,
}));

vi.mock("@/lib/server/search/session-service", () => ({
  abortSearch: abortSearchMock,
  getSearchDetails: getSearchDetailsMock,
  getSearchJobDeltas: getSearchJobDeltasMock,
  normalizeSearchPaginationOptions: (options: {
    cursor?: number;
    pageSize?: number;
    searchSessionId?: string;
  }) => ({
    cursor: Number.isFinite(options.cursor) && (options.cursor ?? 0) > 0 ? options.cursor : 0,
    pageSize:
      Number.isFinite(options.pageSize) && (options.pageSize ?? 0) > 0
        ? Math.min(options.pageSize ?? 50, 100)
        : 50,
    searchSessionId: options.searchSessionId,
  }),
}));

vi.mock("@/lib/server/search/errors", () => ({
  ResourceNotFoundError: resourceNotFoundErrorClass,
}));

import { DELETE, GET } from "@/app/api/searches/[id]/route";
import { POST } from "@/app/api/searches/route";

describe("search API normalization", () => {
  beforeEach(() => {
    abortSearchMock.mockReset();
    getSearchDetailsMock.mockReset();
    getSearchJobDeltasMock.mockReset();
    isInputValidationErrorMock.mockReturnValue(false);
    listRecentSearchesMock.mockReset();
    startSearchFromFiltersMock.mockReset();
    startSearchRerunMock.mockReset();
  });

  it("strips null optional filters and legacy experienceClassification before starting the crawl", async () => {
    startSearchFromFiltersMock.mockResolvedValue({
      queued: true,
      result: {
        search: {
          _id: "search-1",
        },
      },
    });

    const request = new Request("http://localhost/api/searches", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-job-crawler-client-id": "client-1",
      },
      body: JSON.stringify({
        title: "Software Engineer",
        country: "United States",
        state: null,
        city: null,
        experienceClassification: null,
        platforms: ["greenhouse"],
      }),
    });

    const response = await POST(request);

    expect(response.status).toBe(201);
    const callArgs = startSearchFromFiltersMock.mock.calls[0];
    expect(callArgs[0]).toEqual({
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    expect(callArgs[1]).toMatchObject({
      requestOwnerKey: "client-1",
    });
    expect(callArgs[1].signal).toBeDefined();
  });

  it("stops a running search through the delete route", async () => {
    abortSearchMock.mockResolvedValue({
      aborted: true,
      result: {
        search: {
          _id: "search-1",
        },
        crawlRun: {
          status: "aborted",
        },
      },
    });

    const response = await DELETE(new Request("http://localhost/api/searches/search-1", {
      method: "DELETE",
    }), {
      params: Promise.resolve({ id: "search-1" }),
    });
    const payload = (await response.json()) as { aborted?: boolean };

    expect(response.status).toBe(200);
    expect(payload.aborted).toBe(true);
    expect(abortSearchMock).toHaveBeenCalledWith("search-1");
  });

  it("routes delta polling requests to incremental search delivery", async () => {
    getSearchJobDeltasMock.mockResolvedValue({
      search: { _id: "search-1" },
      crawlRun: { status: "running" },
      jobs: [],
      sourceResults: [],
      diagnostics: {},
      delivery: {
        mode: "delta",
        previousCursor: 3,
        cursor: 4,
        previousIndexedCursor: 9,
        indexedCursor: 10,
      },
    });

    const response = await GET(new Request("http://localhost/api/searches/search-1?mode=delta&after=3&indexedAfter=9"), {
      params: Promise.resolve({ id: "search-1" }),
    });

    expect(response.status).toBe(200);
    expect(getSearchJobDeltasMock).toHaveBeenCalledWith("search-1", 3, {
      afterIndexedCursor: 9,
    });
    expect(getSearchDetailsMock).not.toHaveBeenCalled();
  });

  it("passes cursor pagination options to search detail requests", async () => {
    getSearchDetailsMock.mockResolvedValue({
      searchId: "search-1",
      searchSessionId: "session-1",
      totalMatchedCount: 300,
      returnedCount: 50,
      pageSize: 50,
      nextCursor: 100,
      hasMore: true,
      search: { _id: "search-1" },
      searchSession: { _id: "session-1" },
      crawlRun: { status: "completed" },
      jobs: [],
      sourceResults: [],
      diagnostics: {},
    });

    const response = await GET(
      new Request(
        "http://localhost/api/searches/search-1?cursor=50&pageSize=50&searchSessionId=session-1",
      ),
      {
        params: Promise.resolve({ id: "search-1" }),
      },
    );
    const payload = (await response.json()) as {
      totalMatchedCount?: number;
      returnedCount?: number;
      nextCursor?: number;
      hasMore?: boolean;
    };

    expect(response.status).toBe(200);
    expect(getSearchDetailsMock).toHaveBeenCalledWith("search-1", {
      cursor: 50,
      pageSize: 50,
      searchSessionId: "session-1",
    });
    expect(payload).toMatchObject({
      totalMatchedCount: 300,
      returnedCount: 50,
      nextCursor: 100,
      hasMore: true,
    });
  });
});
