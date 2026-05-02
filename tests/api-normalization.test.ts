import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  abortSearchMock,
  getSearchDetailsMock,
  getSearchJobDeltasMock,
  getMongoDbMock,
  isInputValidationErrorMock,
  listRecentSearchesMock,
  listRecentSearchesForApiMock,
  queueSearchRunMock,
  resourceNotFoundErrorClass,
  startSearchFromFiltersMock,
  startSearchRerunMock,
} = vi.hoisted(() => ({
  abortSearchMock: vi.fn(),
  getSearchDetailsMock: vi.fn(),
  getSearchJobDeltasMock: vi.fn(),
  getMongoDbMock: vi.fn(),
  isInputValidationErrorMock: vi.fn(() => false),
  listRecentSearchesMock: vi.fn(),
  listRecentSearchesForApiMock: vi.fn(),
  queueSearchRunMock: vi.fn(),
  resourceNotFoundErrorClass: class ResourceNotFoundError extends Error {},
  startSearchFromFiltersMock: vi.fn(),
  startSearchRerunMock: vi.fn(),
}));

vi.mock("@/lib/server/search/recent-searches", () => ({
  listRecentSearchesForApi: listRecentSearchesForApiMock,
}));

vi.mock("@/lib/server/mongodb", () => ({
  getMongoDb: getMongoDbMock,
}));

vi.mock("@/lib/server/crawler/background-runs", () => ({
  queueSearchRun: queueSearchRunMock,
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

import { DELETE, GET as GET_SEARCH } from "@/app/api/searches/[id]/route";
import { GET as LIST_SEARCHES, POST } from "@/app/api/searches/route";
import { JobCrawlerRepository, type PersistableJob } from "@/lib/server/db/repository";
import { FakeDb } from "@/tests/helpers/fake-db";

let currentDb: FakeDb;

async function seedIndexedJobs(count: number) {
  const repository = new JobCrawlerRepository(currentDb);
  const search = await repository.createSearch(
    {
      title: "Software Engineer",
      country: "United States",
    },
    "2026-04-15T12:00:00.000Z",
  );
  const crawlRun = await repository.createCrawlRun(search._id, "2026-04-15T12:00:00.000Z");

  await repository.persistJobsWithStats(
    crawlRun._id,
    Array.from({ length: count }, (_, index) =>
      createPersistableJob(`db-first-${index}`),
    ),
  );
}

function createPersistableJob(sourceJobId: string): PersistableJob {
  const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;

  return {
    canonicalJobKey: `platform:greenhouse:acme:${sourceJobId}`,
    title: "Software Engineer",
    company: "Acme",
    normalizedCompany: "acme",
    normalizedTitle: "software engineer",
    country: "United States",
    state: "Washington",
    city: "Seattle",
    locationRaw: "Seattle, WA, United States",
    normalizedLocation: "seattle wa united states",
    locationText: "Seattle, WA, United States",
    remoteType: "unknown",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: "acme",
    sourceJobId,
    sourceUrl: canonicalUrl,
    applyUrl: `${canonicalUrl}/apply`,
    resolvedUrl: `${canonicalUrl}/apply`,
    canonicalUrl,
    postingDate: "2026-04-14T00:00:00.000Z",
    postedAt: "2026-04-14T00:00:00.000Z",
    discoveredAt: "2026-04-15T12:00:00.000Z",
    crawledAt: "2026-04-15T12:00:00.000Z",
    sponsorshipHint: "unknown",
    linkStatus: "unknown",
    rawSourceMetadata: {},
    sourceProvenance: [
      {
        sourcePlatform: "greenhouse",
        sourceJobId,
        sourceUrl: canonicalUrl,
        applyUrl: `${canonicalUrl}/apply`,
        resolvedUrl: `${canonicalUrl}/apply`,
        canonicalUrl,
        discoveredAt: "2026-04-15T12:00:00.000Z",
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: [`greenhouse:acme:${sourceJobId}`],
    firstSeenAt: "2026-04-15T12:00:00.000Z",
    lastSeenAt: "2026-04-15T12:00:00.000Z",
    indexedAt: "2026-04-15T12:00:00.000Z",
    isActive: true,
    dedupeFingerprint: sourceJobId,
    companyNormalized: "acme",
    titleNormalized: "software engineer",
    locationNormalized: "seattle wa united states",
    contentFingerprint: sourceJobId,
    contentHash: `content-hash:${sourceJobId}`,
  };
}

describe("search API normalization", () => {
  beforeEach(() => {
    abortSearchMock.mockReset();
    getSearchDetailsMock.mockReset();
    getSearchJobDeltasMock.mockReset();
    isInputValidationErrorMock.mockReturnValue(false);
    listRecentSearchesMock.mockReset();
    listRecentSearchesForApiMock.mockReset();
    getMongoDbMock.mockReset();
    queueSearchRunMock.mockReset();
    startSearchFromFiltersMock.mockReset();
    startSearchRerunMock.mockReset();
    currentDb = new FakeDb();
    getMongoDbMock.mockResolvedValue(currentDb);
    queueSearchRunMock.mockResolvedValue(false);
  });

  it("loads recent searches from the lightweight recent-search service", async () => {
    listRecentSearchesForApiMock.mockResolvedValue([
      {
        _id: "search-1",
        filters: {
          title: "Software Engineer",
          country: "United States",
        },
        createdAt: "2026-04-15T12:00:00.000Z",
        updatedAt: "2026-04-15T12:00:00.000Z",
      },
    ]);

    const response = await LIST_SEARCHES();
    const payload = (await response.json()) as { searches?: unknown[] };

    expect(response.status).toBe(200);
    expect(payload.searches).toHaveLength(1);
    expect(listRecentSearchesForApiMock).toHaveBeenCalledTimes(1);
    expect(listRecentSearchesMock).not.toHaveBeenCalled();
  });

  it("strips null optional filters and returns DB-backed results without executing providers", async () => {
    await seedIndexedJobs(31);

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
    const responsePayload = (await response.json()) as {
      search?: { filters?: unknown };
      jobs?: unknown[];
      returnedCount?: number;
      queuedBackgroundRefresh?: boolean;
      providerCrawlMs?: number;
      timing?: { providerCrawlMs?: number };
    };

    expect(responsePayload.search?.filters).toEqual({
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    expect(responsePayload.jobs).toHaveLength(31);
    expect(responsePayload.returnedCount).toBe(31);
    expect(responsePayload.providerCrawlMs).toBe(0);
    expect(responsePayload.timing?.providerCrawlMs).toBe(0);
    expect(responsePayload.queuedBackgroundRefresh).toBe(false);
    expect(queueSearchRunMock).not.toHaveBeenCalled();
    expect(startSearchFromFiltersMock).not.toHaveBeenCalled();
  });

  it("enqueues low-coverage background refresh without blocking the search response", async () => {
    await seedIndexedJobs(1);
    queueSearchRunMock.mockResolvedValue(true);

    const request = new Request("http://localhost/api/searches", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-job-crawler-client-id": "client-1",
      },
      body: JSON.stringify({
        title: "Software Engineer",
        country: "United States",
      }),
    });

    const response = await POST(request);
    const responsePayload = (await response.json()) as {
      jobs?: unknown[];
      queued?: boolean;
      queuedBackgroundRefresh?: boolean;
      dbSearchMs?: number;
      providerCrawlMs?: number;
      totalSearchMs?: number;
      returnedCount?: number;
    };

    expect(response.status).toBe(201);
    expect(responsePayload.jobs).toHaveLength(1);
    expect(responsePayload.returnedCount).toBe(1);
    expect(responsePayload.queued).toBe(true);
    expect(responsePayload.queuedBackgroundRefresh).toBe(true);
    expect(typeof responsePayload.dbSearchMs).toBe("number");
    expect(responsePayload.providerCrawlMs).toBe(0);
    expect(typeof responsePayload.totalSearchMs).toBe("number");
    expect(queueSearchRunMock).toHaveBeenCalledTimes(1);
    expect(queueSearchRunMock.mock.calls[0]?.[3]).toMatchObject({
      ownerKey: "client-1",
      deferStart: true,
    });
    expect(startSearchFromFiltersMock).not.toHaveBeenCalled();
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

    const response = await GET_SEARCH(
      new Request("http://localhost/api/searches/search-1?mode=delta&after=3&indexedAfter=9"),
      {
        params: Promise.resolve({ id: "search-1" }),
      },
    );

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

    const response = await GET_SEARCH(
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
