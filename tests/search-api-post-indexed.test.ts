import { beforeEach, describe, expect, it, vi } from "vitest";

const { getMongoDbMock } = vi.hoisted(() => ({
  getMongoDbMock: vi.fn(),
}));

vi.mock("@/lib/server/mongodb", () => ({
  getMongoDb: getMongoDbMock,
}));

import { POST } from "@/app/api/searches/route";
import { JobCrawlerRepository, type PersistableJob } from "@/lib/server/db/repository";
import { FakeDb } from "@/tests/helpers/fake-db";

let currentDb: FakeDb;

describe("search POST indexed retrieval", () => {
  beforeEach(() => {
    currentDb = new FakeDb();
    getMongoDbMock.mockReset();
    getMongoDbMock.mockResolvedValue(currentDb);
  });

  it("returns semantically filtered indexed matches instead of raw coarse candidates", async () => {
    const repository = new JobCrawlerRepository(currentDb);
    const search = await repository.createSearch(
      {
        title: "Product Manager",
        country: "United States",
      },
      "2026-04-15T12:00:00.000Z",
    );
    const crawlRun = await repository.createCrawlRun(
      search._id,
      "2026-04-15T12:00:00.000Z",
    );
    await repository.persistJobsWithStats(crawlRun._id, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "product-manager",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        sourceJobId: "senior-product-manager",
      }),
      createPersistableJob({
        title: "Technical Program Manager",
        sourceJobId: "technical-program-manager",
      }),
    ]);

    const response = await POST(
      new Request("http://localhost/api/searches", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-job-crawler-client-id": "client-1",
        },
        body: JSON.stringify({
          title: "Product Manager",
          country: "United States",
        }),
      }),
    );
    const payload = (await response.json()) as {
      candidateCount?: number;
      totalMatchedCount?: number;
      returnedCount?: number;
      queued?: boolean;
      providerCrawlMs?: number;
      jobs?: Array<{ title?: string; rawSourceMetadata?: Record<string, unknown> }>;
    };

    expect(response.status).toBe(201);
    expect(payload.candidateCount).toBeGreaterThanOrEqual(payload.totalMatchedCount ?? 0);
    expect(payload.totalMatchedCount).toBe(2);
    expect(payload.returnedCount).toBe(2);
    expect(payload.queued).toBe(false);
    expect(payload.providerCrawlMs).toBe(0);
    expect(payload.jobs?.map((job) => job.title)).toEqual([
      "Product Manager",
      "Senior Product Manager",
    ]);
    expect(payload.jobs?.map((job) => job.title)).not.toContain(
      "Technical Program Manager",
    );
    expect(payload.jobs?.every((job) => job.rawSourceMetadata?.indexedSearch)).toBe(true);
  });
});

function createPersistableJob(input: {
  title: string;
  sourceJobId: string;
}): PersistableJob {
  const canonicalUrl = `https://example.com/jobs/${input.sourceJobId}`;
  const normalizedTitle = input.title.toLowerCase();

  return {
    canonicalJobKey: `platform:greenhouse:acme:${input.sourceJobId}`,
    title: input.title,
    company: "Acme",
    normalizedCompany: "acme",
    normalizedTitle,
    country: "United States",
    state: undefined,
    city: undefined,
    locationRaw: "Remote - United States",
    normalizedLocation: "remote united states",
    locationText: "Remote - United States",
    remoteType: "remote",
    sourcePlatform: "greenhouse",
    sourceCompanySlug: "acme",
    sourceJobId: input.sourceJobId,
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
        sourceJobId: input.sourceJobId,
        sourceUrl: canonicalUrl,
        applyUrl: `${canonicalUrl}/apply`,
        resolvedUrl: `${canonicalUrl}/apply`,
        canonicalUrl,
        discoveredAt: "2026-04-15T12:00:00.000Z",
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: [`greenhouse:acme:${input.sourceJobId}`],
    firstSeenAt: "2026-04-15T12:00:00.000Z",
    lastSeenAt: "2026-04-15T12:00:00.000Z",
    indexedAt: "2026-04-15T12:00:00.000Z",
    isActive: true,
    dedupeFingerprint: input.sourceJobId,
    companyNormalized: "acme",
    titleNormalized: normalizedTitle,
    locationNormalized: "remote united states",
    contentFingerprint: input.sourceJobId,
    contentHash: `content-hash:${input.sourceJobId}`,
  };
}
