import { describe, expect, it, vi } from "vitest";

import { runSearchIngestionFromFilters } from "@/lib/server/crawler/service";
import { collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { buildJobSearchIndex } from "@/lib/server/search/job-search-index";
import { jobSearchIndexSchema } from "@/lib/types";
import { FakeDb } from "@/tests/helpers/fake-db";

vi.mock("@/lib/server/search/job-search-index", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@/lib/server/search/job-search-index")>();

  return {
    ...actual,
    buildJobSearchIndex: vi.fn(actual.buildJobSearchIndex),
  };
});

function createStubProvider(
  provider: CrawlProvider["provider"],
  crawlSources: CrawlProvider["crawlSources"],
): CrawlProvider {
  return {
    provider,
    supportsSource(source: DiscoveredSource): source is DiscoveredSource {
      return source.platform === provider;
    },
    crawlSources,
  };
}

describe("seed hydration safeguards", () => {
  it("does not call buildJobSearchIndex for empty-title seeds", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-03-29T12:00:00.000Z");
    const buildJobSearchIndexMock = vi.mocked(buildJobSearchIndex);
    buildJobSearchIndexMock.mockClear();

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 0,
      jobs: [
        {
          title: "",
          normalizedTitle: "",
          titleNormalized: "",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "empty-title",
          sourceUrl: "https://example.com/jobs/empty-title",
          applyUrl: "https://example.com/jobs/empty-title/apply",
          canonicalUrl: "https://example.com/jobs/empty-title",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "valid-title",
          sourceUrl: "https://example.com/jobs/valid-title",
          applyUrl: "https://example.com/jobs/valid-title/apply",
          canonicalUrl: "https://example.com/jobs/valid-title",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
      ],
    }));
    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/acme",
            token: "acme",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery,
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now,
      },
    );

    const indexedTitles = buildJobSearchIndexMock.mock.calls.map(([job]) => job.title);

    expect(result.jobs).toHaveLength(1);
    expect(db.snapshot(collectionNames.jobs)).toHaveLength(1);
    expect(indexedTitles).toContain("Software Engineer");
    expect(indexedTitles).not.toContain("");
    expect(result.diagnostics.dropReasonCounts).toMatchObject({
      seed_invalid_empty_title: 1,
    });
  });

  it("keeps searchIndex title fields required by schema", () => {
    const parsed = jobSearchIndexSchema.safeParse({
      titleNormalized: "",
      titleStrippedNormalized: "",
    });

    expect(parsed.success).toBe(false);
    if (!parsed.success) {
      expect(parsed.error.issues.map((issue) => issue.path.join("."))).toEqual([
        "titleNormalized",
        "titleStrippedNormalized",
      ]);
    }
  });
});
