import { describe, expect, it, vi } from "vitest";

import { runSearchFromFilters } from "@/lib/server/crawler/service";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";

import { FakeDb } from "@/tests/helpers/fake-db";

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

describe("pipeline title retrieval", () => {
  it("keeps semantically related unknown-title matches instead of requiring exact titles", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T12:00:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 3,
      matchedCount: 3,
      warningCount: 0,
      jobs: [
        {
          title: "Integration Developer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "integration-developer",
          sourceUrl: "https://example.com/jobs/integration-developer",
          applyUrl: "https://example.com/jobs/integration-developer/apply",
          canonicalUrl: "https://example.com/jobs/integration-developer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Senior Integration Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "integration-engineer",
          sourceUrl: "https://example.com/jobs/integration-engineer",
          applyUrl: "https://example.com/jobs/integration-engineer/apply",
          canonicalUrl: "https://example.com/jobs/integration-engineer",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Solutions Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "solutions-engineer",
          sourceUrl: "https://example.com/jobs/solutions-engineer",
          applyUrl: "https://example.com/jobs/solutions-engineer/apply",
          canonicalUrl: "https://example.com/jobs/solutions-engineer",
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

    const result = await runSearchFromFilters(
      {
        title: "Integration Engineer",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Senior Integration Engineer",
      "Integration Developer",
    ]);
    expect(result.jobs[0]?.rawSourceMetadata).toMatchObject({
      crawlTitleMatch: {
        tier: "canonical_variant",
      },
    });
    expect(result.jobs[1]?.rawSourceMetadata).toMatchObject({
      crawlTitleMatch: {
        tier: "generic_token_overlap",
      },
    });
    expect(result.diagnostics.excludedByTitle).toBe(1);
  });

  it("keeps adjacent concept matches in the pipeline while rejecting cross-family manager roles", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const now = new Date("2026-04-10T12:30:00.000Z");

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 3,
      matchedCount: 3,
      warningCount: 0,
      jobs: [
        {
          title: "Technical Product Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "technical-product-manager",
          sourceUrl: "https://example.com/jobs/technical-product-manager",
          applyUrl: "https://example.com/jobs/technical-product-manager/apply",
          canonicalUrl: "https://example.com/jobs/technical-product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Growth Product Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "growth-product-manager",
          sourceUrl: "https://example.com/jobs/growth-product-manager",
          applyUrl: "https://example.com/jobs/growth-product-manager/apply",
          canonicalUrl: "https://example.com/jobs/growth-product-manager",
          discoveredAt: now.toISOString(),
          rawSourceMetadata: {},
        },
        {
          title: "Technical Program Manager",
          company: "Acme",
          country: "United States",
          locationText: "Remote, United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "technical-program-manager",
          sourceUrl: "https://example.com/jobs/technical-program-manager",
          applyUrl: "https://example.com/jobs/technical-program-manager/apply",
          canonicalUrl: "https://example.com/jobs/technical-program-manager",
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

    const result = await runSearchFromFilters(
      {
        title: "Product Manager",
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

    expect(result.jobs.map((job) => job.title)).toEqual([
      "Growth Product Manager",
      "Technical Product Manager",
    ]);
    expect(result.diagnostics.excludedByTitle).toBe(1);
  });
});
