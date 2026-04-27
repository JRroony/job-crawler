import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { resolveViewState } from "@/components/job-crawler-app";
import { ResultsTable } from "@/components/results-table";
import { runSearchIngestionFromFilters } from "@/lib/server/crawler/service";
import { collectionNames } from "@/lib/server/db/indexes";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import { crawlResponseSchema } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

describe("Greenhouse end-to-end regression", () => {
  it("keeps a valid Greenhouse crawl non-empty through legacy merge, API shape validation, and UI rendering", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);
    const now = new Date("2026-04-08T12:00:00.000Z");

    await db.collection(collectionNames.jobs).insertOne({
      _id: "legacy-greenhouse-job",
      title: "Software Engineer",
      company: "OpenAI",
      country: "United States",
      state: null,
      city: null,
      locationText: "San Francisco, CA",
      experienceLevel: null,
      experienceClassification: null,
      sourcePlatform: "greenhouse",
      sourceJobId: "software-engineer-1",
      sourceUrl: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
      applyUrl: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
      resolvedUrl: null,
      canonicalUrl: null,
      postedAt: null,
      discoveredAt: "2026-03-20T00:00:00.000Z",
      lastValidatedAt: null,
      linkStatus: "unknown",
      rawSourceMetadata: null,
      sourceProvenance: [
        {
          sourcePlatform: "greenhouse",
          sourceJobId: "software-engineer-1",
          sourceUrl: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
          applyUrl: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
          resolvedUrl: null,
          canonicalUrl: null,
          discoveredAt: "2026-03-20T00:00:00.000Z",
          rawSourceMetadata: null,
        },
      ],
      sourceLookupKeys: ["greenhouse:software engineer 1"],
      crawlRunIds: ["run-legacy"],
      companyNormalized: "openai",
      titleNormalized: "software engineer",
      locationNormalized: "san francisco ca united states",
      contentFingerprint: "legacy-greenhouse-fingerprint",
    });

    const discovery: DiscoveryService = {
      async discover() {
        return [
          classifySourceCandidate({
            url: "https://boards.greenhouse.io/openai",
            token: "openai",
            confidence: "high",
            discoveryMethod: "configured_env",
          }),
        ];
      },
    };

    const fetchImpl = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          jobs: [
            {
              id: "software-engineer-1",
              title: "Software Engineer",
              absolute_url: "https://boards.greenhouse.io/openai/jobs/software-engineer-1",
              first_published: "2026-04-01T00:00:00.000Z",
              company_name: "OpenAI",
              location: {
                name: "San Francisco, CA",
              },
            },
          ],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }) as unknown as typeof fetch;

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [createGreenhouseProvider()],
        discovery,
        fetchImpl,
        now,
      },
    );

    const apiResponse = crawlResponseSchema.parse(result);
    const html = renderToStaticMarkup(
      <ResultsTable
        jobs={apiResponse.jobs}
        onRevalidate={vi.fn(async () => undefined)}
        revalidatingIds={[]}
      />,
    );

    expect(apiResponse.jobs).toHaveLength(1);
    expect(apiResponse.jobs[0]).toMatchObject({
      title: "Software Engineer",
      company: "OpenAI",
      country: "United States",
      state: "California",
      city: "San Francisco",
      sourcePlatform: "greenhouse",
    });
    expect(apiResponse.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      fetchedCount: 1,
      matchedCount: 1,
      savedCount: 1,
    });
    expect(resolveViewState(apiResponse)).toBe("success");
    expect(html).toContain("Software Engineer");
    expect(html).toContain("OpenAI");
    expect(html).toContain("Apply");
  });
});
