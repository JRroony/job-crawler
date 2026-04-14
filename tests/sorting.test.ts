import { describe, expect, it } from "vitest";

import {
  explainJobRanking,
  rankJobs,
  sortJobsWithDiagnostics,
} from "@/lib/server/crawler/sort";

const now = new Date("2026-04-13T12:00:00.000Z");

describe("crawl ranking", () => {
  it("ranks exact title matches above weaker semantic matches", () => {
    const ranked = rankJobs(
      [
        buildJob({
          title: "Backend Engineer",
          sourcePlatform: "lever",
          postingDate: "2026-04-13T00:00:00.000Z",
        }),
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "greenhouse",
          postingDate: "2026-04-02T00:00:00.000Z",
        }),
        buildJob({
          title: "Software Developer",
          sourcePlatform: "ashby",
          postingDate: "2026-04-12T00:00:00.000Z",
        }),
      ],
      "Software Engineer",
      now,
    );

    expect(ranked.map(({ job }) => job.title)).toEqual([
      "Software Engineer",
      "Software Developer",
      "Backend Engineer",
    ]);
    expect(ranked[0]?.ranking.relevanceScore).toBeGreaterThan(ranked[1]?.ranking.relevanceScore ?? 0);
  });

  it("ranks more recent relevant jobs above stale ones", () => {
    const ranked = rankJobs(
      [
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "greenhouse",
          postingDate: "2026-04-12T00:00:00.000Z",
        }),
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "lever",
          postingDate: "2026-01-10T00:00:00.000Z",
        }),
      ],
      "Software Engineer",
      now,
    );

    expect(ranked.map(({ job }) => job.sourcePlatform)).toEqual(["greenhouse", "lever"]);
    expect(ranked[0]?.ranking.dateScore).toBeGreaterThan(ranked[1]?.ranking.dateScore ?? 0);
    expect(ranked[0]?.ranking.dateSource).toBe("postingDate");
  });

  it("uses crawled or discovered timestamps as a defined fallback when posting date is missing", () => {
    const ranked = sortJobsWithDiagnostics(
      [
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "lever",
          crawledAt: "2026-04-12T12:00:00.000Z",
          discoveredAt: "2026-04-10T12:00:00.000Z",
        }),
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "ashby",
          discoveredAt: "2026-04-12T10:00:00.000Z",
        }),
        buildJob({
          title: "Software Engineer",
          sourcePlatform: "greenhouse",
          postingDate: "2026-02-01T00:00:00.000Z",
        }),
      ],
      "Software Engineer",
      now,
    );

    expect(ranked.map((job) => job.sourcePlatform)).toEqual(["lever", "ashby", "greenhouse"]);
    expect(ranked[0]?.rawSourceMetadata.crawlRanking).toMatchObject({
      dateSource: "crawledAt",
      usedFallbackDate: true,
    });
    expect(ranked[1]?.rawSourceMetadata.crawlRanking).toMatchObject({
      dateSource: "discoveredAt",
      usedFallbackDate: true,
    });
    expect(ranked[2]?.rawSourceMetadata.crawlRanking).toMatchObject({
      dateSource: "postingDate",
      usedFallbackDate: false,
    });
  });

  it("emits explainable diagnostics for relevance, date handling, and final contributors", () => {
    const job = buildJob({
      title: "Software Engineer",
      sourcePlatform: "greenhouse",
      postingDate: "2026-04-12T00:00:00.000Z",
    });

    expect(explainJobRanking(job, "Software Engineer", now)).toMatchObject({
      relevanceScore: 1000,
      relevanceTier: "exact",
      dateSource: "postingDate",
      dateScore: expect.any(Number),
      finalScore: expect.any(Number),
      finalRankContributors: expect.arrayContaining([
        "relevance=1000",
        expect.stringMatching(/^date=\d+$/),
        "dateSource=postingDate",
      ]),
    });
  });
});

function buildJob(input: {
  title: string;
  sourcePlatform: "greenhouse" | "lever" | "ashby";
  postingDate?: string;
  discoveredAt?: string;
  crawledAt?: string;
}) {
  return {
    title: input.title,
    company: "Acme",
    sourcePlatform: input.sourcePlatform,
    sourceJobId: `${input.sourcePlatform}-${input.title}`,
    sourceUrl: `https://example.com/${input.sourcePlatform}/${encodeURIComponent(input.title)}`,
    applyUrl: `https://example.com/${input.sourcePlatform}/${encodeURIComponent(input.title)}/apply`,
    canonicalUrl: `https://example.com/${input.sourcePlatform}/${encodeURIComponent(input.title)}`,
    postingDate: input.postingDate,
    postedAt: input.postingDate,
    discoveredAt: input.discoveredAt ?? "2026-04-01T00:00:00.000Z",
    crawledAt: input.crawledAt,
    rawSourceMetadata: {},
  };
}
