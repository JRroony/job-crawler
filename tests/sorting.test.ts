import { describe, expect, it } from "vitest";

import { sortJobs } from "@/lib/server/crawler/sort";

describe("sortJobs", () => {
  it("sorts by posted date descending first", () => {
    const jobs = sortJobs([
      {
        title: "B Role",
        sourcePlatform: "lever",
        postedAt: undefined,
      },
      {
        title: "A Role",
        sourcePlatform: "greenhouse",
        postedAt: "2026-03-20T00:00:00.000Z",
      },
      {
        title: "C Role",
        sourcePlatform: "ashby",
        postedAt: "2026-03-21T00:00:00.000Z",
      },
    ]);

    expect(jobs.map((job) => job.title)).toEqual(["C Role", "A Role", "B Role"]);
  });

  it("falls back to source and title when posted date is unavailable", () => {
    const jobs = sortJobs([
      {
        title: "Z Role",
        sourcePlatform: "lever",
        postedAt: undefined,
      },
      {
        title: "A Role",
        sourcePlatform: "ashby",
        postedAt: undefined,
      },
      {
        title: "B Role",
        sourcePlatform: "ashby",
        postedAt: undefined,
      },
    ]);

    expect(jobs.map((job) => `${job.sourcePlatform}:${job.title}`)).toEqual([
      "ashby:A Role",
      "ashby:B Role",
      "lever:Z Role",
    ]);
  });

  it("ranks stronger title matches ahead of newer but broader ones", () => {
    const jobs = sortJobs(
      [
        {
          title: "Backend Engineer",
          sourcePlatform: "lever",
          postedAt: "2026-03-25T00:00:00.000Z",
        },
        {
          title: "SWE",
          sourcePlatform: "ashby",
          postedAt: "2026-03-24T00:00:00.000Z",
        },
        {
          title: "Software Developer",
          sourcePlatform: "greenhouse",
          postedAt: "2026-03-23T00:00:00.000Z",
        },
        {
          title: "Senior Software Engineer",
          sourcePlatform: "ashby",
          postedAt: "2026-03-22T00:00:00.000Z",
        },
        {
          title: "Staff Software Engineer",
          sourcePlatform: "lever",
          postedAt: "2026-03-26T00:00:00.000Z",
        },
        {
          title: "Software Engineer",
          sourcePlatform: "greenhouse",
          postedAt: "2026-03-21T00:00:00.000Z",
        },
      ],
      "Software Engineer",
    );

    expect(jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Staff Software Engineer",
      "Senior Software Engineer",
      "Software Developer",
      "SWE",
      "Backend Engineer",
    ]);
  });
});
