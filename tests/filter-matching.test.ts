import { describe, expect, it } from "vitest";

import {
  getTitleMatchResult,
  matchesFilters,
  normalizeTitleToCanonicalForm,
} from "@/lib/server/crawler/helpers";
import type { JobListing, SearchFilters } from "@/lib/types";

type FilterableJob = Pick<
  JobListing,
  | "title"
  | "company"
  | "country"
  | "state"
  | "city"
  | "locationText"
  | "experienceLevel"
  | "experienceClassification"
  | "rawSourceMetadata"
>;

function createJob(overrides: Partial<FilterableJob> = {}): FilterableJob {
  return {
    title: "Software Engineer",
    company: "Acme",
    country: "United States",
    state: "California",
    city: "San Francisco",
    locationText: "San Francisco, California, United States",
    experienceLevel: "mid",
    experienceClassification: undefined,
    rawSourceMetadata: {},
    ...overrides,
  };
}

function createFilters(overrides: Partial<SearchFilters> = {}): SearchFilters {
  return {
    title: "Software Engineer",
    ...overrides,
  };
}

describe("matchesFilters country matching", () => {
  it.each([
    {
      label: "United States",
      job: createJob({
        country: "United States",
        locationText: "San Francisco, California, United States",
      }),
    },
    {
      label: "USA",
      job: createJob({
        country: "USA",
        locationText: "Austin, Texas, USA",
      }),
    },
    {
      label: "US",
      job: createJob({
        country: "US",
        locationText: "Remote, US",
      }),
    },
    {
      label: "Remote US",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Remote US",
      }),
    },
    {
      label: "Remote, United States",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Remote, United States",
      }),
    },
    {
      label: "United States Remote",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "United States Remote",
      }),
    },
  ])("matches a United States filter against $label", ({ job }) => {
    expect(
      matchesFilters(
        job,
        createFilters({
          country: "United States",
        }),
      ),
    ).toBe(true);
  });

  it("treats United States, USA, and US as the same country concept", () => {
    const job = createJob({
      country: "USA",
      state: undefined,
      city: undefined,
      locationText: "Remote US",
    });

    expect(matchesFilters(job, createFilters({ country: "United States" }))).toBe(true);
    expect(matchesFilters(job, createFilters({ country: "USA" }))).toBe(true);
    expect(matchesFilters(job, createFilters({ country: "US" }))).toBe(true);
  });

  it("does not match a United States filter against a non-US location", () => {
    expect(
      matchesFilters(
        createJob({
          country: "Canada",
          state: "Ontario",
          city: "Toronto",
          locationText: "Toronto, Ontario, Canada",
        }),
        createFilters({
          country: "United States",
        }),
      ),
    ).toBe(false);
  });

  it.each([
    {
      label: "Seattle",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Seattle",
      }),
    },
    {
      label: "San Francisco, CA",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "San Francisco, CA",
      }),
    },
    {
      label: "Austin, Texas",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Austin, Texas",
      }),
    },
    {
      label: "Washington, DC",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Washington, DC",
      }),
    },
    {
      label: "Bellevue WA",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Bellevue WA",
      }),
    },
    {
      label: "East Windsor NJ",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "East Windsor NJ",
      }),
    },
    {
      label: "Remote USA",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Remote USA",
      }),
    },
    {
      label: "Hybrid in Chicago, IL",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Hybrid in Chicago, IL",
      }),
    },
    {
      label: "San Jose, CA",
      job: createJob({
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "San Jose, CA",
      }),
    },
  ])("matches a United States country-only filter against inferred US location $label", ({ job }) => {
    expect(
      matchesFilters(
        job,
        createFilters({
          country: "United States",
        }),
      ),
    ).toBe(true);
  });

  it("does not match a United States country-only filter against Toronto", () => {
    expect(
      matchesFilters(
        createJob({
          country: undefined,
          state: undefined,
          city: undefined,
          locationText: "Toronto",
        }),
        createFilters({
          country: "United States",
        }),
      ),
    ).toBe(false);
  });
});

describe("matchesFilters location filter combinations", () => {
  it("keeps explicit state and city filters working when location text only contains a US state abbreviation", () => {
    const job = createJob({
      country: undefined,
      state: undefined,
      city: undefined,
      locationText: "San Francisco, CA",
    });

    expect(
      matchesFilters(
        job,
        createFilters({
          state: "California",
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        job,
        createFilters({
          state: "CA",
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        job,
        createFilters({
          state: "California",
          city: "San Francisco",
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        job,
        createFilters({
          state: "California",
          city: "Seattle",
        }),
      ),
    ).toBe(false);

    expect(
      matchesFilters(
        job,
        createFilters({
          state: "Washington",
        }),
      ),
    ).toBe(false);
  });

  it("matches a Seattle city filter against hybrid Seattle roles without over-including nearby cities", () => {
    const seattleJob = createJob({
      country: undefined,
      state: undefined,
      city: undefined,
      locationText: "Hybrid in Seattle, WA",
    });
    const bellevueJob = createJob({
      country: undefined,
      state: undefined,
      city: undefined,
      locationText: "Bellevue, WA",
    });

    expect(
      matchesFilters(
        seattleJob,
        createFilters({
          city: "Seattle",
        }),
      ),
    ).toBe(true);
    expect(
      matchesFilters(
        bellevueJob,
        createFilters({
          city: "Seattle",
        }),
      ),
    ).toBe(false);
  });

  it("matches a California state filter against city-state and remote-state forms", () => {
    const stateOnly = createJob({
      country: undefined,
      state: undefined,
      city: undefined,
      locationText: "San Jose, CA",
    });
    const remoteState = createJob({
      country: undefined,
      state: undefined,
      city: undefined,
      locationText: "Remote - California",
    });

    expect(
      matchesFilters(
        stateOnly,
        createFilters({
          state: "California",
        }),
      ),
    ).toBe(true);
    expect(
      matchesFilters(
        remoteState,
        createFilters({
          state: "California",
        }),
      ),
    ).toBe(true);
  });
});

describe("title relevance", () => {
  it("normalizes recognized title variants into a canonical form", () => {
    expect(normalizeTitleToCanonicalForm("Senior SWE")).toBe("software engineer");
    expect(normalizeTitleToCanonicalForm("Software Developer")).toBe("software engineer");
    expect(normalizeTitleToCanonicalForm("Product Analyst")).toBe("data analyst");
    expect(normalizeTitleToCanonicalForm("Decision Scientist")).toBe("data analyst");
    expect(normalizeTitleToCanonicalForm("Lead Product Manager")).toBe("product manager");
  });

  it.each([
    {
      label: "exact software title",
      title: "Software Engineer",
      tier: "exact",
    },
    {
      label: "seniority-insensitive software variant",
      title: "Senior Software Engineer",
      tier: "canonical_variant",
    },
    {
      label: "software synonym",
      title: "Software Developer",
      tier: "synonym",
    },
    {
      label: "software abbreviation",
      title: "SWE",
      tier: "abbreviation",
    },
    {
      label: "related broad software role",
      title: "Backend Engineer",
      tier: "adjacent_concept",
    },
    {
      label: "mobile engineering role",
      title: "Mobile Engineer",
      tier: "adjacent_concept",
    },
    {
      label: "infrastructure engineering role",
      title: "Infrastructure Engineer",
      tier: "adjacent_concept",
    },
  ])("classifies $label", ({ title, tier }) => {
    expect(getTitleMatchResult(title, "Software Engineer")).toMatchObject({
      matches: true,
      tier,
      canonicalQueryTitle: "software engineer",
    });
  });

  it.each([
    "Sales Engineer",
    "Support Engineer",
    "Data Engineer",
    "QA Engineer",
    "Software Engineer in Test",
    "Product Designer",
    "Data Scientist",
  ])("does not overmatch %s for a software engineer query", (title) => {
    expect(getTitleMatchResult(title, "Software Engineer")).toMatchObject({
      matches: false,
      tier: "none",
    });
  });

  it.each([
    {
      label: "data analyst synonym",
      title: "Business Intelligence Analyst",
      tier: "synonym",
    },
    {
      label: "product analytics variant",
      title: "Product Analyst",
      tier: "synonym",
    },
    {
      label: "decision science variant",
      title: "Decision Scientist",
      tier: "synonym",
    },
  ])("classifies $label for a data analyst query", ({ title, tier }) => {
    expect(getTitleMatchResult(title, "Data Analyst")).toMatchObject({
      matches: true,
      tier,
      canonicalQueryTitle: "data analyst",
    });
  });

  it.each([
    "Data Engineer",
    "Data Scientist",
    "Financial Analyst",
    "Sales Analyst",
  ])("does not overmatch %s for a data analyst query", (title) => {
    expect(getTitleMatchResult(title, "Data Analyst")).toMatchObject({
      matches: false,
      tier: "none",
    });
  });
});

describe("matchesFilters title matching", () => {
  it.each([
    "Software Engineer Intern",
    "Software Engineering Intern",
    "Software Engineer, Intern",
    "SWE Intern",
    "Backend Engineer Intern",
    "Frontend Engineer Intern",
    "Full Stack Engineer Intern",
    "Platform Engineer Intern",
  ])("matches internship variant %s for a software engineer query", (title) => {
    expect(
      matchesFilters(
        createJob({
          title,
          experienceLevel: undefined,
        }),
        createFilters({
          title: "Software Engineer",
          experienceLevels: ["intern"],
        }),
      ),
    ).toBe(true);
  });

  it.each([
    "Software Engineer",
    "Senior Software Engineer",
    "Software Engineering Intern",
    "Software Developer",
    "SWE",
    "Backend Engineer",
    "Mobile Engineer",
    "Infrastructure Engineer",
    "Frontend Engineer",
    "Full Stack Engineer",
    "Platform Engineer",
  ])("matches %s for a broad software engineer query", (title) => {
    expect(
      matchesFilters(
        createJob({
          title,
        }),
        createFilters({
          title: "Software Engineer",
        }),
      ),
    ).toBe(true);
  });

  it.each([
    "Sales Engineer",
    "Support Engineer",
    "Data Engineer",
    "QA Engineer",
    "Product Designer",
    "Data Scientist",
  ])("rejects unrelated role %s", (title) => {
    expect(
      matchesFilters(
        createJob({
          title,
        }),
        createFilters({
          title: "Software Engineer",
        }),
      ),
    ).toBe(false);
  });

  it("keeps city and experience filters working alongside title normalization", () => {
    const job = createJob({
      title: "Backend Engineer",
      experienceLevel: "senior",
    });

    expect(
      matchesFilters(
        job,
        createFilters({
          title: "Software Engineer",
          city: "San Francisco",
          experienceLevels: ["senior"],
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        job,
        createFilters({
          title: "Software Engineer",
          city: "New York",
        }),
      ),
    ).toBe(false);

    expect(
      matchesFilters(
        job,
        createFilters({
          title: "Software Engineer",
          experienceLevels: ["mid"],
        }),
      ),
    ).toBe(false);
  });
});

describe("matchesFilters experience matching", () => {
  it.each([
    {
      label: "intern roles from title inference",
      job: createJob({
        title: "Software Engineer Intern",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["intern"],
      }),
    },
    {
      label: "new grad roles from title inference",
      job: createJob({
        title: "New Graduate Software Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["new_grad"],
      }),
    },
    {
      label: "junior roles from title inference",
      job: createJob({
        title: "Junior Software Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["junior"],
      }),
    },
    {
      label: "mid roles from title inference",
      job: createJob({
        title: "Software Engineer II",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["mid"],
      }),
    },
    {
      label: "senior roles from title inference",
      job: createJob({
        title: "Senior Software Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["senior"],
      }),
    },
    {
      label: "staff roles from title inference",
      job: createJob({
        title: "Staff Software Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["staff"],
      }),
    },
    {
      label: "principal roles from title inference",
      job: createJob({
        title: "Principal Software Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["principal"],
      }),
    },
    {
      label: "lead roles from title inference",
      job: createJob({
        title: "Lead Backend Engineer",
        experienceLevel: undefined,
      }),
      filters: createFilters({
        experienceLevels: ["lead"],
      }),
    },
    {
      label: "intern roles from metadata inference",
      job: createJob({
        title: "Software Engineer",
        experienceLevel: undefined,
        rawSourceMetadata: {
          description: "Join our 2026 summer internship program for software engineering students.",
        },
      }),
      filters: createFilters({
        experienceLevels: ["intern"],
      }),
    },
    {
      label: "student program roles from metadata inference",
      job: createJob({
        title: "Software Engineer",
        experienceLevel: undefined,
        rawSourceMetadata: {
          department: "Student Program",
        },
      }),
      filters: createFilters({
        experienceLevels: ["intern"],
      }),
    },
    {
      label: "mid roles from experience range metadata",
      job: createJob({
        title: "Software Engineer",
        experienceLevel: undefined,
        rawSourceMetadata: {
          requirements: "We are looking for candidates with 2-5 years of experience.",
        },
      }),
      filters: createFilters({
        experienceLevels: ["mid"],
      }),
    },
    {
      label: "senior roles from experience range metadata",
      job: createJob({
        title: "Software Engineer",
        experienceLevel: undefined,
        rawSourceMetadata: {
          requirements: "Candidates should bring 5-10 years of experience building APIs.",
        },
      }),
      filters: createFilters({
        experienceLevels: ["senior"],
      }),
    },
  ])("matches $label", ({ job, filters }) => {
    expect(matchesFilters(job, filters)).toBe(true);
  });

  it("matches any selected experience level", () => {
    expect(
      matchesFilters(
        createJob({
          title: "Software Engineer Intern",
          experienceLevel: undefined,
        }),
        createFilters({
          experienceLevels: ["intern", "new_grad"],
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        createJob({
          title: "Senior Software Engineer",
          experienceLevel: undefined,
        }),
        createFilters({
          experienceLevels: ["intern", "new_grad"],
        }),
      ),
    ).toBe(false);
  });

  it("rejects non-matching experience levels after normalization", () => {
    expect(
      matchesFilters(
        createJob({
          title: "Software Engineer Intern",
          experienceLevel: undefined,
        }),
        createFilters({
          experienceLevels: ["senior"],
        }),
      ),
    ).toBe(false);
  });

  it("keeps ambiguous manager-family roles unspecified instead of coercing them into mid", () => {
    expect(
      matchesFilters(
        createJob({
          title: "Product Manager",
          experienceLevel: undefined,
        }),
        createFilters({
          experienceLevels: ["mid"],
        }),
      ),
    ).toBe(false);
  });

  it("keeps strict mode limited to explicit classifications", () => {
    expect(
      matchesFilters(
        createJob({
          title: "Software Engineer",
          experienceLevel: undefined,
          experienceClassification: {
            experienceVersion: 2,
            experienceBand: "senior",
            experienceSource: "description",
            experienceConfidence: "high",
            experienceSignals: [],
            inferredLevel: "senior",
            confidence: "high",
            source: "description",
            reasons: ["Detected senior markers in description."],
            isUnspecified: false,
          },
        }),
        createFilters({
          experienceLevels: ["senior"],
          experienceMatchMode: "strict",
        }),
      ),
    ).toBe(false);
  });

  it("lets balanced mode match medium and high confidence inferred levels", () => {
    expect(
      matchesFilters(
        createJob({
          title: "Software Engineer",
          experienceLevel: undefined,
          experienceClassification: {
            experienceVersion: 2,
            experienceBand: "senior",
            experienceSource: "description",
            experienceConfidence: "medium",
            experienceSignals: [],
            inferredLevel: "senior",
            confidence: "medium",
            source: "description",
            reasons: ["Detected senior markers in description."],
            isUnspecified: false,
          },
        }),
        createFilters({
          experienceLevels: ["senior"],
          experienceMatchMode: "balanced",
        }),
      ),
    ).toBe(true);
  });

  it("keeps low-confidence inferred levels out of balanced mode but includes them in broad mode", () => {
    const job = createJob({
      title: "Software Engineer",
      experienceLevel: undefined,
      experienceClassification: {
        experienceVersion: 2,
        experienceBand: "senior",
        experienceSource: "page_fetch",
        experienceConfidence: "low",
        experienceSignals: [],
        inferredLevel: "senior",
        confidence: "low",
        source: "page_fetch",
        reasons: ["Mapped years of experience from a deep page fetch."],
        isUnspecified: false,
      },
    });

    expect(
      matchesFilters(
        job,
        createFilters({
          experienceLevels: ["senior"],
          experienceMatchMode: "balanced",
        }),
      ),
    ).toBe(false);

    expect(
      matchesFilters(
        job,
        createFilters({
          experienceLevels: ["senior"],
          experienceMatchMode: "broad",
        }),
      ),
    ).toBe(true);
  });

  it("includes unspecified jobs only when explicitly requested or when broad mode is used", () => {
    const job = createJob({
      title: "Software Engineer",
      experienceLevel: undefined,
      experienceClassification: {
        experienceVersion: 2,
        experienceBand: "unknown",
        experienceSource: "unknown",
        experienceConfidence: "none",
        experienceSignals: [],
        confidence: "none",
        source: "unknown",
        reasons: [],
        isUnspecified: true,
      },
      rawSourceMetadata: {},
    });

    expect(
      matchesFilters(
        job,
        createFilters({
          experienceLevels: ["mid"],
        }),
      ),
    ).toBe(false);

    expect(
      matchesFilters(
        job,
        createFilters({
          experienceLevels: ["mid"],
          includeUnspecifiedExperience: true,
        }),
      ),
    ).toBe(true);

    expect(
      matchesFilters(
        job,
        createFilters({
          experienceLevels: ["mid"],
          experienceMatchMode: "broad",
        }),
      ),
    ).toBe(true);
  });

  it.each([
    "Sales Engineer Intern",
    "Support Engineer Intern",
    "Data Scientist Intern",
    "Senior Software Engineer",
    "New Graduate Software Engineer",
  ])("does not overmatch %s for an intern software engineer search", (title) => {
    expect(
      matchesFilters(
        createJob({
          title,
          experienceLevel: undefined,
        }),
        createFilters({
          title: "Software Engineer",
          experienceLevels: ["intern"],
        }),
      ),
    ).toBe(false);
  });
});
