import { describe, expect, it, vi } from "vitest";

import {
  getSearchDetails,
  getSearchJobDeltas,
  runSearchFromFilters,
  runSearchIngestionFromFilters,
  startSearchFromFilters,
} from "@/lib/server/crawler/service";
import { collectionNames } from "@/lib/server/db/collections";
import { JobCrawlerRepository } from "@/lib/server/db/repository";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource, DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";
import { getIndexedJobsForSearch } from "@/lib/server/search/indexed-jobs";
import { buildIndexedJobCandidateQuery } from "@/lib/server/search/job-search-index";
import type { JobListing } from "@/lib/types";

import { FakeDb } from "@/tests/helpers/fake-db";

type PersistableTestJob = Omit<JobListing, "_id" | "crawlRunIds">;

function createPersistableJob(
  overrides: Partial<PersistableTestJob> = {},
): PersistableTestJob {
  const title = overrides.title ?? "Software Engineer";
  const company = overrides.company ?? "Acme";
  const companyNormalized = overrides.companyNormalized ?? company.toLowerCase();
  const titleNormalized = overrides.titleNormalized ?? title.toLowerCase();
  const locationText = overrides.locationText ?? "Seattle, WA";
  const locationNormalized =
    overrides.locationNormalized ??
    locationText
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  const canonicalUrl = overrides.canonicalUrl ?? "https://example.com/jobs/role-1";
  const applyUrl = overrides.applyUrl ?? `${canonicalUrl}/apply`;
  const sourceUrl = overrides.sourceUrl ?? canonicalUrl;
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const sourceCompanySlug = overrides.sourceCompanySlug ?? companyNormalized;
  const sourceJobId = overrides.sourceJobId ?? "role-1";
  const discoveredAt = overrides.discoveredAt ?? "2026-04-10T12:00:00.000Z";
  const crawledAt = overrides.crawledAt ?? discoveredAt;

  return {
    canonicalJobKey:
      overrides.canonicalJobKey ??
      `platform:${sourcePlatform}:${sourceCompanySlug}:${sourceJobId.toLowerCase()}`,
    title,
    company,
    normalizedCompany: companyNormalized,
    normalizedTitle: titleNormalized,
    country: overrides.country ?? "United States",
    state: overrides.state,
    city: overrides.city,
    locationRaw: overrides.locationRaw ?? locationText,
    normalizedLocation: locationNormalized,
    locationText,
    resolvedLocation: overrides.resolvedLocation,
    remoteType: overrides.remoteType ?? "unknown",
    employmentType: overrides.employmentType,
    seniority: overrides.seniority,
    experienceLevel: overrides.experienceLevel,
    experienceClassification: overrides.experienceClassification,
    sourcePlatform,
    sourceCompanySlug,
    sourceJobId,
    sourceUrl,
    applyUrl,
    resolvedUrl: overrides.resolvedUrl ?? applyUrl,
    canonicalUrl,
    postingDate: overrides.postingDate ?? "2026-04-09T00:00:00.000Z",
    postedAt: overrides.postedAt ?? overrides.postingDate ?? "2026-04-09T00:00:00.000Z",
    discoveredAt,
    crawledAt,
    descriptionSnippet: overrides.descriptionSnippet,
    salaryInfo: overrides.salaryInfo,
    sponsorshipHint: overrides.sponsorshipHint ?? "unknown",
    linkStatus: overrides.linkStatus ?? "valid",
    lastValidatedAt: overrides.lastValidatedAt ?? crawledAt,
    rawSourceMetadata: overrides.rawSourceMetadata ?? {},
    sourceProvenance: overrides.sourceProvenance ?? [
      {
        sourcePlatform,
        sourceJobId,
        sourceUrl,
        applyUrl,
        resolvedUrl: overrides.resolvedUrl ?? applyUrl,
        canonicalUrl,
        discoveredAt,
        rawSourceMetadata: {},
      },
    ],
    sourceLookupKeys: overrides.sourceLookupKeys ?? [
      `${sourcePlatform}:${sourceJobId.toLowerCase()}`,
    ],
    firstSeenAt: overrides.firstSeenAt ?? discoveredAt,
    lastSeenAt: overrides.lastSeenAt ?? crawledAt,
    indexedAt: overrides.indexedAt ?? crawledAt,
    isActive: overrides.isActive ?? true,
    closedAt: overrides.closedAt,
    dedupeFingerprint: overrides.dedupeFingerprint ?? `dedupe:${sourceJobId}`,
    companyNormalized,
    titleNormalized,
    locationNormalized,
    contentFingerprint: overrides.contentFingerprint ?? `content:${sourceJobId}`,
    contentHash: overrides.contentHash ?? `content-hash:${sourceJobId}`,
  };
}

function createProviderJob(overrides: {
  title: string;
  sourceJobId: string;
  company?: string;
  country?: string;
  state?: string;
  city?: string;
  locationText?: string;
  sourcePlatform?: "greenhouse" | "lever" | "ashby" | "workday";
}) {
  const company = overrides.company ?? "OpenAI";
  const sourcePlatform = overrides.sourcePlatform ?? "greenhouse";
  const country = overrides.country ?? "United States";
  const locationText = overrides.locationText ?? "Remote - United States";
  const sourceUrl = `https://example.com/${sourcePlatform}/jobs/${overrides.sourceJobId}`;

  return {
    title: overrides.title,
    company,
    country,
    state: overrides.state,
    city: overrides.city,
    locationText,
    resolvedLocation: {
      country,
      region: overrides.state,
      city: overrides.city,
      isRemote: locationText.toLowerCase().includes("remote"),
      isUnitedStates: country === "United States",
      confidence: "high" as const,
      evidence: [
        {
          source: "remote_hint" as const,
          value: locationText,
        },
      ],
    },
    sourcePlatform,
    sourceCompanySlug: company.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
    sourceJobId: overrides.sourceJobId,
    sourceUrl,
    applyUrl: `${sourceUrl}/apply`,
    canonicalUrl: sourceUrl,
    discoveredAt: "2026-04-15T12:00:00.000Z",
    rawSourceMetadata: {
      source: "search-indexed-first-test",
    },
  };
}

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

function createDiscovery(): DiscoveryService {
  return {
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
}

function createDiscoveryFromSources(sources: DiscoveredSource[]): DiscoveryService {
  return {
    async discover() {
      return sources;
    },
  };
}

function createSourceForPlatform(
  platform: "greenhouse" | "workday" | "company_page",
): DiscoveredSource {
  if (platform === "greenhouse") {
    return classifySourceCandidate({
      url: "https://boards.greenhouse.io/acme",
      token: "acme",
      confidence: "high",
      discoveryMethod: "configured_env",
    });
  }

  if (platform === "workday") {
    return classifySourceCandidate({
      url: "https://acme.wd1.myworkdayjobs.com/en-US/acme",
      token: "acme",
      confidence: "high",
      discoveryMethod: "future_search",
    });
  }

  return classifySourceCandidate({
    url: "https://www.acme.example/careers",
    token: "acme",
    companyHint: "Acme",
    confidence: "medium",
    discoveryMethod: "future_search",
  });
}

async function raceSearchStart<T>(
  startPromise: Promise<T>,
  timeoutMs: number,
) {
  const raced = await Promise.race([
    startPromise.then((result) => ({ type: "result" as const, result })),
    new Promise<{ type: "timeout" }>((resolve) =>
      setTimeout(() => resolve({ type: "timeout" }), timeoutMs),
    ),
  ]);

  if (raced.type === "timeout") {
    await startPromise.catch(() => undefined);
    throw new Error(`Search did not return within ${timeoutMs}ms.`);
  }

  return raced.result;
}

async function seedIndexedJobs(
  repository: JobCrawlerRepository,
  jobs: readonly PersistableTestJob[],
) {
  const search = await repository.createSearch(
    {
      title: "Seed Search",
    },
    "2026-04-10T12:00:00.000Z",
  );
  const crawlRun = await repository.createCrawlRun(
    search._id,
    "2026-04-10T12:00:00.000Z",
  );

  await repository.persistJobs(crawlRun._id, [...jobs]);

  return { search, crawlRun };
}

async function createActiveSearchState(
  repository: JobCrawlerRepository,
  filters: {
    title: string;
    country?: string;
    platforms?: Array<"greenhouse" | "lever" | "ashby" | "workday">;
  },
  now = "2026-04-15T12:00:00.000Z",
) {
  const search = await repository.createSearch(filters, now);
  const searchSession = await repository.createSearchSession(search._id, now, {
    status: "running",
  });
  const crawlRun = await repository.createCrawlRun(search._id, now, {
    searchSessionId: searchSession._id,
    stage: "discovering",
  });

  await Promise.all([
    repository.updateSearchLatestSession(search._id, searchSession._id, "running", now),
    repository.updateSearchLatestRun(search._id, crawlRun._id, "running", now),
    repository.updateSearchSession(searchSession._id, {
      latestCrawlRunId: crawlRun._id,
      status: "running",
      updatedAt: now,
    }),
  ]);

  return { search, searchSession, crawlRun };
}

async function waitForQueueToSettle(repository: JobCrawlerRepository, searchId: string) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (!(await repository.hasActiveCrawlQueueEntryForSearch(searchId))) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  throw new Error(`Search queue did not settle for ${searchId}.`);
}

const expectedIngestionDecisionLogKeys = [
  "searchId",
  "searchSessionId",
  "crawlRunId",
  "title",
  "country",
  "indexedCandidateCount",
  "indexedMatchedCount",
  "coverageTarget",
  "coveragePolicyReason",
  "targetJobCount",
  "latestIndexedJobAgeMs",
  "triggerReason",
  "shouldQueueTargetedReplenishment",
  "shouldRequestGenericBackgroundIngestion",
  "shouldRunRequestTimeCrawl",
  "activeQueueAlreadyExists",
  "shouldQueue",
  "requestBackgroundIngestion",
  "backgroundIngestionStatus",
  "allowRequestTimeSupplementalCrawl",
  "allowRequestTimeFreshnessRecovery",
];

describe("jobs-first indexed search", () => {
  it("emits structured search trace logs and attaches the trace to response diagnostics", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 5 }, (_, index) => {
        const sourceJobId = `trace-indexed-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Software Engineer",
          locationText: "Seattle, WA",
          country: "United States",
          state: "Washington",
          city: "Seattle",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      }),
    );
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);

    try {
      const result = await runSearchFromFilters(
        {
          title: "Software Engineer",
          country: "United States",
          state: "Washington",
          city: "Seattle",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          requestOwnerKey: "trace-search",
        },
      );

      const traceLabels = [
        "[search:trace:start]",
        "[search:trace:intent]",
        "[search:trace:candidate-query]",
        "[search:trace:candidate-db-result]",
        "[search:trace:final-filter]",
        "[search:trace:index-first-decision]",
        "[search:trace:response]",
      ];
      const traceCalls = traceLabels.map((label) => {
        const call = infoSpy.mock.calls.find(([actualLabel]) => actualLabel === label);
        expect(call).toBeDefined();
        return call as [string, Record<string, unknown>];
      });
      const traceId = traceCalls[0]?.[1].traceId;

      expect(typeof traceId).toBe("string");
      for (const [, payload] of traceCalls) {
        expect(payload.traceId).toBe(traceId);
        expect(() => JSON.stringify(payload)).not.toThrow();
      }
      expect(traceCalls[2]?.[1]).toMatchObject({
        limit: expect.any(Number),
        sort: expect.any(Object),
        diagnostics: expect.objectContaining({
          strategy: "coarse_prefilter",
        }),
      });
      expect(traceCalls[2]?.[1].filter).toBeDefined();
      expect(traceCalls[3]?.[1]).toMatchObject({
        candidateCountReturned: 5,
        candidateLimit: expect.any(Number),
        sampleCandidateIds: expect.arrayContaining([expect.any(String)]),
        sampleCandidateTitles: expect.arrayContaining(["Software Engineer"]),
      });
      expect(traceCalls[4]?.[1]).toMatchObject({
        evaluatedCount: 5,
        matchedCount: 5,
        excludedByTitle: 0,
        excludedByLocation: 0,
        excludedByExperience: 0,
      });
      expect(traceCalls[5]?.[1]).toMatchObject({
        indexedCandidateCount: 5,
        indexedMatchedCount: 5,
        minimumIndexedCoverage: 5,
        coverageTarget: 5,
        triggerReason: "indexed_coverage_sufficient",
        backgroundIngestionRequested: false,
        shouldQueueSupplemental: false,
        shouldQueueTargetedReplenishment: false,
      });
      expect(traceCalls[6]?.[1]).toMatchObject({
        returnedCount: 5,
        totalMatchedCount: 5,
        searchId: result.search._id,
        searchSessionId: result.searchSession?._id,
      });
      const ingestionDecisionCall = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:decision]",
      ) as [string, Record<string, unknown>] | undefined;
      expect(ingestionDecisionCall).toBeDefined();
      expect(() => JSON.stringify(ingestionDecisionCall?.[1])).not.toThrow();
      expect(Object.keys(ingestionDecisionCall?.[1] ?? {})).toEqual(
        expectedIngestionDecisionLogKeys,
      );
      expect(ingestionDecisionCall?.[1]).toMatchObject({
        searchId: result.search._id,
        searchSessionId: result.searchSession?._id,
        crawlRunId: result.crawlRun._id,
        title: "Software Engineer",
        country: "United States",
        indexedCandidateCount: 5,
        indexedMatchedCount: 5,
        coverageTarget: 5,
        coveragePolicyReason: expect.stringContaining("city_search"),
        targetJobCount: 30,
        triggerReason: "indexed_coverage_sufficient",
        shouldQueueTargetedReplenishment: false,
        shouldRequestGenericBackgroundIngestion: false,
        shouldRunRequestTimeCrawl: false,
        activeQueueAlreadyExists: false,
        shouldQueue: false,
        requestBackgroundIngestion: false,
        backgroundIngestionStatus: "not_requested",
        allowRequestTimeSupplementalCrawl: false,
        allowRequestTimeFreshnessRecovery: false,
        latestIndexedJobAgeMs: expect.any(Number),
      });
      expect(result.diagnostics.searchTrace).toMatchObject({
        traceId,
        start: expect.objectContaining({ traceId }),
        intent: expect.objectContaining({
          traceId,
          title: "Software Engineer",
          country: "United States",
          platforms: ["greenhouse"],
          crawlMode: "balanced",
        }),
        candidateQuery: expect.objectContaining({ traceId }),
        candidateDbResult: expect.objectContaining({ traceId }),
        finalFilter: expect.objectContaining({ traceId, matchedCount: 5 }),
        indexFirstDecision: expect.objectContaining({ traceId }),
        response: expect.objectContaining({ traceId, returnedCount: 5 }),
      });
    } finally {
      infoSpy.mockRestore();
    }
  });

  it("emits ingestion trace queue-request logs when supplemental request-time ingestion is queued", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const crawlSources = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return {
        provider: "greenhouse" as const,
        status: "success" as const,
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });
    const provider = createStubProvider("greenhouse", crawlSources);
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let searchId: string | undefined;

    try {
      const started = await startSearchFromFilters(
        {
          title: "Software Engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          allowRequestTimeSupplementalCrawl: true,
          initialVisibleWaitMs: 0,
          requestOwnerKey: "ingestion-trace-queue",
        },
      );
      searchId = started.result.search._id;
      await waitForQueueToSettle(repository, searchId);

      const decisionCall = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:decision]",
      ) as [string, Record<string, unknown>] | undefined;
      const queueCall = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:trace:queue-request]",
      ) as [string, Record<string, unknown>] | undefined;

      expect(decisionCall).toBeDefined();
      expect(queueCall).toBeDefined();
      expect(() => JSON.stringify(decisionCall?.[1])).not.toThrow();
      expect(() => JSON.stringify(queueCall?.[1])).not.toThrow();
      expect(Object.keys(decisionCall?.[1] ?? {})).toEqual(
        expectedIngestionDecisionLogKeys,
      );
      expect(decisionCall?.[1]).toMatchObject({
        searchId,
        searchSessionId: started.result.searchSession?._id,
        crawlRunId: started.result.crawlRun._id,
        title: "Software Engineer",
        country: "United States",
        indexedCandidateCount: 0,
        indexedMatchedCount: 0,
        coverageTarget: 120,
        coveragePolicyReason: expect.stringContaining("high_demand_role_country_minimum"),
        targetJobCount: 30,
        triggerReason: "explicit_request_time_recovery",
        shouldQueueTargetedReplenishment: false,
        shouldRequestGenericBackgroundIngestion: false,
        shouldRunRequestTimeCrawl: true,
        activeQueueAlreadyExists: false,
        shouldQueue: true,
        requestBackgroundIngestion: false,
        backgroundIngestionStatus: "not_requested",
        allowRequestTimeSupplementalCrawl: true,
        allowRequestTimeFreshnessRecovery: true,
        latestIndexedJobAgeMs: null,
      });
      expect(queueCall?.[1]).toMatchObject({
        searchId,
        searchSessionId: started.result.searchSession?._id,
        crawlRunId: started.result.crawlRun._id,
        ownerKey: "ingestion-trace-queue",
        queuedResult: true,
        isSearchRunPendingResult: true,
      });
      expect(crawlSources).toHaveBeenCalled();
    } finally {
      if (searchId) {
        await waitForQueueToSettle(repository, searchId).catch(() => undefined);
      }
      infoSpy.mockRestore();
    }
  });

  it("queues targeted replenishment when machine learning engineer United States has only 34 indexed matches", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 34 }, (_, index) => {
        const sourceJobId = `mle-indexed-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Machine Learning Engineer",
          locationText: "Remote - United States",
          country: "United States",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      }),
    );
    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let searchId: string | undefined;

    try {
      const started = await startSearchFromFilters(
        {
          title: "machine learning engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          initialVisibleWaitMs: 0,
        },
      );
      searchId = started.result.search._id;
      await waitForQueueToSettle(repository, searchId);

      expect(started.queued).toBe(true);
      expect(started.result.jobs).toHaveLength(34);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: 34,
        supplementalQueued: true,
        targetedReplenishmentQueued: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      expect(started.result.diagnostics.session?.coverageTarget).toBeGreaterThan(34);
      expect(crawlSources).toHaveBeenCalled();

      const decisionCall = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:decision]",
      ) as [string, Record<string, unknown>] | undefined;
      const targetedQueueCall = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:targeted-queue]",
      ) as [string, Record<string, unknown>] | undefined;

      expect(decisionCall?.[1]).toMatchObject({
        indexedMatchedCount: 34,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
        shouldQueueTargetedReplenishment: true,
        shouldRequestGenericBackgroundIngestion: false,
        shouldRunRequestTimeCrawl: false,
        activeQueueAlreadyExists: false,
      });
      expect(decisionCall?.[1]).not.toMatchObject({
        triggerReason: "indexed_coverage_sufficient",
      });
      expect(targetedQueueCall?.[1]).toMatchObject({
        searchId,
        queued: true,
        reason: "insufficient_indexed_coverage_targeted_replenishment",
      });
    } finally {
      if (searchId) {
        await waitForQueueToSettle(repository, searchId).catch(() => undefined);
      }
      infoSpy.mockRestore();
    }
  });

  it("returns immediately with zero jobs and queues fast targeted replenishment when applied scientist United States has empty indexed coverage", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const calls: CrawlProvider["provider"][] = [];
    const providerDelayMs = 300;
    const greenhouse = createStubProvider("greenhouse", async () => {
      calls.push("greenhouse");
      await new Promise((resolve) => setTimeout(resolve, providerDelayMs));
      return {
        provider: "greenhouse" as const,
        status: "success" as const,
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });
    const workday = createStubProvider("workday", async () => {
      calls.push("workday");
      return {
        provider: "workday" as const,
        status: "success" as const,
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });
    const companyPage = createStubProvider("company_page", async () => {
      calls.push("company_page");
      return {
        provider: "company_page" as const,
        status: "success" as const,
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let searchId: string | undefined;

    try {
      const startPromise = startSearchFromFilters(
        {
          title: "applied scientist",
          country: "United States",
          platforms: ["greenhouse", "workday", "company_page"],
        },
        {
          repository,
          providers: [greenhouse, workday, companyPage],
          discovery: createDiscoveryFromSources([
            createSourceForPlatform("greenhouse"),
            createSourceForPlatform("workday"),
            createSourceForPlatform("company_page"),
          ]),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
        },
      );
      const started = await raceSearchStart(startPromise, 150);
      searchId = started.result.search._id;

      expect(started.queued).toBe(true);
      expect(started.result.jobs).toEqual([]);
      expect(started.result.crawlRun.status).toBe("running");
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: 0,
        totalVisibleResultsCount: 0,
        supplementalQueued: true,
        supplementalRunning: true,
        targetedReplenishmentQueued: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      expect(started.result.diagnostics.session?.coverageTarget).toBeGreaterThan(0);

      await waitForQueueToSettle(repository, searchId);
      expect(calls).toEqual(["greenhouse"]);
      expect(infoSpy).toHaveBeenCalledWith(
        "[search:empty-or-low-index-fallback]",
        expect.objectContaining({
          searchId,
          willReturnImmediately: true,
          willQueueTargetedReplenishment: true,
          reason: "insufficient_indexed_coverage_targeted_replenishment",
          indexedMatchedCount: 0,
        }),
      );
      expect(infoSpy).toHaveBeenCalledWith(
        "[ingestion:targeted-queue]",
        expect.objectContaining({
          searchId,
          filters: expect.objectContaining({
            title: "applied scientist",
            country: "United States",
            platforms: ["greenhouse", "workday", "company_page"],
          }),
          selectedProviders: ["greenhouse"],
          skippedSlowProviders: ["workday", "company_page"],
          queued: true,
        }),
      );
      expect(infoSpy).toHaveBeenCalledWith(
        "[crawl:provider-tiering]",
        expect.objectContaining({
          searchId,
          selectedFastProviders: ["greenhouse"],
          selectedSlowProviders: [],
          skippedSlowProviders: ["workday", "company_page"],
        }),
      );
    } finally {
      if (searchId) {
        await waitForQueueToSettle(repository, searchId).catch(() => undefined);
      }
      infoSpy.mockRestore();
    }
  });

  it("applies the same low-index targeted replenishment policy to data analyst Canada and product manager United States", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const scenarios = [
      {
        title: "data analyst",
        country: "Canada",
        indexedTitle: "Data Analyst",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
        sourcePrefix: "generic-da-canada",
      },
      {
        title: "product manager",
        country: "United States",
        indexedTitle: "Product Manager",
        locationText: "Remote - United States",
        state: undefined,
        city: undefined,
        sourcePrefix: "generic-pm-us",
      },
    ];
    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    for (const scenario of scenarios) {
      await seedIndexedJobs(
        repository,
        Array.from({ length: 5 }, (_, index) => {
          const sourceJobId = `${scenario.sourcePrefix}-${index + 1}`;
          const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
          return createPersistableJob({
            title: scenario.indexedTitle,
            country: scenario.country,
            state: scenario.state,
            city: scenario.city,
            locationText: scenario.locationText,
            sourceJobId,
            canonicalUrl,
            applyUrl: `${canonicalUrl}/apply`,
            sourceUrl: canonicalUrl,
            dedupeFingerprint: `dedupe:${sourceJobId}`,
            contentFingerprint: `content:${sourceJobId}`,
            contentHash: `content-hash:${sourceJobId}`,
          });
        }),
      );

      const started = await startSearchFromFilters(
        {
          title: scenario.title,
          country: scenario.country,
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          initialVisibleWaitMs: 0,
        },
      );

      expect(started.queued).toBe(true);
      expect(started.result.jobs).toHaveLength(5);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: 5,
        supplementalQueued: true,
        targetedReplenishmentQueued: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      expect(started.result.diagnostics.session?.coverageTarget).toBeGreaterThan(5);
      await waitForQueueToSettle(repository, started.result.search._id);
    }

    expect(crawlSources).toHaveBeenCalledTimes(scenarios.length);
  });

  it("does not enqueue a duplicate targeted replenishment for the same normalized filters", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 34 }, (_, index) => {
        const sourceJobId = `mle-duplicate-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Machine Learning Engineer",
          locationText: "Remote - United States",
          country: "United States",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      }),
    );
    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return {
        provider: "greenhouse" as const,
        status: "success" as const,
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });
    const provider = createStubProvider("greenhouse", crawlSources);
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let firstSearchId: string | undefined;
    let secondSearchId: string | undefined;

    try {
      const first = await startSearchFromFilters(
        {
          title: "machine learning engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          initialVisibleWaitMs: 0,
        },
      );
      firstSearchId = first.result.search._id;
      expect(first.queued).toBe(true);

      const second = await startSearchFromFilters(
        {
          title: "machine learning engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:01.000Z"),
          initialVisibleWaitMs: 0,
        },
      );
      secondSearchId = second.result.search._id;

      expect(second.queued).toBe(false);
      expect(second.result.diagnostics.session).toMatchObject({
        supplementalQueued: true,
        targetedReplenishmentQueued: false,
        targetedReplenishmentActive: true,
        activeQueueAlreadyExists: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      expect(crawlSources).toHaveBeenCalledTimes(1);
      const duplicateDecision = infoSpy.mock.calls
        .filter(([actualLabel]) => actualLabel === "[ingestion:decision]")
        .at(-1)?.[1] as Record<string, unknown> | undefined;
      expect(duplicateDecision).toMatchObject({
        activeQueueAlreadyExists: true,
        shouldQueueTargetedReplenishment: false,
      });
    } finally {
      if (firstSearchId) {
        await waitForQueueToSettle(repository, firstSearchId).catch(() => undefined);
      }
      if (secondSearchId) {
        await waitForQueueToSettle(repository, secondSearchId).catch(() => undefined);
      }
      infoSpy.mockRestore();
    }
  });

  it("does not apply broad-country coverage targets to reasonable narrow city searches", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 5 }, (_, index) => {
        const sourceJobId = `staff-mle-bellevue-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Staff Machine Learning Engineer",
          locationText: "Bellevue, WA",
          country: "United States",
          state: "Washington",
          city: "Bellevue",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      }),
    );
    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const started = await startSearchFromFilters(
      {
        title: "Staff Machine Learning Engineer",
        country: "United States",
        state: "Washington",
        city: "Bellevue",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [createStubProvider("greenhouse", crawlSources)],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
      },
    );

    expect(started.queued).toBe(false);
    expect(crawlSources).not.toHaveBeenCalled();
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 5,
      coverageTarget: 5,
      supplementalQueued: false,
      triggerReason: "indexed_coverage_sufficient",
    });
    expect(started.result.diagnostics.session?.coveragePolicyReason).toContain("city_search");
  });

  it("persists jobs and logs db write counts when targeted replenishment runs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 34 }, (_, index) => {
        const sourceJobId = `mle-persist-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Machine Learning Engineer",
          locationText: "Remote - United States",
          country: "United States",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      }),
    );
    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        createProviderJob({
          title: "Senior Machine Learning Engineer",
          sourceJobId: "targeted-new-mle",
          locationText: "San Francisco, CA",
          state: "California",
          city: "San Francisco",
        }),
      ],
    }));
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let searchId: string | undefined;

    try {
      const started = await startSearchFromFilters(
        {
          title: "machine learning engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:00:00.000Z"),
          initialVisibleWaitMs: 0,
        },
      );
      searchId = started.result.search._id;
      await waitForQueueToSettle(repository, searchId);

      const pipelineStart = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:pipeline-start]",
      ) as [string, Record<string, unknown>] | undefined;
      const dbWriteResult = infoSpy.mock.calls.find(
        ([actualLabel]) => actualLabel === "[ingestion:db-write-result]",
      ) as [string, Record<string, unknown>] | undefined;

      expect(pipelineStart?.[1]).toMatchObject({
        searchId,
        searchSessionId: started.result.searchSession?._id,
        crawlRunId: started.result.crawlRun._id,
        providerCount: 1,
      });
      expect(dbWriteResult?.[1]).toMatchObject({
        searchId,
        searchSessionId: started.result.searchSession?._id,
        crawlRunId: started.result.crawlRun._id,
        insertedCount: 1,
        updatedCount: 0,
        linkedToRunCount: 1,
        indexedEventCount: 1,
        newVisibleJobCount: 1,
      });

      const final = await getSearchDetails(searchId, { repository });
      expect(final.jobs.map((job) => job.title)).toContain(
        "Senior Machine Learning Engineer",
      );
    } finally {
      if (searchId) {
        await waitForQueueToSettle(repository, searchId).catch(() => undefined);
      }
      infoSpy.mockRestore();
    }
  });

  it("treats runSearchFromFilters as an indexed-first search entry point and keeps ordinary low coverage off the request crawl path", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "indexed-primary",
        canonicalUrl: "https://example.com/jobs/indexed-primary",
        applyUrl: "https://example.com/jobs/indexed-primary/apply",
        sourceUrl: "https://example.com/jobs/indexed-primary",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T11:55:00.000Z",
        crawledAt: "2026-04-15T11:55:00.000Z",
        indexedAt: "2026-04-15T11:55:00.000Z",
      }),
    ]);

    const discover = vi.fn(async () => [
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/acme",
        token: "acme",
        confidence: "high",
        discoveryMethod: "configured_env",
      }),
    ]);

    const initial = await runSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [],
        discovery: { discover },
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "run-search-default",
      },
    );

    expect(initial.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
    expect(initial.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
      source: "jobs_collection",
    });
    expect(initial.crawlRun.status).toBe("completed");
    expect(initial.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: false,
      supplementalRunning: false,
      triggerReason: "insufficient_indexed_coverage_background_requested",
      backgroundIngestion: expect.objectContaining({
        status: expect.stringMatching(/^(started|already_active)$/),
      }),
    });
    expect(discover).not.toHaveBeenCalled();
  });

  it("keeps the full crawl pipeline available through the explicit ingestion entry point", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 1,
      fetchedCount: 2,
      matchedCount: 2,
      warningCount: 0,
      jobs: [
        {
          title: "Software Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote - United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "dedupe-a",
          sourceUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          canonicalUrl: "https://example.com/jobs/shared-role",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
        {
          title: "Software Engineer",
          company: "Acme",
          country: "United States",
          locationText: "Remote - United States",
          sourcePlatform: "greenhouse",
          sourceJobId: "dedupe-b",
          sourceUrl: "https://example.com/jobs/shared-role",
          applyUrl: "https://example.com/jobs/shared-role/apply",
          canonicalUrl: "https://example.com/jobs/shared-role",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
      ],
    }));

    const result = await runSearchIngestionFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
      },
    );

    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]?.title).toBe("Software Engineer");
    expect(result.sourceResults[0]).toMatchObject({
      provider: "greenhouse",
      fetchedCount: 2,
      matchedCount: 2,
      savedCount: 1,
    });
    expect(result.crawlRun.status).toBe("completed");
  });

  it("returns indexed jobs immediately and completes the request session when low fresh coverage should be replenished in the background", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "indexed-seattle",
        canonicalUrl: "https://example.com/jobs/indexed-seattle",
        applyUrl: "https://example.com/jobs/indexed-seattle/apply",
        sourceUrl: "https://example.com/jobs/indexed-seattle",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T11:58:00.000Z",
        crawledAt: "2026-04-15T11:58:00.000Z",
        indexedAt: "2026-04-15T11:58:00.000Z",
      }),
    ]);

    let providerResolved = false;
    const provider = createStubProvider("greenhouse", async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      providerResolved = true;

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 0,
        matchedCount: 0,
        warningCount: 0,
        jobs: [],
      };
    });

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "jobs-first-indexed",
      },
    );

    expect(providerResolved).toBe(false);
    expect(started.queued).toBe(true);
    expect(started.result.jobs).toHaveLength(1);
    expect(started.result.jobs[0]?.title).toBe("Software Engineer");
    expect(started.result.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
      source: "jobs_collection",
    });
    expect(started.result.delivery?.cursor).toBe(1);
    expect(await repository.hasActiveCrawlQueueEntryForSearch(started.result.search._id)).toBe(true);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      targetedReplenishmentQueued: true,
      triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
    });

    const initialDelta = await getSearchJobDeltas(started.result.search._id, 1, {
      repository,
      now: new Date("2026-04-15T12:00:00.000Z"),
    });

    expect(initialDelta.jobs).toEqual([]);
    expect(initialDelta.delivery.cursor).toBe(1);
    await waitForQueueToSettle(repository, started.result.search._id);
  });

  it("skips supplemental crawling when indexed coverage is already sufficient for the session", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "pm-1",
        canonicalUrl: "https://example.com/jobs/pm-1",
        applyUrl: "https://example.com/jobs/pm-1/apply",
        sourceUrl: "https://example.com/jobs/pm-1",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        sourceJobId: "pm-2",
        canonicalUrl: "https://example.com/jobs/pm-2",
        applyUrl: "https://example.com/jobs/pm-2/apply",
        sourceUrl: "https://example.com/jobs/pm-2",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Principal Product Manager",
        sourceJobId: "pm-3",
        canonicalUrl: "https://example.com/jobs/pm-3",
        applyUrl: "https://example.com/jobs/pm-3/apply",
        sourceUrl: "https://example.com/jobs/pm-3",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Group Product Manager",
        sourceJobId: "pm-4",
        canonicalUrl: "https://example.com/jobs/pm-4",
        applyUrl: "https://example.com/jobs/pm-4/apply",
        sourceUrl: "https://example.com/jobs/pm-4",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Lead Product Manager",
        sourceJobId: "pm-5",
        canonicalUrl: "https://example.com/jobs/pm-5",
        applyUrl: "https://example.com/jobs/pm-5/apply",
        sourceUrl: "https://example.com/jobs/pm-5",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
    ]);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    const started = await startSearchFromFilters(
      {
          title: "Product Manager",
          country: "United States",
          state: "Washington",
          city: "Seattle",
          platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:03:00.000Z"),
      },
    );

    expect(started.queued).toBe(false);
    expect(crawlSources).not.toHaveBeenCalled();
    expect(started.result.crawlRun.status).toBe("completed");
    expect(started.result.delivery?.cursor).toBe(5);
    expect(started.result.jobs).toHaveLength(5);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 5,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 5,
      supplementalQueued: false,
      supplementalRunning: false,
      triggerReason: "indexed_coverage_sufficient",
    });

    const delta = await getSearchJobDeltas(
      started.result.search._id,
      started.result.delivery?.cursor ?? 0,
      { repository },
    );

    expect(delta.jobs).toEqual([]);
    expect(delta.delivery.cursor).toBe(5);
  });

  it("paginates final indexed matches without collapsing the total matched count", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(
      repository,
      Array.from({ length: 300 }, (_, index) => {
        const sourceJobId = `pagination-software-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
        return createPersistableJob({
          title: "Software Engineer",
          locationText: "Remote - United States",
          country: "United States",
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
          postingDate: `2026-04-${String((index % 20) + 1).padStart(2, "0")}T00:00:00.000Z`,
          postedAt: `2026-04-${String((index % 20) + 1).padStart(2, "0")}T00:00:00.000Z`,
        });
      }),
    );
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);

    try {
      const active = await createActiveSearchState(
        repository,
        {
          title: "Software Engineer",
          country: "United States",
          platforms: ["greenhouse"],
        },
        "2026-04-15T12:04:00.000Z",
      );

      const firstPage = await getSearchDetails(active.search._id, {
        repository,
        pageSize: 50,
        searchSessionId: active.searchSession._id,
        now: new Date("2026-04-15T12:04:00.000Z"),
      });

      expect(firstPage.jobs).toHaveLength(50);
      expect(firstPage.totalMatchedCount).toBe(300);
      expect(firstPage.finalMatchedCount).toBe(300);
      expect(firstPage.returnedCount).toBe(50);
      expect(firstPage.pageSize).toBe(50);
      expect(firstPage.nextCursor).toBe(50);
      expect(firstPage.hasMore).toBe(true);
      expect(firstPage.searchId).toBe(firstPage.search._id);
      expect(firstPage.searchSessionId).toBe(firstPage.searchSession?._id);
      expect(firstPage.diagnostics.searchResponse).toMatchObject({
        candidateCount: expect.any(Number),
        matchedCount: 300,
        finalMatchedCount: 300,
        totalMatchedCount: 300,
        returnedCount: 50,
        pageSize: 50,
        nextCursor: 50,
        hasMore: true,
      });

      const nextPage = await getSearchDetails(firstPage.search._id, {
        repository,
        cursor: firstPage.nextCursor ?? 0,
        pageSize: 50,
        searchSessionId: firstPage.searchSession?._id,
      });
      const firstPageIds = new Set(firstPage.jobs.map((job) => job._id));
      const nextPageIds = new Set(nextPage.jobs.map((job) => job._id));

      expect(nextPage.jobs).toHaveLength(50);
      expect(nextPage.totalMatchedCount).toBe(300);
      expect(nextPage.returnedCount).toBe(50);
      expect(nextPage.nextCursor).toBe(100);
      expect(nextPage.hasMore).toBe(true);
      expect([...nextPageIds].some((jobId) => firstPageIds.has(jobId))).toBe(false);

      const paginationLogs = infoSpy.mock.calls.filter(
        ([label]) => label === "[search:pagination]",
      ) as Array<[string, Record<string, unknown>]>;
      expect(paginationLogs.length).toBeGreaterThanOrEqual(2);
      expect(paginationLogs[0]?.[1]).toMatchObject({
        searchId: firstPage.search._id,
        searchSessionId: firstPage.searchSession?._id,
        totalMatchedCount: 300,
        returnedCount: 50,
        nextCursor: 50,
        hasMore: true,
      });
    } finally {
      infoSpy.mockRestore();
    }
  }, 10_000);

  it("does not fall back to a full listJobs scan when priming indexed search results", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Remote - United States",
        sourceJobId: "no-full-scan-match",
        canonicalUrl: "https://example.com/jobs/no-full-scan-match",
        applyUrl: "https://example.com/jobs/no-full-scan-match/apply",
        sourceUrl: "https://example.com/jobs/no-full-scan-match",
      }),
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "no-full-scan-noise",
        canonicalUrl: "https://example.com/jobs/no-full-scan-noise",
        applyUrl: "https://example.com/jobs/no-full-scan-noise/apply",
        sourceUrl: "https://example.com/jobs/no-full-scan-noise",
      }),
    ]);

    const listJobsSpy = vi.spyOn(repository, "listJobs");
    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const started = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:04:00.000Z"),
      },
    );

    expect(listJobsSpy).not.toHaveBeenCalled();
    expect(started.result.jobs.map((job) => job.title)).toEqual(["Software Engineer"]);
  });

  it("lets the indexed candidate prefilter reach generic same-family software titles for semantic evaluation", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Developer",
        normalizedTitle: "developer",
        titleNormalized: "developer",
        sourceJobId: "generic-developer",
        canonicalUrl: "https://example.com/jobs/generic-developer",
        applyUrl: "https://example.com/jobs/generic-developer/apply",
        sourceUrl: "https://example.com/jobs/generic-developer",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Product Manager",
        normalizedTitle: "product manager",
        titleNormalized: "product manager",
        sourceJobId: "product-noise",
        canonicalUrl: "https://example.com/jobs/product-noise",
        applyUrl: "https://example.com/jobs/product-noise/apply",
        sourceUrl: "https://example.com/jobs/product-noise",
        locationText: "Remote - United States",
      }),
    ]);

    const result = await getIndexedJobsForSearch(repository, {
      title: "Software Engineer",
      country: "United States",
    });

    expect(result.candidateQuery).toMatchObject({
      usedFamilyRoleFallback: true,
      usedLocationTextFallback: true,
    });
    expect(result.matches.map(({ job }) => job.sourceJobId)).toEqual([
      "generic-developer",
    ]);
    expect(result.matches[0]?.evaluation.titleMatch).toMatchObject({
      tier: "same_family_related",
      matches: true,
    });
  });

  it("recovers legacy indexed US jobs whose persisted document lacks resolvedLocation and country fields", async () => {
    const db = new FakeDb();
    const repository = new JobCrawlerRepository(db);

    await db.collection(collectionNames.jobs).insertOne({
      _id: "legacy-austin",
      title: "Software Engineer",
      company: "Acme",
      locationText: "Austin, TX",
      sourcePlatform: "greenhouse",
      sourceJobId: "legacy-austin",
      sourceUrl: "https://example.com/jobs/legacy-austin",
      applyUrl: "https://example.com/jobs/legacy-austin/apply",
      discoveredAt: "2026-04-15T12:00:00.000Z",
      crawledAt: "2026-04-15T12:00:00.000Z",
      companyNormalized: "acme",
      titleNormalized: "software engineer",
      normalizedTitle: "software engineer",
      locationNormalized: "austin tx",
      normalizedLocation: "austin tx",
      contentFingerprint: "legacy-austin",
      canonicalJobKey: "platform:greenhouse:acme:legacy-austin",
      firstSeenAt: "2026-04-15T12:00:00.000Z",
      lastSeenAt: "2026-04-15T12:00:00.000Z",
      indexedAt: "2026-04-15T12:00:00.000Z",
      isActive: true,
      sourceLookupKeys: ["greenhouse:legacy-austin"],
      sourceProvenance: [],
      crawlRunIds: ["run-legacy-austin"],
      linkStatus: "unknown",
      rawSourceMetadata: {},
    });

    const result = await getIndexedJobsForSearch(repository, {
      title: "Software Engineer",
      country: "United States",
    });

    expect(result.candidateQuery.usedLocationTextFallback).toBe(true);
    expect(result.matches.map(({ job }) => job.sourceJobId)).toEqual([
      "legacy-austin",
    ]);
    expect(result.matches[0]?.evaluation.locationMatch).toMatchObject({
      matches: true,
      jobDiagnostics: {
        raw: "Austin, TX",
        isUnitedStates: true,
      },
    });
  });

  it("returns matching background-indexed jobs in delta responses for an active search session", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:01:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Senior Software Engineer",
        sourceJobId: "background-match",
        canonicalUrl: "https://example.com/jobs/background-match",
        applyUrl: "https://example.com/jobs/background-match/apply",
        sourceUrl: "https://example.com/jobs/background-match",
        locationText: "Remote - United States",
      }),
    ]);

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:01:00.000Z"),
    });

    expect(delta.jobs.map((job) => job.sourceJobId)).toEqual(["background-match"]);
    expect(delta.delivery.cursor).toBe(initial.delivery?.cursor ?? 0);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("does not leak non-matching background-indexed jobs into an active session delta", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:02:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "background-non-match",
        canonicalUrl: "https://example.com/jobs/background-non-match",
        applyUrl: "https://example.com/jobs/background-non-match/apply",
        sourceUrl: "https://example.com/jobs/background-non-match",
        locationText: "Remote - United States",
      }),
    ]);

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:02:00.000Z"),
    });

    expect(delta.jobs).toEqual([]);
    expect(delta.delivery.cursor).toBe(initial.delivery?.cursor ?? 0);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("returns only Canada or Remote Canada jobs for machine learning engineer Canada searches", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const jobs = [
      createPersistableJob({
        title: "Applied AI Engineer",
        country: "Japan",
        locationText: "Tokyo, Japan",
        sourceJobId: "tokyo-ai",
        canonicalUrl: "https://example.com/jobs/tokyo-ai",
        applyUrl: "https://example.com/jobs/tokyo-ai/apply",
        sourceUrl: "https://example.com/jobs/tokyo-ai",
      }),
      createPersistableJob({
        title: "Applied AI Engineer",
        country: "South Korea",
        locationText: "Seoul, South Korea",
        sourceJobId: "seoul-ai",
        canonicalUrl: "https://example.com/jobs/seoul-ai",
        applyUrl: "https://example.com/jobs/seoul-ai/apply",
        sourceUrl: "https://example.com/jobs/seoul-ai",
      }),
      createPersistableJob({
        title: "Machine Learning Engineer",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Toronto, Canada",
        sourceJobId: "toronto-mle",
        canonicalUrl: "https://example.com/jobs/toronto-mle",
        applyUrl: "https://example.com/jobs/toronto-mle/apply",
        sourceUrl: "https://example.com/jobs/toronto-mle",
      }),
      createPersistableJob({
        title: "ML Engineer",
        country: undefined,
        state: undefined,
        city: undefined,
        locationText: "Vancouver, BC",
        sourceJobId: "vancouver-ml",
        canonicalUrl: "https://example.com/jobs/vancouver-ml",
        applyUrl: "https://example.com/jobs/vancouver-ml/apply",
        sourceUrl: "https://example.com/jobs/vancouver-ml",
      }),
      createPersistableJob({
        title: "AI Engineer",
        country: "Canada",
        locationText: "Remote Canada",
        remoteType: "remote",
        sourceJobId: "remote-ca-ai",
        canonicalUrl: "https://example.com/jobs/remote-ca-ai",
        applyUrl: "https://example.com/jobs/remote-ca-ai/apply",
        sourceUrl: "https://example.com/jobs/remote-ca-ai",
      }),
    ];

    await seedIndexedJobs(repository, jobs);

    const indexed = await getIndexedJobsForSearch(repository, {
      title: "machine learning engineer",
      country: "Canada",
    });
    expect(indexed.matches.map(({ job }) => job.sourceJobId).sort()).toEqual([
      "remote-ca-ai",
      "toronto-mle",
      "vancouver-ml",
    ]);
    expect(indexed.candidateCount).toBeGreaterThanOrEqual(3);
    expect(indexed.candidateChannelBreakdown).toMatchObject({
      finalMatchedCount: 3,
      returnedCount: 3,
    });

    const active = await createActiveSearchState(repository, {
      title: "machine learning engineer",
      country: "Canada",
    });
    await repository.persistJobs(
      active.crawlRun._id,
      [jobs[0]!, jobs[1]!, jobs[2]!, jobs[3]!, jobs[4]!],
      { searchSessionId: active.searchSession._id },
    );

    const details = await getSearchDetails(active.search._id, { repository });
    expect(details.jobs.map((job) => job.sourceJobId).sort()).toEqual([
      "remote-ca-ai",
      "toronto-mle",
      "vancouver-ml",
    ]);
    expect(details.jobs.map((job) => job.sourceJobId)).not.toContain("tokyo-ai");
    expect(details.jobs.map((job) => job.sourceJobId)).not.toContain("seoul-ai");
    expect(details.diagnostics.searchResponse).toMatchObject({
      parsedFilters: {
        title: "machine learning engineer",
        country: "Canada",
      },
      candidateCount: expect.any(Number),
      matchedCount: 3,
      excludedByLocationCount: expect.any(Number),
      searchId: active.search._id,
      sessionId: active.searchSession._id,
    });
    expect(
      details.jobs.every((job) =>
        JSON.stringify(job.rawSourceMetadata).includes("locationMatch"),
      ),
    ).toBe(true);
  });

  it("uses multi-channel indexed retrieval so machine learning engineer US recall is not capped by one narrow candidate query", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const makeJobs = (
      count: number,
      title: string,
      prefix: string,
      location: {
        country: string;
        locationText: string;
        state?: string;
        city?: string;
      },
    ) =>
      Array.from({ length: count }, (_, index) => {
        const sourceJobId = `${prefix}-${index + 1}`;
        const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;

        return createPersistableJob({
          title,
          sourceJobId,
          canonicalUrl,
          applyUrl: `${canonicalUrl}/apply`,
          sourceUrl: canonicalUrl,
          country: location.country,
          locationText: location.locationText,
          state: location.state,
          city: location.city,
          dedupeFingerprint: `dedupe:${sourceJobId}`,
          contentFingerprint: `content:${sourceJobId}`,
          contentHash: `content-hash:${sourceJobId}`,
        });
      });
    const outsideLocations = [
      { country: "Canada", locationText: "Toronto, Canada", state: "Ontario", city: "Toronto" },
      { country: "Japan", locationText: "Tokyo, Japan", city: "Tokyo" },
      { country: "South Korea", locationText: "Seoul, South Korea", city: "Seoul" },
    ];
    const outsideJobs = Array.from({ length: 50 }, (_, index) => {
      const location = outsideLocations[index % outsideLocations.length]!;
      const title = index % 2 === 0 ? "AI Engineer" : "Machine Learning Engineer";
      const sourceJobId = `outside-ai-ml-${index + 1}`;
      const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;

      return createPersistableJob({
        title,
        sourceJobId,
        canonicalUrl,
        applyUrl: `${canonicalUrl}/apply`,
        sourceUrl: canonicalUrl,
        country: location.country,
        locationText: location.locationText,
        state: location.state,
        city: location.city,
        dedupeFingerprint: `dedupe:${sourceJobId}`,
        contentFingerprint: `content:${sourceJobId}`,
        contentHash: `content-hash:${sourceJobId}`,
      });
    });

    await seedIndexedJobs(repository, [
      ...makeJobs(100, "Machine Learning Engineer", "mle-us", {
        country: "United States",
        locationText: "Remote - United States",
      }),
      ...makeJobs(100, "ML Engineer", "ml-us", {
        country: "United States",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      ...makeJobs(100, "AI Engineer", "ai-us", {
        country: "United States",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
      }),
      ...makeJobs(100, "Applied AI Engineer", "applied-ai-us", {
        country: "United States",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      ...makeJobs(50, "LLM Engineer", "llm-us", {
        country: "United States",
        locationText: "Remote - California",
        state: "California",
      }),
      ...outsideJobs,
    ]);

    const indexed = await getIndexedJobsForSearch(repository, {
      title: "machine learning engineer",
      country: "United States",
    });

    expect(indexed.matchedCount).toBeGreaterThan(34);
    expect(indexed.matchedCount).toBeGreaterThanOrEqual(400);
    expect(indexed.matches.every(({ job }) => job.country === "United States")).toBe(true);
    const matchedIds = new Set(indexed.matches.map(({ job }) => job.sourceJobId));
    expect(outsideJobs.some((job) => matchedIds.has(job.sourceJobId))).toBe(false);
    expect(indexed.candidateChannelBreakdown).toMatchObject({
      exactTitleCount: expect.any(Number),
      aliasTitleCount: expect.any(Number),
      conceptCount: expect.any(Number),
      familyCount: expect.any(Number),
      geoCount: expect.any(Number),
      legacyTitleFallbackCount: expect.any(Number),
      legacyLocationFallbackCount: expect.any(Number),
      mergedCandidateCount: indexed.candidateCount,
      finalMatchedCount: indexed.matchedCount,
      returnedCount: indexed.matches.length,
    });
    expect(indexed.candidateChannelBreakdown.aliasTitleCount).toBeGreaterThan(0);
    expect(indexed.candidateChannelBreakdown.geoCount).toBeGreaterThan(0);
    expect(indexed.excludedByLocationCount).toBeGreaterThan(0);
  });

  it("dedupes overlapping session and indexed deltas when background and active-session writes converge on the same job", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const active = await createActiveSearchState(repository, {
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
    const initial = await getSearchDetails(active.search._id, { repository });
    const backgroundRun = await repository.createCrawlRun(
      active.search._id,
      "2026-04-15T12:03:00.000Z",
    );

    await repository.persistJobs(backgroundRun._id, [
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "shared-active-background",
        canonicalUrl: "https://example.com/jobs/shared-active-background",
        applyUrl: "https://example.com/jobs/shared-active-background/apply",
        sourceUrl: "https://example.com/jobs/shared-active-background",
        locationText: "Remote - United States",
      }),
    ]);
    await repository.persistJobs(
      active.crawlRun._id,
      [
        createPersistableJob({
          title: "Software Engineer",
          sourceJobId: "shared-active-background",
          canonicalUrl: "https://example.com/jobs/shared-active-background",
          applyUrl: "https://example.com/jobs/shared-active-background/apply",
          sourceUrl: "https://example.com/jobs/shared-active-background",
          locationText: "Remote - United States",
        }),
      ],
      { searchSessionId: active.searchSession._id },
    );

    const delta = await getSearchJobDeltas(active.search._id, initial.delivery?.cursor ?? 0, {
      repository,
      afterIndexedCursor: initial.delivery?.indexedCursor ?? 0,
      now: new Date("2026-04-15T12:03:00.000Z"),
    });

    expect(delta.jobs).toHaveLength(1);
    expect(delta.jobs[0]?.sourceJobId).toBe("shared-active-background");
    expect(delta.delivery.cursor).toBe(1);
    expect(delta.delivery.indexedCursor).toBeGreaterThan(initial.delivery?.indexedCursor ?? 0);
  });

  it("does not make request-time crawl primary when indexed coverage is empty", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const discover = vi.fn(async () => [
      classifySourceCandidate({
        url: "https://boards.greenhouse.io/acme",
        token: "acme",
        confidence: "high",
        discoveryMethod: "configured_env",
      }),
    ]);

    const started = await startSearchFromFilters(
      {
        title: "Business Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [],
        discovery: { discover },
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:10:00.000Z"),
        requestOwnerKey: "empty-index-background-request",
      },
    );

    expect(started.queued).toBe(false);
    expect(started.result.jobs).toEqual([]);
    expect(started.result.crawlRun.status).toBe("completed");
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 0,
      totalVisibleResultsCount: 0,
      supplementalQueued: false,
      supplementalRunning: false,
      triggerReason: "indexed_empty_background_requested",
      backgroundIngestion: expect.objectContaining({
        status: expect.stringMatching(/^(started|already_active)$/),
        systemProfileId: expect.any(String),
      }),
    });
    expect(discover).not.toHaveBeenCalled();
  });

  it("requests background replenishment instead of request-time crawl when sparse indexed coverage is stale in balanced mode", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Data Analyst",
        sourceJobId: "stale-balanced-data-analyst",
        canonicalUrl: "https://example.com/jobs/stale-balanced-data-analyst",
        applyUrl: "https://example.com/jobs/stale-balanced-data-analyst/apply",
        sourceUrl: "https://example.com/jobs/stale-balanced-data-analyst",
        locationText: "Remote - United States",
        postingDate: "2026-03-01T00:00:00.000Z",
        discoveredAt: "2026-03-01T12:00:00.000Z",
        crawledAt: "2026-03-01T12:00:00.000Z",
        indexedAt: "2026-03-01T12:00:00.000Z",
      }),
    ]);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    const started = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "stale-balanced-data-analyst-background",
      },
    );

    expect(started.queued).toBe(true);
    expect(await repository.hasActiveCrawlQueueEntryForSearch(started.result.search._id)).toBe(true);
    expect(started.result.jobs.map((job) => job.title)).toEqual(["Data Analyst"]);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      targetedReplenishmentQueued: true,
      triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
    });
    expect(started.result.diagnostics.session?.latestIndexedJobAgeMs).toBeGreaterThan(0);
    await waitForQueueToSettle(repository, started.result.search._id);
  });

  it("triggers bounded supplemental freshness recovery only when deep mode explicitly asks for it", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Data Analyst",
        sourceJobId: "stale-data-analyst",
        canonicalUrl: "https://example.com/jobs/stale-data-analyst",
        applyUrl: "https://example.com/jobs/stale-data-analyst/apply",
        sourceUrl: "https://example.com/jobs/stale-data-analyst",
        locationText: "Remote - United States",
        postingDate: "2026-03-01T00:00:00.000Z",
        discoveredAt: "2026-03-01T12:00:00.000Z",
        crawledAt: "2026-03-01T12:00:00.000Z",
        indexedAt: "2026-03-01T12:00:00.000Z",
      }),
    ]);

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 1,
      fetchedCount: 1,
      matchedCount: 1,
      warningCount: 0,
      jobs: [
        {
          title: "Senior Data Analyst",
          company: "Acme",
          country: "United States",
          locationText: "Chicago, IL",
          state: "Illinois",
          city: "Chicago",
          sourcePlatform: "greenhouse" as const,
          sourceJobId: "fresh-data-analyst",
          sourceUrl: "https://example.com/jobs/fresh-data-analyst",
          applyUrl: "https://example.com/jobs/fresh-data-analyst/apply",
          canonicalUrl: "https://example.com/jobs/fresh-data-analyst",
          discoveredAt: "2026-04-15T12:00:00.000Z",
          rawSourceMetadata: {},
        },
      ],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    const started = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
        crawlMode: "deep",
        platforms: ["greenhouse"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:00:00.000Z"),
        requestOwnerKey: "stale-data-analyst-recovery",
      },
    );

    expect(started.queued).toBe(true);
    expect(started.result.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 0,
      totalVisibleResultsCount: 1,
      supplementalQueued: true,
      supplementalRunning: true,
      triggerReason: "freshness_recovery",
    });
    expect(started.result.diagnostics.session?.latestIndexedJobAgeMs).toBeGreaterThan(0);

    await new Promise((resolve) => setTimeout(resolve, 100));

    const final = await getSearchDetails(started.result.search._id, {
      repository,
      now: new Date("2026-04-15T12:00:01.000Z"),
    });

    expect(final.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining(["Data Analyst", "Senior Data Analyst"]),
    );
    expect(final.diagnostics.session).toMatchObject({
      indexedResultsCount: 1,
      supplementalResultsCount: 1,
      totalVisibleResultsCount: 2,
      supplementalQueued: true,
      supplementalRunning: false,
      triggerReason: "freshness_recovery",
    });
  });

  it("serves representative United States searches from the index first and queues targeted replenishment below policy target", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const scenarios = [
      {
        title: "Software Engineer",
        jobs: [
          createPersistableJob({
            title: "Software Engineer",
            sourceJobId: "scenario-se-1",
            canonicalUrl: "https://example.com/jobs/scenario-se-1",
            applyUrl: "https://example.com/jobs/scenario-se-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Backend Engineer",
            sourceJobId: "scenario-se-2",
            canonicalUrl: "https://example.com/jobs/scenario-se-2",
            applyUrl: "https://example.com/jobs/scenario-se-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-2",
            locationText: "Seattle, WA",
            state: "Washington",
            city: "Seattle",
          }),
          createPersistableJob({
            title: "Full Stack Engineer",
            sourceJobId: "scenario-se-3",
            canonicalUrl: "https://example.com/jobs/scenario-se-3",
            applyUrl: "https://example.com/jobs/scenario-se-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-se-3",
            locationText: "Austin, TX",
            state: "Texas",
            city: "Austin",
          }),
        ],
      },
      {
        title: "Data Analyst",
        jobs: [
          createPersistableJob({
            title: "Data Analyst",
            sourceJobId: "scenario-da-1",
            canonicalUrl: "https://example.com/jobs/scenario-da-1",
            applyUrl: "https://example.com/jobs/scenario-da-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Senior Data Analyst",
            sourceJobId: "scenario-da-2",
            canonicalUrl: "https://example.com/jobs/scenario-da-2",
            applyUrl: "https://example.com/jobs/scenario-da-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-2",
            locationText: "New York, NY",
            state: "New York",
            city: "New York",
          }),
          createPersistableJob({
            title: "Product Data Analyst",
            sourceJobId: "scenario-da-3",
            canonicalUrl: "https://example.com/jobs/scenario-da-3",
            applyUrl: "https://example.com/jobs/scenario-da-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-da-3",
            locationText: "Chicago, IL",
            state: "Illinois",
            city: "Chicago",
          }),
        ],
      },
      {
        title: "Business Analyst",
        jobs: [
          createPersistableJob({
            title: "Business Analyst",
            sourceJobId: "scenario-ba-1",
            canonicalUrl: "https://example.com/jobs/scenario-ba-1",
            applyUrl: "https://example.com/jobs/scenario-ba-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-1",
            locationText: "Chicago, IL",
            state: "Illinois",
            city: "Chicago",
          }),
          createPersistableJob({
            title: "Senior Business Analyst",
            sourceJobId: "scenario-ba-2",
            canonicalUrl: "https://example.com/jobs/scenario-ba-2",
            applyUrl: "https://example.com/jobs/scenario-ba-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-2",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Business Systems Analyst",
            sourceJobId: "scenario-ba-3",
            canonicalUrl: "https://example.com/jobs/scenario-ba-3",
            applyUrl: "https://example.com/jobs/scenario-ba-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-ba-3",
            locationText: "Boston, MA",
            state: "Massachusetts",
            city: "Boston",
          }),
        ],
      },
      {
        title: "Product Manager",
        jobs: [
          createPersistableJob({
            title: "Product Manager",
            sourceJobId: "scenario-pm-1",
            canonicalUrl: "https://example.com/jobs/scenario-pm-1",
            applyUrl: "https://example.com/jobs/scenario-pm-1/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-1",
            locationText: "Remote - United States",
          }),
          createPersistableJob({
            title: "Senior Product Manager",
            sourceJobId: "scenario-pm-2",
            canonicalUrl: "https://example.com/jobs/scenario-pm-2",
            applyUrl: "https://example.com/jobs/scenario-pm-2/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-2",
            locationText: "Seattle, WA",
            state: "Washington",
            city: "Seattle",
          }),
          createPersistableJob({
            title: "Technical Product Manager",
            sourceJobId: "scenario-pm-3",
            canonicalUrl: "https://example.com/jobs/scenario-pm-3",
            applyUrl: "https://example.com/jobs/scenario-pm-3/apply",
            sourceUrl: "https://example.com/jobs/scenario-pm-3",
            locationText: "Austin, TX",
            state: "Texas",
            city: "Austin",
          }),
        ],
      },
    ] as const;

    for (const scenario of scenarios) {
      await seedIndexedJobs(
        repository,
        Array.from({ length: 120 }, (_, index) => {
          const base = scenario.jobs[index % scenario.jobs.length] as PersistableTestJob;
          const sourceJobId = `${base.sourceJobId}-strong-${index + 1}`;
          const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
          return {
            ...base,
            canonicalJobKey: `platform:${base.sourcePlatform}:${base.sourceCompanySlug}:${sourceJobId}`,
            sourceJobId,
            canonicalUrl,
            applyUrl: `${canonicalUrl}/apply`,
            sourceUrl: canonicalUrl,
            sourceLookupKeys: [`${base.sourcePlatform}:${sourceJobId}`],
            sourceProvenance: [
              {
                sourcePlatform: base.sourcePlatform,
                sourceJobId,
                sourceUrl: canonicalUrl,
                applyUrl: `${canonicalUrl}/apply`,
                resolvedUrl: `${canonicalUrl}/apply`,
                canonicalUrl,
                discoveredAt: base.discoveredAt,
                rawSourceMetadata: {},
              },
            ],
            dedupeFingerprint: `dedupe:${sourceJobId}`,
            contentFingerprint: `content:${sourceJobId}`,
            contentHash: `content-hash:${sourceJobId}`,
          };
        }),
      );
    }

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);

    for (const scenario of scenarios) {
      const started = await startSearchFromFilters(
        {
          title: scenario.title,
          country: "United States",
          crawlMode: "fast",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:20:00.000Z"),
          requestOwnerKey: `representative-${scenario.title}`,
        },
      );

      expect(started.queued).toBe(true);
      expect(started.result.jobs.length).toBeGreaterThanOrEqual(3);
      expect(
        started.result.jobs.every(
          (job) => job.resolvedLocation?.isUnitedStates ?? job.country === "United States",
        ),
      ).toBe(true);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: expect.any(Number),
        supplementalResultsCount: 0,
        totalVisibleResultsCount: expect.any(Number),
        supplementalQueued: true,
        supplementalRunning: true,
        targetedReplenishmentQueued: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      await waitForQueueToSettle(repository, started.result.search._id);
    }

    expect(crawlSources).toHaveBeenCalled();
  });

  it("serves requested role-family and country scenarios from indexed jobs using resolved location evidence", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const withoutTopLevelCountry = (job: PersistableTestJob): PersistableTestJob => {
      const { country: _country, ...rest } = job;
      return rest;
    };
    const scenarioJobs = [
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "scenario-expanded-se-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-1",
        locationText: "Remote - United States",
        resolvedLocation: {
          country: "United States",
          isRemote: true,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - United States" }],
        },
      }),
      createPersistableJob({
        title: "Backend Developer",
        sourceJobId: "scenario-expanded-se-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-2",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        resolvedLocation: {
          country: "United States",
          state: "Washington",
          stateCode: "WA",
          city: "Seattle",
          isRemote: false,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Seattle, WA" }],
        },
      }),
      createPersistableJob({
        title: "Full Stack Engineer",
        sourceJobId: "scenario-expanded-se-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-se-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-se-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-se-3",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
        resolvedLocation: {
          country: "United States",
          state: "Texas",
          stateCode: "TX",
          city: "Austin",
          isRemote: false,
          isUnitedStates: true,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Austin, TX" }],
        },
      }),
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "scenario-expanded-pm-us-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-1",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        sourceJobId: "scenario-expanded-pm-us-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-2",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      createPersistableJob({
        title: "Technical Product Manager",
        sourceJobId: "scenario-expanded-pm-us-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-pm-us-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-pm-us-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-pm-us-3",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
      }),
      createPersistableJob({
        title: "AI Engineer",
        sourceJobId: "scenario-expanded-ai-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-1",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Machine Learning Engineer",
        sourceJobId: "scenario-expanded-ai-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-2",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Applied Scientist",
        sourceJobId: "scenario-expanded-ai-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-3",
        locationText: "Cambridge, MA",
        state: "Massachusetts",
        city: "Cambridge",
      }),
      createPersistableJob({
        title: "Research Scientist",
        sourceJobId: "scenario-expanded-ai-4",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-4",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-4/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-4",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
      }),
      createPersistableJob({
        title: "Data Scientist",
        sourceJobId: "scenario-expanded-ai-5",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-ai-5",
        applyUrl: "https://example.com/jobs/scenario-expanded-ai-5/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-ai-5",
        locationText: "Remote - United States",
      }),
      withoutTopLevelCountry(createPersistableJob({
        title: "Solutions Architect",
        sourceJobId: "scenario-expanded-canada-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-1",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
        resolvedLocation: {
          country: "Canada",
          state: "Ontario",
          stateCode: "ON",
          city: "Toronto",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Toronto, ON" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Cloud Architect",
        sourceJobId: "scenario-expanded-canada-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-2",
        locationText: "Vancouver, BC",
        state: "British Columbia",
        city: "Vancouver",
        resolvedLocation: {
          country: "Canada",
          state: "British Columbia",
          stateCode: "BC",
          city: "Vancouver",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Vancouver, BC" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Solutions Engineer",
        sourceJobId: "scenario-expanded-canada-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-canada-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-canada-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-canada-3",
        locationText: "Remote - Canada",
        resolvedLocation: {
          country: "Canada",
          isRemote: true,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - Canada" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Product Manager",
        sourceJobId: "scenario-expanded-germany-1",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-1",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-1/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-1",
        locationText: "Berlin, Germany",
        state: "Berlin",
        city: "Berlin",
        resolvedLocation: {
          country: "Germany",
          state: "Berlin",
          city: "Berlin",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Berlin, Germany" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Technical Product Manager",
        sourceJobId: "scenario-expanded-germany-2",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-2",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-2/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-2",
        locationText: "Munich, Germany",
        state: "Bavaria",
        city: "Munich",
        resolvedLocation: {
          country: "Germany",
          state: "Bavaria",
          stateCode: "BY",
          city: "Munich",
          isRemote: false,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Munich, Germany" }],
        },
      })),
      withoutTopLevelCountry(createPersistableJob({
        title: "Product Owner",
        sourceJobId: "scenario-expanded-germany-3",
        canonicalUrl: "https://example.com/jobs/scenario-expanded-germany-3",
        applyUrl: "https://example.com/jobs/scenario-expanded-germany-3/apply",
        sourceUrl: "https://example.com/jobs/scenario-expanded-germany-3",
        locationText: "Remote - Germany",
        resolvedLocation: {
          country: "Germany",
          isRemote: true,
          isUnitedStates: false,
          confidence: "high",
          evidence: [{ source: "location_text", value: "Remote - Germany" }],
        },
      })),
    ];

    await seedIndexedJobs(
      repository,
      scenarioJobs.flatMap((base, baseIndex) =>
        Array.from({ length: 40 }, (_, index) => {
          const sourceJobId = `${base.sourceJobId}-expanded-${index + 1}`;
          const canonicalUrl = `https://example.com/jobs/${sourceJobId}`;
          return {
            ...base,
            canonicalJobKey: `platform:${base.sourcePlatform}:${base.sourceCompanySlug}:${sourceJobId}`,
            sourceJobId,
            canonicalUrl,
            applyUrl: `${canonicalUrl}/apply`,
            sourceUrl: canonicalUrl,
            sourceLookupKeys: [`${base.sourcePlatform}:${sourceJobId}`],
            sourceProvenance: [
              {
                sourcePlatform: base.sourcePlatform,
                sourceJobId,
                sourceUrl: canonicalUrl,
                applyUrl: `${canonicalUrl}/apply`,
                resolvedUrl: `${canonicalUrl}/apply`,
                canonicalUrl,
                discoveredAt: base.discoveredAt,
                rawSourceMetadata: {},
              },
            ],
            dedupeFingerprint: `dedupe:expanded:${baseIndex}:${index}`,
            contentFingerprint: `content:expanded:${baseIndex}:${index}`,
            contentHash: `content-hash:expanded:${baseIndex}:${index}`,
          };
        }),
      ),
    );

    const crawlSources: CrawlProvider["crawlSources"] = vi.fn(async () => ({
      provider: "greenhouse" as const,
      status: "success" as const,
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));
    const provider = createStubProvider("greenhouse", crawlSources);
    const scenarios = [
      {
        title: "software engineer",
        country: "United States",
        expectedTitles: ["Software Engineer", "Backend Developer", "Full Stack Engineer"],
      },
      {
        title: "product manager",
        country: "United States",
        expectedTitles: ["Product Manager", "Senior Product Manager", "Technical Product Manager"],
      },
      {
        title: "applied scientist",
        country: "United States",
        expectedTitles: ["Applied Scientist", "Research Scientist", "Data Scientist"],
      },
      {
        title: "research scientist",
        country: "United States",
        expectedTitles: ["Research Scientist", "Applied Scientist", "Data Scientist"],
      },
      {
        title: "ai engineer",
        country: "United States",
        expectedTitles: ["AI Engineer", "Machine Learning Engineer", "Applied Scientist"],
      },
      {
        title: "solution architect",
        country: "Canada",
        expectedTitles: ["Solutions Architect", "Cloud Architect", "Solutions Engineer"],
      },
      {
        title: "product manager",
        country: "Germany",
        expectedTitles: ["Product Manager", "Technical Product Manager", "Product Owner"],
      },
    ];

    for (const scenario of scenarios) {
      const started = await startSearchFromFilters(
        {
          title: scenario.title,
          country: scenario.country,
          crawlMode: "fast",
          platforms: ["greenhouse"],
        },
        {
          repository,
          providers: [provider],
          discovery: createDiscovery(),
          fetchImpl: vi.fn() as unknown as typeof fetch,
          now: new Date("2026-04-15T12:30:00.000Z"),
          requestOwnerKey: `expanded-${scenario.title}-${scenario.country}`,
        },
      );

      expect(started.queued).toBe(true);
      expect(started.result.jobs.map((job) => job.title)).toEqual(
        expect.arrayContaining(scenario.expectedTitles),
      );
      expect(started.result.jobs.length).toBeGreaterThanOrEqual(scenario.expectedTitles.length);
      expect(started.result.jobs.every((job) => job.rawSourceMetadata.indexedSearch)).toBe(true);
      expect(
        started.result.jobs.every(
          (job) => job.resolvedLocation?.country === scenario.country || job.country === scenario.country,
        ),
      ).toBe(true);
      expect(started.result.diagnostics.session).toMatchObject({
        indexedResultsCount: expect.any(Number),
        supplementalResultsCount: 0,
        totalVisibleResultsCount: expect.any(Number),
        supplementalQueued: true,
        supplementalRunning: true,
        targetedReplenishmentQueued: true,
        triggerReason: "insufficient_indexed_coverage_targeted_replenishment",
      });
      await waitForQueueToSettle(repository, started.result.search._id);
      expect(started.result.jobs[0]?.rawSourceMetadata.indexedSearch).toMatchObject({
        candidateQuery: expect.objectContaining({
          strategy: "coarse_prefilter",
          usedLocationPrefilter: true,
        }),
        titleMatch: expect.objectContaining({
          explanation: expect.any(String),
        }),
        locationMatch: expect.objectContaining({
          matches: true,
          explanation: expect.any(String),
        }),
      });
    }

    expect(crawlSources).toHaveBeenCalled();
  });

  it("serves broad family-aware indexed retrieval scenarios while preserving title precision", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Backend Developer",
        sourceJobId: "family-software-backend",
        canonicalUrl: "https://example.com/jobs/family-software-backend",
        applyUrl: "https://example.com/jobs/family-software-backend/apply",
        sourceUrl: "https://example.com/jobs/family-software-backend",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
      }),
      createPersistableJob({
        title: "Platform Engineer",
        sourceJobId: "family-software-platform",
        canonicalUrl: "https://example.com/jobs/family-software-platform",
        applyUrl: "https://example.com/jobs/family-software-platform/apply",
        sourceUrl: "https://example.com/jobs/family-software-platform",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Business Intelligence Analyst",
        sourceJobId: "family-data-analytics-bi",
        canonicalUrl: "https://example.com/jobs/family-data-analytics-bi",
        applyUrl: "https://example.com/jobs/family-data-analytics-bi/apply",
        sourceUrl: "https://example.com/jobs/family-data-analytics-bi",
        country: "Canada",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
      }),
      createPersistableJob({
        title: "Product Analyst",
        sourceJobId: "family-data-analytics-product",
        canonicalUrl: "https://example.com/jobs/family-data-analytics-product",
        applyUrl: "https://example.com/jobs/family-data-analytics-product/apply",
        sourceUrl: "https://example.com/jobs/family-data-analytics-product",
        country: "Canada",
        locationText: "Vancouver, BC",
        state: "British Columbia",
        city: "Vancouver",
      }),
      createPersistableJob({
        title: "AI Engineer",
        sourceJobId: "family-ml-ai",
        canonicalUrl: "https://example.com/jobs/family-ml-ai",
        applyUrl: "https://example.com/jobs/family-ml-ai/apply",
        sourceUrl: "https://example.com/jobs/family-ml-ai",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Applied Scientist",
        sourceJobId: "family-ml-applied",
        canonicalUrl: "https://example.com/jobs/family-ml-applied",
        applyUrl: "https://example.com/jobs/family-ml-applied/apply",
        sourceUrl: "https://example.com/jobs/family-ml-applied",
        locationText: "Cambridge, MA",
        state: "Massachusetts",
        city: "Cambridge",
      }),
      createPersistableJob({
        title: "Technical Product Manager",
        sourceJobId: "family-product-technical",
        canonicalUrl: "https://example.com/jobs/family-product-technical",
        applyUrl: "https://example.com/jobs/family-product-technical/apply",
        sourceUrl: "https://example.com/jobs/family-product-technical",
        country: "Canada",
        locationText: "Remote - Canada",
      }),
      createPersistableJob({
        title: "Product Owner",
        sourceJobId: "family-product-owner",
        canonicalUrl: "https://example.com/jobs/family-product-owner",
        applyUrl: "https://example.com/jobs/family-product-owner/apply",
        sourceUrl: "https://example.com/jobs/family-product-owner",
        country: "Canada",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
      }),
      createPersistableJob({
        title: "Cloud Engineer",
        sourceJobId: "family-devops-cloud",
        canonicalUrl: "https://example.com/jobs/family-devops-cloud",
        applyUrl: "https://example.com/jobs/family-devops-cloud/apply",
        sourceUrl: "https://example.com/jobs/family-devops-cloud",
        country: "Germany",
        locationText: "Berlin, Germany",
      }),
      createPersistableJob({
        title: "Security Engineer",
        sourceJobId: "family-devops-security",
        canonicalUrl: "https://example.com/jobs/family-devops-security",
        applyUrl: "https://example.com/jobs/family-devops-security/apply",
        sourceUrl: "https://example.com/jobs/family-devops-security",
        country: "Germany",
        locationText: "Munich, Germany",
      }),
      createPersistableJob({
        title: "Recruiter",
        sourceJobId: "family-noise-recruiter",
        canonicalUrl: "https://example.com/jobs/family-noise-recruiter",
        applyUrl: "https://example.com/jobs/family-noise-recruiter/apply",
        sourceUrl: "https://example.com/jobs/family-noise-recruiter",
        country: "Germany",
        locationText: "Berlin, Germany",
      }),
      createPersistableJob({
        title: "Finance Manager",
        sourceJobId: "family-noise-finance",
        canonicalUrl: "https://example.com/jobs/family-noise-finance",
        applyUrl: "https://example.com/jobs/family-noise-finance/apply",
        sourceUrl: "https://example.com/jobs/family-noise-finance",
        country: "United States",
        locationText: "Remote - United States",
      }),
    ]);

    const scenarios = [
      {
        title: "software engineer",
        country: "United States",
        expectedIds: ["family-software-backend", "family-software-platform"],
      },
      {
        title: "data analyst",
        country: "Canada",
        expectedIds: ["family-data-analytics-bi", "family-data-analytics-product"],
      },
      {
        title: "machine learning engineer",
        country: "United States",
        expectedIds: ["family-ml-ai", "family-ml-applied"],
      },
      {
        title: "product manager",
        country: "Canada",
        expectedIds: ["family-product-technical", "family-product-owner"],
      },
      {
        title: "devops engineer",
        country: "Germany",
        expectedIds: ["family-devops-cloud", "family-devops-security"],
      },
    ];

    for (const scenario of scenarios) {
      const result = await getIndexedJobsForSearch(repository, {
        title: scenario.title,
        country: scenario.country,
      });
      const ids = result.matches.map(({ job }) => job.sourceJobId);

      expect(ids).toEqual(expect.arrayContaining(scenario.expectedIds));
      expect(ids).not.toEqual(expect.arrayContaining([
        "family-noise-recruiter",
        "family-noise-finance",
      ]));
      expect(result.matches.every(({ evaluation }) => evaluation.titleMatch.matches)).toBe(true);
      expect(result.matches.every(({ job }) => job.rawSourceMetadata.indexedSearch)).toBe(true);
      expect(result.candidateQuery).toMatchObject({
        strategy: "coarse_prefilter",
        usedFamilyRoleFallback: true,
        usedLocationPrefilter: true,
      });
    }
  });

  it("validates final indexed retrieval scenarios across countries, titles, and company-page sources", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "final-se-us-exact",
        canonicalUrl: "https://example.com/jobs/final-se-us-exact",
        applyUrl: "https://example.com/jobs/final-se-us-exact/apply",
        sourceUrl: "https://example.com/jobs/final-se-us-exact",
        locationText: "Remote - United States",
      }),
      createPersistableJob({
        title: "Backend Developer",
        sourceJobId: "final-se-us-related",
        canonicalUrl: "https://example.com/jobs/final-se-us-related",
        applyUrl: "https://example.com/jobs/final-se-us-related/apply",
        sourceUrl: "https://example.com/jobs/final-se-us-related",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "final-se-ca-exact",
        canonicalUrl: "https://example.com/jobs/final-se-ca-exact",
        applyUrl: "https://example.com/jobs/final-se-ca-exact/apply",
        sourceUrl: "https://example.com/jobs/final-se-ca-exact",
        country: "Canada",
        locationText: "Toronto, ON",
        state: "Ontario",
        city: "Toronto",
      }),
      createPersistableJob({
        title: "Platform Engineer",
        sourceJobId: "final-se-ca-related",
        canonicalUrl: "https://example.com/jobs/final-se-ca-related",
        applyUrl: "https://example.com/jobs/final-se-ca-related/apply",
        sourceUrl: "https://example.com/jobs/final-se-ca-related",
        country: "Canada",
        locationText: "Vancouver, BC",
        state: "British Columbia",
        city: "Vancouver",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "Data Analyst",
        sourceJobId: "final-da-il-exact",
        canonicalUrl: "https://example.com/jobs/final-da-il-exact",
        applyUrl: "https://example.com/jobs/final-da-il-exact/apply",
        sourceUrl: "https://example.com/jobs/final-da-il-exact",
        country: "Israel",
        locationText: "Tel Aviv, Israel",
        state: "Tel Aviv District",
        city: "Tel Aviv",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "Business Intelligence Analyst",
        sourceJobId: "final-da-il-related",
        canonicalUrl: "https://example.com/jobs/final-da-il-related",
        applyUrl: "https://example.com/jobs/final-da-il-related/apply",
        sourceUrl: "https://example.com/jobs/final-da-il-related",
        country: "Israel",
        locationText: "Jerusalem, Israel",
        state: "Jerusalem District",
        city: "Jerusalem",
      }),
      createPersistableJob({
        title: "Machine Learning Engineer",
        sourceJobId: "final-mle-us-exact",
        canonicalUrl: "https://example.com/jobs/final-mle-us-exact",
        applyUrl: "https://example.com/jobs/final-mle-us-exact/apply",
        sourceUrl: "https://example.com/jobs/final-mle-us-exact",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
      }),
      createPersistableJob({
        title: "Applied Scientist",
        sourceJobId: "final-mle-us-related",
        canonicalUrl: "https://example.com/jobs/final-mle-us-related",
        applyUrl: "https://example.com/jobs/final-mle-us-related/apply",
        sourceUrl: "https://example.com/jobs/final-mle-us-related",
        locationText: "Cambridge, MA",
        state: "Massachusetts",
        city: "Cambridge",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "Product Manager",
        sourceJobId: "final-pm-ca-exact",
        canonicalUrl: "https://example.com/jobs/final-pm-ca-exact",
        applyUrl: "https://example.com/jobs/final-pm-ca-exact/apply",
        sourceUrl: "https://example.com/jobs/final-pm-ca-exact",
        country: "Canada",
        locationText: "Remote - Canada",
      }),
      createPersistableJob({
        title: "Product Owner",
        sourceJobId: "final-pm-ca-related",
        canonicalUrl: "https://example.com/jobs/final-pm-ca-related",
        applyUrl: "https://example.com/jobs/final-pm-ca-related/apply",
        sourceUrl: "https://example.com/jobs/final-pm-ca-related",
        country: "Canada",
        locationText: "Montreal, Quebec",
        state: "Quebec",
        city: "Montreal",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "DevOps Engineer",
        sourceJobId: "final-devops-de-exact",
        canonicalUrl: "https://example.com/jobs/final-devops-de-exact",
        applyUrl: "https://example.com/jobs/final-devops-de-exact/apply",
        sourceUrl: "https://example.com/jobs/final-devops-de-exact",
        country: "Germany",
        locationText: "Berlin, Germany",
        state: "Berlin",
        city: "Berlin",
      }),
      createPersistableJob({
        title: "Cloud Engineer",
        sourceJobId: "final-devops-de-related",
        canonicalUrl: "https://example.com/jobs/final-devops-de-related",
        applyUrl: "https://example.com/jobs/final-devops-de-related/apply",
        sourceUrl: "https://example.com/jobs/final-devops-de-related",
        country: "Germany",
        locationText: "Munich, Germany",
        state: "Bavaria",
        city: "Munich",
        sourcePlatform: "company_page",
      }),
      createPersistableJob({
        title: "Software Engineer",
        sourceJobId: "final-noise-wrong-country",
        canonicalUrl: "https://example.com/jobs/final-noise-wrong-country",
        applyUrl: "https://example.com/jobs/final-noise-wrong-country/apply",
        sourceUrl: "https://example.com/jobs/final-noise-wrong-country",
        country: "Germany",
        locationText: "Berlin, Germany",
      }),
      createPersistableJob({
        title: "Recruiter",
        sourceJobId: "final-noise-wrong-title",
        canonicalUrl: "https://example.com/jobs/final-noise-wrong-title",
        applyUrl: "https://example.com/jobs/final-noise-wrong-title/apply",
        sourceUrl: "https://example.com/jobs/final-noise-wrong-title",
        country: "Canada",
        locationText: "Toronto, ON",
      }),
    ]);

    const scenarios = [
      {
        title: "software engineer",
        country: "United States",
        expectedIds: ["final-se-us-exact", "final-se-us-related"],
        relatedId: "final-se-us-related",
      },
      {
        title: "software engineer",
        country: "Canada",
        expectedIds: ["final-se-ca-exact", "final-se-ca-related"],
        relatedId: "final-se-ca-related",
      },
      {
        title: "data analyst",
        country: "Israel",
        expectedIds: ["final-da-il-exact", "final-da-il-related"],
        relatedId: "final-da-il-related",
      },
      {
        title: "machine learning engineer",
        country: "United States",
        expectedIds: ["final-mle-us-exact", "final-mle-us-related"],
        relatedId: "final-mle-us-related",
      },
      {
        title: "product manager",
        country: "Canada",
        expectedIds: ["final-pm-ca-exact", "final-pm-ca-related"],
        relatedId: "final-pm-ca-related",
      },
      {
        title: "devops engineer",
        country: "Germany",
        expectedIds: ["final-devops-de-exact", "final-devops-de-related"],
        relatedId: "final-devops-de-related",
      },
    ];

    for (const scenario of scenarios) {
      const result = await getIndexedJobsForSearch(repository, {
        title: scenario.title,
        country: scenario.country,
      });
      const ids = result.matches.map(({ job }) => job.sourceJobId);

      expect(ids).toEqual(expect.arrayContaining(scenario.expectedIds));
      expect(ids).toContain(scenario.relatedId);
      expect(ids).not.toContain("final-noise-wrong-country");
      expect(ids).not.toContain("final-noise-wrong-title");
      expect(result.matches.every(({ evaluation }) => evaluation.titleMatch.matches)).toBe(true);
      expect(
        result.matches.every(
          ({ evaluation }) => evaluation.locationMatch?.jobDiagnostics.country === scenario.country,
        ),
      ).toBe(true);
      expect(result.matches.some(({ job }) => job.sourcePlatform === "company_page")).toBe(true);
      expect(result.candidateCount).toBeGreaterThanOrEqual(result.matches.length);
      expect(result.candidateChannelBreakdown).toMatchObject({
        finalMatchedCount: result.matches.length,
        returnedCount: result.matches.length,
      });
      expect(result.requestTimeEvaluationCount).toBe(result.candidateCount);
      expect(result.candidateQuery).toMatchObject({
        strategy: "coarse_prefilter",
        usedSearchReadyTitleKeys: true,
        usedSearchReadyLocationKeys: true,
        usedLocationPrefilter: true,
      });
      expect(result.timingsMs.total).toBeGreaterThanOrEqual(0);
    }
  });

  it("keeps semantic title, US location, experience filters, and ranking on indexed jobs", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    await seedIndexedJobs(repository, [
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Remote - United States",
        sourceJobId: "software-exact",
        canonicalUrl: "https://example.com/jobs/software-exact",
        applyUrl: "https://example.com/jobs/software-exact/apply",
        sourceUrl: "https://example.com/jobs/software-exact",
        experienceLevel: "mid",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T12:00:00.000Z",
        crawledAt: "2026-04-14T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Backend Developer",
        locationText: "Austin, TX",
        state: "Texas",
        city: "Austin",
        sourceJobId: "software-semantic",
        canonicalUrl: "https://example.com/jobs/software-semantic",
        applyUrl: "https://example.com/jobs/software-semantic/apply",
        sourceUrl: "https://example.com/jobs/software-semantic",
        experienceLevel: "mid",
        postingDate: "2026-04-12T00:00:00.000Z",
        discoveredAt: "2026-04-12T12:00:00.000Z",
        crawledAt: "2026-04-12T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Senior Software Engineer",
        locationText: "Seattle, WA",
        state: "Washington",
        city: "Seattle",
        sourceJobId: "software-senior",
        canonicalUrl: "https://example.com/jobs/software-senior",
        applyUrl: "https://example.com/jobs/software-senior/apply",
        sourceUrl: "https://example.com/jobs/software-senior",
        experienceLevel: "senior",
        postingDate: "2026-04-15T00:00:00.000Z",
        discoveredAt: "2026-04-15T12:00:00.000Z",
        crawledAt: "2026-04-15T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Software Engineer",
        locationText: "Toronto, ON, Canada",
        country: "Canada",
        sourceJobId: "software-canada",
        canonicalUrl: "https://example.com/jobs/software-canada",
        applyUrl: "https://example.com/jobs/software-canada/apply",
        sourceUrl: "https://example.com/jobs/software-canada",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T12:00:00.000Z",
        crawledAt: "2026-04-13T12:00:00.000Z",
      }),
      createPersistableJob({
        title: "Data Analyst",
        locationText: "Remote - US",
        sourceJobId: "data-analyst-exact",
        canonicalUrl: "https://example.com/jobs/data-analyst-exact",
        applyUrl: "https://example.com/jobs/data-analyst-exact/apply",
        sourceUrl: "https://example.com/jobs/data-analyst-exact",
        experienceLevel: "mid",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T13:00:00.000Z",
        crawledAt: "2026-04-14T13:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Intelligence Analyst",
        locationText: "New York, NY",
        state: "New York",
        city: "New York",
        sourceJobId: "data-analyst-semantic",
        canonicalUrl: "https://example.com/jobs/data-analyst-semantic",
        applyUrl: "https://example.com/jobs/data-analyst-semantic/apply",
        sourceUrl: "https://example.com/jobs/data-analyst-semantic",
        experienceLevel: "mid",
        postingDate: "2026-04-11T00:00:00.000Z",
        discoveredAt: "2026-04-11T13:00:00.000Z",
        crawledAt: "2026-04-11T13:00:00.000Z",
      }),
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "product-manager-noise",
        canonicalUrl: "https://example.com/jobs/product-manager-noise",
        applyUrl: "https://example.com/jobs/product-manager-noise/apply",
        sourceUrl: "https://example.com/jobs/product-manager-noise",
        postingDate: "2026-04-14T00:00:00.000Z",
        discoveredAt: "2026-04-14T15:00:00.000Z",
        crawledAt: "2026-04-14T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Analyst",
        locationText: "Chicago, IL",
        state: "Illinois",
        city: "Chicago",
        sourceJobId: "business-analyst-exact",
        canonicalUrl: "https://example.com/jobs/business-analyst-exact",
        applyUrl: "https://example.com/jobs/business-analyst-exact/apply",
        sourceUrl: "https://example.com/jobs/business-analyst-exact",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T15:00:00.000Z",
        crawledAt: "2026-04-13T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Business Systems Analyst",
        locationText: "Remote - United States",
        sourceJobId: "business-analyst-semantic",
        canonicalUrl: "https://example.com/jobs/business-analyst-semantic",
        applyUrl: "https://example.com/jobs/business-analyst-semantic/apply",
        sourceUrl: "https://example.com/jobs/business-analyst-semantic",
        postingDate: "2026-04-12T00:00:00.000Z",
        discoveredAt: "2026-04-12T15:00:00.000Z",
        crawledAt: "2026-04-12T15:00:00.000Z",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        locationText: "Remote - California",
        sourceJobId: "product-manager-semantic",
        canonicalUrl: "https://example.com/jobs/product-manager-semantic",
        applyUrl: "https://example.com/jobs/product-manager-semantic/apply",
        sourceUrl: "https://example.com/jobs/product-manager-semantic",
        postingDate: "2026-04-13T00:00:00.000Z",
        discoveredAt: "2026-04-13T16:00:00.000Z",
        crawledAt: "2026-04-13T16:00:00.000Z",
      }),
    ]);

    const provider = createStubProvider("greenhouse", async () => ({
      provider: "greenhouse",
      status: "success",
      sourceCount: 0,
      fetchedCount: 0,
      matchedCount: 0,
      warningCount: 0,
      jobs: [],
    }));

    const softwareSearch = await startSearchFromFilters(
      {
        title: "Software Engineer",
        country: "United States",
        experienceLevels: ["mid"],
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:05:00.000Z"),
      },
    );

    expect(softwareSearch.result.jobs.map((job) => job.title)).toEqual([
      "Software Engineer",
      "Backend Developer",
    ]);
    expect(softwareSearch.result.jobs.every((job) => job.rawSourceMetadata.indexedSearch)).toBe(true);
    expect(softwareSearch.result.jobs[0]?.rawSourceMetadata.crawlRanking).toMatchObject({
      relevanceTier: "exact",
    });
    expect(softwareSearch.result.jobs[1]?.rawSourceMetadata.crawlRanking).toMatchObject({
      relevanceTier: expect.not.stringMatching(/^exact$/),
    });

    const dataSearch = await startSearchFromFilters(
      {
        title: "Data Analyst",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:06:00.000Z"),
      },
    );

    expect(dataSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Data Analyst",
        "Business Intelligence Analyst",
      ]),
    );
    expect(dataSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Product Manager" }),
      ]),
    );
    expect(dataSearch.result.jobs[0]?.locationText).toBe("Remote - US");
    expect(dataSearch.result.jobs[1]?.locationText).toBe("New York, NY");

    const businessSearch = await startSearchFromFilters(
      {
        title: "Business Analyst",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:07:00.000Z"),
      },
    );

    expect(businessSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Business Analyst",
        "Business Systems Analyst",
      ]),
    );
    expect(businessSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Product Manager" }),
      ]),
    );

    const productSearch = await startSearchFromFilters(
      {
        title: "Product Manager",
        country: "United States",
      },
      {
        repository,
        providers: [provider],
        discovery: createDiscovery(),
        fetchImpl: vi.fn() as unknown as typeof fetch,
        now: new Date("2026-04-15T12:08:00.000Z"),
      },
    );

    expect(productSearch.result.jobs.map((job) => job.title)).toEqual(
      expect.arrayContaining([
        "Product Manager",
        "Senior Product Manager",
      ]),
    );
    expect(productSearch.result.jobs).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Business Analyst" }),
        expect.objectContaining({ title: "Data Analyst" }),
      ]),
    );
  }, 10_000);

  it("uses a selective coarse indexed candidate set before final precision filtering", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const seededJobs = [
      createPersistableJob({
        title: "Product Manager",
        locationText: "Remote - United States",
        sourceJobId: "candidate-product-exact",
        canonicalUrl: "https://example.com/jobs/candidate-product-exact",
        applyUrl: "https://example.com/jobs/candidate-product-exact/apply",
        sourceUrl: "https://example.com/jobs/candidate-product-exact",
      }),
      createPersistableJob({
        title: "Senior Product Manager",
        locationText: "Los Angeles, CA",
        state: "California",
        city: "Los Angeles",
        sourceJobId: "candidate-product-senior",
        canonicalUrl: "https://example.com/jobs/candidate-product-senior",
        applyUrl: "https://example.com/jobs/candidate-product-senior/apply",
        sourceUrl: "https://example.com/jobs/candidate-product-senior",
      }),
      createPersistableJob({
        title: "Technical Program Manager",
        locationText: "Remote - United States",
        sourceJobId: "candidate-program-nearby",
        canonicalUrl: "https://example.com/jobs/candidate-program-nearby",
        applyUrl: "https://example.com/jobs/candidate-program-nearby/apply",
        sourceUrl: "https://example.com/jobs/candidate-program-nearby",
      }),
      createPersistableJob({
        title: "Business Analyst",
        locationText: "Chicago, IL",
        state: "Illinois",
        city: "Chicago",
        sourceJobId: "candidate-business-noise",
        canonicalUrl: "https://example.com/jobs/candidate-business-noise",
        applyUrl: "https://example.com/jobs/candidate-business-noise/apply",
        sourceUrl: "https://example.com/jobs/candidate-business-noise",
      }),
      createPersistableJob({
        title: "Recruiter",
        locationText: "Remote - United States",
        sourceJobId: "candidate-recruiter-noise",
        canonicalUrl: "https://example.com/jobs/candidate-recruiter-noise",
        applyUrl: "https://example.com/jobs/candidate-recruiter-noise/apply",
        sourceUrl: "https://example.com/jobs/candidate-recruiter-noise",
      }),
    ];

    await seedIndexedJobs(repository, seededJobs);

    const indexedSearch = await getIndexedJobsForSearch(repository, {
      title: "Product Manager",
      country: "United States",
    });

    expect(indexedSearch.candidateCount).toBeGreaterThanOrEqual(indexedSearch.matches.length);
    expect(indexedSearch.candidateQuery).toMatchObject({
      strategy: "coarse_prefilter",
      titleFamily: "product",
      usedLocationPrefilter: true,
    });
    expect(indexedSearch.matches.map(({ job }) => job.title)).toEqual([
      "Product Manager",
      "Senior Product Manager",
    ]);
    expect(indexedSearch.matches.every(({ job }) => job.rawSourceMetadata.indexedSearch)).toBe(true);
    expect(indexedSearch.matches[0]?.job.rawSourceMetadata.indexedSearch).toMatchObject({
      candidateCount: indexedSearch.candidateCount,
      candidateQuery: expect.objectContaining({
        strategy: "coarse_prefilter",
      }),
    });
  });

  it("builds DB-friendly search-ready candidate queries instead of regex-first full-list scans", () => {
    const query = buildIndexedJobCandidateQuery({
      title: "Machine Learning Engineer",
      country: "Canada",
      experienceLevels: ["senior"],
      platforms: ["greenhouse"],
    });

    expect(JSON.stringify(query.filter)).toContain("searchIndex.titleSearchKeys");
    expect(JSON.stringify(query.filter)).toContain("searchIndex.locationSearchKeys");
    expect(JSON.stringify(query.filter)).toContain("searchIndex.experienceSearchKeys");
    expect(query.diagnostics).toMatchObject({
      usedSearchReadyTitleKeys: true,
      usedSearchReadyLocationKeys: true,
      usedSearchReadyExperienceKeys: true,
      usedLocationPrefilter: true,
      usedExperiencePrefilter: true,
    });
  });

  it("keeps candidate counts and request-time refinement bounded across countries and role families", async () => {
    const repository = new JobCrawlerRepository(new FakeDb());
    const scenarios = [
      {
        title: "software engineer",
        country: "United States",
        match: createPersistableJob({
          title: "Software Engineer",
          locationText: "Remote - United States",
          sourceJobId: "bounded-se-us",
          canonicalUrl: "https://example.com/jobs/bounded-se-us",
          applyUrl: "https://example.com/jobs/bounded-se-us/apply",
          sourceUrl: "https://example.com/jobs/bounded-se-us",
        }),
      },
      {
        title: "software engineer",
        country: "Canada",
        match: createPersistableJob({
          title: "Backend Developer",
          locationText: "Toronto, ON",
          state: "Ontario",
          city: "Toronto",
          country: "Canada",
          resolvedLocation: {
            country: "Canada",
            state: "Ontario",
            stateCode: "ON",
            city: "Toronto",
            isRemote: false,
            isUnitedStates: false,
            confidence: "high",
            evidence: [{ source: "location_text", value: "Toronto, ON" }],
          },
          sourceJobId: "bounded-se-ca",
          canonicalUrl: "https://example.com/jobs/bounded-se-ca",
          applyUrl: "https://example.com/jobs/bounded-se-ca/apply",
          sourceUrl: "https://example.com/jobs/bounded-se-ca",
        }),
      },
      {
        title: "data analyst",
        country: "Israel",
        match: createPersistableJob({
          title: "Data Analyst",
          locationText: "Tel Aviv, Israel",
          state: "Tel Aviv District",
          city: "Tel Aviv",
          country: "Israel",
          resolvedLocation: {
            country: "Israel",
            state: "Tel Aviv District",
            city: "Tel Aviv",
            isRemote: false,
            isUnitedStates: false,
            confidence: "high",
            evidence: [{ source: "location_text", value: "Tel Aviv, Israel" }],
          },
          sourceJobId: "bounded-da-il",
          canonicalUrl: "https://example.com/jobs/bounded-da-il",
          applyUrl: "https://example.com/jobs/bounded-da-il/apply",
          sourceUrl: "https://example.com/jobs/bounded-da-il",
        }),
      },
      {
        title: "machine learning engineer",
        country: "United States",
        match: createPersistableJob({
          title: "Machine Learning Engineer",
          locationText: "New York, NY",
          state: "New York",
          city: "New York",
          sourceJobId: "bounded-mle-us",
          canonicalUrl: "https://example.com/jobs/bounded-mle-us",
          applyUrl: "https://example.com/jobs/bounded-mle-us/apply",
          sourceUrl: "https://example.com/jobs/bounded-mle-us",
        }),
      },
      {
        title: "product manager",
        country: "Canada",
        match: createPersistableJob({
          title: "Product Manager",
          locationText: "Remote - Canada",
          country: "Canada",
          resolvedLocation: {
            country: "Canada",
            isRemote: true,
            isUnitedStates: false,
            confidence: "high",
            evidence: [{ source: "location_text", value: "Remote - Canada" }],
          },
          sourceJobId: "bounded-pm-ca",
          canonicalUrl: "https://example.com/jobs/bounded-pm-ca",
          applyUrl: "https://example.com/jobs/bounded-pm-ca/apply",
          sourceUrl: "https://example.com/jobs/bounded-pm-ca",
        }),
      },
    ];
    const noise = Array.from({ length: 40 }, (_, index) =>
      createPersistableJob({
        title: index % 2 === 0 ? "Recruiter" : "Account Executive",
        locationText: index % 3 === 0 ? "Berlin, Germany" : "Remote - United States",
        country: index % 3 === 0 ? "Germany" : "United States",
        resolvedLocation:
          index % 3 === 0
            ? {
                country: "Germany",
                city: "Berlin",
                state: "Berlin",
                isRemote: false,
                isUnitedStates: false,
                confidence: "high",
                evidence: [{ source: "location_text", value: "Berlin, Germany" }],
              }
            : undefined,
        sourceJobId: `bounded-noise-${index}`,
        canonicalUrl: `https://example.com/jobs/bounded-noise-${index}`,
        applyUrl: `https://example.com/jobs/bounded-noise-${index}/apply`,
        sourceUrl: `https://example.com/jobs/bounded-noise-${index}`,
      }),
    );

    await seedIndexedJobs(repository, [
      ...scenarios.map((scenario) => scenario.match),
      ...noise,
    ]);

    for (const scenario of scenarios) {
      const result = await getIndexedJobsForSearch(repository, {
        title: scenario.title,
        country: scenario.country,
      });

      expect(result.matches.map(({ job }) => job.sourceJobId)).toContain(
        scenario.match.sourceJobId,
      );
      expect(result.candidateCount).toBeGreaterThanOrEqual(result.matches.length);
      expect(result.requestTimeEvaluationCount).toBe(result.candidateCount);
      expect(result.requestTimeExcludedCount).toBeGreaterThan(0);
      expect(result.candidateQuery).toMatchObject({
        usedSearchReadyTitleKeys: true,
        usedSearchReadyLocationKeys: true,
      });
      expect(result.timingsMs.total).toBeGreaterThanOrEqual(0);
    }
  });
});

async function waitForSearchJobCount(
  searchId: string,
  expectedCount: number,
  runtime: Parameters<typeof getSearchDetails>[1],
) {
  const deadline = Date.now() + 2_000;
  let latest = await getSearchDetails(searchId, runtime);

  while (Date.now() < deadline) {
    if (latest.jobs.length >= expectedCount) {
      return latest;
    }

    await new Promise((resolve) => setTimeout(resolve, 25));
    latest = await getSearchDetails(searchId, runtime);
  }

  return latest;
}
