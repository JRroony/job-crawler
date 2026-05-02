import type {
  DiscoveredSource,
  GreenhouseDiscoveredSource,
} from "@/lib/server/discovery/types";
import type {
  CrawlProvider,
  NormalizedJobSeed,
} from "@/lib/server/providers/types";

export const deterministicTestCompany = "Deterministic Test Company";
export const deterministicTestCompanySlug = "deterministic-test-company";
export const deterministicTestJobId = "deterministic-test-job-001";
export const deterministicTestJobUrl =
  "https://example.com/jobs/deterministic-test-job-001";
export const deterministicTestSourceId = "greenhouse:deterministic-test-company";

export function createDeterministicFakeSource(): GreenhouseDiscoveredSource {
  return {
    id: deterministicTestSourceId,
    platform: "greenhouse",
    url: deterministicTestJobUrl,
    token: deterministicTestCompanySlug,
    companyHint: deterministicTestCompany,
    confidence: "high",
    discoveryMethod: "manual_config",
    boardUrl: deterministicTestJobUrl,
  };
}

export function createDeterministicFakeJobSeed(now = new Date()): NormalizedJobSeed {
  const timestamp = now.toISOString();

  return {
    title: "Software Engineer",
    company: deterministicTestCompany,
    normalizedCompany: "deterministic test company",
    normalizedTitle: "software engineer",
    titleNormalized: "software engineer",
    country: "United States",
    state: "WA",
    city: "Seattle",
    locationRaw: "Seattle, WA",
    normalizedLocation: "seattle wa",
    locationText: "Seattle, WA",
    resolvedLocation: {
      country: "United States",
      state: "WA",
      city: "Seattle",
      isRemote: false,
      isUnitedStates: true,
      confidence: "high",
      evidence: [
        {
          source: "structured_fields",
          value: "Seattle, WA",
        },
      ],
    },
    sourcePlatform: "greenhouse",
    sourceCompanySlug: deterministicTestCompanySlug,
    sourceJobId: deterministicTestJobId,
    sourceUrl: deterministicTestJobUrl,
    applyUrl: deterministicTestJobUrl,
    canonicalUrl: deterministicTestJobUrl,
    discoveredAt: timestamp,
    crawledAt: timestamp,
    rawSourceMetadata: {
      source: "deterministic-persistence-gate",
      fakeProvider: true,
      greenhouseBoardToken: deterministicTestCompanySlug,
    },
  };
}

export function createDeterministicFakeProvider(): CrawlProvider {
  return {
    provider: "greenhouse",
    supportsSource(source: DiscoveredSource): source is DiscoveredSource {
      return source.platform === "greenhouse" && source.id === deterministicTestSourceId;
    },
    async crawlSources(context, sources) {
      await context.throwIfCanceled?.();

      const supportedSources = sources.filter(
        (source) => source.platform === "greenhouse" && source.id === deterministicTestSourceId,
      );

      if (supportedSources.length === 0) {
        return {
          provider: "greenhouse",
          status: "unsupported",
          sourceCount: 0,
          fetchedCount: 0,
          matchedCount: 0,
          warningCount: 1,
          errorMessage: "deterministic_fake_source_not_found",
          jobs: [],
          diagnostics: createProviderDiagnostics({
            sourceCount: 0,
            fetchedCount: 0,
            validSeedCount: 0,
            sourceObservations: [],
          }),
        };
      }

      const job = createDeterministicFakeJobSeed(context.now);

      return {
        provider: "greenhouse",
        status: "success",
        sourceCount: 1,
        fetchedCount: 1,
        matchedCount: 1,
        warningCount: 0,
        jobs: [job],
        diagnostics: createProviderDiagnostics({
          sourceCount: 1,
          fetchedCount: 1,
          validSeedCount: 1,
          sourceObservations: [
            {
              sourceId: deterministicTestSourceId,
              succeeded: true,
              errorType: "none",
            },
          ],
        }),
      };
    },
  };
}

function createProviderDiagnostics(input: {
  sourceCount: number;
  fetchedCount: number;
  validSeedCount: number;
  sourceObservations: NonNullable<
    Awaited<ReturnType<CrawlProvider["crawlSources"]>>["diagnostics"]
  >["sourceObservations"];
}): NonNullable<Awaited<ReturnType<CrawlProvider["crawlSources"]>>["diagnostics"]> {
  return {
    provider: "greenhouse",
    discoveryCount: input.sourceCount,
    sourceCount: input.sourceCount,
    sourceSucceededCount: input.sourceObservations?.filter((source) => source.succeeded).length ?? 0,
    sourceTimedOutCount: 0,
    sourceFailedCount: 0,
    sourceSkippedCount: 0,
    fetchCount: 0,
    fetchedCount: input.fetchedCount,
    parseSuccessCount: input.validSeedCount,
    parseFailureCount: 0,
    rawFetchedCount: input.fetchedCount,
    parsedSeedCount: input.validSeedCount,
    validSeedCount: input.validSeedCount,
    invalidSeedCount: 0,
    jobsEmittedViaOnBatch: 0,
    sourceObservations: input.sourceObservations,
    dropReasonCounts: {},
    sampleDropReasons: [],
    sampleInvalidSeeds: [],
  };
}
