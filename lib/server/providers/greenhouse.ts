import "server-only";

import {
  buildLocationText,
  matchesFiltersWithoutExperience,
  normalizeComparableText,
  runWithConcurrency,
} from "@/lib/server/crawler/helpers";
import { getEnv } from "@/lib/server/env";
import {
  buildExperiencePrompt,
  buildSeed,
  coercePostedAt,
  defaultCompanyName,
  finalizeProviderResult,
} from "@/lib/server/providers/shared";
import type { CrawlProvider, ProviderResult } from "@/lib/server/providers/types";

type GreenhouseApiResponse = {
  jobs?: Array<{
    id: number | string;
    title?: string;
    absolute_url?: string;
    updated_at?: string;
    first_published?: string;
    company_name?: string;
    content?: string;
    location?: { name?: string };
    offices?: Array<{ name?: string; location?: { name?: string | null } | null }>;
    departments?: Array<{ name?: string }>;
    metadata?: Array<{ name?: string; value?: string | null }>;
  }>;
};

type GreenhouseJob = NonNullable<GreenhouseApiResponse["jobs"]>[number];

const greenhouseStructuredExperiencePattern =
  /\b(intern(ship)?|co op|cooperative education|apprentice(ship)?|working student|student(?: program| opportunity| role| position)?|for students|new grad|new graduate|recent grad|recent graduate|entry level|early career|junior|associate|mid level|senior|staff|principal|distinguished|level 2|level 3|\d+(?:\.\d+)?\s*(?:\+|plus)?\s*(?:-|to|–|—)?\s*\d*(?:\.\d+)?\s*(?:years?|yrs?|yoe))\b/i;

export function normalizeGreenhouseJob(input: {
  companyToken: string;
  discoveredAt: string;
  job: GreenhouseJob;
}) {
  const locationText = buildGreenhouseLocationText(input.job);
  const experienceHint = buildGreenhouseExperienceHint(input.job);

  return buildSeed({
    title: input.job.title ?? "Untitled role",
    companyToken: input.companyToken,
    company: defaultCompanyName(input.companyToken, input.job.company_name),
    locationText,
    sourcePlatform: "greenhouse",
    sourceJobId: String(input.job.id),
    sourceUrl: input.job.absolute_url ?? `https://boards.greenhouse.io/${input.companyToken}`,
    applyUrl: input.job.absolute_url ?? `https://boards.greenhouse.io/${input.companyToken}`,
    canonicalUrl: input.job.absolute_url,
    postedAt: coercePostedAt(input.job.first_published ?? input.job.updated_at),
    rawSourceMetadata: {
      greenhouseJob: input.job,
      greenhouseExperienceHint: experienceHint,
    },
    discoveredAt: input.discoveredAt,
    experienceHint,
  });
}

export function createGreenhouseProvider(): CrawlProvider {
  return {
    provider: "greenhouse",
    async crawl(context) {
      const tokens = getEnv().greenhouseBoardTokens;
      if (tokens.length === 0) {
        return unsupportedResult("No Greenhouse board tokens are configured.");
      }

      const warnings: string[] = [];
      let fetchedCount = 0;

      const boards = await runWithConcurrency(
        tokens,
        async (companyToken) => {
          try {
            const response = await context.fetchImpl(
              `https://boards-api.greenhouse.io/v1/boards/${companyToken}/jobs?content=true`,
              {
                method: "GET",
                headers: {
                  Accept: "application/json",
                },
                cache: "no-store",
              },
            );

            if (!response.ok) {
              throw new Error(`Greenhouse returned ${response.status} for ${companyToken}.`);
            }

            const payload = (await response.json()) as GreenhouseApiResponse;
            const jobs = payload.jobs ?? [];
            fetchedCount += jobs.length;

            return jobs
              .map((job) =>
                normalizeGreenhouseJob({
                  companyToken,
                  discoveredAt: context.now.toISOString(),
                  job,
                }),
              )
              .filter((job) => matchesFiltersWithoutExperience(job, context.filters));
          } catch (error) {
            warnings.push(
              error instanceof Error
                ? error.message
                : `Greenhouse board ${companyToken} failed unexpectedly.`,
            );
            return [];
          }
        },
        3,
      );

      const jobs = boards.flat();

      return finalizeProviderResult({
        provider: "greenhouse",
        jobs,
        fetchedCount,
        warnings,
      });
    },
  };
}

function unsupportedResult(message: string): ProviderResult {
  return {
    provider: "greenhouse",
    status: "unsupported",
    jobs: [],
    fetchedCount: 0,
    matchedCount: 0,
    errorMessage: message,
  };
}

function buildGreenhouseLocationText(job: GreenhouseJob) {
  const candidates = dedupeComparableStrings([
    job.metadata?.find((entry) => entry.name?.toLowerCase().includes("location"))?.value ?? undefined,
    job.location?.name,
    ...(job.offices ?? []).flatMap((office) => [office.location?.name ?? undefined, office.name]),
  ]);

  return buildLocationText(candidates) || "Location unavailable";
}

function buildGreenhouseExperienceHint(job: GreenhouseJob) {
  return buildExperiencePrompt(
    job.title,
    ...collectStructuredMetadataHints(job.metadata),
    ...collectStructuredNames(job.departments),
    ...collectStructuredNames(job.offices),
    stripGreenhouseMarkup(job.content),
  );
}

function collectStructuredMetadataHints(
  metadata: GreenhouseJob["metadata"],
) {
  return (metadata ?? [])
    .map((entry) => [entry.name?.trim(), entry.value?.trim()].filter(Boolean).join(": "))
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function collectStructuredNames(
  records: Array<{ name?: string }> | undefined,
) {
  return (records ?? [])
    .map((record) => record.name?.trim())
    .filter((value): value is string => Boolean(value))
    .filter((value) => greenhouseStructuredExperiencePattern.test(value));
}

function stripGreenhouseMarkup(value?: string) {
  return (value ?? "")
    .replace(/&lt;\/?[^&]+&gt;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&(nbsp|amp|quot|apos|#39|#x27);/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeComparableStrings(values: Array<string | undefined | null>) {
  const seen = new Set<string>();
  const results: string[] = [];

  for (const value of values) {
    const trimmed = value?.trim();
    if (!trimmed) {
      continue;
    }

    const comparable = normalizeComparableText(trimmed);
    if (!comparable || seen.has(comparable)) {
      continue;
    }

    seen.add(comparable);
    results.push(trimmed);
  }

  return results;
}
