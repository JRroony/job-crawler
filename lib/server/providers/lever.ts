import "server-only";

import { matchesFiltersWithoutExperience, runWithConcurrency } from "@/lib/server/crawler/helpers";
import { getEnv } from "@/lib/server/env";
import {
  buildExperiencePrompt,
  buildSeed,
  coercePostedAt,
  defaultCompanyName,
  finalizeProviderResult,
} from "@/lib/server/providers/shared";
import type { CrawlProvider, ProviderResult } from "@/lib/server/providers/types";

type LeverPosting = {
  id?: string;
  text?: string;
  description?: string;
  descriptionPlain?: string;
  workplaceType?: string;
  lists?: Array<{
    text?: string;
    content?: string;
  }>;
  country?: string;
  hostedUrl?: string;
  applyUrl?: string;
  createdAt?: number;
  categories?: {
    location?: string;
    allLocations?: string[];
    commitment?: string;
    team?: string;
    department?: string;
  };
};

export function normalizeLeverJob(input: {
  siteToken: string;
  discoveredAt: string;
  job: LeverPosting;
}) {
  const locationText =
    input.job.categories?.location ??
    input.job.categories?.allLocations?.[0] ??
    "Location unavailable";

  return buildSeed({
    title: input.job.text ?? "Untitled role",
    companyToken: input.siteToken,
    company: defaultCompanyName(input.siteToken),
    locationText,
    sourcePlatform: "lever",
    sourceJobId: input.job.id ?? input.job.hostedUrl ?? input.job.applyUrl ?? input.siteToken,
    sourceUrl: input.job.hostedUrl ?? `https://jobs.lever.co/${input.siteToken}`,
    applyUrl: input.job.applyUrl ?? input.job.hostedUrl ?? `https://jobs.lever.co/${input.siteToken}`,
    canonicalUrl: input.job.hostedUrl,
    postedAt: coercePostedAt(input.job.createdAt),
    rawSourceMetadata: {
      leverJob: input.job,
    },
    discoveredAt: input.discoveredAt,
    explicitCountry: input.job.country,
    experienceHint: buildLeverExperienceHint(input.job),
  });
}

export function createLeverProvider(): CrawlProvider {
  return {
    provider: "lever",
    async crawl(context) {
      const tokens = getEnv().leverSiteTokens;
      if (tokens.length === 0) {
        return unsupportedResult("No Lever site tokens are configured.");
      }

      const warnings: string[] = [];
      let fetchedCount = 0;

      const sites = await runWithConcurrency(
        tokens,
        async (siteToken) => {
          try {
            const response = await context.fetchImpl(
              `https://api.lever.co/v0/postings/${siteToken}?mode=json`,
              {
                method: "GET",
                headers: {
                  Accept: "application/json",
                },
                cache: "no-store",
              },
            );

            if (!response.ok) {
              throw new Error(`Lever returned ${response.status} for ${siteToken}.`);
            }

            const payload = (await response.json()) as LeverPosting[];
            fetchedCount += payload.length;

            return payload
              .map((job) =>
                normalizeLeverJob({
                  siteToken,
                  discoveredAt: context.now.toISOString(),
                  job,
                }),
              )
              .filter((job) => matchesFiltersWithoutExperience(job, context.filters));
          } catch (error) {
            warnings.push(
              error instanceof Error
                ? error.message
                : `Lever site ${siteToken} failed unexpectedly.`,
            );
            return [];
          }
        },
        3,
      );

      const jobs = sites.flat();

      return finalizeProviderResult({
        provider: "lever",
        jobs,
        fetchedCount,
        warnings,
      });
    },
  };
}

function unsupportedResult(message: string): ProviderResult {
  return {
    provider: "lever",
    status: "unsupported",
    jobs: [],
    fetchedCount: 0,
    matchedCount: 0,
    errorMessage: message,
  };
}

function buildLeverExperienceHint(job: LeverPosting) {
  return buildExperiencePrompt(
    job.text,
    job.categories?.commitment,
    job.categories?.team,
    job.categories?.department,
    job.workplaceType,
    job.descriptionPlain,
    job.description,
    ...collectLeverListHints(job.lists),
  );
}

function collectLeverListHints(lists?: LeverPosting["lists"]) {
  return (lists ?? [])
    .flatMap((entry) => [entry.text, entry.content])
    .filter((value): value is string => Boolean(value?.trim()));
}
