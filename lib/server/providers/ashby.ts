import "server-only";

import { matchesFiltersWithoutExperience, runWithConcurrency } from "@/lib/server/crawler/helpers";
import { getEnv } from "@/lib/server/env";
import {
  buildExperiencePrompt,
  buildSeed,
  coercePostedAt,
  deepCollect,
  extractWindowAppData,
  extractNextData,
  finalizeProviderResult,
  firstString,
} from "@/lib/server/providers/shared";
import type { CrawlProvider, ProviderResult } from "@/lib/server/providers/types";

type AshbyCandidate = {
  id?: string;
  title?: string;
  location?: string;
  locationName?: string;
  jobUrl?: string;
  absoluteUrl?: string;
  url?: string;
  employmentType?: string;
  departmentName?: string;
  teamName?: string;
  seniority?: string;
  descriptionHtml?: string;
  descriptionPlain?: string;
  jobDescription?: string;
  requirements?: string;
  summary?: string;
  createdAt?: string;
  publishedAt?: string;
  updatedAt?: string;
};

export function normalizeAshbyCandidate(input: {
  companyToken: string;
  discoveredAt: string;
  candidate: AshbyCandidate;
}) {
  const sourceUrl =
    input.candidate.jobUrl ??
    input.candidate.absoluteUrl ??
    input.candidate.url ??
    `https://jobs.ashbyhq.com/${input.companyToken}`;

  return buildSeed({
    title: input.candidate.title ?? "Untitled role",
    companyToken: input.companyToken,
    locationText:
      input.candidate.locationName ??
      input.candidate.location ??
      "Location unavailable",
    sourcePlatform: "ashby",
    sourceJobId: input.candidate.id ?? sourceUrl,
    sourceUrl,
    applyUrl: sourceUrl,
    canonicalUrl: sourceUrl,
    postedAt: coercePostedAt(
      input.candidate.publishedAt ??
        input.candidate.createdAt ??
        input.candidate.updatedAt,
    ),
    rawSourceMetadata: {
      ashbyJob: input.candidate,
    },
    discoveredAt: input.discoveredAt,
    experienceHint: buildAshbyExperienceHint(input.candidate),
  });
}

export function createAshbyProvider(): CrawlProvider {
  return {
    provider: "ashby",
    async crawl(context) {
      const tokens = getEnv().ashbyBoardTokens;
      if (tokens.length === 0) {
        return unsupportedResult("No Ashby board tokens are configured.");
      }

      const warnings: string[] = [];
      let fetchedCount = 0;

      const boards = await runWithConcurrency(
        tokens,
        async (companyToken) => {
          try {
            const response = await context.fetchImpl(
              `https://jobs.ashbyhq.com/${companyToken}`,
              {
                method: "GET",
                headers: {
                  Accept: "text/html,application/xhtml+xml",
                },
                cache: "no-store",
              },
            );

            if (!response.ok) {
              throw new Error(`Ashby returned ${response.status} for ${companyToken}.`);
            }

            const html = await response.text();
            const extracted = extractAshbyCandidates(html, companyToken);
            fetchedCount += extracted.length;

            return extracted
              .map((candidate) =>
                normalizeAshbyCandidate({
                  companyToken,
                  discoveredAt: context.now.toISOString(),
                  candidate,
                }),
              )
              .filter((job) => matchesFiltersWithoutExperience(job, context.filters));
          } catch (error) {
            warnings.push(
              error instanceof Error
                ? error.message
                : `Ashby board ${companyToken} failed unexpectedly.`,
            );
            return [];
          }
        },
        2,
      );

      const jobs = boards.flat();

      return finalizeProviderResult({
        provider: "ashby",
        jobs,
        fetchedCount,
        warnings,
      });
    },
  };
}

function extractAshbyCandidates(html: string, companyToken: string) {
  const appDataCandidates = extractAshbyCandidatesFromAppData(html, companyToken);
  if (appDataCandidates) {
    return appDataCandidates;
  }

  const nextData = extractNextData(html);
  const collected = nextData
    ? deepCollect(nextData, (record) => {
        const title = firstString(record, ["title", "jobTitle", "name"]);
        const url = firstString(record, ["jobUrl", "absoluteUrl", "url"]);
        return Boolean(title && url && String(url).includes(companyToken));
      }).map(
        (record) =>
          ({
            id: firstString(record, ["id", "jobId", "slug"]),
            title: firstString(record, ["title", "jobTitle", "name"]),
            location:
              firstString(record, ["locationName", "location", "jobLocationText"]) ??
              "Location unavailable",
            jobUrl: firstString(record, ["jobUrl", "absoluteUrl", "url"]),
            employmentType: firstString(record, ["employmentType", "jobType"]),
            departmentName: firstString(record, ["departmentName", "department"]),
            teamName: firstString(record, ["teamName", "team"]),
            seniority: firstString(record, ["seniority", "level"]),
            descriptionHtml: firstString(record, ["descriptionHtml", "description"]),
            descriptionPlain: firstString(record, ["descriptionPlain"]),
            jobDescription: firstString(record, ["jobDescription", "overview"]),
            requirements: firstString(record, ["requirements", "qualifications"]),
            summary: firstString(record, ["summary", "aboutRole"]),
            createdAt: firstString(record, ["createdAt", "datePosted"]),
            publishedAt: firstString(record, ["publishedAt"]),
            updatedAt: firstString(record, ["updatedAt"]),
          }) satisfies AshbyCandidate,
      )
    : [];

  if (collected.length > 0) {
    return collected;
  }

  const links = Array.from(
    html.matchAll(
      new RegExp(
        `href=["'](https:\\/\\/jobs\\.ashbyhq\\.com\\/${companyToken}[^"']+)["']`,
        "gi",
      ),
    ),
  )
    .map((match) => match[1])
    .filter(Boolean);

  return links.map(
    (link, index) =>
      ({
        id: `${companyToken}-${index}`,
        title: `Ashby role ${index + 1}`,
        location: "Location unavailable",
        jobUrl: link,
      }) satisfies AshbyCandidate,
  );
}

function extractAshbyCandidatesFromAppData(html: string, companyToken: string) {
  const appData = extractWindowAppData(html) as
    | {
        jobBoard?: {
          jobPostings?: Array<Record<string, unknown>>;
        };
      }
    | undefined;

  const postings = appData?.jobBoard?.jobPostings;
  if (!Array.isArray(postings)) {
    return undefined;
  }

  return postings
    .filter((posting) => Boolean(firstString(posting, ["id", "jobId"]) && firstString(posting, ["title"])))
    .map((posting) => {
      const postingId = firstString(posting, ["id", "jobId"]);
      const sourceUrl =
        firstString(posting, ["jobUrl", "absoluteUrl", "url"]) ??
        `https://jobs.ashbyhq.com/${companyToken}/${postingId}`;

      return {
        id: postingId,
        title: firstString(posting, ["title"]),
        locationName: firstString(posting, ["locationName", "location"]),
        employmentType: firstString(posting, ["employmentType"]),
        departmentName: firstString(posting, ["departmentName", "department"]),
        teamName: firstString(posting, ["teamName", "team"]),
        seniority: firstString(posting, ["seniority", "level"]),
        descriptionHtml: firstString(posting, ["descriptionHtml", "description"]),
        descriptionPlain: firstString(posting, ["descriptionPlain"]),
        jobDescription: firstString(posting, ["jobDescription", "overview"]),
        requirements: firstString(posting, ["requirements", "qualifications"]),
        summary: firstString(posting, ["summary", "aboutRole"]),
        publishedAt: firstString(posting, ["publishedDate", "publishedAt"]),
        updatedAt: firstString(posting, ["updatedAt"]),
        jobUrl: sourceUrl,
      } satisfies AshbyCandidate;
    });
}

function unsupportedResult(message: string): ProviderResult {
  return {
    provider: "ashby",
    status: "unsupported",
    jobs: [],
    fetchedCount: 0,
    matchedCount: 0,
    errorMessage: message,
  };
}

function buildAshbyExperienceHint(candidate: AshbyCandidate) {
  return buildExperiencePrompt(
    candidate.title,
    candidate.employmentType,
    candidate.departmentName,
    candidate.teamName,
    candidate.seniority,
    candidate.descriptionPlain,
    candidate.descriptionHtml,
    candidate.jobDescription,
    candidate.requirements,
    candidate.summary,
  );
}
