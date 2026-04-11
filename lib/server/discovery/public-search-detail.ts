import "server-only";

import type { NormalizedJobSeed } from "@/lib/server/providers/types";
import type { PublicSearchCandidate } from "@/lib/server/discovery/public-search-candidates";
import { extractAshbyJobFromDetailUrl } from "@/lib/server/providers/ashby";
import { extractGreenhouseJobFromDetailUrl } from "@/lib/server/providers/greenhouse";
import { extractLeverJobFromDetailUrl } from "@/lib/server/providers/lever";
import { extractWorkdayJobFromDetailUrl } from "@/lib/server/providers/workday";

export async function extractDirectJobFromPublicSearchCandidate(input: {
  candidate: PublicSearchCandidate;
  companyHint?: string;
  discoveredAt: string;
  fetchImpl: typeof fetch;
}): Promise<NormalizedJobSeed | undefined> {
  const { candidate } = input;
  if (candidate.kind !== "detail" || !candidate.detailToken || !candidate.detailJobId) {
    return undefined;
  }

  if (candidate.platform === "greenhouse") {
    return extractGreenhouseJobFromDetailUrl({
      detailUrl: candidate.url,
      boardSlug: candidate.detailToken,
      jobId: candidate.detailJobId,
      companyHint: input.companyHint,
      discoveredAt: input.discoveredAt,
      fetchImpl: input.fetchImpl,
    });
  }

  if (candidate.platform === "lever") {
    return extractLeverJobFromDetailUrl({
      detailUrl: candidate.url,
      siteToken: candidate.detailToken,
      jobId: candidate.detailJobId,
      companyHint: input.companyHint,
      discoveredAt: input.discoveredAt,
      fetchImpl: input.fetchImpl,
    });
  }

  if (candidate.platform === "ashby") {
    return extractAshbyJobFromDetailUrl({
      detailUrl: candidate.url,
      companyToken: candidate.detailToken,
      companyHint: input.companyHint,
      discoveredAt: input.discoveredAt,
      fetchImpl: input.fetchImpl,
    });
  }

  if (candidate.platform === "workday") {
    return extractWorkdayJobFromDetailUrl({
      detailUrl: candidate.url,
      source: {
        url: candidate.recoveredSource?.url ?? candidate.url,
        token: candidate.recoveredSource?.token ?? candidate.detailToken,
        sitePath:
          candidate.recoveredSource?.platform === "workday"
            ? candidate.recoveredSource.sitePath
            : undefined,
        careerSitePath:
          candidate.recoveredSource?.platform === "workday"
            ? candidate.recoveredSource.careerSitePath
            : undefined,
        companyHint: input.companyHint,
        apiUrl:
          candidate.recoveredSource?.platform === "workday"
            ? candidate.recoveredSource.apiUrl
            : undefined,
      },
      discoveredAt: input.discoveredAt,
      fetchImpl: input.fetchImpl,
    });
  }

  return undefined;
}
