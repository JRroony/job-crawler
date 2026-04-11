import "server-only";

import { parseAshbyUrl } from "@/lib/server/discovery/ashby-url";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { parseGreenhouseUrl } from "@/lib/server/discovery/greenhouse-url";
import { parseLeverUrl } from "@/lib/server/discovery/lever-url";
import type {
  DiscoveredSource,
  DiscoveredPlatform,
  DiscoveryMethod,
} from "@/lib/server/discovery/types";
import { parseWorkdayUrl } from "@/lib/server/discovery/workday-url";

export const publicSearchCandidateKinds = [
  "detail",
  "source",
  "other",
] as const;

export type PublicSearchCandidateKind = (typeof publicSearchCandidateKinds)[number];

export type PublicSearchCandidate = {
  url: string;
  platform: DiscoveredPlatform;
  kind: PublicSearchCandidateKind;
  recoveredSource?: DiscoveredSource;
  recoveryKind: "detail_recovery" | "source_classification" | "none";
  detailToken?: string;
  detailJobId?: string;
};

export function classifyPublicSearchCandidate(
  url: string,
  discoveryMethod: DiscoveryMethod,
): PublicSearchCandidate {
  const greenhouse = parseGreenhouseUrl(url);
  if (greenhouse?.boardSlug) {
    return {
      url: greenhouse.jobId && greenhouse.canonicalJobUrl
        ? greenhouse.canonicalJobUrl
        : greenhouse.canonicalBoardUrl ?? url,
      platform: "greenhouse",
      kind: greenhouse.jobId ? "detail" : "source",
      recoveredSource: classifySourceCandidate({
        url,
        discoveryMethod,
      }),
      recoveryKind: greenhouse.jobId ? "detail_recovery" : "source_classification",
      detailToken: greenhouse.boardSlug,
      detailJobId: greenhouse.jobId,
    };
  }

  const lever = parseLeverUrl(url);
  if (lever?.siteToken) {
    return {
      url: lever.jobId && lever.canonicalJobUrl
        ? lever.canonicalJobUrl
        : lever.canonicalHostedUrl ?? url,
      platform: "lever",
      kind: lever.jobId ? "detail" : "source",
      recoveredSource: classifySourceCandidate({
        url,
        discoveryMethod,
      }),
      recoveryKind: lever.jobId ? "detail_recovery" : "source_classification",
      detailToken: lever.siteToken,
      detailJobId: lever.jobId,
    };
  }

  const ashby = parseAshbyUrl(url);
  if (ashby?.companyToken) {
    return {
      url: ashby.jobPath && ashby.canonicalJobUrl
        ? ashby.canonicalJobUrl
        : ashby.canonicalBoardUrl ?? url,
      platform: "ashby",
      kind: ashby.jobPath ? "detail" : "source",
      recoveredSource: classifySourceCandidate({
        url,
        discoveryMethod,
      }),
      recoveryKind: ashby.jobPath ? "detail_recovery" : "source_classification",
      detailToken: ashby.companyToken,
      detailJobId: ashby.jobPath,
    };
  }

  const workday = parseWorkdayUrl(url);
  if (workday) {
    return {
      url: workday.canonicalJobUrl ?? workday.canonicalSourceUrl ?? url,
      platform: "workday",
      kind: workday.kind === "job" ? "detail" : "source",
      recoveredSource: classifySourceCandidate({
        url,
        discoveryMethod,
      }),
      recoveryKind: workday.kind === "job" ? "detail_recovery" : "source_classification",
      detailToken: workday.token,
      detailJobId: workday.jobPath,
    };
  }

  const recoveredSource = classifySourceCandidate({
    url,
    discoveryMethod,
  });

  return {
    url,
    platform: recoveredSource.platform,
    kind: recoveredSource.platform === "company_page" ? "source" : "other",
    recoveredSource,
    recoveryKind: recoveredSource.platform === "company_page"
      ? "source_classification"
      : "none",
  };
}
