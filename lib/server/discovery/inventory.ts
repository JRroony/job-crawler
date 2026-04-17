import "server-only";

import { z } from "zod";

import { slugToLabel } from "@/lib/server/crawler/helpers";
import { discoverCatalogSources } from "@/lib/server/discovery/catalog";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import { getDefaultGreenhouseRegistryEntries } from "@/lib/server/discovery/greenhouse-registry";
import { discoveryMethods, type DiscoveredSource } from "@/lib/server/discovery/types";
import type { AppEnv } from "@/lib/server/env";
import { crawlerPlatformSchema } from "@/lib/types";

export const sourceInventoryOrigins = [
  "greenhouse_registry",
  "configured_env",
  "manual_config",
  "curated_catalog",
  "public_search",
] as const;

export const sourceInventoryOriginSchema = z.enum(sourceInventoryOrigins);

export const sourceInventorySourceTypes = [
  "ats_board",
  "job_detail",
  "company_page",
  "feed",
  "career_site",
  "unknown",
] as const;

export const sourceInventorySourceTypeSchema = z.enum(sourceInventorySourceTypes);

export const sourceInventoryStatuses = ["active", "paused", "disabled"] as const;

export const sourceInventoryStatusSchema = z.enum(sourceInventoryStatuses);

export const sourceInventoryHealthStates = [
  "healthy",
  "degraded",
  "failing",
  "unknown",
] as const;

export const sourceInventoryHealthSchema = z.enum(sourceInventoryHealthStates);

export const sourceInventoryRecordSchema = z.object({
  _id: z.string().min(1),
  platform: crawlerPlatformSchema,
  url: z.string().url(),
  sourceType: sourceInventorySourceTypeSchema,
  sourceKey: z.string().min(1),
  token: z.string().min(1).optional(),
  companyHint: z.string().min(1).optional(),
  confidence: z.enum(["high", "medium", "low"]),
  inventoryOrigin: sourceInventoryOriginSchema,
  originalDiscoveryMethod: z.enum(discoveryMethods),
  jobId: z.string().min(1).optional(),
  boardUrl: z.string().url().optional(),
  hostedUrl: z.string().url().optional(),
  apiUrl: z.string().url().optional(),
  pageType: z.enum(["json_feed", "json_ld_page", "html_page"]).optional(),
  sitePath: z.string().min(1).optional(),
  careerSitePath: z.string().min(1).optional(),
  jobUrl: z.string().url().optional(),
  status: sourceInventoryStatusSchema.default("active"),
  health: sourceInventoryHealthSchema.default("unknown"),
  crawlPriority: z.number().int().nonnegative().default(0),
  inventoryRank: z.number().int().nonnegative().default(0),
  failureCount: z.number().int().nonnegative().default(0),
  consecutiveFailures: z.number().int().nonnegative().default(0),
  lastFailureReason: z.string().min(1).optional(),
  firstSeenAt: z.string().datetime(),
  lastSeenAt: z.string().datetime(),
  lastRefreshedAt: z.string().datetime(),
  lastCrawledAt: z.string().datetime().optional(),
  lastSucceededAt: z.string().datetime().optional(),
  lastFailedAt: z.string().datetime().optional(),
  nextEligibleAt: z.string().datetime().optional(),
});

export type SourceInventoryRecord = z.infer<typeof sourceInventoryRecordSchema>;

type DiscoveryEnvSnapshot = Pick<
  AppEnv,
  "greenhouseBoardTokens" | "leverSiteTokens" | "ashbyBoardTokens" | "companyPageSources"
>;

export function buildSourceInventorySeeds(
  env: DiscoveryEnvSnapshot,
): SourceInventoryRecord[] {
  const seeds: SourceInventoryRecord[] = [];

  getDefaultGreenhouseRegistryEntries().forEach((entry, index) => {
    seeds.push(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://boards.greenhouse.io/${entry.token}`,
          token: entry.token,
          companyHint: entry.companyHint,
          confidence: "high",
          discoveryMethod: "platform_registry",
        }),
        {
          now: new Date(0).toISOString(),
          inventoryOrigin: "greenhouse_registry",
          inventoryRank: index,
        },
      ),
    );
  });

  env.greenhouseBoardTokens.forEach((token, index) => {
    seeds.push(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://boards.greenhouse.io/${token}`,
          token,
          companyHint: slugToLabel(token),
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
        {
          now: new Date(0).toISOString(),
          inventoryOrigin: "configured_env",
          inventoryRank: index,
        },
      ),
    );
  });

  env.leverSiteTokens.forEach((token, index) => {
    seeds.push(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://jobs.lever.co/${token}`,
          token,
          companyHint: slugToLabel(token),
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
        {
          now: new Date(0).toISOString(),
          inventoryOrigin: "configured_env",
          inventoryRank: 10_000 + index,
        },
      ),
    );
  });

  env.ashbyBoardTokens.forEach((token, index) => {
    seeds.push(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: `https://jobs.ashbyhq.com/${token}`,
          token,
          companyHint: slugToLabel(token),
          confidence: "high",
          discoveryMethod: "configured_env",
        }),
        {
          now: new Date(0).toISOString(),
          inventoryOrigin: "configured_env",
          inventoryRank: 20_000 + index,
        },
      ),
    );
  });

  env.companyPageSources.forEach((source, index) => {
    seeds.push(
      toSourceInventoryRecord(
        classifySourceCandidate({
          url: source.url,
          companyHint: source.company,
          pageType: source.type,
          confidence: source.type === "json_feed" ? "high" : "medium",
          discoveryMethod: "manual_config",
        }),
        {
          now: new Date(0).toISOString(),
          inventoryOrigin: "manual_config",
          inventoryRank: 30_000 + index,
        },
      ),
    );
  });

  discoverCatalogSources().forEach((source, index) => {
    seeds.push(
      toSourceInventoryRecord(source, {
        now: new Date(0).toISOString(),
        inventoryOrigin: "curated_catalog",
        inventoryRank: 40_000 + index,
      }),
    );
  });

  return dedupeSourceInventoryRecords(seeds);
}

export function toSourceInventoryRecord(
  source: DiscoveredSource,
  input: {
    now: string;
    inventoryOrigin: SourceInventoryRecord["inventoryOrigin"];
    inventoryRank?: number;
  },
): SourceInventoryRecord {
  return sourceInventoryRecordSchema.parse({
    _id: source.id,
    platform: source.platform,
    url: source.url,
    sourceType: resolveSourceInventorySourceType(source),
    sourceKey: resolveSourceInventorySourceKey(source),
    token: source.token,
    companyHint: source.companyHint,
    confidence: source.confidence,
    inventoryOrigin: input.inventoryOrigin,
    originalDiscoveryMethod: source.discoveryMethod,
    jobId: "jobId" in source ? source.jobId : undefined,
    boardUrl: "boardUrl" in source ? source.boardUrl : undefined,
    hostedUrl: "hostedUrl" in source ? source.hostedUrl : undefined,
    apiUrl: "apiUrl" in source ? source.apiUrl : undefined,
    pageType: "pageType" in source ? source.pageType : undefined,
    sitePath: "sitePath" in source ? source.sitePath : undefined,
    careerSitePath: "careerSitePath" in source ? source.careerSitePath : undefined,
    jobUrl: "jobUrl" in source ? source.jobUrl : undefined,
    status: "active",
    health: "unknown",
    crawlPriority: input.inventoryRank ?? 0,
    inventoryRank: input.inventoryRank ?? 0,
    failureCount: 0,
    consecutiveFailures: 0,
    firstSeenAt: input.now,
    lastSeenAt: input.now,
    lastRefreshedAt: input.now,
    nextEligibleAt: input.now,
  });
}

export function toDiscoveredSourceFromInventory(
  record: SourceInventoryRecord,
): DiscoveredSource {
  return {
    id: record._id,
    platform: record.platform,
    url: record.url,
    token: record.token,
    companyHint: record.companyHint,
    confidence: record.confidence,
    discoveryMethod: "source_inventory",
    ...(record.jobId ? { jobId: record.jobId } : {}),
    ...(record.boardUrl ? { boardUrl: record.boardUrl } : {}),
    ...(record.hostedUrl ? { hostedUrl: record.hostedUrl } : {}),
    ...(record.apiUrl ? { apiUrl: record.apiUrl } : {}),
    ...(record.pageType ? { pageType: record.pageType } : {}),
    ...(record.sitePath ? { sitePath: record.sitePath } : {}),
    ...(record.careerSitePath ? { careerSitePath: record.careerSitePath } : {}),
    ...(record.jobUrl ? { jobUrl: record.jobUrl } : {}),
  } as DiscoveredSource;
}

function dedupeSourceInventoryRecords(records: SourceInventoryRecord[]) {
  const deduped = new Map<string, SourceInventoryRecord>();

  for (const record of records) {
    const existing = deduped.get(record._id);
    if (!existing || record.inventoryRank < existing.inventoryRank) {
      deduped.set(record._id, record);
    }
  }

  return Array.from(deduped.values());
}

export function inventoryOriginFromDiscoveryMethod(
  method: SourceInventoryRecord["originalDiscoveryMethod"],
): SourceInventoryRecord["inventoryOrigin"] {
  switch (method) {
    case "platform_registry":
      return "greenhouse_registry";
    case "configured_env":
      return "configured_env";
    case "manual_config":
      return "manual_config";
    case "curated_catalog":
      return "curated_catalog";
    case "future_search":
    case "source_inventory":
      return "public_search";
    default:
      return "public_search";
  }
}

function resolveSourceInventorySourceType(
  source: DiscoveredSource,
): SourceInventoryRecord["sourceType"] {
  if (source.platform === "company_page") {
    if (source.pageType === "json_feed") {
      return "feed";
    }

    return "company_page";
  }

  if ("jobId" in source && source.jobId) {
    return "job_detail";
  }

  if (source.platform === "workday") {
    return source.careerSitePath ? "career_site" : "unknown";
  }

  if (
    source.platform === "greenhouse" ||
    source.platform === "lever" ||
    source.platform === "ashby" ||
    source.platform === "smartrecruiters"
  ) {
    return "ats_board";
  }

  return "unknown";
}

function resolveSourceInventorySourceKey(source: DiscoveredSource) {
  if (source.token) {
    return source.token;
  }

  if (source.companyHint) {
    return `${source.platform}:${source.companyHint.toLowerCase()}`;
  }

  return source.id;
}
