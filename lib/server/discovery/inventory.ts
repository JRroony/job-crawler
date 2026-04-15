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
] as const;

export const sourceInventoryOriginSchema = z.enum(sourceInventoryOrigins);

export const sourceInventoryRecordSchema = z.object({
  _id: z.string().min(1),
  platform: crawlerPlatformSchema,
  url: z.string().url(),
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
  inventoryRank: z.number().int().nonnegative().default(0),
  firstSeenAt: z.string().datetime(),
  lastSeenAt: z.string().datetime(),
  lastRefreshedAt: z.string().datetime(),
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
    inventoryRank: input.inventoryRank ?? 0,
    firstSeenAt: input.now,
    lastSeenAt: input.now,
    lastRefreshedAt: input.now,
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
