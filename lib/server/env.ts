import "server-only";

import { z } from "zod";

import { resolveGreenhouseRegistryTokens } from "@/lib/server/discovery/greenhouse-registry";
import {
  parseSourceRegistryConfig,
  parseWorkdaySourceRegistryConfig,
} from "@/lib/server/discovery/source-registry";
import { companyPageSourceConfigSchema } from "@/lib/types";

const envSchema = z.object({
  MONGODB_URI: z.string().url().default("mongodb://127.0.0.1:27017/job_crawler"),
  MONGODB_SERVER_SELECTION_TIMEOUT_MS: z.coerce.number().int().positive().default(1500),
  MONGODB_UNAVAILABLE_COOLDOWN_MS: z.coerce.number().int().nonnegative().default(15000),
  LINK_VALIDATION_TTL_MINUTES: z.coerce.number().int().positive().default(360),
  GREENHOUSE_BOARD_TOKENS: z.string().default(""),
  GREENHOUSE_BOARD_REGISTRY_APPEND: z.string().default(""),
  LEVER_SITE_TOKENS: z.string().default("figma,plaid,robinhood"),
  ASHBY_BOARD_TOKENS: z.string().default("notion,ramp,replit"),
  SOURCE_REGISTRY_CONFIG: z.string().optional(),
  WORKDAY_SOURCE_CONFIG: z.string().optional(),
  COMPANY_PAGE_SOURCE_CONFIG: z.string().optional(),
  PUBLIC_SEARCH_DISCOVERY_ENABLED: z
    .enum(["true", "false"])
    .default("true")
    .transform((value) => value === "true"),
  PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: z.coerce.number().int().positive().default(20),
  PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES: z.coerce.number().int().positive().default(120),
  PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES: z.coerce.number().int().positive().default(96),
  PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY: z.coerce.number().int().positive().default(4),
  GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES: z.coerce.number().int().positive().default(32),
  CRAWL_MAX_SOURCES: z.coerce.number().int().positive().default(40),
  CRAWL_PROVIDER_TIMEOUT_MS: z.coerce.number().int().positive().default(9000),
  CRAWL_PROGRESS_UPDATE_INTERVAL_MS: z.coerce.number().int().positive().default(250),
  CRAWL_GLOBAL_TIMEOUT_MS: z.coerce.number().int().positive().default(60000),
  CRAWL_INITIAL_VISIBLE_WAIT_MS: z.coerce.number().int().nonnegative().default(400),
  CRAWL_TARGET_JOB_COUNT: z.coerce.number().int().positive().default(30),
  CRAWL_EARLY_VISIBLE_TARGET: z.coerce.number().int().positive().default(30),
  SEARCH_MIN_COVERAGE_FAST: z.coerce.number().int().positive().default(12),
  SEARCH_MIN_COVERAGE_BALANCED: z.coerce.number().int().positive().default(30),
  SEARCH_MIN_COVERAGE_DEEP: z.coerce.number().int().positive().default(60),
  SEARCH_BROAD_COUNTRY_MIN_COVERAGE: z.coerce.number().int().positive().default(75),
  SEARCH_HIGH_DEMAND_ROLE_MIN_COVERAGE: z.coerce.number().int().positive().default(120),
  INDEXED_SEARCH_MERGED_CANDIDATE_LIMIT: z.coerce.number().int().positive().default(5000),
  INDEXED_SEARCH_CHANNEL_CANDIDATE_LIMIT: z.coerce.number().int().positive().default(5000),
  BACKGROUND_INGESTION_ENABLED: z
    .enum(["true", "false"])
    .default("true")
    .transform((value) => value === "true"),
  BACKGROUND_INGESTION_INTERVAL_MS: z.coerce.number().int().positive().default(120000),
  BACKGROUND_INGESTION_PROFILES_PER_CYCLE: z.coerce.number().int().positive().default(4),
  BACKGROUND_INGESTION_MAX_SOURCES_PER_CYCLE: z.coerce.number().int().positive().default(160),
  BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS: z.coerce.number().int().positive().default(120000),
  BACKGROUND_INGESTION_STALE_AFTER_MS: z.coerce.number().int().positive().default(1800000),
  BACKGROUND_INGESTION_RUN_TIMEOUT_MS: z.coerce.number().int().positive().default(900000),
});

type ParsedEnv = z.infer<typeof envSchema>;

let cachedEnv: ParsedEnv | null = null;

function parseCompanyPageConfig(value?: string) {
  if (!value) {
    return [];
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    return z.array(companyPageSourceConfigSchema).parse(parsed);
  } catch {
    return [];
  }
}

export function getEnv() {
  if (!cachedEnv) {
    cachedEnv = envSchema.parse({
      MONGODB_URI: process.env.MONGODB_URI ?? "mongodb://127.0.0.1:27017/job_crawler",
      MONGODB_SERVER_SELECTION_TIMEOUT_MS:
        process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "1500",
      MONGODB_UNAVAILABLE_COOLDOWN_MS:
        process.env.MONGODB_UNAVAILABLE_COOLDOWN_MS ?? "15000",
      LINK_VALIDATION_TTL_MINUTES: process.env.LINK_VALIDATION_TTL_MINUTES ?? "360",
      GREENHOUSE_BOARD_TOKENS: process.env.GREENHOUSE_BOARD_TOKENS ?? "",
      GREENHOUSE_BOARD_REGISTRY_APPEND:
        process.env.GREENHOUSE_BOARD_REGISTRY_APPEND ?? "",
      LEVER_SITE_TOKENS: process.env.LEVER_SITE_TOKENS ?? "figma,plaid,robinhood",
      ASHBY_BOARD_TOKENS: process.env.ASHBY_BOARD_TOKENS ?? "notion,ramp,replit",
      SOURCE_REGISTRY_CONFIG: process.env.SOURCE_REGISTRY_CONFIG,
      WORKDAY_SOURCE_CONFIG: process.env.WORKDAY_SOURCE_CONFIG,
      COMPANY_PAGE_SOURCE_CONFIG: process.env.COMPANY_PAGE_SOURCE_CONFIG,
      PUBLIC_SEARCH_DISCOVERY_ENABLED:
        process.env.PUBLIC_SEARCH_DISCOVERY_ENABLED ?? "true",
      PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS:
        process.env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS ?? "20",
      PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES:
        process.env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES ?? "120",
      PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES:
        process.env.PUBLIC_SEARCH_DISCOVERY_MAX_QUERIES ?? "96",
      PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY:
        process.env.PUBLIC_SEARCH_DISCOVERY_QUERY_CONCURRENCY ?? "4",
      GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES:
        process.env.GREENHOUSE_DISCOVERY_MAX_LOCATION_CLAUSES ?? "32",
      CRAWL_MAX_SOURCES:
        process.env.CRAWL_MAX_SOURCES ?? "40",
      CRAWL_PROVIDER_TIMEOUT_MS:
        process.env.CRAWL_PROVIDER_TIMEOUT_MS ?? "9000",
      CRAWL_PROGRESS_UPDATE_INTERVAL_MS:
        process.env.CRAWL_PROGRESS_UPDATE_INTERVAL_MS ?? "250",
      CRAWL_GLOBAL_TIMEOUT_MS:
        process.env.CRAWL_GLOBAL_TIMEOUT_MS ?? "60000",
      CRAWL_INITIAL_VISIBLE_WAIT_MS:
        process.env.CRAWL_INITIAL_VISIBLE_WAIT_MS ?? "400",
      CRAWL_TARGET_JOB_COUNT:
        process.env.CRAWL_TARGET_JOB_COUNT ?? "30",
      CRAWL_EARLY_VISIBLE_TARGET:
        process.env.CRAWL_EARLY_VISIBLE_TARGET ?? "30",
      SEARCH_MIN_COVERAGE_FAST:
        process.env.SEARCH_MIN_COVERAGE_FAST ?? "12",
      SEARCH_MIN_COVERAGE_BALANCED:
        process.env.SEARCH_MIN_COVERAGE_BALANCED ?? "30",
      SEARCH_MIN_COVERAGE_DEEP:
        process.env.SEARCH_MIN_COVERAGE_DEEP ?? "60",
      SEARCH_BROAD_COUNTRY_MIN_COVERAGE:
        process.env.SEARCH_BROAD_COUNTRY_MIN_COVERAGE ?? "75",
      SEARCH_HIGH_DEMAND_ROLE_MIN_COVERAGE:
        process.env.SEARCH_HIGH_DEMAND_ROLE_MIN_COVERAGE ?? "120",
      INDEXED_SEARCH_MERGED_CANDIDATE_LIMIT:
        process.env.INDEXED_SEARCH_MERGED_CANDIDATE_LIMIT ?? "5000",
      INDEXED_SEARCH_CHANNEL_CANDIDATE_LIMIT:
        process.env.INDEXED_SEARCH_CHANNEL_CANDIDATE_LIMIT ?? "5000",
      BACKGROUND_INGESTION_ENABLED:
        process.env.BACKGROUND_INGESTION_ENABLED ?? "true",
      BACKGROUND_INGESTION_INTERVAL_MS:
        process.env.BACKGROUND_INGESTION_INTERVAL_MS ?? "120000",
      BACKGROUND_INGESTION_PROFILES_PER_CYCLE:
        process.env.BACKGROUND_INGESTION_PROFILES_PER_CYCLE ?? "4",
      BACKGROUND_INGESTION_MAX_SOURCES_PER_CYCLE:
        process.env.BACKGROUND_INGESTION_MAX_SOURCES_PER_CYCLE ?? "160",
      BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS:
        process.env.BACKGROUND_INGESTION_PROVIDER_TIMEOUT_MS ?? "120000",
      BACKGROUND_INGESTION_STALE_AFTER_MS:
        process.env.BACKGROUND_INGESTION_STALE_AFTER_MS ?? "1800000",
      BACKGROUND_INGESTION_RUN_TIMEOUT_MS:
        process.env.BACKGROUND_INGESTION_RUN_TIMEOUT_MS ?? "900000",
    });
  }

  return {
    ...cachedEnv,
    greenhouseBoardTokens: resolveGreenhouseRegistryTokens(
      tokenize(cachedEnv.GREENHOUSE_BOARD_REGISTRY_APPEND),
      tokenize(cachedEnv.GREENHOUSE_BOARD_TOKENS),
    ),
    leverSiteTokens: tokenize(cachedEnv.LEVER_SITE_TOKENS),
    ashbyBoardTokens: tokenize(cachedEnv.ASHBY_BOARD_TOKENS),
    sourceRegistryEntries: [
      ...parseSourceRegistryConfig(cachedEnv.SOURCE_REGISTRY_CONFIG),
      ...parseWorkdaySourceRegistryConfig(cachedEnv.WORKDAY_SOURCE_CONFIG),
    ],
    companyPageSources: parseCompanyPageConfig(cachedEnv.COMPANY_PAGE_SOURCE_CONFIG),
  };
}

function tokenize(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export type AppEnv = ReturnType<typeof getEnv>;
