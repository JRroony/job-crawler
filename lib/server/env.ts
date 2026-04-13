import "server-only";

import { z } from "zod";

import { resolveGreenhouseRegistryTokens } from "@/lib/server/discovery/greenhouse-registry";
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
