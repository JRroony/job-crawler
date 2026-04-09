import "server-only";

import { z } from "zod";

import { resolveGreenhouseRegistryTokens } from "@/lib/server/discovery/greenhouse-registry";
import { companyPageSourceConfigSchema } from "@/lib/types";

const envSchema = z.object({
  MONGODB_URI: z.string().url().default("mongodb://127.0.0.1:27017/job_crawler"),
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
  PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS: z.coerce.number().int().positive().default(8),
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
        process.env.PUBLIC_SEARCH_DISCOVERY_MAX_RESULTS ?? "8",
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
