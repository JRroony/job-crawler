import "server-only";

import { z } from "zod";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { SourceInventoryRecord } from "@/lib/server/discovery/inventory";
import {
  getDefaultWorkdaySourceRegistryEntries,
  parseWorkdaySourceRegistryConfig as parseWorkdayRegistryConfig,
  resolveWorkdayRegistryEntryApiUrl,
  resolveWorkdayRegistryEntryHost,
  resolveWorkdayRegistryEntrySourceUrl,
  resolveWorkdayRegistryEntryToken,
  workdaySourceRegistryEntrySchema,
  type WorkdaySourceRegistryEntry,
} from "@/lib/server/discovery/workday-registry";

const registryPlatforms = ["lever", "ashby", "workday"] as const;

const sourceRegistryHealthSchema = z.enum(["healthy", "degraded", "failing", "unknown"]);
const sourceRegistryStatusSchema = z.enum(["active", "paused", "disabled"]);

const sourceRegistryCommonSchema = z.object({
  platform: z.enum(registryPlatforms),
  company: z.string().min(1),
  companyHint: z.string().min(1).optional(),
  confidence: z.enum(["high", "medium", "low"]).default("high"),
  canonicalListUrl: z.string().url().optional(),
  coverageTags: z.array(z.string().min(1)).default([]),
  companyMetadata: z.record(z.string(), z.unknown()).default({}),
  status: sourceRegistryStatusSchema.default("active"),
  health: sourceRegistryHealthSchema.default("unknown"),
  crawlPriority: z.number().int().nonnegative().optional(),
});

const leverRegistryEntrySchema = sourceRegistryCommonSchema.extend({
  platform: z.literal("lever"),
  token: z.string().min(1),
});

const ashbyRegistryEntrySchema = sourceRegistryCommonSchema.extend({
  platform: z.literal("ashby"),
  token: z.string().min(1),
});

const workdayRegistryEntrySchema = sourceRegistryCommonSchema.extend({
  platform: z.literal("workday"),
  token: z.string().min(1).optional(),
  tenant: z.string().min(1),
  sitePath: z.string().min(1).optional(),
  careerSitePath: z.string().min(1),
  host: z.string().min(1).optional(),
  apiUrl: z.string().url().optional(),
});

export const sourceRegistryEntrySchema = z.discriminatedUnion("platform", [
  leverRegistryEntrySchema,
  ashbyRegistryEntrySchema,
  workdayRegistryEntrySchema,
]);

export type SourceRegistryEntry = z.output<typeof sourceRegistryEntrySchema>;
export type SourceRegistryEntryInput = z.input<typeof sourceRegistryEntrySchema>;

type RegistryPlatform = SourceRegistryEntry["platform"];

const defaultSourceRegistryEntries = [
  lever("employ", "Employ", ["united_states", "hr_tech"]),
  lever("sugarcrm", "SugarCRM", ["united_states", "saas"]),
  lever("jobgether", "Jobgether", ["remote", "united_states"]),
  lever("applydigital", "Apply Digital", ["global", "canada"]),
  lever("shyftlabs", "ShyftLabs", ["global", "data"]),
  lever("dnb", "Dun & Bradstreet", ["global", "data"]),
  lever("meds", "Meds.com", ["united_states", "healthcare"]),
  lever("Flex", "Flex", ["remote", "united_states", "fintech"]),
  lever("revealtech", "Reveal Technology", ["remote", "united_states"]),
  lever("immuta", "Immuta", ["remote", "united_states", "data"]),
  lever("kraken123", "Kraken", ["remote", "united_states"]),
  lever("canarytechnologies", "Canary Technologies", ["remote", "united_states"]),
  lever("perforce", "Perforce", ["remote", "united_states"]),
  lever("arcadia", "Arcadia", ["remote", "united_states", "healthcare"]),
  lever("empassion.com", "Empassion", ["remote", "united_states", "healthcare"]),

  lever("figma", "Figma", ["global", "canada_possible"]),
  lever("plaid", "Plaid", ["global", "canada_possible"]),
  lever("robinhood", "Robinhood", ["global", "canada_possible"]),
  lever("sourcegraph", "Sourcegraph", ["remote", "canada_possible"]),
  lever("postman", "Postman", ["global", "canada_possible"]),
  lever("wealthsimple", "Wealthsimple", ["canada", "fintech"]),
  lever("koho", "KOHO", ["canada", "fintech"]),
  lever("applyboard", "ApplyBoard", ["canada", "education"]),
  lever("stackadapt", "StackAdapt", ["canada", "advertising"]),
  lever("ecobee", "ecobee", ["canada", "iot"]),
  lever("clutch", "Clutch", ["canada", "marketplace"]),
  lever("clearco", "Clearco", ["canada", "fintech"]),
  lever("wave", "Wave", ["canada", "fintech"]),
  lever("faire", "Faire", ["global", "canada_possible"]),
  lever("benchling", "Benchling", ["global", "canada_possible"]),
  lever("zapier", "Zapier", ["remote", "canada_possible"]),

  ashby("ClickHouse", "ClickHouse", ["global", "data"]),
  ashby("Watershed", "Watershed", ["united_states", "climate"]),
  ashby("Sierra", "Sierra", ["united_states", "ai"]),
  ashby("Cursor", "Cursor", ["global", "ai"]),
  ashby("Anysphere", "Anysphere", ["global", "ai"]),
  ashby("Harvey", "Harvey", ["united_states", "ai"]),
  ashby("dbt%20Labs", "dbt Labs", ["global", "data"]),
  ashby("Weights%20%26%20Biases", "Weights & Biases", ["global", "ai"]),
  ashby("Writer", "Writer", ["global", "ai"]),
  ashby("Coda", "Coda", ["global", "productivity"]),
  ashby("Astral", "Astral", ["remote", "developer_tools"]),
  ashby("Cohesity", "Cohesity", ["global", "data"]),
  ashby("Crusoe", "Crusoe", ["united_states", "infrastructure"]),
  ashby("Decagon", "Decagon", ["united_states", "ai"]),
  ashby("Eight%20Sleep", "Eight Sleep", ["global", "consumer"]),
  ashby("Glean", "Glean", ["global", "ai"]),
  ashby("LangChain", "LangChain", ["global", "ai", "developer_tools"]),
  ashby("Luma%20AI", "Luma AI", ["global", "ai"]),
  ashby("Modern%20Treasury", "Modern Treasury", ["global", "fintech"]),
  ashby("Mistral%20AI", "Mistral AI", ["global", "ai"]),
  ashby("Nylas", "Nylas", ["global", "developer_tools"]),
  ashby("Pave", "Pave", ["global", "hr_tech"]),
  ashby("Pinecone", "Pinecone", ["global", "ai", "data"]),
  ashby("Render", "Render", ["global", "developer_tools"]),
  ashby("Retool", "Retool", ["global", "developer_tools"]),
  ashby("Rippling", "Rippling", ["global", "hr_tech"]),
  ashby("Tailscale", "Tailscale", ["remote", "developer_tools"]),

  ashby("notion", "Notion", ["global", "canada_possible"]),
  ashby("ramp", "Ramp", ["global", "canada_possible"]),
  ashby("replit", "Replit", ["global", "canada_possible"]),
  ashby("linear", "Linear", ["remote", "canada"]),
  ashby("vercel", "Vercel", ["global", "canada_possible"]),
  ashby("cohere", "Cohere", ["canada", "ai"]),
  ashby("perplexity", "Perplexity", ["global", "canada_possible"]),
  ashby("anthropic", "Anthropic", ["global", "canada_possible"]),
  ashby("replicate", "Replicate", ["global", "canada_possible"]),
  ashby("runway", "Runway", ["global", "canada_possible"]),
  ashby("modal", "Modal", ["global", "canada_possible"]),
  ashby("hex", "Hex", ["global", "canada_possible"]),
  ashby("air", "Air", ["global", "canada_possible"]),
  ashby("mercury", "Mercury", ["global", "canada_possible"]),
  ashby("airtable", "Airtable", ["global", "canada_possible"]),
  ashby("deel", "Deel", ["remote", "canada_possible"]),

  ...getDefaultWorkdaySourceRegistryEntries().map(workday),
] satisfies SourceRegistryEntryInput[];

export function getDefaultSourceRegistryEntries(
  platforms?: readonly RegistryPlatform[],
): SourceRegistryEntry[] {
  const selected = platforms ? new Set(platforms) : undefined;
  return defaultSourceRegistryEntries
    .filter((entry) => !selected || selected.has(entry.platform))
    .map((entry) => sourceRegistryEntrySchema.parse(entry));
}

export function parseSourceRegistryConfig(value?: string): SourceRegistryEntry[] {
  if (!value?.trim()) {
    return [];
  }

  try {
    return z.array(sourceRegistryEntrySchema).parse(JSON.parse(value));
  } catch {
    return [];
  }
}

export function parseWorkdaySourceRegistryConfig(value?: string): SourceRegistryEntry[] {
  return parseWorkdayRegistryConfig(value).map((entry) =>
    sourceRegistryEntrySchema.parse(workday(entry)),
  );
}

export function sourceRegistryEntryToDiscoveredSource(
  entry: SourceRegistryEntry,
): DiscoveredSource {
  const source = classifySourceCandidate({
    url: resolveRegistryEntryUrl(entry),
    token: resolveRegistryEntryToken(entry),
    companyHint: entry.companyHint ?? entry.company,
    confidence: entry.confidence,
    discoveryMethod: "platform_registry",
  });

  if (entry.platform !== "workday" || source.platform !== "workday") {
    return source;
  }

  return {
    ...source,
    token: resolveRegistryEntryToken(entry),
    sitePath: entry.sitePath ?? source.sitePath,
    careerSitePath: entry.careerSitePath,
    apiUrl: entry.apiUrl ?? source.apiUrl,
  };
}

export function buildSourceInventoryRegistryMetadata(entry: SourceRegistryEntry) {
  return {
    registryPlatform: entry.platform,
    company: entry.company,
    coverageTags: entry.coverageTags,
    companyMetadata: entry.companyMetadata,
    ...(entry.platform === "workday" ? buildWorkdayRegistryMetadata(entry) : {}),
    canadaRelevant:
      entry.coverageTags.includes("canada") ||
      entry.coverageTags.includes("canada_possible"),
    unitedStatesRelevant:
      entry.coverageTags.includes("united_states") ||
      entry.coverageTags.includes("global"),
  };
}

function buildWorkdayRegistryMetadata(
  entry: Extract<SourceRegistryEntry, { platform: "workday" }>,
) {
  const workdayEntry = workdaySourceRegistryEntrySchema.parse(entry);

  return {
    workdayTenant: entry.tenant,
    workdayCareerSitePath: entry.careerSitePath,
    workdaySitePath: entry.sitePath ?? entry.careerSitePath,
    workdayHost: resolveRegistryEntryHost(entry),
    workdayApiUrl: entry.apiUrl ?? resolveWorkdayRegistryEntryApiUrl(workdayEntry),
  };
}

export function applyRegistryInventoryMetadata(
  record: SourceInventoryRecord,
  entry: SourceRegistryEntry,
): SourceInventoryRecord {
  return {
    ...record,
    status: entry.status,
    health: entry.health,
    crawlPriority: entry.crawlPriority ?? record.crawlPriority,
    sourceMetadata: {
      ...record.sourceMetadata,
      ...buildSourceInventoryRegistryMetadata(entry),
    },
  };
}

function lever(token: string, company: string, coverageTags: string[]): SourceRegistryEntryInput {
  return {
    platform: "lever",
    token,
    company,
    canonicalListUrl: `https://jobs.lever.co/${token}`,
    coverageTags,
    companyMetadata: {},
  };
}

function ashby(token: string, company: string, coverageTags: string[]): SourceRegistryEntryInput {
  return {
    platform: "ashby",
    token,
    company,
    canonicalListUrl: `https://jobs.ashbyhq.com/${token}`,
    coverageTags,
    companyMetadata: {},
  };
}

function workday(entry: WorkdaySourceRegistryEntry): SourceRegistryEntryInput {
  return {
    platform: "workday",
    tenant: entry.tenant,
    token: resolveWorkdayRegistryEntryToken(entry),
    careerSitePath: entry.careerSitePath,
    sitePath: entry.sitePath,
    host: resolveWorkdayRegistryEntryHost(entry),
    canonicalListUrl: resolveWorkdayRegistryEntrySourceUrl(entry),
    apiUrl: resolveWorkdayRegistryEntryApiUrl(entry),
    company: entry.company,
    companyHint: entry.companyHint,
    confidence: entry.confidence,
    coverageTags: entry.coverageTags,
    companyMetadata: entry.companyMetadata,
    status: entry.status,
    health: entry.health,
    crawlPriority: entry.crawlPriority,
  };
}

function resolveRegistryEntryUrl(entry: SourceRegistryEntry) {
  if (entry.canonicalListUrl) {
    return entry.canonicalListUrl;
  }

  if (entry.platform === "lever") {
    return `https://jobs.lever.co/${entry.token}`;
  }

  if (entry.platform === "ashby") {
    return `https://jobs.ashbyhq.com/${entry.token}`;
  }

  const host = resolveRegistryEntryHost(entry);
  const sitePath = entry.sitePath ?? entry.careerSitePath;
  return `https://${host}/${sitePath}`;
}

function resolveRegistryEntryToken(entry: SourceRegistryEntry) {
  if (entry.platform === "workday") {
    return entry.token ?? `${entry.tenant}:${entry.careerSitePath.toLowerCase()}`;
  }

  return entry.token;
}

function resolveRegistryEntryHost(entry: Extract<SourceRegistryEntry, { platform: "workday" }>) {
  return workdaySourceRegistryEntrySchema.parse(entry).host ?? `${entry.tenant}.wd1.myworkdayjobs.com`;
}
