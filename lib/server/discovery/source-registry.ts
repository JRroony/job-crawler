import "server-only";

import { z } from "zod";

import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { SourceInventoryRecord } from "@/lib/server/discovery/inventory";

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

  workday("salesforce", "Salesforce", "External_Career_Site", "https://salesforce.wd1.myworkdayjobs.com/External_Career_Site", ["global", "canada_possible"]),
  workday("adobe", "Adobe", "external_experienced", "https://adobe.wd5.myworkdayjobs.com/external_experienced", ["global", "canada_possible"]),
  workday("intuit", "Intuit", "Intuit", "https://intuit.wd1.myworkdayjobs.com/Intuit", ["global", "canada_possible"]),
  workday("amd", "AMD", "External", "https://amd.wd1.myworkdayjobs.com/External", ["global", "canada_possible"]),
  workday("mastercard", "Mastercard", "CorporateCareers", "https://mastercard.wd1.myworkdayjobs.com/CorporateCareers", ["global", "canada_possible"]),
  workday("atlassian", "Atlassian", "Atlassian", "https://atlassian.wd5.myworkdayjobs.com/Atlassian", ["global", "canada_possible"]),
  workday("servicenow", "ServiceNow", "External", "https://servicenow.wd1.myworkdayjobs.com/External", ["global", "canada_possible"]),
  workday("workday", "Workday", "Workday", "https://workday.wd5.myworkdayjobs.com/Workday", ["global", "canada_possible"]),
  workday("capitalone", "Capital One", "Capital_One", "https://capitalone.wd1.myworkdayjobs.com/Capital_One", ["global", "canada_possible"]),
  workday("citi", "Citi", "2", "https://citi.wd5.myworkdayjobs.com/2", ["global", "canada_possible"]),
  workday("lululemon", "lululemon", "lululemon_careers", "https://lululemon.wd3.myworkdayjobs.com/lululemon_careers", ["canada", "retail"]),
  workday("rbc", "RBC", "RBCGLOBAL1", "https://rbc.wd3.myworkdayjobs.com/RBCGLOBAL1", ["canada", "finance"]),
  workday("td", "TD", "TD_Bank_Careers", "https://td.wd3.myworkdayjobs.com/TD_Bank_Careers", ["canada", "finance"]),
  workday("telus", "TELUS", "TELUS_External_Careers", "https://telus.wd3.myworkdayjobs.com/TELUS_External_Careers", ["canada", "telecom"]),
  workday("opentext", "OpenText", "OpenText", "https://opentext.wd3.myworkdayjobs.com/OpenText", ["canada", "software"]),
  workday("sunlife", "Sun Life", "Experienced-Jobs", "https://sunlife.wd3.myworkdayjobs.com/Experienced-Jobs", ["canada", "finance"]),
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

export function sourceRegistryEntryToDiscoveredSource(
  entry: SourceRegistryEntry,
): DiscoveredSource {
  return classifySourceCandidate({
    url: resolveRegistryEntryUrl(entry),
    token: resolveRegistryEntryToken(entry),
    companyHint: entry.companyHint ?? entry.company,
    confidence: entry.confidence,
    discoveryMethod: "platform_registry",
  });
}

export function buildSourceInventoryRegistryMetadata(entry: SourceRegistryEntry) {
  return {
    registryPlatform: entry.platform,
    company: entry.company,
    coverageTags: entry.coverageTags,
    companyMetadata: entry.companyMetadata,
    canadaRelevant:
      entry.coverageTags.includes("canada") ||
      entry.coverageTags.includes("canada_possible"),
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

function workday(
  tenant: string,
  company: string,
  careerSitePath: string,
  canonicalListUrl: string,
  coverageTags: string[],
): SourceRegistryEntryInput {
  return {
    platform: "workday",
    tenant,
    token: `${tenant}:${careerSitePath.toLowerCase()}`,
    careerSitePath,
    canonicalListUrl,
    company,
    coverageTags,
    companyMetadata: {},
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

  const host = `${entry.tenant}.wd1.myworkdayjobs.com`;
  const sitePath = entry.sitePath ?? entry.careerSitePath;
  return `https://${host}/${sitePath}`;
}

function resolveRegistryEntryToken(entry: SourceRegistryEntry) {
  if (entry.platform === "workday") {
    return entry.token ?? `${entry.tenant}:${entry.careerSitePath.toLowerCase()}`;
  }

  return entry.token;
}
