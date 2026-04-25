import "server-only";

import { z } from "zod";

import {
  buildCanonicalWorkdayApiListUrl,
  buildCanonicalWorkdaySourceUrl,
} from "@/lib/server/discovery/workday-url";

const workdaySourceHealthSchema = z.enum(["healthy", "degraded", "failing", "unknown"]);
const workdaySourceStatusSchema = z.enum(["active", "paused", "disabled"]);

const workdayHostnameSchema = z
  .string()
  .min(1)
  .transform((value) => normalizeWorkdayRegistryHostname(value))
  .refine((value) => value.endsWith(".myworkdayjobs.com"), {
    message: "Workday registry host must be a myworkdayjobs.com hostname.",
  });

export const workdaySourceRegistryEntrySchema = z.object({
  tenant: z.string().min(1),
  company: z.string().min(1),
  companyHint: z.string().min(1).optional(),
  careerSitePath: z.string().min(1),
  sitePath: z.string().min(1).optional(),
  host: workdayHostnameSchema.optional(),
  canonicalListUrl: z.string().url().optional(),
  token: z.string().min(1).optional(),
  apiUrl: z.string().url().optional(),
  confidence: z.enum(["high", "medium", "low"]).default("high"),
  coverageTags: z.array(z.string().min(1)).default([]),
  companyMetadata: z.record(z.string(), z.unknown()).default({}),
  status: workdaySourceStatusSchema.default("active"),
  health: workdaySourceHealthSchema.default("unknown"),
  crawlPriority: z.number().int().nonnegative().optional(),
});

export const workdaySourceRegistryConfigEntrySchema = z.preprocess(
  (value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return value;
    }

    const record = value as Record<string, unknown>;
    const host = record.host ?? record.hostname;

    return {
      ...record,
      ...(host ? { host } : {}),
    };
  },
  workdaySourceRegistryEntrySchema,
);

export type WorkdaySourceRegistryEntry = z.output<
  typeof workdaySourceRegistryEntrySchema
>;
export type WorkdaySourceRegistryEntryInput = z.input<
  typeof workdaySourceRegistryEntrySchema
>;

const defaultWorkdaySourceRegistryEntries = [
  workdaySource("salesforce", "Salesforce", "External_Career_Site", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("adobe", "Adobe", "external_experienced", "wd5", [
    "global",
    "canada_possible",
  ]),
  workdaySource("intuit", "Intuit", "Intuit", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("amd", "AMD", "External", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("mastercard", "Mastercard", "CorporateCareers", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("atlassian", "Atlassian", "Atlassian", "wd5", [
    "global",
    "canada_possible",
  ]),
  workdaySource("servicenow", "ServiceNow", "External", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("workday", "Workday", "Workday", "wd5", [
    "global",
    "canada_possible",
  ]),
  workdaySource("capitalone", "Capital One", "Capital_One", "wd1", [
    "global",
    "canada_possible",
  ]),
  workdaySource("citi", "Citi", "2", "wd5", [
    "global",
    "canada_possible",
  ]),
  workdaySource("exactsciences", "Exact Sciences", "Exact_Sciences", "wd1", [
    "united_states",
    "healthcare",
  ]),
  workdaySource("rb", "Federal Reserve Banks", "FRS", "wd5", [
    "united_states",
    "finance",
    "public_sector",
  ]),
  workdaySource("uasys", "University of Arkansas System", "UASYS", "wd5", [
    "united_states",
    "education",
  ]),
  workdaySource("lululemon", "lululemon", "lululemon_careers", "wd3", [
    "canada",
    "retail",
  ]),
  workdaySource("rbc", "RBC", "RBCGLOBAL1", "wd3", ["canada", "finance"]),
  workdaySource("td", "TD", "TD_Bank_Careers", "wd3", ["canada", "finance"]),
  workdaySource("telus", "TELUS", "TELUS_External_Careers", "wd3", [
    "canada",
    "telecom",
  ]),
  workdaySource("opentext", "OpenText", "OpenText", "wd3", [
    "canada",
    "software",
  ]),
  workdaySource("sunlife", "Sun Life", "Experienced-Jobs", "wd3", [
    "canada",
    "finance",
  ]),
] satisfies WorkdaySourceRegistryEntryInput[];

export function getDefaultWorkdaySourceRegistryEntries(): WorkdaySourceRegistryEntry[] {
  return defaultWorkdaySourceRegistryEntries.map((entry) =>
    workdaySourceRegistryEntrySchema.parse(entry),
  );
}

export function parseWorkdaySourceRegistryConfig(value?: string): WorkdaySourceRegistryEntry[] {
  if (!value?.trim()) {
    return [];
  }

  try {
    return z.array(workdaySourceRegistryConfigEntrySchema).parse(JSON.parse(value));
  } catch {
    return [];
  }
}

export function resolveWorkdayRegistryEntrySourceUrl(entry: WorkdaySourceRegistryEntry) {
  if (entry.canonicalListUrl) {
    return entry.canonicalListUrl;
  }

  return buildCanonicalWorkdaySourceUrl(
    `https://${resolveWorkdayRegistryEntryHost(entry)}`,
    entry.sitePath ?? entry.careerSitePath,
  );
}

export function resolveWorkdayRegistryEntryApiUrl(entry: WorkdaySourceRegistryEntry) {
  return (
    entry.apiUrl ??
    buildCanonicalWorkdayApiListUrl(
      `https://${resolveWorkdayRegistryEntryHost(entry)}`,
      entry.tenant,
      entry.careerSitePath,
    )
  );
}

export function resolveWorkdayRegistryEntryToken(entry: WorkdaySourceRegistryEntry) {
  return entry.token ?? `${entry.tenant}:${entry.careerSitePath.toLowerCase()}`;
}

export function resolveWorkdayRegistryEntryHost(entry: WorkdaySourceRegistryEntry) {
  return entry.host ?? `${entry.tenant}.wd1.myworkdayjobs.com`;
}

function workdaySource(
  tenant: string,
  company: string,
  careerSitePath: string,
  shard: `wd${number}`,
  coverageTags: string[],
): WorkdaySourceRegistryEntryInput {
  return {
    tenant,
    company,
    careerSitePath,
    host: `${tenant}.${shard}.myworkdayjobs.com`,
    coverageTags,
    companyMetadata: {},
  };
}

function normalizeWorkdayRegistryHostname(value: string) {
  return value
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/.*$/, "")
    .toLowerCase();
}
