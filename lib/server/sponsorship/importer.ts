import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  createCompanySponsorshipProfile,
  type CompanySponsorshipProfileRepository,
} from "./company-profile";
import type { CompanySponsorshipProfile } from "./classifier";

type ImportableSponsorshipRecord = {
  companyName: string;
  aliases?: string[];
  domain?: string;
  sourcePlatform?: string;
  h1bSponsorLikely?: boolean | null;
  confidence?: "high" | "medium" | "low" | "none";
  evidenceSummary?: string;
  positiveEvidenceCount?: number;
  negativeEvidenceCount?: number;
  recentEvidenceCount?: number;
  historicalEvidenceCount?: number;
};

type SponsorshipImportResult = {
  insertedCount: number;
  updatedCount: number;
  skippedCount: number;
  errors: string[];
};

/**
 * Import company sponsorship profiles from a JSON file.
 * Supports arrays of ImportableSponsorshipRecord.
 */
export async function importSponsorshipCompanies(
  repository: CompanySponsorshipProfileRepository,
  filePath: string,
): Promise<SponsorshipImportResult> {
  const resolvedPath = resolve(filePath);
  const raw = await readFile(resolvedPath, "utf-8");
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch (error) {
    throw new Error(
      `Failed to parse sponsorship import file: ${error instanceof Error ? error.message : "Unknown error"}`,
    );
  }

  const records = Array.isArray(data) ? data : [data];
  const result: SponsorshipImportResult = {
    insertedCount: 0,
    updatedCount: 0,
    skippedCount: 0,
    errors: [],
  };

  for (const record of records) {
    try {
      const normalized = normalizeImportRecord(record);
      const existing = await repository.findByName(normalized.companyName);
      const profile = createCompanySponsorshipProfile({
        companyName: normalized.companyName,
        aliases: normalized.aliases,
        domain: normalized.domain,
        sourcePlatform: normalized.sourcePlatform,
        h1bSponsorLikely: normalized.h1bSponsorLikely ?? null,
        confidence: normalized.confidence ?? "none",
        evidenceSummary: normalized.evidenceSummary ?? "No evidence summary provided.",
        positiveEvidenceCount: normalized.positiveEvidenceCount,
        negativeEvidenceCount: normalized.negativeEvidenceCount,
        recentEvidenceCount: normalized.recentEvidenceCount,
        historicalEvidenceCount: normalized.historicalEvidenceCount,
      });

      if (existing) {
        const merged = mergeSponsorshipProfiles(existing, profile);
        await repository.upsert(merged);
        result.updatedCount += 1;
      } else {
        await repository.upsert(profile);
        result.insertedCount += 1;
      }
    } catch (error) {
      result.skippedCount += 1;
      result.errors.push(
        `Failed to import record: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  return result;
}

/**
 * Import company sponsorship profiles from a JSON/CSV string in memory.
 */
export async function importSponsorshipCompaniesFromString(
  repository: CompanySponsorshipProfileRepository,
  content: string,
): Promise<SponsorshipImportResult> {
  let data: unknown;
  try {
    data = JSON.parse(content);
  } catch {
    // Try CSV parsing
    data = parseSimpleCSV(content);
  }

  const records = Array.isArray(data) ? data : [data];
  const result: SponsorshipImportResult = {
    insertedCount: 0,
    updatedCount: 0,
    skippedCount: 0,
    errors: [],
  };

  for (const record of records) {
    try {
      const normalized = normalizeImportRecord(record);
      const existing = await repository.findByName(normalized.companyName);
      const profile = createCompanySponsorshipProfile({
        companyName: normalized.companyName,
        aliases: normalized.aliases,
        domain: normalized.domain,
        sourcePlatform: normalized.sourcePlatform,
        h1bSponsorLikely: normalized.h1bSponsorLikely ?? null,
        confidence: normalized.confidence ?? "none",
        evidenceSummary: normalized.evidenceSummary ?? "No evidence summary provided.",
        positiveEvidenceCount: normalized.positiveEvidenceCount,
        negativeEvidenceCount: normalized.negativeEvidenceCount,
        recentEvidenceCount: normalized.recentEvidenceCount,
        historicalEvidenceCount: normalized.historicalEvidenceCount,
      });

      if (existing) {
        const merged = mergeSponsorshipProfiles(existing, profile);
        await repository.upsert(merged);
        result.updatedCount += 1;
      } else {
        await repository.upsert(profile);
        result.insertedCount += 1;
      }
    } catch (error) {
      result.skippedCount += 1;
      result.errors.push(
        `Failed to import record: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  return result;
}

/**
 * Parse a simple CSV with headers:
 * companyName,aliases,domain,h1bSponsorLikely,confidence,evidenceSummary
 */
function parseSimpleCSV(content: string): ImportableSponsorshipRecord[] {
  const lines = content
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length < 2) return [];

  const headers = lines[0].split(",").map((h) => h.trim());
  const records: ImportableSponsorshipRecord[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(",").map((v) => v.trim());
    const record: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      record[headers[j]] = values[j] ?? "";
    }
    records.push({
      companyName: record["companyName"] ?? "",
      aliases: record["aliases"] ? record["aliases"].split(";").map((a) => a.trim()) : [],
      domain: record["domain"] || undefined,
      h1bSponsorLikely: record["h1bSponsorLikely"]
        ? record["h1bSponsorLikely"] === "true"
          ? true
          : record["h1bSponsorLikely"] === "false"
            ? false
            : null
        : null,
      confidence: (record["confidence"] as ImportableSponsorshipRecord["confidence"]) || undefined,
      evidenceSummary: record["evidenceSummary"] || undefined,
    });
  }

  return records;
}

function normalizeImportRecord(
  record: unknown,
): ImportableSponsorshipRecord {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    throw new Error("Invalid import record: must be an object");
  }

  const r = record as Record<string, unknown>;

  return {
    companyName: String(r.companyName ?? "").trim(),
    aliases: Array.isArray(r.aliases)
      ? r.aliases.map((a) => String(a).trim()).filter(Boolean)
      : [],
    domain: typeof r.domain === "string" ? r.domain.trim() : undefined,
    sourcePlatform: typeof r.sourcePlatform === "string" ? r.sourcePlatform : undefined,
    h1bSponsorLikely:
      r.h1bSponsorLikely === true
        ? true
        : r.h1bSponsorLikely === false
          ? false
          : null,
    confidence: ["high", "medium", "low", "none"].includes(String(r.confidence))
      ? (String(r.confidence) as "high" | "medium" | "low" | "none")
      : "none",
    evidenceSummary: typeof r.evidenceSummary === "string" ? r.evidenceSummary : undefined,
    positiveEvidenceCount:
      typeof r.positiveEvidenceCount === "number" ? r.positiveEvidenceCount : undefined,
    negativeEvidenceCount:
      typeof r.negativeEvidenceCount === "number" ? r.negativeEvidenceCount : undefined,
    recentEvidenceCount:
      typeof r.recentEvidenceCount === "number" ? r.recentEvidenceCount : undefined,
    historicalEvidenceCount:
      typeof r.historicalEvidenceCount === "number"
        ? r.historicalEvidenceCount
        : undefined,
  };
}

function mergeSponsorshipProfiles(
  existing: CompanySponsorshipProfile,
  incoming: CompanySponsorshipProfile,
): CompanySponsorshipProfile {
  const existingAliases = new Set(existing.aliases);
  for (const alias of incoming.aliases) {
    existingAliases.add(alias);
  }

  return {
    ...existing,
    ...incoming,
    aliases: Array.from(existingAliases).sort(),
    evidenceSources: dedupeEvidenceTypes([
      ...existing.evidenceSources,
      ...incoming.evidenceSources,
    ]),
    lastVerifiedAt: incoming.lastVerifiedAt ?? existing.lastVerifiedAt,
    positiveEvidenceCount: Math.max(
      existing.positiveEvidenceCount,
      incoming.positiveEvidenceCount,
    ),
    negativeEvidenceCount: Math.max(
      existing.negativeEvidenceCount,
      incoming.negativeEvidenceCount,
    ),
    recentEvidenceCount: Math.max(
      existing.recentEvidenceCount,
      incoming.recentEvidenceCount,
    ),
    historicalEvidenceCount: Math.max(
      existing.historicalEvidenceCount,
      incoming.historicalEvidenceCount,
    ),
    _id: existing._id ?? incoming._id,
  };
}

import type { SponsorshipEvidenceType } from "./classifier";

function dedupeEvidenceTypes(values: SponsorshipEvidenceType[]): SponsorshipEvidenceType[] {
  return Array.from(new Set(values.filter(Boolean)));
}
