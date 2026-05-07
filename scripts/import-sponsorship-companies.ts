#!/usr/bin/env tsx
/**
 * npm run import:sponsorship-companies [file.json]
 *
 * Read a local JSON/CSV file and upsert company sponsorship profiles.
 * Prints insertedCount and updatedCount.
 * Does not require external web access.
 */

import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { InMemoryCompanySponsorshipProfileRepository } from "../lib/server/sponsorship/company-profile";
import { importSponsorshipCompanies } from "../lib/server/sponsorship/importer";

async function main() {
  const args = process.argv.slice(2);
  const fileArg = args[0];

  if (!fileArg) {
    console.error("Usage: npm run import:sponsorship-companies -- <file.json>");
    console.error("  Or: tsx scripts/import-sponsorship-companies.ts <file.json>");
    process.exit(1);
  }

  const filePath = resolve(fileArg);

  if (!existsSync(filePath)) {
    console.error(`File not found: ${filePath}`);
    process.exit(1);
  }

  console.log(`[import:sponsorship-companies] Reading ${filePath}...`);

  const repository = new InMemoryCompanySponsorshipProfileRepository();

  try {
    const result = await importSponsorshipCompanies(repository, filePath);

    console.log("\n=== Import Result ===");
    console.log(`Inserted: ${result.insertedCount}`);
    console.log(`Updated: ${result.updatedCount}`);
    console.log(`Skipped: ${result.skippedCount}`);
    console.log(`Total profiles: ${await repository.count()}`);

    if (result.errors.length > 0) {
      console.log("\nErrors:");
      for (const error of result.errors) {
        console.error(`  - ${error}`);
      }
    }

    // Print sample profiles
    const profiles = await repository.list();
    if (profiles.length > 0) {
      console.log("\n=== Sample Profiles ===");
      for (const profile of profiles.slice(0, 5)) {
        console.log(`  ${profile.companyName}`);
        console.log(`    Normalized: ${profile.companyNormalized}`);
        console.log(`    Aliases: ${profile.aliases.join(", ") || "(none)"}`);
        console.log(`    H-1B Likely: ${profile.h1bSponsorLikely === true ? "yes" : profile.h1bSponsorLikely === false ? "no" : "unknown"}`);
        console.log(`    Confidence: ${profile.confidence}`);
        console.log(`    Evidence: ${profile.evidenceSummary}`);
        console.log();
      }
    }

    if (result.errors.length > 0) {
      process.exit(1);
    }

    console.log("[PASS] Sponsorship companies imported successfully.");
  } catch (error) {
    console.error(`[import:sponsorship-companies] Failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  }
}

main();