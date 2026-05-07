#!/usr/bin/env tsx
/**
 * npm run diagnose:sponsorship
 *
 * Scan recent jobs and report sponsorship classification distribution.
 * Fails with non-zero exit code on critical misclassifications.
 */

import { MongoClient, type Db } from "mongodb";

import { classifySponsorship, matchCompanyProfile, type CompanySponsorshipProfile, type SponsorshipClassification } from "../lib/server/sponsorship/classifier";
import { InMemoryCompanySponsorshipProfileRepository } from "../lib/server/sponsorship/company-profile";
import { importSponsorshipCompaniesFromString } from "../lib/server/sponsorship/importer";

const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";

async function main() {
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const databaseName = new URL(mongoUri).pathname.replace(/^\//, "") || "job_crawler";
  const client = new MongoClient(mongoUri, {
    serverSelectionTimeoutMS: 30_000,
  });

  console.log("[diagnose:sponsorship] Connecting to MongoDB...");
  await client.connect();
  const db = client.db(databaseName);
  await db.command({ ping: 1 });

  // Load company profiles (use in-memory for now; external datasets can be loaded later)
  const profileRepo = new InMemoryCompanySponsorshipProfileRepository();

  // Load a small set of test company profiles for validation
  const sampleProfiles: Partial<CompanySponsorshipProfile>[] = [
    {
      companyName: "SponsorCorp",
      companyNormalized: "sponsorcorp",
      aliases: ["Sponsor Corp International"],
      domain: "sponsorcorp.com",
      h1bSponsorLikely: true,
      confidence: "high",
      evidenceSummary: "Historical H-1B filings show regular sponsorship",
      positiveEvidenceCount: 50,
      negativeEvidenceCount: 0,
      recentEvidenceCount: 10,
      historicalEvidenceCount: 50,
      evidenceSources: ["company_historical_sponsor"],
      lastVerifiedAt: new Date().toISOString(),
    },
    {
      companyName: "NeverSponsor Inc",
      companyNormalized: "neversponsor inc",
      aliases: [],
      domain: "neversponsor.com",
      h1bSponsorLikely: false,
      confidence: "high",
      evidenceSummary: "Company policy explicitly states no visa sponsorship",
      positiveEvidenceCount: 0,
      negativeEvidenceCount: 20,
      recentEvidenceCount: 5,
      historicalEvidenceCount: 5,
      evidenceSources: ["company_policy_page"],
      lastVerifiedAt: new Date().toISOString(),
    },
    {
      companyName: "TestCompany",
      companyNormalized: "testcompany",
      aliases: ["Test Company Inc"],
      domain: "testcompany.com",
      h1bSponsorLikely: null,
      confidence: "none",
      evidenceSummary: "No sponsorship data available",
      positiveEvidenceCount: 0,
      negativeEvidenceCount: 0,
      recentEvidenceCount: 0,
      historicalEvidenceCount: 0,
      evidenceSources: [],
      lastVerifiedAt: new Date().toISOString(),
    },
  ];

  for (const profile of sampleProfiles) {
    await profileRepo.upsert(profile as CompanySponsorshipProfile);
  }

  console.log(`[diagnose:sponsorship] Loaded ${await profileRepo.count()} company profiles`);

  // Fetch recent jobs
  const jobs = await db
    .collection("jobs")
    .find({}, { sort: { lastSeenAt: -1 }, limit: 100 })
    .toArray();

  console.log(`[diagnose:sponsorship] Scanning ${jobs.length} recent jobs`);

  const classifications: SponsorshipClassification[] = [];
  const distribution: Record<string, number> = {
    supported: 0,
    not_supported: 0,
    unknown: 0,
  };
  const confidenceDistribution: Record<string, number> = {
    high: 0,
    medium: 0,
    low: 0,
    none: 0,
  };

  let failures = 0;

  for (const doc of jobs) {
    const job = doc as Record<string, unknown>;
    const company = String(job.company ?? "");
    const profile = await profileRepo.findByName(company);
    const classification = classifySponsorship(
      ({
        title: String(job.title ?? ""),
        descriptionSnippet: String(job.descriptionSnippet ?? ""),
        company,
        sourcePlatform: (job.sourcePlatform ?? "greenhouse") as string,
        rawSourceMetadata: (job.rawSourceMetadata ?? {}) as Record<string, unknown>,
      } as Parameters<typeof classifySponsorship>[0]),
      profile,
    );

    classifications.push(classification);
    distribution[classification.sponsorshipHint] += 1;
    confidenceDistribution[classification.sponsorshipConfidence] += 1;
  }

  console.log("\n=== Sponsorship Hint Distribution ===");
  for (const [hint, count] of Object.entries(distribution)) {
    console.log(`  ${hint}: ${count} (${((count / jobs.length) * 100).toFixed(1)}%)`);
  }

  console.log("\n=== Confidence Distribution ===");
  for (const [confidence, count] of Object.entries(confidenceDistribution)) {
    console.log(`  ${confidence}: ${count} (${((count / jobs.length) * 100).toFixed(1)}%)`);
  }

  console.log("\n=== Sample Classifications ===");

  // Print sample supported jobs
  const supportedJobs = classifications.filter((c) => c.sponsorshipHint === "supported");
  if (supportedJobs.length > 0) {
    console.log("\n--- Supported (sample up to 3) ---");
    for (const c of supportedJobs.slice(0, 3)) {
      console.log(`  Hint: ${c.sponsorshipHint} | Confidence: ${c.sponsorshipConfidence}`);
      console.log(`  Reason: ${c.sponsorshipReason}`);
      console.log(`  Evidence: ${c.sponsorshipEvidence.length} pieces`);
      if (c.sponsorshipEvidence.length > 0) {
        for (const e of c.sponsorshipEvidence.slice(0, 3)) {
          console.log(`    - [${e.signal}] ${e.matchedText} (${e.rationale})`);
        }
      }
      console.log();
    }
  }

  // Print sample not_supported jobs
  const notSupportedJobs = classifications.filter((c) => c.sponsorshipHint === "not_supported");
  if (notSupportedJobs.length > 0) {
    console.log("\n--- Not Supported (sample up to 3) ---");
    for (const c of notSupportedJobs.slice(0, 3)) {
      console.log(`  Hint: ${c.sponsorshipHint} | Confidence: ${c.sponsorshipConfidence}`);
      console.log(`  Reason: ${c.sponsorshipReason}`);
      console.log(`  Evidence: ${c.sponsorshipEvidence.length} pieces`);
      if (c.sponsorshipEvidence.length > 0) {
        for (const e of c.sponsorshipEvidence.slice(0, 3)) {
          console.log(`    - [${e.signal}] ${e.matchedText} (${e.rationale})`);
        }
      }
      console.log();
    }
  }

  // Print sample unknown jobs
  const unknownJobs = classifications.filter((c) => c.sponsorshipHint === "unknown");
  if (unknownJobs.length > 0) {
    console.log("\n--- Unknown (sample up to 3) ---");
    for (const c of unknownJobs.slice(0, 3)) {
      console.log(`  Hint: ${c.sponsorshipHint} | Confidence: ${c.sponsorshipConfidence}`);
      console.log(`  Reason: ${c.sponsorshipReason}`);
      console.log(`  Evidence: ${c.sponsorshipEvidence.length} pieces`);
      if (c.sponsorshipEvidence.length > 0) {
        for (const e of c.sponsorshipEvidence.slice(0, 3)) {
          console.log(`    - [${e.signal}] ${e.matchedText} (${e.rationale})`);
        }
      }
      console.log();
    }
  }

  // Validation gates

  // Gate 1: If every job is unknown when job descriptions contain sponsorship language
  let jobsWithSponsorshipLanguage = 0;
  for (const c of classifications) {
    if (c.sponsorshipEvidence.some(
      (e) => e.source === "job_description_positive" || e.source === "job_description_negative",
    )) {
      jobsWithSponsorshipLanguage += 1;
    }
  }
  const allUnknown = classifications.length > 0 && distribution["unknown"] === classifications.length;
  if (allUnknown && jobsWithSponsorshipLanguage > 0) {
    console.error(
      "\n[FAIL] All jobs are unknown despite sponsorship language in descriptions. " +
        `${jobsWithSponsorshipLanguage} jobs contain sponsorship language.`,
    );
    failures += 1;
  }

  // Gate 2: Explicit "no sponsorship" text is never classified as supported
  for (const c of classifications) {
    const hasExplicitNoSponsorship = c.sponsorshipEvidence.some(
      (e) => e.signal === "negative" && e.confidence === "high",
    );
    if (hasExplicitNoSponsorship && c.sponsorshipHint === "supported") {
      console.error(
        `[FAIL] Job with explicit "no sponsorship" text classified as supported: ${c.sponsorshipReason}`,
      );
      failures += 1;
    }
  }

  // Gate 3: Ambiguous authorization text is never classified as not_supported
  for (const c of classifications) {
    const hasOnlyAmbiguous = c.sponsorshipEvidence.length > 0 &&
      c.sponsorshipEvidence.every((e) => e.signal === "ambiguous");
    if (hasOnlyAmbiguous && c.sponsorshipHint === "not_supported") {
      console.error(
        `[FAIL] Ambiguous work authorization text classified as not_supported: ${c.sponsorshipReason}`,
      );
      failures += 1;
    }
  }

  console.log("\n=== Validation Summary ===");
  console.log(`Jobs scanned: ${jobs.length}`);
  console.log(`Jobs with sponsorship language: ${jobsWithSponsorshipLanguage}`);
  console.log(`Failures: ${failures}`);

  await client.close();

  if (failures > 0) {
    console.error(`\n[FAIL] diagnose:sponsorship found ${failures} validation failure(s).`);
    process.exitCode = 1;
  } else {
    console.log("\n[PASS] All sponsorship classification gates passed.");
  }
}

main().catch((error) => {
  console.error("[diagnose:sponsorship] Fatal error:", error);
  process.exitCode = 1;
});