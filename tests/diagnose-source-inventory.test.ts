import { describe, expect, it } from "vitest";

import {
  collectKnownSourceTerms,
  dedupeKnownSourceTerms,
  findFailureReasonContamination,
} from "@/scripts/diagnose-source-inventory";

describe("source inventory diagnostic", () => {
  it("does not treat platform terms in source-specific failure reasons as contamination", () => {
    const records = [
      {
        _id: "greenhouse:rootcause2026050503241727hchb",
        token: "rootcause2026050503241727hchb",
        sourceKey: "greenhouse:rootcause2026050503241727hchb",
        companyHint: "Ingestion Root Cause Diagnostics",
        lastFailureReason:
          "Source greenhouse:rootcause2026050503241727hchb failed while crawling.",
      },
      {
        _id: "greenhouse:greenhouse",
        token: "greenhouse",
        sourceKey: "greenhouse",
        companyHint: "Greenhouse",
      },
    ];

    const knownTerms = dedupeKnownSourceTerms(records.flatMap(collectKnownSourceTerms));

    expect(findFailureReasonContamination(records, knownTerms)).toEqual([]);
  });

  it("still detects a source failure reason that names a different source", () => {
    const records = [
      {
        _id: "greenhouse:openai",
        token: "openai",
        sourceKey: "openai",
        companyHint: "OpenAI",
      },
      {
        _id: "greenhouse:stripe",
        token: "stripe",
        sourceKey: "stripe",
        companyHint: "Stripe",
        lastFailureReason: "Greenhouse returned 404 for openai.",
      },
    ];

    const knownTerms = dedupeKnownSourceTerms(records.flatMap(collectKnownSourceTerms));

    expect(findFailureReasonContamination(records, knownTerms)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceId: "greenhouse:stripe",
          mentionedOtherSourceId: "greenhouse:openai",
          matchedTerm: "openai",
        }),
      ]),
    );
  });
});
