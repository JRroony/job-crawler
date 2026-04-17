# Normalized Job Model

`JobListing` is the canonical job-search entity for this repository.

The canonical product fields are:

- `company`
- `normalizedCompany`
- `title`
- `normalizedTitle`
- `locationRaw`
- `normalizedLocation`
- `remoteType`
- `employmentType`
- `seniority`
- `postingDate`
- `discoveredAt`
- `crawledAt`
- `sourcePlatform`
- `sourceCompanySlug`
- `canonicalUrl`
- `descriptionSnippet`
- `salaryInfo`
- `sponsorshipHint`
- `dedupeFingerprint`

Supporting operational fields remain on the record for validation, persistence, and provenance:

- `canonicalJobKey`
- `applyUrl`
- `resolvedUrl`
- `linkStatus`
- `lastValidatedAt`
- `resolvedLocation`
- `sourceLookupKeys`
- `sourceProvenance`
- `rawSourceMetadata`
- `crawlRunIds`
- `firstSeenAt`
- `lastSeenAt`
- `indexedAt`
- `isActive`
- `closedAt`
- `contentHash`
- `searchIndex`

Compatibility notes:

- `locationText` is kept as a compatibility alias for `locationRaw`.
- `postedAt` is kept as a compatibility alias for `postingDate`.
- `experienceLevel` remains the top-level resolved filter field, while `experienceClassification` stores the structured explanation: `experienceVersion`, `experienceBand`, `experienceSource`, `experienceConfidence`, `experienceSignals`, plus compatibility `explicitLevel` / `inferredLevel`.
- `companyNormalized`, `titleNormalized`, `locationNormalized`, and `contentFingerprint` are compatibility aliases for the canonical normalized and dedupe fields.
- `searchIndex` stores query-friendly title facets used for coarse candidate retrieval before higher-precision title/location/experience evaluation.
- `canonicalJobKey`, lifecycle timestamps, and `contentHash` are backfilled by repository reads for older stored documents that do not have them yet.
- Repository reads backfill the canonical fields when older stored documents only contain the legacy aliases.

Provider guidance:

- Providers should populate canonical fields through `buildSeed` whenever the source exposes them directly.
- Raw provider payloads still belong in `rawSourceMetadata`, but downstream filtering and ranking should prefer the canonical fields first and only fall back to raw metadata when needed.
