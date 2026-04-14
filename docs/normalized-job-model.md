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

- `applyUrl`
- `resolvedUrl`
- `linkStatus`
- `lastValidatedAt`
- `resolvedLocation`
- `sourceLookupKeys`
- `sourceProvenance`
- `rawSourceMetadata`
- `crawlRunIds`

Compatibility notes:

- `locationText` is kept as a compatibility alias for `locationRaw`.
- `postedAt` is kept as a compatibility alias for `postingDate`.
- `companyNormalized`, `titleNormalized`, `locationNormalized`, and `contentFingerprint` are compatibility aliases for the canonical normalized and dedupe fields.
- Repository reads backfill the canonical fields when older stored documents only contain the legacy aliases.

Provider guidance:

- Providers should populate canonical fields through `buildSeed` whenever the source exposes them directly.
- Raw provider payloads still belong in `rawSourceMetadata`, but downstream filtering and ranking should prefer the canonical fields first and only fall back to raw metadata when needed.
