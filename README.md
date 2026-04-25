# Job Crawler

Job Crawler is a Next.js App Router application for running transparent, source-driven crawls against configured and query-discovered public job sources, normalizing listings into one model, and storing searches plus crawl history in MongoDB.

The project is intentionally conservative:

- Greenhouse discovery starts from a maintained board registry, with public ATS search used only as an additive supplement
- providers only run against discovered sources they explicitly support
- link validation is deferred by default unless the selected crawl mode asks for inline validation
- unsupported or partial platforms stay labeled as such in the UI instead of being presented as active crawlers

## Stack

- Next.js App Router
- TypeScript with strict checking
- Tailwind CSS
- Zod
- MongoDB via the official `mongodb` Node.js driver
- Vitest

## Local setup

### What you need installed

- Node.js
  Use Node 20 LTS or newer. This repo does not currently pin a version with `.nvmrc`, but the stack expects a modern Node runtime.
- npm
  Comes with Node.js.
- MongoDB
  Required for app data persistence.
- Docker Desktop
  Optional. Use this instead of installing MongoDB directly if you want to run the included container.

### First-time setup

1. Copy `.env.example` to `.env.local`.
2. Start MongoDB locally.
3. Install dependencies.
4. Run the dev server.

```bash
npm install
npm run dev
```

If you want to use the default local database settings, `.env.local` only needs:

```bash
MONGODB_URI=mongodb://127.0.0.1:27017/job_crawler
```

Default MongoDB connection string:

```bash
mongodb://127.0.0.1:27017/job_crawler
```

## Local MongoDB with Docker

If you do not already have MongoDB running locally:

```bash
docker compose up -d
```

This exposes MongoDB on `127.0.0.1:27017`.

### Suggested install methods

Install Node.js:

- macOS with Homebrew: `brew install node@20`
- With `nvm`: install `nvm`, then run `nvm install 20 && nvm use 20`
- Windows: install Node.js 20 LTS from the official installer

Install MongoDB:

- Use Docker Desktop, then run `docker compose up -d`
- Or install MongoDB Community Edition locally and keep it running on port `27017`

## Scripts

```bash
npm run dev
npm run build
npm run typecheck
npm test
```

## Current product behavior

- Search by target title, country, state, city, experience levels, experience matching mode, crawl mode, and selected platforms.
- Discover registry-backed Greenhouse boards, configured sources, and query-discovered public ATS sources before any provider-specific fetch work starts.
- Route sources only to the provider families that support them.
- Normalize all results into one `JobListing` shape with source provenance, experience classification, validation state, and dedupe keys.
- Deduplicate by canonical URL, resolved URL, apply URL, source lookup keys, and finally normalized content fingerprint.
- Persist searches, crawl runs, per-provider source results, jobs, and link-validation history in MongoDB.
- Show diagnostics for source discovery, provider failures, title/location/experience exclusions, dedupe loss, and deferred validation.

## Discovery-first architecture

The crawler now runs in a discovery-first pipeline:

1. `searchFiltersSchema` validates and normalizes the requested search filters.
2. Discovery resolves the Greenhouse registry, configured seeds, and query-discovered public ATS sources for the selected platform scope.
3. Source-driven providers receive only the discovered source types they support.
4. Providers normalize raw records into shared job seeds.
5. The pipeline applies title, location, and experience filtering.
6. Matched jobs are hydrated into persistable records and deduped.
7. Validation runs according to crawl mode.
8. Results and diagnostics are persisted through the repository layer.

This keeps discovery, provider execution, filtering, validation, and persistence cleanly separated.

## Crawl modes and validation

- `fast`
  Saves matched jobs quickly and defers validation.
- `balanced`
  Validates the newest saved links inline, then defers the rest.
- `deep`
  Validates all saved links inline before the run finishes.

Deferred validation is a feature, not a failure state. Fresh jobs can intentionally remain `linkStatus: "unknown"` until manual revalidation, a background job, or a later explicit validation pass updates them.

## Experience matching

The crawler supports three experience matching modes:

- `strict`
  Keeps only explicit or strong experience matches.
- `balanced`
  Uses the default mix of explicit and stronger inferred matches.
- `broad`
  Allows looser inferred matches and automatically includes unspecified roles.

Jobs store both:

- `experienceLevel`
  The resolved level used for filtering and export.
- `experienceClassification`
  The persisted explanation of explicit level, inferred level, band, source, confidence, matched signals, version, reasons, and whether the job should be treated as unspecified.

## Platform selection

Platform selection flows through the full crawl:

- the UI submits `filters.platforms`
- discovery only returns sources for the selected implemented families
- provider routing only runs the selected implemented providers
- if no platform is selected, the crawler defaults to all implemented platforms

Implemented platform families:

- Greenhouse
- Lever
- Ashby
- SmartRecruiters
- Workday
- Company page

Greenhouse is the reliability focus of the current MVP. The other enabled families remain available, but they are not the hard requirement this iteration is optimized around.

Visible but not active crawler targets:

- LinkedIn
  Labeled as limited. Not an active crawler provider.
- Indeed
  Labeled as limited. Not an active crawler provider.

## Folder structure

```text
app/
  api/
  globals.css
  layout.tsx
  page.tsx
components/
  job-crawler-app.tsx
  results-table.tsx
  job-crawler/
lib/
  types.ts
  utils.ts
  server/
    crawler/
    db/
    discovery/
    mongodb.ts
    net/
    providers/
tests/
```

## Important server modules

### UI

- `app/page.tsx`
  Loads recent searches server-side.
- `components/job-crawler-app.tsx`
  Owns the operational search UI, run summaries, diagnostics, source coverage, and result-state messaging.
- `components/job-crawler/*`
  Breaks the main UI into focused panels for controls, recent searches, run summary, and source coverage.
- `components/results-table.tsx`
  Renders normalized saved jobs, source lineage, experience confidence, and validation state.

### Crawl pipeline

- `lib/server/crawler/service.ts`
  Entry points for starting, rerunning, loading, and revalidating crawls.
- `lib/server/crawler/pipeline.ts`
  The discovery-first execution pipeline, including filtering, dedupe, validation strategy, and persistence.
- `lib/server/crawler/link-validation.ts`
  Shared validation logic with HEAD-first checks, GET fallback, and stale-page detection.
- `lib/server/net/fetcher.ts`
  Server-side fetch wrapper with timeouts, retry rules, backoff, and normalized error typing.

### Source discovery and providers

- `lib/server/discovery/service.ts`
  Resolves the Greenhouse registry, configured public sources, and optional public-search additions, then narrows them to the selected implemented platforms.
- `lib/server/discovery/greenhouse-registry.ts`
  Holds the built-in Greenhouse board registry and helpers for merging env-provided additions.
- `lib/server/providers/*`
  Source-driven providers. Each provider only handles the source family it explicitly supports.

## Extending the Greenhouse registry

- The app ships with a maintained built-in Greenhouse board registry so Greenhouse crawling is not dependent on search-engine discovery.
- To add more public Greenhouse boards locally, append comma-separated tokens in `.env.local` with `GREENHOUSE_BOARD_REGISTRY_APPEND`.
- `GREENHOUSE_BOARD_TOKENS` is still supported as a legacy alias, but `GREENHOUSE_BOARD_REGISTRY_APPEND` is the preferred setting going forward.

## Extending non-Greenhouse source supply

- Lever and Ashby also ship with registry-backed public board seeds, so recurring ingestion has durable non-Greenhouse inventory before search-time discovery runs.
- Add structured Lever, Ashby, or Workday entries with `SOURCE_REGISTRY_CONFIG`.
- Workday can also be configured with `WORKDAY_SOURCE_CONFIG`, a JSON array of `{ "tenant", "host", "careerSitePath", "company" }` records. The app derives the display URL, API URL, token, persistence key, and inventory metadata from those durable fields.

### Persistence

- `lib/server/db/repository.ts`
  The MongoDB repository boundary. It validates writes, normalizes legacy records on read, and keeps MongoDB details out of business logic.
- `lib/server/db/indexes.ts`
  Declares collection names and indexes.

## Persistence model

### `searches`

Stores normalized search filters and latest-run tracking.

Important fields:

- `_id`
- `filters`
- `latestCrawlRunId`
- `createdAt`
- `updatedAt`
- `lastStatus`

### `crawlRuns`

Stores one crawl execution per search with operational metadata.

Important fields:

- `_id`
- `searchId`
- `startedAt`
- `finishedAt`
- `status`
- `discoveredSourcesCount`
- `crawledSourcesCount`
- `totalFetchedJobs`
- `totalMatchedJobs`
- `dedupedJobs`
- `validationMode`
- `providerSummary`
- `diagnostics`
- `errorMessage`

### `crawlSourceResults`

Stores per-provider results for a crawl run.

Important fields:

- `_id`
- `crawlRunId`
- `searchId`
- `provider`
- `status`
- `sourceCount`
- `fetchedCount`
- `matchedCount`
- `savedCount`
- `warningCount`
- `errorMessage`
- `startedAt`
- `finishedAt`

### `jobs`

Stores normalized, export-ready job records plus crawler lineage.

Important fields:

- `_id`
- `company`
- `title`
- `country`
- `state`
- `city`
- `locationText`
- `applyUrl`
- `sourcePlatform`
- `sourceJobId`
- `postedAt`
- `experienceLevel`
- `experienceClassification`
- `linkStatus`
- `lastValidatedAt`
- `sourceProvenance`
- `sourceLookupKeys`
- `crawlRunIds`
- `companyNormalized`
- `titleNormalized`
- `locationNormalized`
- `contentFingerprint`

### `linkValidations`

Stores point-in-time validation decisions.

Important fields:

- `_id`
- `jobId`
- `applyUrl`
- `resolvedUrl`
- `canonicalUrl`
- `status`
- `method`
- `httpStatus`
- `checkedAt`
- `errorMessage`
- `staleMarkers`

## Indexes

### `searches`

- `createdAt DESC`
- `latestCrawlRunId`

### `jobs`

- `crawlRunIds, postedAt DESC, sourcePlatform ASC, title ASC`
- `sourcePlatform, postedAt DESC, companyNormalized, titleNormalized`
- `sourceLookupKeys`
- `canonicalUrl`
- `resolvedUrl`
- `applyUrl`
- `contentFingerprint`
- `linkStatus, lastValidatedAt DESC`

### `crawlRuns`

- `searchId, startedAt DESC`
- `status, startedAt DESC`
- `validationMode, startedAt DESC`

### `crawlSourceResults`

- `crawlRunId, provider`
- `searchId, finishedAt DESC`

### `linkValidations`

- `jobId, checkedAt DESC`
- `applyUrl, checkedAt DESC`

## Supported and limited platforms

### Greenhouse

- Uses public board APIs.
- Coverage depends on configured board tokens.

### Lever

- Uses public postings APIs.
- Coverage depends on configured site tokens.

### Ashby

- Uses public Ashby board pages and public-data extraction heuristics.
- Reliability depends on the public page structure remaining parseable.

### Company page

- Supports configured JSON feeds, JSON-LD pages, and public HTML career pages.
- Coverage depends on the configured page list and whether the page exposes enough public structure to normalize jobs.

### Workday

- Supports discovered Workday sources through the public JSON endpoint when available.
- Falls back to public HTML/detail-page recovery for supported public Workday URLs.
- Coverage depends on tenant/site path recovery and public response shape.

### LinkedIn and Indeed

- Not active crawler providers.
- The UI keeps them visible as limited paths so the product does not overclaim support.
- No login-only scraping, CAPTCHA bypass, bot evasion, or hidden/private API access is implemented.

## Current limitations

- Discovery is configuration-driven. There is no true public-web source discovery yet.
- LinkedIn and Indeed are intentionally not treated as active crawler platforms.
- Company-page extraction works best when feeds or structured HTML/JSON-LD data are present.
- Validation is point-in-time and can remain deferred after a crawl by design.

## Testing coverage

Vitest coverage includes:

- filter validation
- discovery classification and filtering
- provider normalization and crawl isolation
- diagnostics accumulation
- dedupe behavior
- sorting behavior
- link validation
- MongoDB repository behavior through a fake DB adapter
- UI result-state logic

## Notes

- This app uses local MongoDB only.
- It does not use Prisma, PostgreSQL, Supabase, Firebase, or MongoDB Atlas.
- The repository layer normalizes older Mongo documents on read so the richer crawler model does not require an immediate migration.
