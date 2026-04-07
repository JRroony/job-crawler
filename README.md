# Job Crawler

Job Crawler is a Next.js App Router application that crawls public job sources, normalizes listings into one model, validates apply links, dedupes overlaps, and stores searches plus crawl history in local MongoDB.

## Stack

- Next.js App Router
- TypeScript
- Tailwind CSS
- Zod
- MongoDB via the official `mongodb` Node.js driver
- Vitest

## Local setup

1. Copy `.env.example` to `.env.local`.
2. Start MongoDB locally.
3. Install dependencies.
4. Run the dev server.

```bash
npm install
npm run dev
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

## Scripts

```bash
npm run dev
npm run build
npm run typecheck
npm test
```

## Product behavior

- Search by target job title, country, state, city, and experience level.
- Crawl multiple public sources through provider adapters.
- Normalize all results into one `JobListing` shape.
- Deduplicate first by canonical or resolved URL, then by normalized company + title + location.
- Validate links at crawl time and store `applyUrl`, `resolvedUrl`, `linkStatus`, and `lastValidatedAt`.
- Revalidate stale links before displaying persisted results and on manual per-job revalidation.
- Persist searches, crawl runs, source results, jobs, and link validations in MongoDB.
- Sort results by `postedAt DESC`, then `sourcePlatform ASC`, then `title ASC`, with `"Date unavailable"` shown when needed.

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
lib/
  types.ts
  utils.ts
  server/
    crawler/
    db/
    mongodb.ts
    providers/
tests/
```

## Architecture notes

### UI

- `app/page.tsx` loads recent searches server-side when MongoDB is available.
- `components/job-crawler-app.tsx` is the interactive client shell.
- `components/results-table.tsx` renders normalized results and per-job revalidation actions.

### Server

- `lib/server/mongodb.ts` centralizes the server-only MongoDB client and database access.
- `lib/server/db/repository.ts` owns MongoDB persistence logic.
- `lib/server/crawler/service.ts` orchestrates crawling, provider isolation, filtering, validation, dedupe, sorting, and persistence.
- `lib/server/crawler/link-validation.ts` handles HEAD-first validation, GET fallback, redirect following, and stale-page detection.
- `lib/server/providers/*` contains source adapters instead of one monolithic crawler file.

### Source strategy

- Greenhouse: public board JSON API.
- Lever: public postings JSON API.
- Ashby: public Ashby board pages with public data extraction heuristics.
- Company pages: configured JSON feeds or JSON-LD `JobPosting` pages.
- LinkedIn and Indeed: intentionally limited and openly marked unsupported unless a compliant public path is available.

## MongoDB collection design

### `searches`

- Stores normalized search filters and the latest crawl run id for that search.

Example fields:

- `_id`
- `filters`
- `latestCrawlRunId`
- `createdAt`
- `updatedAt`
- `lastStatus`

### `crawlRuns`

- Stores one crawl execution per search.

Example fields:

- `_id`
- `searchId`
- `startedAt`
- `finishedAt`
- `status`
- `totalFetchedJobs`
- `totalMatchedJobs`
- `dedupedJobs`
- `warnings`
- `errorMessage`

### `crawlSourceResults`

- Stores per-provider outcomes for a crawl run.

Example fields:

- `_id`
- `crawlRunId`
- `searchId`
- `provider`
- `status`
- `fetchedCount`
- `matchedCount`
- `savedCount`
- `warnings`
- `errorMessage`
- `startedAt`
- `finishedAt`

### `jobs`

- Stores normalized job listings and source provenance.

Example fields:

- `_id`
- `title`
- `company`
- `country`
- `state`
- `city`
- `locationText`
- `experienceLevel`
- `sourcePlatform`
- `sourceJobId`
- `sourceUrl`
- `applyUrl`
- `resolvedUrl`
- `canonicalUrl`
- `postedAt`
- `discoveredAt`
- `linkStatus`
- `lastValidatedAt`
- `rawSourceMetadata`
- `sourceProvenance`
- `sourceLookupKeys`
- `crawlRunIds`
- `companyNormalized`
- `titleNormalized`
- `locationNormalized`
- `contentFingerprint`

### `linkValidations`

- Stores each saved validation decision for a job link.

Example fields:

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

## Indexes and why they exist

### `searches`

- `createdAt DESC`
  Keeps recent search history fast on the homepage.
- `latestCrawlRunId`
  Supports quick joins from a search to its newest crawl.

### `jobs`

- `crawlRunIds, postedAt DESC, sourcePlatform ASC, title ASC`
  Optimizes result listing for the latest crawl while aligning with the requested sort order.
- `sourceLookupKeys`
  Supports source-level lookups when the same job reappears from the same provider.
- `canonicalUrl`
  Supports URL-first dedupe when a canonical URL is available.
- `resolvedUrl`
  Supports URL-first dedupe after redirects are followed.
- `contentFingerprint`
  Supports fallback dedupe when URL keys are missing.

### `crawlRuns`

- `searchId, startedAt DESC`
  Retrieves the newest run for a search quickly.
- `status, startedAt DESC`
  Helps inspect recent crawl health and failures.

### `crawlSourceResults`

- `crawlRunId, provider`
  Loads source-by-source results for one crawl efficiently.
- `searchId, finishedAt DESC`
  Supports crawl history and source debugging over time.

### `linkValidations`

- `jobId, checkedAt DESC`
  Loads recent validations for a given job.
- `applyUrl, checkedAt DESC`
  Reuses fresh validation results and avoids unnecessary revalidation.

## Source limitations

### Greenhouse

- Works through public board APIs and is usually the most reliable source in this project.
- Coverage depends on configured public board tokens.

### Lever

- Works through public postings APIs.
- Coverage depends on configured public site tokens.

### Ashby

- Uses public Ashby board pages and public-data extraction heuristics.
- Reliability depends on the Ashby page structure remaining public and parseable.

### Company pages

- Supports configured JSON feeds and JSON-LD `JobPosting` pages.
- The app ships with the provider implementation but expects the actual company-page list in `COMPANY_PAGE_SOURCE_CONFIG`.

### LinkedIn

- Only compliant public collection paths are allowed.
- No login-only scraping.
- No CAPTCHA bypass.
- No bot evasion.
- No hidden or private APIs.
- The current implementation is intentionally limited and warns clearly instead of pretending support is reliable.

### Indeed

- Only compliant public collection paths are allowed.
- No login-only scraping.
- No CAPTCHA bypass.
- No bot evasion.
- No hidden or private APIs.
- The current implementation is intentionally limited and warns clearly instead of pretending support is reliable.

## Testing coverage

Vitest coverage includes:

- filter validation
- provider normalization
- provider isolation when one source fails
- dedupe behavior
- sorting behavior with and without posted dates
- link validation
- MongoDB persistence logic via a fake DB adapter
- API validation failures

## Notes

- This app uses local MongoDB only.
- It does not use Prisma, PostgreSQL, Supabase, Firebase, or MongoDB Atlas.
- Job links are never treated as valid forever. Validation is point-in-time and refreshed on a TTL policy or on demand.
