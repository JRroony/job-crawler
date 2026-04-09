# AGENTS.md

## Project goal
The job crawler must successfully crawl jobs and return non-empty, valid results to the UI.

## Definition of done
A task is not complete unless all of the following are true:
1. The project builds successfully
2. All existing tests pass
3. The crawler verification script passes
4. The jobs API returns non-empty results
5. The UI can render the returned job list without empty-state errors caused by bad data shape

## Working rules
- Always find the root cause, not just a surface-level patch
- Do not stop after a single fix attempt
- After every meaningful code change, run the full validation flow
- If a validation step fails, inspect the logs, identify the failure point, fix it, and rerun validation
- Prefer minimal, targeted changes over broad refactors unless a refactor is necessary to remove the root cause
- Preserve existing working behavior unless it directly blocks the crawler flow
- Add temporary debug logs only when needed, and remove noisy logs before finishing
- If selectors, parsing, or filtering are suspicious, verify with real response payloads or rendered HTML before changing logic
- If the crawler succeeds but the UI is empty, inspect API mapping, response shape, client-side transformation, and rendering guards

## Validation flow
After every meaningful code change, run these commands in order:

1. npm run build
2. npm test
3. node scripts/verify-crawl.js

If the project has a dev server or API server requirement for validation, start only what is necessary and keep the scope minimal.

## Required debugging checklist
When the crawler returns zero jobs or bad results, investigate these layers in order:

### Fetch layer
- bad request URL
- wrong query construction
- auth / headers / anti-bot blocking
- timeout / retry issues
- unexpected status code
- empty raw payload

### Parse layer
- broken selectors
- changed DOM structure
- JSON extraction mismatch
- pagination parsing issues
- field extraction issues for title / company / location / link / posted date

### Normalize / filter layer
- invalid field mapping
- over-aggressive filtering dropping valid jobs
- duplicate removal removing too much
- date parsing logic breaking sort or eligibility
- location filter incorrectly excluding jobs

### Persistence / API layer
- crawler output not persisted
- API reading from wrong collection / table / in-memory store
- serialization mismatch
- endpoint returns wrong shape or empty array despite crawler success

### UI layer
- endpoint not called
- wrong API base URL
- response mapping mismatch
- loading/error state masks valid results
- rendering expects fields that crawler does not provide
- client-side filtering removes all results

## Verification expectations
The verification script should fail unless all of the following are true:
- at least one job is returned
- each sampled job has a non-empty title
- each sampled job has a non-empty company
- each sampled job has a usable link
- no obviously malformed records dominate the result set
- the API response shape matches what the UI expects

## Final output format
When finishing, always report:
1. Root cause
2. Files changed
3. Validation results
4. Remaining risks
5. Suggested next hardening steps

## Constraints
- Do not claim success unless validation actually passed
- Do not stop at “build passes” if the crawler still returns empty results
- Do not stop at “crawler fetched jobs” if the UI still cannot render them