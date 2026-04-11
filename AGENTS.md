# AGENTS.md

## Project goal
This repository is a job retrieval system, not a literal exact-match crawler.

The system should behave like a semantic retrieval pipeline:
- expand user-entered job titles into related search queries
- discover public job sources and job detail URLs
- recover sources from detail URLs when possible
- normalize jobs into a shared model
- filter results using title relevance and location resolution
- preserve both recall and precision

The system must not depend mainly on exact title matches.
The system must not depend only on explicit "US" text for US jobs.

## Non-negotiable rules
- Do not stop at analysis only. Make code changes.
- Do not solve retrieval problems by only adding a few hardcoded synonyms.
- Do not overclaim platform support in the UI.
- Do not treat source discovery as the only retrieval path.
- Do not rely on exact/full string title matching as the main relevance method.
- Do not rely only on raw `locationText` or explicit "US" strings for US filtering.

## Required workflow
For every non-trivial task:
1. Audit the existing implementation first.
2. Identify root causes in the current codebase.
3. Implement changes.
4. Add or update tests.
5. Run:
   - `npm run typecheck`
   - `npm test`
6. Inspect failures.
7. Fix issues and rerun until green.

## Retrieval principles
- Separate recall from precision.
- Separate source discovery from job-detail harvesting.
- Support detail URL recovery into source discovery.
- Use family-aware and concept-aware title matching.
- Use result-time US location inference.
- Prefer controlled recall rather than brittle exact-match behavior.
- Add diagnostics so retrieval failures are explainable.

## Specialized guidance
Also follow:
- `docs/skills/retrieval-architecture.md`
- `docs/skills/title-relevance.md`
- `docs/skills/us-location-resolution.md`
- `docs/skills/validation-loop.md`