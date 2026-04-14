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

## Definition of done
A task is not complete unless all of the following are true:
- relevant code has been changed
- relevant tests have been added or updated
- `npm run typecheck` passes
- `npm test` passes
- the implementation is validated against the task expectation, not just against compile success
- diagnostics or logs are sufficient to explain major retrieval decisions
- the final output includes root cause, files changed, validation evidence, and remaining limitations

## Anti-patterns to avoid
- Do not fix retrieval problems only by changing UI labels or wording.
- Do not fix low recall only by relaxing filters without preserving precision.
- Do not fix platform support claims without validating actual provider behavior.
- Do not patch duplicate rendering issues only in React keys if backend identity/dedupe is wrong.
- Do not claim semantic retrieval if the implementation still depends mainly on exact title strings.

## Root-cause discipline
Before making changes, identify which layer is actually responsible:
- query expansion
- source discovery
- detail URL harvesting
- source recovery
- normalization
- title relevance
- location resolution
- filtering
- dedupe
- ranking
- UI presentation

Do not assume the first visible symptom is the root cause.

## Retrieval validation
For retrieval-related changes, do not rely on unit tests alone.
Also validate behavior using representative scenarios such as:
- software engineer + United States
- data analyst + United States
- business analyst + United States
- product manager + United States

Check whether results improve in recall, precision, and explainability.
If the repository already has benchmark or fixture-based evaluation, run it.
If it does not, add lightweight validation coverage.

## Enforcement
For any retrieval-related task, do not stop after producing analysis or a plan.
You must:
1. inspect the current implementation
2. identify the actual root cause
3. implement code changes
4. add or update tests
5. run `npm run typecheck`
6. run `npm test`
7. inspect failures
8. fix issues and rerun until green
9. validate the retrieval behavior against realistic query scenarios
10. report concrete evidence, not just claims