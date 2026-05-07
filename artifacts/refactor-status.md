# Refactor Status

## 2026-05-05T22:15:00Z
- Failing gate: none yet; `npm run diagnose:ingestion-root-cause` was the Phase 1 persistence gate.
- Root cause: in-sandbox run failed before app code with `tsx` IPC `listen EPERM`; escalated run connected to real MongoDB and passed.
- Files changed: `artifacts/refactor-status.md`.
- Test added: none for this pass-only status entry.
- Command result: `npm run diagnose:ingestion-root-cause` passed outside the sandbox; storageMode `mongodb`, insertedCount `1`, linkedToRunCount `1`, indexedEventCount `1`, crawlRunJobEventsCount `1`, terminalRunsWithRunningSourceResultsCount `0`.
- Next blocker: run `npm run typecheck` and `npm test`, then continue through diagnostic gates to find the first failing code gate.

## 2026-05-05T22:18:00Z
- Failing gate: `npm test`.
- Root cause: `tests/pipeline-title-retrieval.test.ts` used a fixed 35 ms sleep to prove supplemental Greenhouse persistence. Under full-suite load the event loop could delay the supplemental persistence past the sleep even though the targeted test and logs showed the DB write happened before baseline completion.
- Files changed: `tests/pipeline-title-retrieval.test.ts`, `artifacts/refactor-status.md`.
- Test added: updated the supplemental persistence regression to block the baseline provider with an explicit promise and poll until the supplemental job is visible while the baseline job is still absent.
- Command result: targeted vitest passed; `npm run typecheck` passed; `npm test` passed with 843 tests passing and 1 skipped.
- Next blocker: run the remaining diagnostics: ingestion persistence, provider lifecycle, source inventory, search, and sponsorship.

## 2026-05-05T22:20:00Z
- Failing gate: `npm run diagnose:source-inventory`.
- Root cause: the source inventory diagnostic treated generic crawler platform terms, especially `greenhouse`, as source identity terms. A source-specific failure reason like `Source greenhouse:rootcause... failed while crawling.` was falsely marked as cross-source contamination because another inventory record had token `greenhouse`.
- Files changed: `scripts/diagnose-source-inventory.ts`, `tests/diagnose-source-inventory.test.ts`, `artifacts/refactor-status.md`.
- Test added: `tests/diagnose-source-inventory.test.ts` covers the platform-token false positive and verifies true cross-source contamination still fails.
- Command result: `npx vitest run tests/diagnose-source-inventory.test.ts --reporter verbose` passed; `npm run typecheck` passed; `npm run diagnose:source-inventory` passed with 6,861 inventory records and 0 violations; `npm test` passed with 845 tests passing and 1 skipped.
- Next blocker: run `npm run diagnose:search`.

## 2026-05-05T22:26:00Z
- Failing gate: `npm run diagnose:search`.
- Root cause: the diagnostic required manual `--title` and `--location` flags, so the required no-argument command failed before validating DB-first search.
- Files changed: `scripts/diagnose-search-coverage.ts`, `tests/diagnose-search-coverage.test.ts`, `artifacts/refactor-status.md`.
- Test added: updated `tests/diagnose-search-coverage.test.ts` to assert no-arg representative DB-first scenarios for software engineer, data analyst, business analyst, and product manager in the United States.
- Command result: targeted diagnostic helper test passed; `npm run typecheck` passed; `npm run diagnose:search` passed with indexed DB results for all four default scenarios and `providerCrawlMs: 0`; `npm test` passed with 846 tests passing and 1 skipped.
- Next blocker: run `npm run diagnose:sponsorship`.

## 2026-05-05T22:31:00Z
- Failing gate: none; `npm run diagnose:sponsorship` was the sponsorship evidence gate.
- Root cause: no sponsorship gate failure was found. The diagnostic connected to MongoDB, loaded company sponsorship profiles, scanned recent jobs, and confirmed the rule checks had zero failures.
- Files changed: `artifacts/refactor-status.md`.
- Test added: none for this pass-only status entry.
- Command result: `npm run diagnose:sponsorship` passed; loaded 3 company profiles, scanned 100 recent jobs, and reported 0 validation failures.
- Next blocker: rerun the full required validation command list and address any first failing gate.

## 2026-05-05T22:38:00Z
- Failing gate: none; this was the final required validation sweep.
- Root cause: no remaining validation failure was found after the targeted fixes to the flaky supplemental persistence test, source-inventory contamination diagnostic, and no-argument search diagnostic.
- Files changed: `artifacts/refactor-status.md`.
- Test added: none for this pass-only status entry.
- Command result: `npm run typecheck` passed; `npm test` passed with 846 tests passing and 1 skipped; `npm run diagnose:ingestion-root-cause` passed; `npm run diagnose:ingestion-persistence` passed; `npm run diagnose:provider-lifecycle` passed; `npm run diagnose:source-inventory` passed; `npm run diagnose:search` passed; `npm run diagnose:sponsorship` passed.
- Next blocker: none from the required gate list.
