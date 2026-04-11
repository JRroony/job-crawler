# Validation Loop Skill

## Purpose
Non-trivial changes in this repo must be implemented and validated, not only planned.

## Required behavior

### 1. Audit first
Before modifying code:
- inspect the relevant implementation
- identify likely root causes
- identify affected modules
- understand current diagnostics and tests

### 2. Make real code changes
Do not stop at:
- analysis
- TODO comments
- broad suggestions
- partial pseudocode

### 3. Add or update tests
Every meaningful retrieval change should be covered by tests.

This includes:
- unit tests for logic
- integration tests for pipeline behavior when appropriate

### 4. Run validation commands
After changes, run at minimum:
- `npm run typecheck`
- `npm test`

If targeted tests are helpful, run those too.

### 5. Fix failures and rerun
If anything fails:
- inspect the failure carefully
- fix the implementation or tests
- rerun validation
- continue until green

### 6. Do not stop at partial completion
A task is not complete if:
- code changed but no tests were updated
- tests were added but not run
- typecheck fails
- tests fail
- the implementation still obviously violates project retrieval rules

### 7. Report what changed
At the end, summarize:
- root causes found
- files changed
- tests added or updated
- commands run
- results of those commands
- remaining limitations if any

## Anti-patterns
Do not:
- propose a plan and stop
- make a small patch without validation
- skip tests because the change "seems simple"
- claim completion without running commands