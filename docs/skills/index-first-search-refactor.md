# Index-First Search Refactor Skill

## Purpose
Large architecture refactors in this repository must move the system toward index-first search.

The target architecture is not crawl-first request handling.
Search should primarily serve indexed jobs, with crawling used to improve the index and supplement gaps.

## Required architecture rules

### 1. Search must be index-first
For user search requests:
1. query indexed jobs first
2. rank, filter, and deliver indexed results first
3. use request-time crawl only when it supplements the indexed path

Do not make the request path depend mainly on live crawling.

### 2. Background ingestion owns discovery and crawling
Background ingestion is responsible for:
- source discovery
- source recovery
- source crawling
- normalization
- dedupe
- index freshness

Do not shift these responsibilities into the synchronous search request path as the primary design.

### 3. Request-time crawl is supplemental only
Request-time crawl may be used for:
- targeted recovery when indexed coverage is missing
- bounded freshness checks
- additive incremental delivery into an active search session

It must not become the default or dominant retrieval path.

### 4. Search sessions are first-class
Search sessions must be explicit system concepts.

They should support:
- a durable search identity
- incremental result delivery
- status visibility
- explainable retrieval progress
- linkage between indexed results and supplemental work

Do not treat sessions as a UI-only concern.

### 5. Incremental delivery is first-class
Large searches should be able to:
- return available indexed results quickly
- stream or poll for additional results
- preserve ordering, cursors, and diagnostics across updates

Do not force users to wait for all background work before seeing useful results.

### 6. Durable cancellation and durable background control are required
Long-running supplemental work must support durable control.

This includes:
- persisted cancellation requests
- persisted worker heartbeats or equivalent liveness tracking
- resumable or inspectable background state
- behavior that remains correct across process restarts

Do not rely only on in-memory abort flags for architecture that claims durable control.

### 7. Use the self-repair validation loop on every non-trivial step
Every non-trivial refactor step must follow this loop:
1. audit
2. root cause
3. implement
4. tests
5. typecheck
6. test
7. inspect failures
8. fix
9. rerun until green

Do not stop after planning, partial code movement, or one validation pass.

### 8. Completion requires behavior improvement
A refactor is not done because modules moved or responsibilities were renamed.

Completion requires evidence of actual behavior improvement such as:
- search requests serving indexed jobs first
- supplemental crawl staying bounded and non-primary
- session progress and incremental delivery working as intended
- durable cancellation or background control behaving correctly
- retrieval quality, latency, explainability, or operational control improving in real scenarios

## Validation expectations
For meaningful refactor milestones:
- add or update tests for the affected architecture behavior
- run `npm run typecheck`
- run `npm test`
- inspect failures and fix them before claiming completion
- validate the changed behavior against realistic search flows, not just compile success

## Anti-patterns
Do not:
- present crawl-first request execution as the target architecture
- move discovery and crawling into the request path and call it a refactor win
- treat request-time crawl as the main retrieval path
- omit sessions, incremental delivery, or durable control from the design
- claim success based only on code movement or renaming
