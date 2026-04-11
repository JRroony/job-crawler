# Retrieval Architecture Skill

## Purpose
This project is a retrieval pipeline, not just a board crawler.

The retrieval system should:
- discover public job sources
- harvest job detail URLs from public search
- recover parent sources from detail URLs
- directly extract jobs from detail pages when feasible
- run provider crawlers against recovered sources
- dedupe and normalize results
- filter using title relevance and location resolution

## Core design rules

### 1. Treat retrieval as a multi-stage system
The ideal high-level flow is:

1. user query
2. title query expansion
3. location clause expansion
4. public search / discovery
5. candidate URL harvesting
6. classify candidate URLs
7. direct job extraction if possible
8. source recovery if possible
9. provider crawl of recovered sources
10. normalization
11. title relevance filtering
12. location filtering
13. dedupe
14. persistence and diagnostics

Do not collapse all of this into one weak source-only discovery step.

### 2. Source discovery alone is not enough
Google returns many indexed job detail pages.
The crawler must not ignore job detail URLs simply because their board/source was not preconfigured.

A harvested URL may be:
- a board/source URL
- a job detail URL
- a careers landing page
- a structured feed
- an irrelevant page

Each type should be handled intentionally.

### 3. Support detail-first recovery
If public search returns a job detail page:
- try to extract job metadata directly
- try to recover the parent board/source
- enqueue the recovered source for provider crawling

Do not discard detail URLs just because they are not already known sources.

### 4. Platform-specific recovery is required
Implement and maintain stronger source recovery heuristics for:
- Greenhouse
- Lever
- Ashby
- Workday

Examples:
- recover Greenhouse board tokens from job detail URLs
- recover Lever site/company tokens from detail URLs
- recover Ashby board/company identifiers from detail URLs
- recover Workday tenant/career site identifiers from public URLs

### 5. Query planning should be budgeted, not crippled
Use query budgets, but do not stop too aggressively.
Prefer:
- exact / canonical title queries first
- high-yield locations first
- broader / long-tail queries later

Avoid:
- executing only a tiny subset of relevant queries
- plateauing too early without diagnostics
- letting broad US query plans explode uncontrollably

### 6. Diagnostics are mandatory
Every retrieval path should produce explainable diagnostics.

Track at least:
- generated title queries
- executed queries
- candidate URLs harvested
- candidate URLs by type
- direct job extractions
- recovered sources from detail URLs
- provider runs enqueued
- raw jobs before dedupe
- jobs after dedupe
- title exclusions
- location exclusions

### 7. Provider routing should be explicit
Only send a source to providers that truly support it.
Do not blur unsupported vs limited vs active platforms.

### 8. Dedupe should preserve recall before collapsing
Do not dedupe too early in ways that destroy recall.
Prefer:
- harvest first
- normalize
- preserve source lineage
- dedupe with canonical URL / resolved URL / source IDs / content fingerprint

## Anti-patterns
Do not:
- treat source discovery as the whole system
- discard detail URLs too early
- rely only on preconfigured board registries
- assume public search results are already source URLs
- add retrieval complexity without diagnostics