# Title Relevance Skill

## Purpose
Title retrieval must be semantic and generalized.

Users may search many different job titles.
The system must retrieve titles that are meaningfully related to the input, without relying on full exact match.

Examples of supported inputs include but are not limited to:
- software engineer
- backend engineer
- data engineer
- data analyst
- business analyst
- product manager
- program manager
- technical writer
- recruiter
- technical recruiter
- qa engineer
- sdet
- support engineer
- sales engineer

## Core design rules

### 1. Infer role family first
Before broadening title queries, infer a likely role family.

Example families:
- software_engineering
- data_analytics
- product
- program_management
- recruiting
- quality_assurance
- writing_documentation
- support
- sales
- operations

Title broadening must stay mostly within the same family.

### 2. Infer role concept second
After family inference, infer a more specific title concept.

Examples:
- software_engineer
- backend_engineer
- data_engineer
- product_manager
- recruiter
- qa_engineer
- technical_writer

### 3. Use a taxonomy model
A title concept should ideally include:
- canonical title
- aliases
- abbreviations
- adjacent concepts
- broad discovery queries
- optional negative keywords or negative concepts

### 4. Generate prioritized query variants
For user-entered titles, generate:
- original query
- normalized canonical query
- safe synonym queries
- abbreviation queries where appropriate
- adjacent concept queries
- limited family-broad queries where appropriate

Do not over-expand into unrelated roles.

### 5. Unknown titles need fallback heuristics
If a title is not explicitly defined:
- preserve the original input
- normalize it
- strip seniority markers
- detect likely head role words such as:
  - engineer
  - developer
  - analyst
  - manager
  - recruiter
  - writer
  - designer
  - specialist
  - coordinator
- infer a likely family when possible
- generate safe lightweight variants

Do not degrade to near-zero quality just because the title is unknown.

### 6. Strip seniority separately
Seniority should not dominate role understanding.

Handle tokens like:
- junior
- jr
- associate
- senior
- sr
- lead
- staff
- principal
- i / ii / iii / iv / v

These should usually be separated from the role concept.

### 7. Use score-based relevance, not only match/no-match
Use a numeric title relevance score.

Useful tiers include:
- exact
- canonical_variant
- synonym
- abbreviation
- adjacent_concept
- same_family_related
- generic_token_overlap
- none

Apply penalties such as:
- conflicting_family
- strong_negative_keyword
- explicit negative concept mismatch

### 8. Enforce family-aware precision
Examples:
- `data engineer` should match:
  - data engineer
  - analytics engineer
  - data platform engineer
- `data engineer` should not broadly admit:
  - software engineer
  - backend engineer
  - frontend engineer

Likewise:
- `recruiter` should not pull in software roles
- `technical writer` should not pull in generic engineer roles
- `qa engineer` should stay within QA / test / SDET family unless broad mode explicitly allows more

### 9. Internal thresholding should support precision modes
Design internal thresholds for:
- strict
- balanced
- broad

Even if the UI does not expose all modes yet, the implementation should support them cleanly.

## Testing expectations
Add tests for:
- software engineer expansion
- data engineer precision
- data analyst expansion
- business analyst expansion
- product manager expansion
- program manager expansion
- recruiter expansion
- technical writer expansion
- qa engineer expansion
- unknown-title fallback behavior
- family mismatch penalties