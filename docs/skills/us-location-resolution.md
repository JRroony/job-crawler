# US Location Resolution Skill

## Purpose
Broad US searches must work even when jobs do not explicitly say "US" or "United States".

Real US jobs often use forms such as:
- Austin, TX
- Bellevue, WA
- Seattle, Washington
- New York, NY
- Chicago, IL
- San Jose, CA
- Remote - US
- Remote - California
- Remote within the United States
- San Francisco Bay Area, CA

The crawler must retrieve and retain these jobs correctly.

## Core design rules

### 1. Separate discovery-time and result-time location logic
There are two different responsibilities:

#### A. Discovery-time location expansion
Use location clauses to increase search recall.

Examples:
- united states
- usa
- us
- remote us
- remote united states
- state names
- city + state
- metro clauses

#### B. Result-time location resolution
For each fetched job, infer whether it is a US job using all available evidence.

Do not confuse these two layers.

### 2. Broad US search must not depend only on "US"
When a user selects country = United States:
- generate country clauses
- generate remote-US clauses
- generate state clauses
- generate city-state / metro clauses

Use budgets and priorities, but do not rely only on:
- us
- usa
- united states

### 3. Use multiple evidence sources per job
Resolved location should use all relevant evidence:
- provider structured fields
- page embedded JSON
- JSON-LD
- visible location text
- office metadata
- description text
- remote eligibility text

### 4. Normalize to one resolved location model
Each job should have a normalized location inference such as:
- country
- state
- city
- isRemote
- isUnitedStates
- confidence
- evidence

This enables consistent filtering across platforms.

### 5. Infer US membership from city/state evidence
A job should count as US if any strong evidence indicates it is US, even if explicit country is missing.

Examples that should count as US:
- Austin, TX
- Bellevue, WA
- Seattle, Washington
- New York, NY
- Chicago, Illinois
- San Jose, CA
- Cambridge, MA
- Remote - California
- Remote - US
- Remote in the United States

### 6. City/state inference should be robust
Support:
- state abbreviations
- full state names
- city + state combinations
- remote-US patterns
- remote-state patterns
- metro-area style phrasing when possible

Do not depend only on a tiny hardcoded metro list.

### 7. US filtering should happen after resolution
When the user searches for US jobs:
- do not filter only using raw `country`
- do not filter only using raw `locationText`
- filter using resolved US inference

### 8. Preserve evidence for debugging
When a job is classified as US or non-US, diagnostics should make it explainable.

Useful evidence examples:
- "Matched state code TX from location field"
- "Detected remote-US phrase in description"
- "Resolved Seattle, WA from embedded JSON"
- "Detected California office metadata"

## Testing expectations
Add tests for:
- Austin TX resolves as US
- Bellevue WA resolves as US
- Seattle Washington resolves as US
- New York NY resolves as US
- Remote US resolves as US
- Remote California resolves as US
- missing explicit country but valid US city/state still passes
- non-US locations do not incorrectly pass