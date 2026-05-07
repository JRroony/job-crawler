import { analyzeTitle } from "@/lib/server/title-retrieval";
import {
  titleConceptCatalog,
  titleFamilyCatalog,
} from "@/lib/server/title-retrieval/catalog";
import type { TitleConceptDefinition } from "@/lib/server/title-retrieval/types";
import type { ExpandedRoleQuery, JobSearchIntent } from "./types";

/**
 * Expand job titles into related title families and role variants.
 * This is a deterministic expander — no LLM calls required.
 *
 * It uses the existing title concept catalog to produce structured
 * expanded queries with relevance scores and tiers.
 */
export function expandJobTitle(intent: JobSearchIntent): ExpandedRoleQuery[] {
  const titleAnalysis = analyzeTitle(intent.rawTitle);
  const queries: ExpandedRoleQuery[] = [];

  // Find matching concept(s) from the catalog
  const matchingConcepts = findMatchingConcepts(titleAnalysis.normalized);

  if (matchingConcepts.length === 0) {
    // No catalog match — use the raw normalized title as the only query
    queries.push({
      queryTitle: intent.normalizedTitle || intent.rawTitle,
      canonicalTitle: intent.normalizedTitle,
      relevanceScore: 1.0,
      tier: "anchor",
      isBroadMatch: false,
    });
    return queries;
  }

  // Add anchor (best match) first
  const anchor = matchingConcepts[0];
  queries.push({
    queryTitle: anchor.canonicalTitle,
    canonicalTitle: anchor.canonicalTitle,
    family: anchor.family,
    conceptId: anchor.id,
    relevanceScore: 1.0,
    tier: "anchor",
    isBroadMatch: false,
  });

  // Add core variants (aliases and adjacent concepts from the anchor)
  for (const alias of anchor.aliases ?? []) {
    queries.push({
      queryTitle: alias,
      canonicalTitle: anchor.canonicalTitle,
      family: anchor.family,
      conceptId: anchor.id,
      relevanceScore: 0.9,
      tier: "core",
      isBroadMatch: false,
    });
  }

  // Add abbreviations
  for (const abbrev of anchor.abbreviations ?? []) {
    queries.push({
      queryTitle: abbrev,
      canonicalTitle: anchor.canonicalTitle,
      family: anchor.family,
      conceptId: anchor.id,
      relevanceScore: 0.85,
      tier: "core",
      isBroadMatch: false,
    });
  }

  // Add adjacent concepts
  for (const adjacentId of anchor.adjacentConceptIds ?? []) {
    const adjacentConcept = findConceptById(adjacentId);
    if (adjacentConcept && adjacentConcept.id !== anchor.id) {
      queries.push({
        queryTitle: adjacentConcept.canonicalTitle,
        canonicalTitle: adjacentConcept.canonicalTitle,
        family: adjacentConcept.family,
        conceptId: adjacentConcept.id,
        relevanceScore: 0.8,
        tier: "adjacent",
        isBroadMatch: false,
      });
    }
  }

  // Add broad discovery queries from the family
  const family = findFamilyById(anchor.family);
  if (family) {
    for (const broadQuery of family.broadDiscoveryQueries ?? []) {
      // Avoid duplicates
      if (!queries.some((q) => q.queryTitle === broadQuery)) {
        queries.push({
          queryTitle: broadQuery,
          canonicalTitle: anchor.canonicalTitle,
          family: anchor.family,
          conceptId: anchor.id,
          relevanceScore: 0.7,
          tier: "supplemental",
          isBroadMatch: true,
        });
      }
    }
  }

  // Add any additional supplemental queries from matching concepts
  for (let i = 1; i < matchingConcepts.length; i++) {
    const concept = matchingConcepts[i];
    if (!queries.some((q) => q.conceptId === concept.id)) {
      queries.push({
        queryTitle: concept.canonicalTitle,
        canonicalTitle: concept.canonicalTitle,
        family: concept.family,
        conceptId: concept.id,
        relevanceScore: 0.6,
        tier: "supplemental",
        isBroadMatch: true,
      });
    }
  }

  return queries;
}

/**
 * Find matching title concepts for a normalized title.
 * Returns concepts sorted by relevance (best match first).
 */
function findMatchingConcepts(normalizedTitle: string): TitleConceptDefinition[] {
  const lower = normalizedTitle.toLowerCase();
  const scored: Array<{ concept: TitleConceptDefinition; score: number }> = [];

  for (const concept of titleConceptCatalog) {
    let score = 0;

    // Exact canonical match
    if (concept.canonicalTitle.toLowerCase() === lower) {
      score = 100;
    }

    // Alias match
    for (const alias of concept.aliases ?? []) {
      if (alias.toLowerCase() === lower) {
        score = Math.max(score, 90);
      }
    }

    // Contains match
    if (lower.includes(concept.canonicalTitle.toLowerCase()) ||
        concept.canonicalTitle.toLowerCase().includes(lower)) {
      score = Math.max(score, 70);
    }

    if (score > 0) {
      scored.push({ concept, score });
    }
  }

  // Sort by score descending
  scored.sort((a, b) => b.score - a.score);

  return scored.map((s) => s.concept);
}

/**
 * Find a concept by its ID.
 */
function findConceptById(id: string): TitleConceptDefinition | undefined {
  return titleConceptCatalog.find((c) => c.id === id);
}

/**
 * Find a family by its ID.
 */
function findFamilyById(id: string | undefined): (typeof titleFamilyCatalog)[number] | undefined {
  if (!id) return undefined;
  return titleFamilyCatalog.find((f) => f.id === id);
}

/**
 * Build US-centric discovery clauses for a country-wide search.
 * Returns common US city/state pairs for targeted location discovery.
 */
export function buildUSLocationClauses(maxClauses: number = 8): string[] {
  const usCityStatePairs = [
    "Seattle WA",
    "Bellevue WA",
    "Redmond WA",
    "San Francisco CA",
    "San Jose CA",
    "New York NY",
    "Austin TX",
    "Dallas TX",
    "Houston TX",
    "Chicago IL",
    "Boston MA",
    "Atlanta GA",
    "Denver CO",
    "Los Angeles CA",
    "Irvine CA",
    "Raleigh NC",
    "Charlotte NC",
    "Phoenix AZ",
    "Miami FL",
    "Jersey City NJ",
    "Washington DC",
    "Portland OR",
    "Salt Lake City UT",
    "Philadelphia PA",
    "Pittsburgh PA",
    "Minneapolis MN",
    "Tampa FL",
    "Orlando FL",
    "San Diego CA",
    "Columbus OH",
  ];

  return usCityStatePairs.slice(0, maxClauses);
}

/**
 * Build location clauses from a geo intent for discovery.
 */
export function buildLocationClauses(
  country?: string,
  state?: string,
  city?: string,
  isRemote?: boolean,
  maxClauses: number = 8,
): string[] {
  const clauses: string[] = [];

  if (city && state) {
    clauses.push(`${city} ${state}`);
    // Add US Metro area bonus for US cities
    if (country === "United States" || country === "US") {
      const usClauses = buildUSLocationClauses(maxClauses - 1);
      // Find clauses that match the same state
      const stateMatching = usClauses.filter((c) => c.endsWith(state!));
      clauses.push(...stateMatching.slice(0, 3));
    }
  } else if (state) {
    clauses.push(state);
  } else if (country === "United States" || country === "US") {
    // US-wide search: add city/state discovery clauses
    // Reserve a slot for Remote if needed so it is not truncated away
    const cityMax = isRemote ? Math.max(1, maxClauses - 1) : maxClauses;
    clauses.push(...buildUSLocationClauses(cityMax));
  }

  if (isRemote && !clauses.some((c) => c.toLowerCase().includes("remote"))) {
    clauses.push("Remote");
  }

  return clauses.slice(0, maxClauses);
}