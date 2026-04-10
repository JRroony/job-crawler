import "server-only";

import type { ExperienceClassification, SearchFilters } from "@/lib/types";
import type { NormalizedJobSeed, ProviderResult } from "@/lib/server/providers/types";
import type { TitleMatchResult } from "@/lib/server/title-retrieval";
import { resolveJobLocation } from "@/lib/server/location-resolution";

import {
  buildLocationText,
  canonicalizeUrl,
  classifyExperience,
  evaluateSearchFilters,
  parseLocationText,
  slugToLabel,
} from "@/lib/server/crawler/helpers";
import { isUnitedStatesValue, resolveUsState } from "@/lib/server/locations/us";

export function coercePostedAt(value: unknown) {
  if (!value) {
    return undefined;
  }

  const parsed = new Date(typeof value === "number" ? value : String(value));
  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }

  return parsed.toISOString();
}

export function defaultCompanyName(token: string, fallback?: string) {
  return fallback?.trim() || slugToLabel(token);
}

export function buildSeed(input: {
  title: string;
  companyToken: string;
  company?: string;
  locationText?: string;
  sourcePlatform: NormalizedJobSeed["sourcePlatform"];
  sourceJobId: string;
  sourceUrl: string;
  applyUrl?: string;
  canonicalUrl?: string;
  postedAt?: string;
  rawSourceMetadata: Record<string, unknown>;
  discoveredAt: string;
  explicitCountry?: string;
  explicitState?: string;
  explicitCity?: string;
  explicitExperienceLevel?: NormalizedJobSeed["experienceLevel"];
  explicitExperienceSource?: ExperienceClassification["source"];
  explicitExperienceReasons?: string[];
  structuredExperienceHints?: Array<string | undefined>;
  descriptionExperienceHints?: Array<string | undefined>;
  pageFetchExperienceHints?: Array<string | undefined>;
}) {
  const locationText = input.locationText?.trim() || "Location unavailable";
  const parsedLocation = parseLocationText(locationText);
  const experienceClassification = classifyExperience({
    title: input.title.trim(),
    explicitExperienceLevel: input.explicitExperienceLevel,
    explicitExperienceSource: input.explicitExperienceSource,
    explicitExperienceReasons: input.explicitExperienceReasons,
    structuredExperienceHints: input.structuredExperienceHints,
    descriptionExperienceHints: input.descriptionExperienceHints,
    pageFetchExperienceHints: input.pageFetchExperienceHints,
    rawSourceMetadata: input.rawSourceMetadata,
  });
  const experienceLevel =
    experienceClassification.explicitLevel ?? experienceClassification.inferredLevel;
  const explicitCountry = normalizeCountry(input.explicitCountry);
  const explicitState = normalizeState(input.explicitState);
  const resolvedLocation = resolveJobLocation({
    country: explicitCountry ?? parsedLocation.country,
    state: explicitState ?? parsedLocation.state,
    city: input.explicitCity ?? parsedLocation.city,
    locationText,
    rawSourceMetadata: input.rawSourceMetadata,
  });

  return {
    title: input.title.trim(),
    company: defaultCompanyName(input.companyToken, input.company),
    country: resolvedLocation.country ?? explicitCountry ?? parsedLocation.country,
    state: resolvedLocation.state ?? explicitState ?? parsedLocation.state,
    city: resolvedLocation.city ?? input.explicitCity ?? parsedLocation.city,
    locationText,
    resolvedLocation,
    experienceLevel,
    experienceClassification,
    sourcePlatform: input.sourcePlatform,
    sourceJobId: input.sourceJobId,
    sourceUrl: input.sourceUrl,
    applyUrl: input.applyUrl ?? input.sourceUrl,
    canonicalUrl:
      canonicalizeUrl(input.canonicalUrl ?? "") ??
      canonicalizeUrl(input.sourceUrl) ??
      canonicalizeUrl(input.applyUrl ?? input.sourceUrl),
    postedAt: input.postedAt,
    discoveredAt: input.discoveredAt,
    rawSourceMetadata: input.rawSourceMetadata,
  };
}

function normalizeCountry(value?: string) {
  const trimmed = value?.trim();
  if (!trimmed) {
    return undefined;
  }

  return isUnitedStatesValue(trimmed) ? "United States" : trimmed;
}

function normalizeState(value?: string) {
  const trimmed = value?.trim();
  if (!trimmed) {
    return undefined;
  }

  return resolveUsState(trimmed) ?? trimmed;
}

export function finalizeProviderResult<P extends ProviderResult["provider"]>(input: {
  provider: P;
  jobs: NormalizedJobSeed[];
  sourceCount: number;
  fetchedCount: number;
  warnings: string[];
  excludedByTitle?: number;
  excludedByLocation?: number;
}): ProviderResult<P> {
  const hasWarnings = input.warnings.length > 0;

  return {
    provider: input.provider,
    status: hasWarnings ? (input.jobs.length > 0 ? "partial" : "failed") : "success",
    jobs: input.jobs,
    sourceCount: input.sourceCount,
    fetchedCount: input.fetchedCount,
    matchedCount: input.jobs.length,
    warningCount: input.warnings.length,
    excludedByTitle: input.excludedByTitle ?? 0,
    excludedByLocation: input.excludedByLocation ?? 0,
    errorMessage: hasWarnings ? input.warnings.join(" ") : undefined,
  } satisfies ProviderResult<P>;
}

export function unsupportedProviderResult<P extends ProviderResult["provider"]>(
  provider: P,
  message: string,
  sourceCount = 0,
): ProviderResult<P> {
  return {
    provider,
    status: "unsupported",
    jobs: [],
    sourceCount,
    fetchedCount: 0,
    matchedCount: 0,
    warningCount: 0,
    excludedByTitle: 0,
    excludedByLocation: 0,
    errorMessage: message,
  };
}

export function filterProviderSeeds(
  seeds: NormalizedJobSeed[],
  filters: SearchFilters,
) {
  const matchedSeeds: NormalizedJobSeed[] = [];
  let excludedByTitle = 0;
  let excludedByLocation = 0;

  for (const seed of seeds) {
    // Providers only prefilter on title and location so the pipeline can own the
    // final experience-stage accounting and keep each exclusion bucket single-purpose.
    const evaluation = evaluateSearchFilters(seed, filters, {
      includeExperience: false,
    });

    if (evaluation.matches) {
      matchedSeeds.push(attachTitleMatchMetadata(seed, evaluation.titleMatch));
      continue;
    }

    if (evaluation.reason === "title") {
      excludedByTitle += 1;
      continue;
    }

    excludedByLocation += 1;
  }

  return {
    jobs: matchedSeeds,
    excludedByTitle,
    excludedByLocation,
  };
}

function attachTitleMatchMetadata(
  seed: NormalizedJobSeed,
  titleMatch: TitleMatchResult,
) {
  return {
    ...seed,
    rawSourceMetadata: {
      ...seed.rawSourceMetadata,
      crawlTitleMatch: {
        tier: titleMatch.tier,
        score: titleMatch.score,
        canonicalQueryTitle: titleMatch.canonicalQueryTitle,
        canonicalJobTitle: titleMatch.canonicalJobTitle,
        explanation: titleMatch.explanation,
        matchedTerms: titleMatch.matchedTerms,
        penalties: titleMatch.penalties,
      },
      ...(seed.resolvedLocation
        ? {
            crawlResolvedLocation: {
              country: seed.resolvedLocation.country,
              state: seed.resolvedLocation.state,
              stateCode: seed.resolvedLocation.stateCode,
              city: seed.resolvedLocation.city,
              isRemote: seed.resolvedLocation.isRemote,
              isUnitedStates: seed.resolvedLocation.isUnitedStates,
              confidence: seed.resolvedLocation.confidence,
            },
          }
        : {}),
    },
  };
}

export function collectJsonLdJobPostings(html: string) {
  const matches = Array.from(
    html.matchAll(
      /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi,
    ),
  );

  const jobs: Record<string, unknown>[] = [];

  for (const match of matches) {
    try {
      const parsed = JSON.parse(match[1]) as unknown;
      visitJsonLd(parsed, jobs);
    } catch {
      continue;
    }
  }

  return jobs;
}

function visitJsonLd(value: unknown, jobs: Record<string, unknown>[]) {
  if (!value) {
    return;
  }

  if (Array.isArray(value)) {
    value.forEach((entry) => visitJsonLd(entry, jobs));
    return;
  }

  if (typeof value !== "object") {
    return;
  }

  const record = value as Record<string, unknown>;
  const type = record["@type"];

  if (type === "JobPosting") {
    jobs.push(record);
  }

  Object.values(record).forEach((entry) => visitJsonLd(entry, jobs));
}

export function extractNextData(html: string) {
  const match = html.match(
    /<script[^>]+id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i,
  );

  if (!match) {
    return undefined;
  }

  try {
    return JSON.parse(match[1]) as unknown;
  } catch {
    return undefined;
  }
}

export function extractWindowAppData(html: string) {
  return extractSerializedWindowObject(html, "window.__appData");
}

export function collectJsonScriptPayloads(html: string) {
  const matches = Array.from(
    html.matchAll(
      /<script[^>]+type=["']application\/(?:json|[^"']+\+json)["'][^>]*>([\s\S]*?)<\/script>/gi,
    ),
  );

  const payloads: unknown[] = [];

  for (const match of matches) {
    try {
      payloads.push(JSON.parse(match[1]) as unknown);
    } catch {
      continue;
    }
  }

  return payloads;
}

export function deepCollect(value: unknown, predicate: (item: Record<string, unknown>) => boolean) {
  const results: Record<string, unknown>[] = [];

  visit(value);

  return results;

  function visit(node: unknown) {
    if (!node) {
      return;
    }

    if (Array.isArray(node)) {
      node.forEach(visit);
      return;
    }

    if (typeof node !== "object") {
      return;
    }

    const record = node as Record<string, unknown>;
    if (predicate(record)) {
      results.push(record);
    }

    Object.values(record).forEach(visit);
  }
}

function extractSerializedWindowObject(html: string, marker: string) {
  const markerIndex = html.indexOf(marker);
  if (markerIndex === -1) {
    return undefined;
  }

  const startIndex = html.indexOf("{", markerIndex);
  if (startIndex === -1) {
    return undefined;
  }

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = startIndex; index < html.length; index += 1) {
    const character = html[index];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (character === "\\") {
      escaped = true;
      continue;
    }

    if (character === "\"") {
      inString = !inString;
      continue;
    }

    if (inString) {
      continue;
    }

    if (character === "{") {
      depth += 1;
      continue;
    }

    if (character !== "}") {
      continue;
    }

    depth -= 1;

    if (depth !== 0) {
      continue;
    }

    try {
      return JSON.parse(html.slice(startIndex, index + 1)) as unknown;
    } catch {
      return undefined;
    }
  }

  return undefined;
}

export function firstString(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return undefined;
}

export function resolveUrl(value: string | undefined, baseUrl: string) {
  if (!value?.trim()) {
    return undefined;
  }

  try {
    return new URL(value, baseUrl).toString();
  } catch {
    return undefined;
  }
}

export function decodeHtmlEntities(value?: string) {
  return (value ?? "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&(apos|#39|#x27);/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_match, codePoint) => String.fromCharCode(Number(codePoint)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, codePoint) =>
      String.fromCharCode(Number.parseInt(codePoint, 16)),
    );
}

export function stripHtml(value?: string) {
  return decodeHtmlEntities(
    (value ?? "")
      .replace(/<(br|\/p|\/div|\/li|\/span|\/section|\/article|\/h[1-6]|\/td|\/tr)[^>]*>/gi, "\n")
      .replace(/<[^>]+>/g, " "),
  )
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

export function locationFromJsonLd(record: Record<string, unknown>) {
  const direct = firstString(record, ["jobLocationText", "location", "locationText"]);
  if (direct) {
    return direct;
  }

  const jobLocation = record.jobLocation;
  if (Array.isArray(jobLocation) && jobLocation.length > 0) {
    return locationFromJsonLd(jobLocation[0] as Record<string, unknown>);
  }

  if (jobLocation && typeof jobLocation === "object") {
    const address =
      (jobLocation as Record<string, unknown>).address as Record<string, unknown> | undefined;

    if (address) {
      return buildLocationText([
        typeof address.addressLocality === "string" ? address.addressLocality : undefined,
        typeof address.addressRegion === "string" ? address.addressRegion : undefined,
        typeof address.addressCountry === "string" ? address.addressCountry : undefined,
      ]);
    }
  }

  return "Location unavailable";
}

export function companyFromJsonLd(record: Record<string, unknown>, fallback: string) {
  const hiringOrganization = record.hiringOrganization;
  if (hiringOrganization && typeof hiringOrganization === "object") {
    const name = (hiringOrganization as Record<string, unknown>).name;
    if (typeof name === "string" && name.trim()) {
      return name.trim();
    }
  }

  return fallback;
}
