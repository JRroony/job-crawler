import "server-only";

import type {
  EmploymentType,
  ExperienceClassification,
  ExperienceLevel,
  RemoteType,
  SalaryInfo,
  SponsorshipHint,
} from "@/lib/types";
import type {
  NormalizedJobSeed,
  ProviderDiagnostics,
  ProviderResult,
} from "@/lib/server/providers/types";
import { resolveJobLocation } from "@/lib/server/location-resolution";

import {
  buildLocationText,
  canonicalizeUrl,
  classifyExperience,
  normalizeComparableText,
  parseLocationText,
  slugToLabel,
} from "@/lib/server/crawler/helpers";
import { isUnitedStatesValue, resolveUsState } from "@/lib/server/locations/us";

export type ProviderSeedDropReason =
  | "seed_invalid_empty_title"
  | "seed_invalid_placeholder_title"
  | "seed_invalid_missing_company"
  | "seed_invalid_missing_source_job_id"
  | "seed_invalid_invalid_source_url"
  | "seed_invalid_invalid_apply_url"
  | "seed_invalid_empty_location"
  | "seed_normalization_failed"
  | "hydrate_invalid_search_index"
  | "persistable_schema_validation_failed";

export type ProviderSeedDiagnosticContext = {
  provider?: NormalizedJobSeed["sourcePlatform"];
  sourceUrl?: string;
  sourceId?: string;
  sourceJobId?: string;
  company?: string;
  rawTitle?: unknown;
  applyUrl?: string;
};

export type ProviderSeedValidationDrop = {
  seed: NormalizedJobSeed;
  reason: ProviderSeedDropReason | string;
  message: string;
  context: ProviderSeedDiagnosticContext;
};

export type ProviderSeedValidationResult =
  | {
      ok: true;
      seed: NormalizedJobSeed;
    }
  | {
      ok: false;
      drop: ProviderSeedValidationDrop;
    };

export type ProviderInvalidSeedSample = {
  provider?: NormalizedJobSeed["sourcePlatform"];
  sourceUrl?: string;
  sourceJobId?: string;
  company?: string;
  rawTitle?: string;
  applyUrl?: string;
  reason: string;
};

export type ProviderSeedBatchValidationResult = {
  jobs: NormalizedJobSeed[];
  dropped: ProviderSeedValidationDrop[];
  warnings: string[];
  dropReasonCounts: Record<string, number>;
  sampleDropReasons: string[];
  sampleInvalidSeeds: ProviderInvalidSeedSample[];
};

const placeholderProviderTitleComparables = new Set([
  "untitled role",
  "unknown",
  "n a",
  "na",
  "job opening",
]);

const placeholderCompanyComparables = new Set([
  "unknown",
  "n a",
  "na",
]);

const maxDiagnosticSamples = 8;

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

export function deriveNormalizedTitle(title: string) {
  const trimmedTitle = title.trim();
  const comparableTitle = normalizeComparableText(trimmedTitle);

  if (comparableTitle) {
    return comparableTitle;
  }

  return trimmedTitle.toLowerCase().replace(/\s+/g, " ").trim();
}

export function normalizeProviderJobSeed(seed: NormalizedJobSeed): NormalizedJobSeed {
  const title = normalizeProviderTitle(seed.title);
  const normalizedTitle =
    normalizeTitleAlias(seed.normalizedTitle) ??
    normalizeTitleAlias(seed.titleNormalized) ??
    deriveNormalizedTitle(title);

  return {
    ...seed,
    title,
    normalizedTitle,
    titleNormalized: normalizedTitle,
  };
}

export function validateProviderSeedCandidate(
  seed: NormalizedJobSeed,
  context: Partial<ProviderSeedDiagnosticContext> = {},
): ProviderSeedValidationResult {
  const seedRecord = seed as NormalizedJobSeed & Record<string, unknown>;
  const rawTitle = context.rawTitle ?? seedRecord.title;
  const diagnosticContext = buildProviderSeedDiagnosticContext(seed, {
    ...context,
    rawTitle,
  });

  if (typeof rawTitle !== "string" || rawTitle.trim().length === 0) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_empty_title",
        message: "Job seed title is empty after trimming.",
        context: diagnosticContext,
      },
    };
  }

  if (isPlaceholderProviderTitle(rawTitle)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_placeholder_title",
        message: "Job seed title is a placeholder fallback.",
        context: diagnosticContext,
      },
    };
  }

  if (!readDiagnosticString(seed.company) || isPlaceholderCompany(seed.company)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_missing_company",
        message: "Job seed is missing a real company name.",
        context: diagnosticContext,
      },
    };
  }

  if (!readDiagnosticString(seed.sourceJobId)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_missing_source_job_id",
        message: "Job seed is missing a stable source job id.",
        context: diagnosticContext,
      },
    };
  }

  if (!isValidAbsoluteUrl(seed.sourceUrl)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_invalid_source_url",
        message: "Job seed sourceUrl is missing or invalid.",
        context: diagnosticContext,
      },
    };
  }

  if (!isValidAbsoluteUrl(seed.applyUrl)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_invalid_apply_url",
        message: "Job seed applyUrl is missing or invalid.",
        context: diagnosticContext,
      },
    };
  }

  if (!readDiagnosticString(seed.locationText)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_invalid_empty_location",
        message: "Job seed locationText is empty after trimming.",
        context: diagnosticContext,
      },
    };
  }

  if (!readDiagnosticString(seed.sourcePlatform)) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_normalization_failed",
        message: "Job seed is missing a source platform.",
        context: diagnosticContext,
      },
    };
  }

  return normalizeValidProviderSeed(seed, diagnosticContext);
}

export function normalizeValidProviderSeed(
  seed: NormalizedJobSeed,
  context: Partial<ProviderSeedDiagnosticContext> = {},
): ProviderSeedValidationResult {
  let normalizedSeed: NormalizedJobSeed;

  try {
    normalizedSeed = normalizeProviderJobSeed(seed);
  } catch (error) {
    return {
      ok: false,
      drop: {
        seed,
        reason: "seed_normalization_failed",
        message: error instanceof Error ? error.message : "Job seed normalization failed.",
        context: buildProviderSeedDiagnosticContext(seed, context),
      },
    };
  }

  const diagnosticContext = buildProviderSeedDiagnosticContext(normalizedSeed, context);
  const titleSearchText = normalizeComparableText(
    normalizedSeed.normalizedTitle || normalizedSeed.title,
  );
  const strippedTitleSearchText = normalizeComparableText(normalizedSeed.title);

  if (!titleSearchText || !strippedTitleSearchText) {
    return {
      ok: false,
      drop: {
        seed: normalizedSeed,
        reason: "hydrate_invalid_search_index",
        message: "Job seed title cannot produce a non-empty search index title.",
        context: diagnosticContext,
      },
    };
  }

  return {
    ok: true,
    seed: normalizedSeed,
  };
}

export function validateNormalizedJobSeedForHydration(
  seed: NormalizedJobSeed,
  context: Partial<ProviderSeedDiagnosticContext> = {},
): ProviderSeedValidationResult {
  return validateProviderSeedCandidate(seed, context);
}

export function validateProviderSeedBatch(input: {
  provider: NormalizedJobSeed["sourcePlatform"];
  jobs: NormalizedJobSeed[];
  warnings?: string[];
  context?: Partial<ProviderSeedDiagnosticContext>;
}): ProviderSeedBatchValidationResult {
  const jobs: NormalizedJobSeed[] = [];
  const dropped: ProviderSeedValidationDrop[] = [];
  const warnings = [...(input.warnings ?? [])];

  for (const seed of input.jobs) {
    const validation = validateProviderSeedCandidate(seed, {
      provider: input.provider,
      ...input.context,
    });

    if (validation.ok) {
      jobs.push(validation.seed);
      continue;
    }

    dropped.push(validation.drop);
    warnings.push(
      `Dropped ${input.provider} job seed ${validation.drop.context.sourceJobId ?? "unknown"}: ${validation.drop.reason}`,
    );
  }

  const dropReasonCounts = buildDropReasonCounts(dropped);

  return {
    jobs,
    dropped,
    warnings,
    dropReasonCounts,
    sampleDropReasons: Object.keys(dropReasonCounts).slice(0, maxDiagnosticSamples),
    sampleInvalidSeeds: sampleInvalidSeeds(dropped, maxDiagnosticSamples),
  };
}

export function finalizeProviderResultWithSeedValidation<
  P extends ProviderResult["provider"],
>(input: {
  provider: P;
  jobs: NormalizedJobSeed[];
  sourceCount: number;
  fetchedCount: number;
  warnings: string[];
  diagnostics?: ProviderDiagnostics<P>;
  didExecuteSuccessfully?: boolean;
}): ProviderResult<P> {
  const validation = validateProviderSeedBatch({
    provider: input.provider,
    jobs: input.jobs,
    warnings: input.warnings,
  });
  const diagnostics = mergeProviderSeedValidationDiagnostics({
    provider: input.provider,
    sourceCount: input.sourceCount,
    fetchedCount: input.fetchedCount,
    parsedSeedCount: input.jobs.length,
    validSeedCount: validation.jobs.length,
    validation,
    existing: input.diagnostics,
  });
  const warningCount = validation.warnings.length;
  const hasWarningsOrDrops = warningCount > 0 || diagnostics.invalidSeedCount > 0;
  const executedSuccessfully =
    input.didExecuteSuccessfully || input.fetchedCount > 0 || validation.jobs.length > 0;

  return {
    provider: input.provider,
    status: hasWarningsOrDrops
      ? validation.jobs.length > 0 || executedSuccessfully
        ? "partial"
        : "failed"
      : "success",
    jobs: validation.jobs,
    sourceCount: input.sourceCount,
    fetchedCount: input.fetchedCount,
    matchedCount: validation.jobs.length,
    warningCount,
    errorMessage: hasWarningsOrDrops
      ? validation.warnings.join(" ") || "One or more provider job seeds were dropped."
      : undefined,
    diagnostics,
  } satisfies ProviderResult<P>;
}

export function buildProviderSeedDiagnosticContext(
  seed: NormalizedJobSeed,
  context: Partial<ProviderSeedDiagnosticContext> = {},
): ProviderSeedDiagnosticContext {
  const metadata = isRecord(seed.rawSourceMetadata) ? seed.rawSourceMetadata : {};
  const sourceId =
    context.sourceId ??
    readDiagnosticString(metadata.sourceId) ??
    readDiagnosticString(metadata.greenhouseSourceId);

  return {
    provider: context.provider ?? seed.sourcePlatform,
    sourceUrl:
      context.sourceUrl ??
      seed.sourceUrl ??
      readDiagnosticString(metadata.sourceUrl) ??
      readDiagnosticString(metadata.greenhouseBoardUrl),
    sourceId,
    sourceJobId:
      context.sourceJobId ??
      seed.sourceJobId ??
      readDiagnosticString(metadata.sourceJobId) ??
      readDiagnosticString(metadata.greenhouseJobId),
    company:
      context.company ??
      seed.company ??
      readDiagnosticString(metadata.company) ??
      readDiagnosticString(metadata.companyName),
    rawTitle: context.rawTitle ?? (seed as NormalizedJobSeed & Record<string, unknown>).title,
    applyUrl: context.applyUrl ?? seed.applyUrl,
  };
}

function mergeProviderSeedValidationDiagnostics<P extends ProviderResult["provider"]>(input: {
  provider: P;
  sourceCount: number;
  fetchedCount: number;
  parsedSeedCount: number;
  validSeedCount: number;
  validation: ProviderSeedBatchValidationResult;
  existing?: ProviderDiagnostics<P>;
}): ProviderDiagnostics<P> {
  const dropReasonCounts = {
    ...(input.existing?.dropReasonCounts ?? {}),
  };

  Object.entries(input.validation.dropReasonCounts).forEach(([reason, count]) => {
    dropReasonCounts[reason] = (dropReasonCounts[reason] ?? 0) + count;
  });

  const existingInvalidSeedCount = input.existing?.invalidSeedCount ?? 0;
  const invalidSeedCount = existingInvalidSeedCount + input.validation.dropped.length;
  const sampleInvalidSeeds = [
    ...(input.existing?.sampleInvalidSeeds ?? []),
    ...input.validation.sampleInvalidSeeds,
  ].slice(0, maxDiagnosticSamples);

  return {
    ...input.existing,
    provider: input.provider,
    discoveryCount: input.existing?.discoveryCount ?? input.sourceCount,
    fetchCount: input.existing?.fetchCount ?? (input.fetchedCount > 0 ? 1 : 0),
    parseSuccessCount: input.validSeedCount,
    parseFailureCount:
      (input.existing?.parseFailureCount ?? 0) + input.validation.dropped.length,
    rawFetchedCount: input.existing?.rawFetchedCount ?? input.fetchedCount,
    parsedSeedCount: input.existing?.parsedSeedCount ?? input.parsedSeedCount,
    validSeedCount: input.validSeedCount,
    invalidSeedCount,
    dropReasonCounts,
    sampleDropReasons: Object.keys(dropReasonCounts).slice(0, maxDiagnosticSamples),
    sampleInvalidSeeds,
  };
}

function buildDropReasonCounts(dropped: ProviderSeedValidationDrop[]) {
  return dropped.reduce<Record<string, number>>((counts, drop) => {
    counts[drop.reason] = (counts[drop.reason] ?? 0) + 1;
    return counts;
  }, {});
}

function sampleInvalidSeeds(
  dropped: ProviderSeedValidationDrop[],
  limit: number,
): ProviderInvalidSeedSample[] {
  return dropped.slice(0, limit).map((drop) => {
    const rawTitle =
      typeof drop.context.rawTitle === "string"
        ? truncateRawTitleDiagnostic(drop.context.rawTitle)
        : typeof drop.seed.title === "string"
          ? truncateRawTitleDiagnostic(drop.seed.title)
          : undefined;

    return {
      provider: drop.context.provider,
      sourceUrl: truncateDiagnosticValue(drop.context.sourceUrl),
      sourceJobId: truncateDiagnosticValue(drop.context.sourceJobId),
      company: truncateDiagnosticValue(drop.context.company),
      rawTitle,
      applyUrl: truncateDiagnosticValue(drop.context.applyUrl),
      reason: drop.reason,
    };
  });
}

function truncateDiagnosticValue(value: unknown, maxLength = 240) {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  return trimmed.length <= maxLength ? trimmed : `${trimmed.slice(0, maxLength - 3)}...`;
}

function truncateRawTitleDiagnostic(value: string, maxLength = 240) {
  return value.length <= maxLength ? value : `${value.slice(0, maxLength - 3)}...`;
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
  explicitEmploymentType?: EmploymentType | string;
  explicitSeniority?: ExperienceLevel | string;
  structuredExperienceHints?: Array<string | undefined>;
  descriptionExperienceHints?: Array<string | undefined>;
  pageFetchExperienceHints?: Array<string | undefined>;
  descriptionSnippet?: string;
  salaryInfo?: SalaryInfo;
  sponsorshipHint?: SponsorshipHint;
  crawledAt?: string;
}) {
  const title = normalizeProviderTitle(input.title);
  const normalizedTitle = deriveNormalizedTitle(title);
  const locationText = input.locationText?.trim() || "Location unavailable";
  const parsedLocation = parseLocationText(locationText);
  const experienceClassification = classifyExperience({
    title,
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
  const company = defaultCompanyName(input.companyToken, input.company);
  const resolvedLocation = resolveJobLocation({
    country: explicitCountry ?? parsedLocation.country,
    state: explicitState ?? parsedLocation.state,
    city: input.explicitCity ?? parsedLocation.city,
    locationText,
    rawSourceMetadata: input.rawSourceMetadata,
  });
  const remoteType = inferRemoteType(locationText, resolvedLocation, input.rawSourceMetadata);
  const employmentType = normalizeEmploymentType(input.explicitEmploymentType);
  const seniority = normalizeSeniority(input.explicitSeniority) ?? experienceLevel;
  const postingDate = input.postedAt;
  const crawledAt = input.crawledAt ?? input.discoveredAt;
  const descriptionSnippet =
    normalizeDescriptionSnippet(input.descriptionSnippet) ??
    buildDescriptionSnippet([
      ...(input.descriptionExperienceHints ?? []),
      ...(input.pageFetchExperienceHints ?? []),
    ]);
  const salaryInfo =
    input.salaryInfo ??
    extractSalaryInfo(input.rawSourceMetadata, [
      ...(input.descriptionExperienceHints ?? []),
      ...(input.pageFetchExperienceHints ?? []),
    ]);
  const sponsorshipHint =
    input.sponsorshipHint ?? inferSponsorshipHint([
      ...(input.descriptionExperienceHints ?? []),
      ...(input.pageFetchExperienceHints ?? []),
      safeJson(input.rawSourceMetadata),
    ]);

  return {
    title,
    company,
    normalizedCompany: normalizeComparableText(company),
    normalizedTitle,
    titleNormalized: normalizedTitle,
    country: resolvedLocation.country ?? explicitCountry ?? parsedLocation.country,
    state: resolvedLocation.state ?? explicitState ?? parsedLocation.state,
    city: resolvedLocation.city ?? input.explicitCity ?? parsedLocation.city,
    locationRaw: locationText,
    normalizedLocation: normalizeComparableText(
      buildLocationText([
        resolvedLocation.city ?? input.explicitCity ?? parsedLocation.city,
        resolvedLocation.state ?? explicitState ?? parsedLocation.state,
        resolvedLocation.country ?? explicitCountry ?? parsedLocation.country,
      ]) || locationText,
    ),
    locationText,
    resolvedLocation,
    remoteType,
    employmentType,
    seniority,
    experienceLevel,
    experienceClassification,
    sourcePlatform: input.sourcePlatform,
    sourceCompanySlug: normalizeSourceCompanySlug(input.companyToken),
    sourceJobId: input.sourceJobId,
    sourceUrl: input.sourceUrl,
    applyUrl: input.applyUrl ?? input.sourceUrl,
    canonicalUrl:
      canonicalizeUrl(input.canonicalUrl ?? "") ??
      canonicalizeUrl(input.sourceUrl) ??
      canonicalizeUrl(input.applyUrl ?? input.sourceUrl),
    postingDate,
    postedAt: postingDate,
    discoveredAt: input.discoveredAt,
    crawledAt,
    descriptionSnippet,
    ...(salaryInfo ? { salaryInfo } : {}),
    sponsorshipHint,
    rawSourceMetadata: input.rawSourceMetadata,
  };
}

function normalizeProviderTitle(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeTitleAlias(value?: string) {
  const normalized = normalizeComparableText(value);
  return normalized || undefined;
}

function readDiagnosticString(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPlaceholderProviderTitle(value: unknown) {
  if (typeof value !== "string") {
    return false;
  }

  return placeholderProviderTitleComparables.has(normalizeComparableText(value));
}

function isPlaceholderCompany(value: unknown) {
  if (typeof value !== "string") {
    return false;
  }

  return placeholderCompanyComparables.has(normalizeComparableText(value));
}

function isValidAbsoluteUrl(value: unknown) {
  if (typeof value !== "string" || !value.trim()) {
    return false;
  }

  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function normalizeSourceCompanySlug(value: string) {
  const normalized = normalizeComparableText(value);
  return normalized ? normalized.replace(/\s+/g, "-") : undefined;
}

function inferRemoteType(
  locationText: string,
  resolvedLocation: NormalizedJobSeed["resolvedLocation"],
  rawSourceMetadata: Record<string, unknown>,
): RemoteType {
  const searchable = [locationText, safeJson(rawSourceMetadata)].filter(Boolean).join(" ");

  if (/\bhybrid\b/i.test(searchable)) {
    return "hybrid";
  }

  if (resolvedLocation?.isRemote || /\b(remote|work from home|distributed)\b/i.test(searchable)) {
    return "remote";
  }

  if (locationText.trim()) {
    return "onsite";
  }

  return "unknown";
}

function normalizeEmploymentType(value?: EmploymentType | string) {
  const normalized = normalizeComparableText(value ?? "");

  if (!normalized) {
    return undefined;
  }

  if (/\bfull ?time\b/.test(normalized)) {
    return "full_time" as const;
  }

  if (/\bpart ?time\b/.test(normalized)) {
    return "part_time" as const;
  }

  if (/\b(contract|contractor|consultant)\b/.test(normalized)) {
    return "contract" as const;
  }

  if (/\btemporary|temp\b/.test(normalized)) {
    return "temporary" as const;
  }

  if (/\b(intern|internship)\b/.test(normalized)) {
    return "internship" as const;
  }

  if (/\b(apprentice|apprenticeship)\b/.test(normalized)) {
    return "apprenticeship" as const;
  }

  if (/\bseasonal\b/.test(normalized)) {
    return "seasonal" as const;
  }

  if (/\b(freelance|freelancer)\b/.test(normalized)) {
    return "freelance" as const;
  }

  return "unknown" as const;
}

function normalizeSeniority(value?: ExperienceLevel | string) {
  const normalized = normalizeComparableText(value ?? "");

  if (!normalized) {
    return undefined;
  }

  if (/\bintern/.test(normalized)) {
    return "intern" as const;
  }

  if (/\b(new grad|graduate|entry level|early career)\b/.test(normalized)) {
    return "new_grad" as const;
  }

  if (/\b(junior|jr)\b/.test(normalized)) {
    return "junior" as const;
  }

  if (/\b(mid|ii|level 2)\b/.test(normalized)) {
    return "mid" as const;
  }

  if (/\b(senior|sr|iii|level 3)\b/.test(normalized)) {
    return "senior" as const;
  }

  if (/\b(lead|manager|director|head)\b/.test(normalized)) {
    return "lead" as const;
  }

  if (/\b(staff|mts|smts|iv|level 4)\b/.test(normalized)) {
    return "staff" as const;
  }

  if (/\b(principal|distinguished|fellow|pmts|lmts|v|level 5)\b/.test(normalized)) {
    return "principal" as const;
  }

  return undefined;
}

function normalizeDescriptionSnippet(value?: string) {
  return buildDescriptionSnippet([value]);
}

function buildDescriptionSnippet(values: Array<string | undefined>) {
  for (const value of values) {
    const normalized = value
      ?.replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    if (!normalized) {
      continue;
    }

    return normalized.length <= 280 ? normalized : `${normalized.slice(0, 277).trimEnd()}...`;
  }

  return undefined;
}

function inferSponsorshipHint(values: Array<string | undefined>): SponsorshipHint {
  const searchable = values.filter(Boolean).join(" ");

  if (!searchable.trim()) {
    return "unknown";
  }

  if (
    /\b(no|not|without|unable to|cannot)\s+(offer |provide |support )?(visa|immigration|sponsorship)\b/i.test(
      searchable,
    ) ||
    /\b(visa|immigration) sponsorship is not available\b/i.test(searchable)
  ) {
    return "not_supported";
  }

  if (
    /\b(visa sponsorship|immigration support|h-1b|h1b|will sponsor|can sponsor|sponsorship available|open to visa)\b/i.test(
      searchable,
    )
  ) {
    return "supported";
  }

  return "unknown";
}

function extractSalaryInfo(
  rawSourceMetadata: Record<string, unknown>,
  freeTextValues: Array<string | undefined>,
) {
  const baseSalary = extractBaseSalary(rawSourceMetadata);
  if (baseSalary) {
    return baseSalary;
  }

  for (const value of freeTextValues) {
    const parsed = parseSalaryText(value);
    if (parsed) {
      return parsed;
    }
  }

  return undefined;
}

function extractBaseSalary(rawSourceMetadata: Record<string, unknown>): SalaryInfo | undefined {
  const baseSalary = deepFindRecord(rawSourceMetadata, "baseSalary");
  if (!baseSalary) {
    return undefined;
  }

  const currency = readNestedString(baseSalary, ["currency", "currencyCode"]);
  const rawText = readNestedString(baseSalary, ["value", "text"]) ?? readNestedString(baseSalary, ["text"]);
  const valueRecord = readNestedRecord(baseSalary, ["value"]);
  const minAmount = readNestedNumber(valueRecord ?? baseSalary, ["minValue", "minvalue", "value"]);
  const maxAmount = readNestedNumber(valueRecord ?? baseSalary, ["maxValue", "maxvalue", "value"]);
  const interval = normalizeSalaryInterval(
    readNestedString(valueRecord ?? baseSalary, ["unitText", "interval"]),
  );

  if (
    typeof minAmount === "undefined" &&
    typeof maxAmount === "undefined" &&
    !currency &&
    !rawText
  ) {
    return undefined;
  }

  return {
    ...(typeof minAmount === "number" ? { minAmount } : {}),
    ...(typeof maxAmount === "number" ? { maxAmount } : {}),
    ...(currency ? { currency } : {}),
    ...(rawText ? { rawText } : {}),
    interval,
  };
}

function parseSalaryText(value?: string): SalaryInfo | undefined {
  if (!value) {
    return undefined;
  }

  const match = value.match(
    /\$?\s?(\d[\d,]*(?:\.\d+)?)\s*(?:-|to)\s*\$?\s?(\d[\d,]*(?:\.\d+)?)\s*(per|\/)\s*(hour|day|week|month|year|yr)?/i,
  );
  if (match) {
    return {
      minAmount: Number(match[1].replace(/,/g, "")),
      maxAmount: Number(match[2].replace(/,/g, "")),
      interval: normalizeSalaryInterval(match[4]),
      rawText: value.trim(),
    };
  }

  return undefined;
}

function normalizeSalaryInterval(value?: string): SalaryInfo["interval"] {
  const normalized = normalizeComparableText(value ?? "");

  if (/hour/.test(normalized)) {
    return "hour";
  }

  if (/day/.test(normalized)) {
    return "day";
  }

  if (/week/.test(normalized)) {
    return "week";
  }

  if (/month/.test(normalized)) {
    return "month";
  }

  if (/(year|yr|annual)/.test(normalized)) {
    return "year";
  }

  return "unknown";
}

function deepFindRecord(value: unknown, targetKey: string): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = deepFindRecord(entry, targetKey);
      if (found) {
        return found;
      }
    }
    return undefined;
  }

  const record = value as Record<string, unknown>;
  const direct = record[targetKey];
  if (direct && typeof direct === "object" && !Array.isArray(direct)) {
    return direct as Record<string, unknown>;
  }

  for (const entry of Object.values(record)) {
    const found = deepFindRecord(entry, targetKey);
    if (found) {
      return found;
    }
  }

  return undefined;
}

function readNestedRecord(
  record: Record<string, unknown>,
  keys: string[],
): Record<string, unknown> | undefined {
  for (const key of keys) {
    const value = record[key];
    if (value && typeof value === "object" && !Array.isArray(value)) {
      return value as Record<string, unknown>;
    }
  }

  return undefined;
}

function readNestedString(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return undefined;
}

function readNestedNumber(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }

  return undefined;
}

function safeJson(value: unknown) {
  try {
    return JSON.stringify(value) ?? "";
  } catch {
    return "";
  }
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
  diagnostics?: ProviderDiagnostics<P>;
  didExecuteSuccessfully?: boolean;
}): ProviderResult<P> {
  return finalizeProviderResultWithSeedValidation(input);
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
    errorMessage: message,
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
