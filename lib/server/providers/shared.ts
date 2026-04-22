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
  const title = seed.title.trim();
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
  const title = input.title.trim();
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

function normalizeTitleAlias(value?: string) {
  const normalized = normalizeComparableText(value);
  return normalized || undefined;
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
}): ProviderResult<P> {
  const hasWarnings = input.warnings.length > 0;
  const jobs = input.jobs.map(normalizeProviderJobSeed);

  return {
    provider: input.provider,
    status: hasWarnings ? (jobs.length > 0 ? "partial" : "failed") : "success",
    jobs,
    sourceCount: input.sourceCount,
    fetchedCount: input.fetchedCount,
    matchedCount: jobs.length,
    warningCount: input.warnings.length,
    errorMessage: hasWarnings ? input.warnings.join(" ") : undefined,
    diagnostics: input.diagnostics,
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
