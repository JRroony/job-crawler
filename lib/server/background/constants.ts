import type { SearchFilters } from "@/lib/types";

export const backgroundIngestionOwnerKey = "system:background-ingestion";

const minuteMs = 60_000;
const hourMs = 60 * minuteMs;
const dayMs = 24 * hourMs;

export type SystemSearchProfileGeography = {
  id: string;
  label: string;
  scope: "country" | "state" | "province" | "city";
  country: string;
  state?: string;
  city?: string;
  priorityOffset: number;
  variantTiers: readonly number[];
};

type SystemSearchPlatform = NonNullable<SearchFilters["platforms"]>[number];

export type SystemSearchPlatformPreference = {
  mode: "preference" | "restriction";
  platforms: readonly SystemSearchPlatform[];
};

export type SystemSearchProfile = {
  id: string;
  label: string;
  canonicalJobFamily: string;
  queryTitleVariant: string;
  titleVariantTier: 0 | 1 | 2;
  geography: SystemSearchProfileGeography;
  platformPreference?: SystemSearchPlatformPreference;
  priority: number;
  rotationIndex: number;
  enabled: boolean;
  cadenceMs: number;
  cooldownMs: number;
  lastRunAt?: string;
  nextEligibleAt?: string;
  successCount: number;
  failureCount: number;
  consecutiveFailureCount: number;
  filters: SearchFilters;
};

export type SystemSearchProfileRunState = {
  profileId: string;
  searchId?: string;
  latestCrawlRunId?: string;
  lastRunAt?: string;
  lastFinishedAt?: string;
  lastStatus?: "running" | "completed" | "partial" | "failed" | "aborted";
  successCount?: number;
  failureCount?: number;
  consecutiveFailureCount?: number;
  nextEligibleAt?: string;
};

type RoleTitleVariant = {
  title: string;
  tier: 0 | 1 | 2;
};

type RoleFamilyTemplate = {
  id: string;
  label: string;
  priority: number;
  cadenceMs?: number;
  cooldownMs?: number;
  platformPreference?: SystemSearchPlatformPreference;
  variants: readonly RoleTitleVariant[];
};

const defaultCadenceMs = 18 * hourMs;
const defaultCooldownMs = 2 * hourMs;

const roleFamilyTemplates: readonly RoleFamilyTemplate[] = [
  roleFamily("software_engineer", "Software Engineer", 10, [
    variant("software engineer", 0),
    variant("software development engineer", 1),
    variant("software developer", 1),
    variant("application engineer", 2),
  ]),
  roleFamily("backend_engineer", "Backend Engineer", 12, [
    variant("backend engineer", 0),
    variant("back end engineer", 1),
    variant("backend developer", 1),
    variant("server side engineer", 2),
  ]),
  roleFamily("frontend_engineer", "Frontend Engineer", 14, [
    variant("frontend engineer", 0),
    variant("front end engineer", 1),
    variant("frontend developer", 1),
    variant("web engineer", 2),
  ]),
  roleFamily("full_stack_engineer", "Full Stack Engineer", 16, [
    variant("full stack engineer", 0),
    variant("fullstack engineer", 1),
    variant("full stack developer", 1),
  ]),
  roleFamily("java_developer", "Java Developer", 18, [
    variant("java developer", 0),
    variant("java engineer", 1),
    variant("backend java developer", 2),
  ]),
  roleFamily("platform_engineer", "Platform Engineer", 20, [
    variant("platform engineer", 0),
    variant("infrastructure engineer", 1),
    variant("cloud platform engineer", 1),
    variant("developer platform engineer", 2),
  ]),
  roleFamily("devops_engineer", "DevOps Engineer", 22, [
    variant("devops engineer", 0),
    variant("dev ops engineer", 1),
    variant("cloud infrastructure engineer", 1),
    variant("build and release engineer", 2),
  ]),
  roleFamily("site_reliability_engineer", "Site Reliability Engineer", 24, [
    variant("site reliability engineer", 0),
    variant("sre", 1),
    variant("production engineer", 1),
    variant("reliability engineer", 2),
  ]),
  roleFamily("data_engineer", "Data Engineer", 26, [
    variant("data engineer", 0),
    variant("analytics engineer", 1),
    variant("data platform engineer", 1),
    variant("etl developer", 2),
  ]),
  roleFamily("data_analyst", "Data Analyst", 28, [
    variant("data analyst", 0),
    variant("business intelligence analyst", 1),
    variant("analytics analyst", 1),
    variant("reporting analyst", 2),
  ]),
  roleFamily("business_analyst", "Business Analyst", 30, [
    variant("business analyst", 0),
    variant("business systems analyst", 1),
    variant("systems analyst", 1),
    variant("requirements analyst", 2),
  ]),
  roleFamily("product_analyst", "Product Analyst", 32, [
    variant("product analyst", 0),
    variant("product data analyst", 1),
    variant("growth analyst", 1),
    variant("user insights analyst", 2),
  ]),
  roleFamily("machine_learning_engineer", "Machine Learning Engineer", 34, [
    variant("machine learning engineer", 0),
    variant("ml engineer", 1),
    variant("ml platform engineer", 1),
    variant("machine learning infrastructure engineer", 2),
  ]),
  roleFamily("ai_engineer", "AI Engineer", 36, [
    variant("ai engineer", 0),
    variant("artificial intelligence engineer", 1),
    variant("applied ai engineer", 1),
    variant("generative ai engineer", 2),
  ]),
  roleFamily("applied_scientist", "Applied Scientist", 38, [
    variant("applied scientist", 0),
    variant("applied machine learning scientist", 1),
    variant("applied ai scientist", 1),
  ]),
  roleFamily("research_scientist", "Research Scientist", 40, [
    variant("research scientist", 0),
    variant("machine learning research scientist", 1),
    variant("ai research scientist", 1),
  ]),
  roleFamily("product_manager", "Product Manager", 42, [
    variant("product manager", 0),
    variant("product owner", 1),
    variant("platform product manager", 2),
  ]),
  roleFamily("technical_product_manager", "Technical Product Manager", 44, [
    variant("technical product manager", 0),
    variant("product manager technical", 1),
    variant("platform product manager", 1),
  ]),
  roleFamily("program_manager", "Program Manager", 46, [
    variant("program manager", 0),
    variant("operations program manager", 1),
    variant("business program manager", 1),
  ]),
  roleFamily("technical_program_manager", "Technical Program Manager", 48, [
    variant("technical program manager", 0),
    variant("engineering program manager", 1),
    variant("technical project manager", 1),
  ]),
  roleFamily("qa_engineer", "QA Engineer", 50, [
    variant("qa engineer", 0),
    variant("quality assurance engineer", 1),
    variant("sdet", 1),
    variant("test automation engineer", 2),
  ]),
  roleFamily("security_engineer", "Security Engineer", 52, [
    variant("security engineer", 0),
    variant("application security engineer", 1),
    variant("cloud security engineer", 1),
    variant("product security engineer", 2),
  ]),
  roleFamily("solutions_engineer", "Solutions Engineer", 54, [
    variant("solutions engineer", 0),
    variant("solution architect", 1),
    variant("customer engineer", 1),
    variant("implementation engineer", 2),
  ]),
  roleFamily("customer_success_manager", "Customer Success Manager", 56, [
    variant("customer success manager", 0),
    variant("client success manager", 1),
    variant("technical account manager", 1),
    variant("customer success engineer", 2),
  ]),
  roleFamily("sales_engineer", "Sales Engineer", 58, [
    variant("sales engineer", 0),
    variant("solutions consultant", 1),
    variant("pre sales engineer", 1),
    variant("technical sales engineer", 2),
  ]),
  roleFamily("technical_writer", "Technical Writer", 60, [
    variant("technical writer", 0),
    variant("documentation writer", 1),
    variant("documentation engineer", 1),
    variant("api writer", 2),
  ]),
];

const geographyTemplates: readonly SystemSearchProfileGeography[] = [
  countryGeography("us", "United States", "United States", 0, [0, 1, 2]),
  countryGeography("canada", "Canada", "Canada", 4, [0, 1, 2]),
  stateGeography("us_ca", "California", "CA", 20),
  stateGeography("us_wa", "Washington", "WA", 22),
  stateGeography("us_ny", "New York", "NY", 24),
  stateGeography("us_tx", "Texas", "TX", 26),
  stateGeography("us_ma", "Massachusetts", "MA", 28),
  stateGeography("us_il", "Illinois", "IL", 30),
  stateGeography("us_nj", "New Jersey", "NJ", 32),
  stateGeography("us_va", "Virginia", "VA", 34),
  stateGeography("us_ga", "Georgia", "GA", 36),
  stateGeography("us_nc", "North Carolina", "NC", 38),
  stateGeography("us_co", "Colorado", "CO", 40),
  stateGeography("us_fl", "Florida", "FL", 42),
  provinceGeography("ca_on", "Ontario", "ON", 44, [0, 1]),
  provinceGeography("ca_bc", "British Columbia", "BC", 46, [0, 1]),
  provinceGeography("ca_qc", "Quebec", "QC", 48, [0, 1]),
  provinceGeography("ca_ab", "Alberta", "AB", 49, [0, 1]),
  cityGeography("us_seattle_wa", "Seattle, WA", "United States", "WA", "Seattle", 50),
  cityGeography("us_bellevue_wa", "Bellevue, WA", "United States", "WA", "Bellevue", 52),
  cityGeography("us_redmond_wa", "Redmond, WA", "United States", "WA", "Redmond", 54),
  cityGeography("us_san_francisco_ca", "San Francisco, CA", "United States", "CA", "San Francisco", 56),
  cityGeography("us_san_jose_ca", "San Jose, CA", "United States", "CA", "San Jose", 58),
  cityGeography("us_new_york_city_ny", "New York City, NY", "United States", "NY", "New York City", 60),
  cityGeography("us_austin_tx", "Austin, TX", "United States", "TX", "Austin", 62),
  cityGeography("us_boston_ma", "Boston, MA", "United States", "MA", "Boston", 64),
  cityGeography("ca_toronto_on", "Toronto, ON", "Canada", "ON", "Toronto", 70),
  cityGeography("ca_waterloo_on", "Waterloo, ON", "Canada", "ON", "Waterloo", 72),
  cityGeography("ca_ottawa_on", "Ottawa, ON", "Canada", "ON", "Ottawa", 74),
  cityGeography("ca_vancouver_bc", "Vancouver, BC", "Canada", "BC", "Vancouver", 76),
  cityGeography("ca_montreal_qc", "Montreal, QC", "Canada", "QC", "Montreal", 78),
  cityGeography("ca_calgary_ab", "Calgary, AB", "Canada", "AB", "Calgary", 80),
];

export const backgroundSystemSearchProfiles: SystemSearchProfile[] =
  generateBackgroundSystemSearchProfiles();

export const legacyBackgroundIngestionSearchFilters: SearchFilters = {
  title: "Background Inventory Refresh",
  crawlMode: "deep",
};

export const backgroundIngestionSearchFilters: SearchFilters =
  backgroundSystemSearchProfiles[0]?.filters ?? legacyBackgroundIngestionSearchFilters;

export function listBackgroundSystemSearchProfiles() {
  return backgroundSystemSearchProfiles.map(copySystemSearchProfile);
}

export function listBackgroundSystemRoleFamilies() {
  return roleFamilyTemplates.map((family) => ({
    id: family.id,
    label: family.label,
    variants: family.variants.map((titleVariant) => ({ ...titleVariant })),
  }));
}

export function listBackgroundSystemGeographies() {
  return geographyTemplates.map(copyGeography);
}

export function selectBackgroundSystemSearchProfiles(input: {
  now: Date;
  intervalMs: number;
  maxProfiles: number;
  profileRunStates?: readonly SystemSearchProfileRunState[];
}) {
  const maxProfiles = Math.max(0, Math.floor(input.maxProfiles));
  if (maxProfiles <= 0) {
    return [];
  }

  const nowMs = input.now.getTime();
  const statesByProfileId = new Map(
    (input.profileRunStates ?? []).map((state) => [state.profileId, state]),
  );
  const hydratedProfiles = backgroundSystemSearchProfiles
    .filter((profile) => profile.enabled)
    .map((profile) =>
      hydrateSystemSearchProfile(profile, statesByProfileId.get(profile.id), input.intervalMs),
    );
  const eligibleProfiles = hydratedProfiles.filter((profile) => {
    const nextEligibleMs = safeParseTime(profile.nextEligibleAt) ?? 0;
    return nextEligibleMs <= nowMs;
  });

  if (eligibleProfiles.length === 0) {
    return [];
  }

  const cycle = Math.floor(nowMs / Math.max(1, input.intervalMs));
  const rotationStart = cycle % backgroundSystemSearchProfiles.length;

  return eligibleProfiles
    .map((profile) => ({
      profile,
      rotationDistance: positiveModulo(profile.rotationIndex - rotationStart, backgroundSystemSearchProfiles.length),
      healthPenalty: profile.consecutiveFailureCount,
      overdueMs: Math.max(0, nowMs - (safeParseTime(profile.nextEligibleAt) ?? 0)),
    }))
    .sort((left, right) => {
      if (left.rotationDistance !== right.rotationDistance) {
        return left.rotationDistance - right.rotationDistance;
      }

      if (left.healthPenalty !== right.healthPenalty) {
        return left.healthPenalty - right.healthPenalty;
      }

      if (left.profile.priority !== right.profile.priority) {
        return left.profile.priority - right.profile.priority;
      }

      if (left.overdueMs !== right.overdueMs) {
        return right.overdueMs - left.overdueMs;
      }

      return left.profile.id.localeCompare(right.profile.id);
    })
    .slice(0, maxProfiles)
    .map((entry) => copySystemSearchProfile(entry.profile));
}

export function resolveBackgroundSystemSearchProfileByFilters(filters: SearchFilters) {
  return backgroundSystemSearchProfiles.find((profile) =>
    searchFiltersEqual(profile.filters, filters),
  );
}

export function isBackgroundIngestionSearchFilters(filters: SearchFilters) {
  return searchFiltersEqual(filters, legacyBackgroundIngestionSearchFilters);
}

function generateBackgroundSystemSearchProfiles() {
  const dedupedProfiles = new Map<string, SystemSearchProfile>();

  for (const family of roleFamilyTemplates) {
    for (const titleVariant of family.variants) {
      for (const geography of geographyTemplates) {
        if (!geography.variantTiers.includes(titleVariant.tier)) {
          continue;
        }

        const profile = createSystemSearchProfile({
          family,
          titleVariant,
          geography,
        });
        const duplicateKey = createProfileDuplicateKey(profile);
        const existing = dedupedProfiles.get(duplicateKey);

        if (!existing || compareProfilePriority(profile, existing) < 0) {
          dedupedProfiles.set(duplicateKey, profile);
        }
      }
    }
  }

  return Array.from(dedupedProfiles.values())
    .sort(compareProfileRotation)
    .map((profile, rotationIndex) => ({
      ...profile,
      rotationIndex,
    }));
}

function createSystemSearchProfile(input: {
  family: RoleFamilyTemplate;
  titleVariant: RoleTitleVariant;
  geography: SystemSearchProfileGeography;
}): SystemSearchProfile {
  const filterPlatforms =
    input.family.platformPreference?.mode === "restriction"
      ? [...input.family.platformPreference.platforms]
      : undefined;
  const filters: SearchFilters = {
    title: input.titleVariant.title,
    country: input.geography.country,
    ...(input.geography.state ? { state: input.geography.state } : {}),
    ...(input.geography.city ? { city: input.geography.city } : {}),
    ...(filterPlatforms ? { platforms: filterPlatforms } : {}),
    crawlMode: "balanced",
  };
  const priority =
    input.family.priority * 1_000 +
    input.titleVariant.tier * 100 +
    input.geography.priorityOffset;
  const id = [
    input.family.id,
    slugify(input.titleVariant.title),
    input.geography.id,
    filterPlatforms?.join("_"),
  ]
    .filter(Boolean)
    .join("__");

  return {
    id,
    label: `${input.family.label} / ${input.titleVariant.title} / ${input.geography.label}`,
    canonicalJobFamily: input.family.id,
    queryTitleVariant: input.titleVariant.title,
    titleVariantTier: input.titleVariant.tier,
    geography: copyGeography(input.geography),
    ...(input.family.platformPreference
      ? {
          platformPreference: {
            mode: input.family.platformPreference.mode,
            platforms: [...input.family.platformPreference.platforms],
          },
        }
      : {}),
    priority,
    rotationIndex: 0,
    enabled: true,
    cadenceMs: input.family.cadenceMs ?? defaultCadenceMs,
    cooldownMs: input.family.cooldownMs ?? defaultCooldownMs,
    successCount: 0,
    failureCount: 0,
    consecutiveFailureCount: 0,
    filters,
  };
}

function hydrateSystemSearchProfile(
  profile: SystemSearchProfile,
  state: SystemSearchProfileRunState | undefined,
  intervalMs: number,
): SystemSearchProfile {
  if (!state) {
    return copySystemSearchProfile(profile);
  }

  const successCount = Math.max(0, state.successCount ?? 0);
  const failureCount = Math.max(0, state.failureCount ?? 0);
  const consecutiveFailureCount = Math.max(0, state.consecutiveFailureCount ?? 0);
  const lastRunAt = state.lastFinishedAt ?? state.lastRunAt;
  const nextEligibleAt =
    state.nextEligibleAt ??
    resolveProfileNextEligibleAt({
      profile,
      lastRunAt,
      lastStatus: state.lastStatus,
      consecutiveFailureCount,
      intervalMs,
    });

  return {
    ...copySystemSearchProfile(profile),
    lastRunAt,
    nextEligibleAt,
    successCount,
    failureCount,
    consecutiveFailureCount,
  };
}

function resolveProfileNextEligibleAt(input: {
  profile: SystemSearchProfile;
  lastRunAt?: string;
  lastStatus?: SystemSearchProfileRunState["lastStatus"];
  consecutiveFailureCount: number;
  intervalMs: number;
}) {
  const lastRunMs = safeParseTime(input.lastRunAt);
  if (lastRunMs == null) {
    return new Date(0).toISOString();
  }

  const status = input.lastStatus;
  if (status === "failed" || status === "aborted") {
    const failureMultiplier = Math.min(16, 2 ** Math.min(4, input.consecutiveFailureCount));
    return new Date(
      lastRunMs +
        clamp(
          input.profile.cooldownMs * failureMultiplier,
          Math.max(input.intervalMs, input.profile.cooldownMs),
          7 * dayMs,
        ),
    ).toISOString();
  }

  if (status === "running") {
    return new Date(
      lastRunMs + Math.max(input.intervalMs, Math.min(input.profile.cooldownMs, hourMs)),
    ).toISOString();
  }

  return new Date(lastRunMs + Math.max(input.intervalMs, input.profile.cadenceMs)).toISOString();
}

function createProfileDuplicateKey(profile: SystemSearchProfile) {
  return [
    normalizeProfileKey(profile.queryTitleVariant),
    normalizeProfileKey(profile.geography.country),
    normalizeProfileKey(profile.geography.state),
    normalizeProfileKey(profile.geography.city),
    profile.platformPreference?.mode === "restriction"
      ? profile.platformPreference.platforms.join(",")
      : "",
  ].join("|");
}

function compareProfilePriority(left: SystemSearchProfile, right: SystemSearchProfile) {
  if (left.priority !== right.priority) {
    return left.priority - right.priority;
  }

  if (left.geography.scope !== right.geography.scope) {
    return geographyScopeRank(left.geography.scope) - geographyScopeRank(right.geography.scope);
  }

  return left.id.localeCompare(right.id);
}

function compareProfileRotation(left: SystemSearchProfile, right: SystemSearchProfile) {
  if (left.titleVariantTier !== right.titleVariantTier) {
    return left.titleVariantTier - right.titleVariantTier;
  }

  const leftScopeRank = geographyScopeRank(left.geography.scope);
  const rightScopeRank = geographyScopeRank(right.geography.scope);
  if (leftScopeRank !== rightScopeRank) {
    return leftScopeRank - rightScopeRank;
  }

  if (left.geography.scope === "country" && right.geography.scope === "country") {
    const familyDelta =
      roleFamilyRotationRank(left.canonicalJobFamily) -
      roleFamilyRotationRank(right.canonicalJobFamily);
    if (familyDelta !== 0) {
      return familyDelta;
    }

    if (left.geography.priorityOffset !== right.geography.priorityOffset) {
      return left.geography.priorityOffset - right.geography.priorityOffset;
    }

    return left.id.localeCompare(right.id);
  }

  if (left.geography.priorityOffset !== right.geography.priorityOffset) {
    return left.geography.priorityOffset - right.geography.priorityOffset;
  }

  if (left.priority !== right.priority) {
    return left.priority - right.priority;
  }

  return left.id.localeCompare(right.id);
}

function roleFamily(
  id: string,
  label: string,
  priority: number,
  variants: readonly RoleTitleVariant[],
): RoleFamilyTemplate {
  return {
    id,
    label,
    priority,
    variants,
  };
}

function variant(title: string, tier: RoleTitleVariant["tier"]): RoleTitleVariant {
  return { title, tier };
}

function countryGeography(
  id: string,
  label: string,
  country: string,
  priorityOffset: number,
  variantTiers: readonly number[],
): SystemSearchProfileGeography {
  return {
    id,
    label,
    scope: "country",
    country,
    priorityOffset,
    variantTiers,
  };
}

function stateGeography(
  id: string,
  label: string,
  state: string,
  priorityOffset: number,
): SystemSearchProfileGeography {
  return {
    id,
    label,
    scope: "state",
    country: "United States",
    state,
    priorityOffset,
    variantTiers: [0],
  };
}

function provinceGeography(
  id: string,
  label: string,
  provinceCode: string,
  priorityOffset: number,
  variantTiers: readonly number[],
): SystemSearchProfileGeography {
  return {
    id,
    label,
    scope: "province",
    country: "Canada",
    state: provinceCode,
    priorityOffset,
    variantTiers,
  };
}

function cityGeography(
  id: string,
  label: string,
  country: string,
  state: string,
  city: string,
  priorityOffset: number,
): SystemSearchProfileGeography {
  return {
    id,
    label,
    scope: "city",
    country,
    state,
    city,
    priorityOffset,
    variantTiers: [0],
  };
}

function copySystemSearchProfile(profile: SystemSearchProfile): SystemSearchProfile {
  return {
    id: profile.id,
    label: profile.label,
    canonicalJobFamily: profile.canonicalJobFamily,
    queryTitleVariant: profile.queryTitleVariant,
    titleVariantTier: profile.titleVariantTier,
    geography: copyGeography(profile.geography),
    ...(profile.platformPreference
      ? {
          platformPreference: {
            mode: profile.platformPreference.mode,
            platforms: [...profile.platformPreference.platforms],
          },
        }
      : {}),
    priority: profile.priority,
    rotationIndex: profile.rotationIndex,
    enabled: profile.enabled,
    cadenceMs: profile.cadenceMs,
    cooldownMs: profile.cooldownMs,
    ...(profile.lastRunAt ? { lastRunAt: profile.lastRunAt } : {}),
    ...(profile.nextEligibleAt ? { nextEligibleAt: profile.nextEligibleAt } : {}),
    successCount: profile.successCount,
    failureCount: profile.failureCount,
    consecutiveFailureCount: profile.consecutiveFailureCount,
    filters: copySearchFilters(profile.filters),
  };
}

function copyGeography(geography: SystemSearchProfileGeography): SystemSearchProfileGeography {
  return {
    id: geography.id,
    label: geography.label,
    scope: geography.scope,
    country: geography.country,
    ...(geography.state ? { state: geography.state } : {}),
    ...(geography.city ? { city: geography.city } : {}),
    priorityOffset: geography.priorityOffset,
    variantTiers: [...geography.variantTiers],
  };
}

function copySearchFilters(filters: SearchFilters): SearchFilters {
  return {
    title: filters.title,
    ...(filters.country ? { country: filters.country } : {}),
    ...(filters.state ? { state: filters.state } : {}),
    ...(filters.city ? { city: filters.city } : {}),
    ...(filters.platforms ? { platforms: [...filters.platforms] } : {}),
    ...(filters.crawlMode ? { crawlMode: filters.crawlMode } : {}),
    ...(filters.experienceLevels ? { experienceLevels: [...filters.experienceLevels] } : {}),
    ...(filters.experienceMatchMode ? { experienceMatchMode: filters.experienceMatchMode } : {}),
    ...(filters.includeUnspecifiedExperience ? { includeUnspecifiedExperience: true } : {}),
  };
}

function searchFiltersEqual(left: SearchFilters, right: SearchFilters) {
  return (
    left.title === right.title &&
    normalizeOptional(left.country) === normalizeOptional(right.country) &&
    normalizeOptional(left.state) === normalizeOptional(right.state) &&
    normalizeOptional(left.city) === normalizeOptional(right.city) &&
    (left.crawlMode ?? "balanced") === (right.crawlMode ?? "balanced") &&
    compareStringArrays(left.platforms, right.platforms) &&
    compareStringArrays(left.experienceLevels, right.experienceLevels) &&
    normalizeOptional(left.experienceMatchMode) === normalizeOptional(right.experienceMatchMode) &&
    Boolean(left.includeUnspecifiedExperience) === Boolean(right.includeUnspecifiedExperience)
  );
}

function normalizeOptional(value?: string) {
  return value ?? "";
}

function compareStringArrays(left?: readonly string[], right?: readonly string[]) {
  const normalizedLeft = left ?? [];
  const normalizedRight = right ?? [];

  return (
    normalizedLeft.length === normalizedRight.length &&
    normalizedLeft.every((value, index) => value === normalizedRight[index])
  );
}

function normalizeProfileKey(value?: string) {
  return (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[-_/]+/g, " ")
    .replace(/\bfront\s*end\b/g, "frontend")
    .replace(/\bback\s*end\b/g, "backend")
    .replace(/\bfull\s*stack\b/g, "fullstack")
    .replace(/\bdev\s*ops\b/g, "devops")
    .replace(/\bpre\s*sales\b/g, "presales")
    .replace(/\s+/g, " ");
}

function slugify(value: string) {
  return normalizeProfileKey(value).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function geographyScopeRank(scope: SystemSearchProfileGeography["scope"]) {
  switch (scope) {
    case "country":
      return 0;
    case "state":
      return 1;
    case "province":
      return 1;
    case "city":
      return 2;
    default:
      return 3;
  }
}

function roleFamilyRotationRank(familyId: string) {
  const index = roleFamilyTemplates.findIndex((family) => family.id === familyId);
  return index >= 0 ? index : Number.MAX_SAFE_INTEGER;
}

function positiveModulo(value: number, divisor: number) {
  return ((value % divisor) + divisor) % divisor;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function safeParseTime(value?: string) {
  if (!value) {
    return undefined;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}
