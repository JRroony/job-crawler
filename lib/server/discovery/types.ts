import type {
  CompanyPageSourceConfig,
  DiscoveryStageDiagnostics,
  SearchFilters,
} from "@/lib/types";

export const discoveredPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "company_page",
  "workday",
  "unknown",
] as const;

export const discoveryConfidenceLevels = ["high", "medium", "low"] as const;

export const discoveryMethods = [
  "platform_registry",
  "configured_env",
  "manual_config",
  "curated_catalog",
  "future_search",
] as const;

export type DiscoveredPlatform = (typeof discoveredPlatforms)[number];
export type DiscoveryConfidence = (typeof discoveryConfidenceLevels)[number];
export type DiscoveryMethod = (typeof discoveryMethods)[number];

type BaseDiscoveredSource = {
  id: string;
  platform: DiscoveredPlatform;
  url: string;
  token?: string;
  companyHint?: string;
  confidence: DiscoveryConfidence;
  discoveryMethod: DiscoveryMethod;
};

export type GreenhouseDiscoveredSource = BaseDiscoveredSource & {
  platform: "greenhouse";
  token?: string;
  boardUrl?: string;
  apiUrl?: string;
};

export type LeverDiscoveredSource = BaseDiscoveredSource & {
  platform: "lever";
  token?: string;
  hostedUrl?: string;
  apiUrl?: string;
};

export type AshbyDiscoveredSource = BaseDiscoveredSource & {
  platform: "ashby";
  token?: string;
  boardUrl?: string;
};

export type CompanyPageDiscoveredSource = BaseDiscoveredSource & {
  platform: "company_page";
  companyHint: string;
  pageType: CompanyPageSourceConfig["type"];
};

export type WorkdayDiscoveredSource = BaseDiscoveredSource & {
  platform: "workday";
};

export type UnknownDiscoveredSource = BaseDiscoveredSource & {
  platform: "unknown";
};

export type DiscoveredSource =
  | GreenhouseDiscoveredSource
  | LeverDiscoveredSource
  | AshbyDiscoveredSource
  | CompanyPageDiscoveredSource
  | WorkdayDiscoveredSource
  | UnknownDiscoveredSource;

export type SourceClassificationCandidate = {
  url: string;
  token?: string;
  companyHint?: string;
  confidence?: DiscoveryConfidence;
  discoveryMethod: DiscoveryMethod;
  pageType?: CompanyPageSourceConfig["type"];
};

export type DiscoveryInput = {
  filters: SearchFilters;
  now: Date;
  fetchImpl?: typeof fetch;
};

export type DiscoveryExecution = {
  sources: DiscoveredSource[];
  diagnostics: DiscoveryStageDiagnostics;
};

export type DiscoveryService = {
  discover(input: DiscoveryInput): Promise<DiscoveredSource[]>;
  discoverWithDiagnostics?(input: DiscoveryInput): Promise<DiscoveryExecution>;
};

export function isGreenhouseSource(source: DiscoveredSource): source is GreenhouseDiscoveredSource {
  return source.platform === "greenhouse";
}

export function isLeverSource(source: DiscoveredSource): source is LeverDiscoveredSource {
  return source.platform === "lever";
}

export function isAshbySource(source: DiscoveredSource): source is AshbyDiscoveredSource {
  return source.platform === "ashby";
}

export function isCompanyPageSource(
  source: DiscoveredSource,
): source is CompanyPageDiscoveredSource {
  return source.platform === "company_page";
}
