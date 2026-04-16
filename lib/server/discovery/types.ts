import type {
  CompanyPageSourceConfig,
  DiscoveryStageDiagnostics,
  SearchFilters,
} from "@/lib/types";
import type { NormalizedJobSeed } from "@/lib/server/providers/types";

export const discoveredPlatforms = [
  "greenhouse",
  "lever",
  "ashby",
  "smartrecruiters",
  "company_page",
  "workday",
  "unknown",
] as const;

export const discoveryConfidenceLevels = ["high", "medium", "low"] as const;

export const discoveryMethods = [
  "source_inventory",
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
  jobId?: string;
  boardUrl?: string;
  apiUrl?: string;
};

export type LeverDiscoveredSource = BaseDiscoveredSource & {
  platform: "lever";
  token?: string;
  jobId?: string;
  hostedUrl?: string;
  apiUrl?: string;
};

export type AshbyDiscoveredSource = BaseDiscoveredSource & {
  platform: "ashby";
  token?: string;
  jobId?: string;
  boardUrl?: string;
};

export type SmartRecruitersDiscoveredSource = BaseDiscoveredSource & {
  platform: "smartrecruiters";
  token?: string;
  jobId?: string;
  boardUrl?: string;
  jobUrl?: string;
};

export type CompanyPageDiscoveredSource = BaseDiscoveredSource & {
  platform: "company_page";
  companyHint: string;
  pageType: CompanyPageSourceConfig["type"];
};

export type WorkdayDiscoveredSource = BaseDiscoveredSource & {
  platform: "workday";
  token?: string;
  jobId?: string;
  sitePath?: string;
  careerSitePath?: string;
  apiUrl?: string;
};

export type UnknownDiscoveredSource = BaseDiscoveredSource & {
  platform: "unknown";
};

export type DiscoveredSource =
  | GreenhouseDiscoveredSource
  | LeverDiscoveredSource
  | AshbyDiscoveredSource
  | SmartRecruitersDiscoveredSource
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
  jobs?: NormalizedJobSeed[];
  diagnostics: DiscoveryStageDiagnostics;
};

export type DiscoveryExecutionStage = {
  label: "baseline" | "public_search" | "full";
  sources: DiscoveredSource[];
  jobs?: NormalizedJobSeed[];
  diagnostics?: DiscoveryStageDiagnostics;
};

export type DiscoveryService = {
  discover(input: DiscoveryInput): Promise<DiscoveredSource[]>;
  discoverWithDiagnostics?(input: DiscoveryInput): Promise<DiscoveryExecution>;
  discoverInStages?(input: DiscoveryInput): Promise<DiscoveryExecutionStage[]>;
  discoverBaseline?(input: DiscoveryInput): Promise<DiscoveryExecutionStage>;
  discoverSupplemental?(
    input: DiscoveryInput,
    context: { baselineSources: DiscoveredSource[] },
  ): Promise<DiscoveryExecutionStage>;
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

export function isSmartRecruitersSource(
  source: DiscoveredSource,
): source is SmartRecruitersDiscoveredSource {
  return source.platform === "smartrecruiters";
}

export function isWorkdaySource(source: DiscoveredSource): source is WorkdayDiscoveredSource {
  return source.platform === "workday";
}
