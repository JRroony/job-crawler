import type {
  AshbyDiscoveredSource,
  CompanyPageDiscoveredSource,
  DiscoveredSource,
  GreenhouseDiscoveredSource,
  LeverDiscoveredSource,
  WorkdayDiscoveredSource,
} from "@/lib/server/discovery/types";
import type {
  EmploymentType,
  ExperienceClassification,
  ExperienceLevel,
  ProviderPlatform,
  RemoteType,
  ResolvedLocation,
  SalaryInfo,
  SearchFilters,
  SponsorshipHint,
} from "@/lib/types";

export type FetchLike = typeof fetch;

export type NormalizedJobSeed = {
  title: string;
  company: string;
  normalizedCompany?: string;
  normalizedTitle?: string;
  country?: string;
  state?: string;
  city?: string;
  locationRaw?: string;
  normalizedLocation?: string;
  locationText: string;
  resolvedLocation?: ResolvedLocation;
  remoteType?: RemoteType;
  employmentType?: EmploymentType;
  seniority?: ExperienceLevel;
  experienceLevel?: ExperienceLevel;
  experienceClassification?: ExperienceClassification;
  sourcePlatform: ProviderPlatform;
  sourceCompanySlug?: string;
  sourceJobId: string;
  sourceUrl: string;
  applyUrl: string;
  canonicalUrl?: string;
  postingDate?: string;
  postedAt?: string;
  discoveredAt: string;
  crawledAt?: string;
  descriptionSnippet?: string;
  salaryInfo?: SalaryInfo;
  sponsorshipHint?: SponsorshipHint;
  dedupeFingerprint?: string;
  rawSourceMetadata: Record<string, unknown>;
};

export type ProviderExecutionContext = {
  fetchImpl: FetchLike;
  now: Date;
  filters: SearchFilters;
  signal?: AbortSignal;
  onBatch?: (batch: ProviderBatchProgress) => Promise<void> | void;
};

type ProviderSourceMap = {
  greenhouse: GreenhouseDiscoveredSource;
  lever: LeverDiscoveredSource;
  ashby: AshbyDiscoveredSource;
  workday: WorkdayDiscoveredSource;
  company_page: CompanyPageDiscoveredSource;
  linkedin_limited: never;
  indeed_limited: never;
};

export type ProviderSourceFor<P extends ProviderPlatform> = ProviderSourceMap[P];

export type ProviderResult<P extends ProviderPlatform = ProviderPlatform> = {
  provider: P;
  status: "success" | "partial" | "failed" | "unsupported";
  jobs: NormalizedJobSeed[];
  sourceCount?: number;
  fetchedCount: number;
  matchedCount: number;
  warningCount?: number;
  errorMessage?: string;
  diagnostics?: ProviderDiagnostics<P>;
};

export type ProviderBatchProgress<P extends ProviderPlatform = ProviderPlatform> = {
  provider: P;
  jobs: NormalizedJobSeed[];
  sourceCount?: number;
  fetchedCount: number;
};

export type ProviderDiagnostics<P extends ProviderPlatform = ProviderPlatform> = {
  provider: P;
  discoveryCount: number;
  fetchCount: number;
  parseSuccessCount: number;
  parseFailureCount: number;
  dropReasonCounts: Record<string, number>;
  sampleDropReasons: string[];
};

export type SourceDrivenProvider<
  P extends ProviderPlatform,
  TSource extends DiscoveredSource | never = ProviderSourceFor<P>,
> = {
  provider: P;
  supportsSource(source: DiscoveredSource): source is TSource;
  crawlSources(
    context: ProviderExecutionContext,
    sources: readonly TSource[],
  ): Promise<ProviderResult<P>>;
};

export type CrawlProvider = SourceDrivenProvider<ProviderPlatform, DiscoveredSource>;

export function defineProvider<P extends ProviderPlatform>(
  provider: SourceDrivenProvider<P, ProviderSourceFor<P>>,
): CrawlProvider {
  return provider as CrawlProvider;
}
