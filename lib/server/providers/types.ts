import type {
  AshbyDiscoveredSource,
  CompanyPageDiscoveredSource,
  DiscoveredSource,
  GreenhouseDiscoveredSource,
  LeverDiscoveredSource,
  WorkdayDiscoveredSource,
} from "@/lib/server/discovery/types";
import type {
  ExperienceClassification,
  ExperienceLevel,
  ProviderPlatform,
  ResolvedLocation,
  SearchFilters,
} from "@/lib/types";

export type FetchLike = typeof fetch;

export type NormalizedJobSeed = {
  title: string;
  company: string;
  country?: string;
  state?: string;
  city?: string;
  locationText: string;
  resolvedLocation?: ResolvedLocation;
  experienceLevel?: ExperienceLevel;
  experienceClassification?: ExperienceClassification;
  sourcePlatform: ProviderPlatform;
  sourceJobId: string;
  sourceUrl: string;
  applyUrl: string;
  canonicalUrl?: string;
  postedAt?: string;
  discoveredAt: string;
  rawSourceMetadata: Record<string, unknown>;
};

export type ProviderExecutionContext = {
  fetchImpl: FetchLike;
  now: Date;
  filters: SearchFilters;
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
