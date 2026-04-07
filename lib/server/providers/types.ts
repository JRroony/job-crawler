import type { ExperienceLevel, ProviderPlatform, SearchFilters } from "@/lib/types";

export type FetchLike = typeof fetch;

export type NormalizedJobSeed = {
  title: string;
  company: string;
  country?: string;
  state?: string;
  city?: string;
  locationText: string;
  experienceLevel?: ExperienceLevel;
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

export type ProviderResult = {
  provider: ProviderPlatform;
  status: "success" | "partial" | "failed" | "unsupported";
  jobs: NormalizedJobSeed[];
  fetchedCount: number;
  matchedCount: number;
  errorMessage?: string;
};

export type CrawlProvider = {
  provider: ProviderPlatform;
  crawl(context: ProviderExecutionContext): Promise<ProviderResult>;
};
