import type { CrawlLinkValidationMode } from "@/lib/server/crawler/pipeline";
import type { JobCrawlerRepository } from "@/lib/server/db/repository";
import { getRepository } from "@/lib/server/db/repository";
import type { DiscoveryService } from "@/lib/server/discovery/types";
import type { CrawlProvider } from "@/lib/server/providers/types";

export type JobCrawlerRuntime = {
  repository?: JobCrawlerRepository;
  ensureIndexes?: boolean;
  providers?: CrawlProvider[];
  discovery?: DiscoveryService;
  fetchImpl?: typeof fetch;
  now?: Date;
  deepExperienceInference?: boolean;
  linkValidationMode?: CrawlLinkValidationMode;
  refreshStaleJobLinks?: boolean;
  inlineValidationTopN?: number;
  providerTimeoutMs?: number;
  sourceTimeoutMs?: number;
  progressUpdateIntervalMs?: number;
  earlyVisibleTarget?: number;
  initialVisibleWaitMs?: number;
  allowRequestTimeSupplementalCrawl?: boolean;
  requestOwnerKey?: string;
  ingestionQueueReason?: string;
  signal?: AbortSignal;
};

export async function resolveRepository(
  repository?: JobCrawlerRepository,
  options: { ensureIndexes?: boolean } = {},
) {
  return repository ?? getRepository(undefined, {
    ensureIndexes: options.ensureIndexes,
  });
}
