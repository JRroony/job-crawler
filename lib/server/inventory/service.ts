import "server-only";

import type {
  JobCrawlerRepository,
  SourceInventoryObservation,
} from "@/lib/server/db/repository";
import { refreshSourceInventory } from "@/lib/server/discovery/service";
import type { DiscoveredSource } from "@/lib/server/discovery/types";
import type { SourceInventoryRecord } from "@/lib/server/discovery/inventory";
import type { CrawlProvider } from "@/lib/server/providers/types";
import {
  planRecurringInventorySourceSelection,
  type RecurringInventorySelectionPlan,
} from "@/lib/server/inventory/selection";
import { resolveRepository } from "@/lib/server/services/runtime";

export async function refreshPersistentSourceInventory(runtime: {
  repository?: JobCrawlerRepository;
  now?: Date;
} = {}) {
  const repository = await resolveRepository(runtime.repository);
  return refreshSourceInventory({
    repository,
    now: runtime.now ?? new Date(),
  });
}

export async function listPersistentSourceInventory(runtime: {
  repository?: JobCrawlerRepository;
  platforms?: SourceInventoryRecord["platform"][];
} = {}) {
  const repository = await resolveRepository(runtime.repository);
  return repository.listSourceInventory(runtime.platforms);
}

export async function upsertDiscoveredSourcesIntoPersistentInventory(runtime: {
  repository?: JobCrawlerRepository;
  sources: DiscoveredSource[];
  observedAt?: Date;
}) {
  const repository = await resolveRepository(runtime.repository);
  return repository.upsertDiscoveredSourcesIntoInventory(
    runtime.sources,
    (runtime.observedAt ?? new Date()).toISOString(),
  );
}

export async function recordPersistentSourceInventoryObservations(runtime: {
  repository?: JobCrawlerRepository;
  observations: SourceInventoryObservation[];
}) {
  const repository = await resolveRepository(runtime.repository);
  return repository.recordSourceInventoryObservations(runtime.observations);
}

export function planPersistentInventoryRecurringCrawl(runtime: {
  inventory: SourceInventoryRecord[];
  providers: CrawlProvider[];
  now: Date;
  maxSources: number;
  intervalMs: number;
}): RecurringInventorySelectionPlan {
  return planRecurringInventorySourceSelection(runtime);
}
