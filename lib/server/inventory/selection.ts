import "server-only";

import { toDiscoveredSourceFromInventory, type SourceInventoryRecord } from "@/lib/server/discovery/inventory";
import type { CrawlProvider } from "@/lib/server/providers/types";

const minuteMs = 60_000;
const hourMs = 60 * minuteMs;
const dayMs = 24 * hourMs;

export type InventorySchedulingSkipReason =
  | "unsupported_provider"
  | "status_disabled"
  | "status_paused"
  | "freshness_cooldown"
  | "health_backoff"
  | "capacity_deprioritized";

type InventoryFreshnessBucket =
  | "never_crawled"
  | "eligible_due"
  | "cooling_down"
  | "retry_backoff";

export type InventorySchedulingDiagnostics = {
  inventorySources: number;
  crawlableSources: number;
  eligibleSources: number;
  selectedSources: number;
  skippedByReason: Record<string, number>;
  freshnessBuckets: Record<string, number>;
  selectedByPlatform: Record<string, number>;
  selectedByProvider: Record<string, number>;
  selectedByHealth: Record<string, number>;
  selectedSourceIds: string[];
  skippedSourceSamples: string[];
};

export type RecurringInventorySelectionPlan = {
  selectedRecords: SourceInventoryRecord[];
  diagnostics: InventorySchedulingDiagnostics;
};

export function planRecurringInventorySourceSelection(input: {
  inventory: SourceInventoryRecord[];
  providers: CrawlProvider[];
  now: Date;
  maxSources: number;
  intervalMs: number;
  prioritySourceIds?: string[];
}): RecurringInventorySelectionPlan {
  const nowMs = input.now.getTime();
  const prioritySourceIds = new Set(input.prioritySourceIds ?? []);
  const skippedByReason: Record<string, number> = {};
  const freshnessBuckets: Record<string, number> = {};
  const selectedByPlatform: Record<string, number> = {};
  const selectedByProvider: Record<string, number> = {};
  const selectedByHealth: Record<string, number> = {};
  const skippedSourceSamples: string[] = [];
  const eligible: Array<{
    record: SourceInventoryRecord;
    overdueMs: number;
    nextEligibleAt: string;
    freshnessBucket: InventoryFreshnessBucket;
  }> = [];
  let crawlableSources = 0;

  for (const record of input.inventory) {
    const source = toDiscoveredSourceFromInventory(record);
    const supported = input.providers.some((provider) => provider.supportsSource(source));
    if (!supported) {
      incrementCount(skippedByReason, "unsupported_provider");
      pushSample(skippedSourceSamples, `${record._id}:unsupported_provider`);
      continue;
    }

    crawlableSources += 1;

    if (record.status === "disabled") {
      incrementCount(skippedByReason, "status_disabled");
      pushSample(skippedSourceSamples, `${record._id}:status_disabled`);
      continue;
    }

    if (record.status === "paused") {
      incrementCount(skippedByReason, "status_paused");
      pushSample(skippedSourceSamples, `${record._id}:status_paused`);
      continue;
    }

    const nextEligibleAt = resolveSourceNextEligibleAt(record, {
      intervalMs: input.intervalMs,
    });
    const nextEligibleMs = safeParseTime(nextEligibleAt) ?? nowMs;
    const freshnessBucket = classifyFreshnessBucket(record, nextEligibleMs, nowMs);
    incrementCount(freshnessBuckets, freshnessBucket);

    if (nextEligibleMs > nowMs) {
      const reason =
        freshnessBucket === "retry_backoff" ? "health_backoff" : "freshness_cooldown";
      incrementCount(skippedByReason, reason);
      pushSample(skippedSourceSamples, `${record._id}:${reason}`);
      continue;
    }

    eligible.push({
      record,
      overdueMs: Math.max(0, nowMs - nextEligibleMs),
      nextEligibleAt,
      freshnessBucket,
    });
  }

  eligible.sort((left, right) => compareCandidates(left, right));

  const selectedCandidates = selectRecurringInventoryCandidates(
    eligible,
    input.maxSources,
    prioritySourceIds,
  );
  const selectedIds = new Set(selectedCandidates.map((candidate) => candidate.record._id));
  const selectedRecords = selectedCandidates.map((candidate) => candidate.record);
  for (const candidate of eligible.filter((entry) => !selectedIds.has(entry.record._id))) {
    incrementCount(skippedByReason, "capacity_deprioritized");
    pushSample(skippedSourceSamples, `${candidate.record._id}:capacity_deprioritized`);
  }

  for (const record of selectedRecords) {
    incrementCount(selectedByPlatform, record.platform);
    incrementCount(selectedByHealth, record.health);
    const source = toDiscoveredSourceFromInventory(record);
    for (const provider of input.providers) {
      if (provider.supportsSource(source)) {
        incrementCount(selectedByProvider, provider.provider);
      }
    }
  }

  return {
    selectedRecords,
    diagnostics: {
      inventorySources: input.inventory.length,
      crawlableSources,
      eligibleSources: eligible.length,
      selectedSources: selectedRecords.length,
      skippedByReason,
      freshnessBuckets,
      selectedByPlatform,
      selectedByProvider,
      selectedByHealth,
      selectedSourceIds: selectedRecords.slice(0, 12).map((record) => record._id),
      skippedSourceSamples,
    },
  };
}

function selectRecurringInventoryCandidates(
  eligible: Array<{
    record: SourceInventoryRecord;
    overdueMs: number;
    nextEligibleAt: string;
    freshnessBucket: InventoryFreshnessBucket;
  }>,
  maxSources: number,
  prioritySourceIds: Set<string> = new Set(),
) {
  const limit = Math.max(0, Math.floor(maxSources));
  if (limit <= 0 || eligible.length <= limit) {
    return eligible.slice(0, limit);
  }

  const priorityCandidates = eligible.filter((candidate) =>
    prioritySourceIds.has(candidate.record._id),
  );
  const neverCrawledCandidates = eligible.filter(
    (candidate) => candidate.freshnessBucket === "never_crawled",
  );
  const selected: typeof eligible = [];
  const selectedIds = new Set<string>();
  const priorityReserve =
    priorityCandidates.length > 0
      ? Math.min(priorityCandidates.length, Math.max(1, Math.ceil(limit * 0.5)))
      : 0;
  const neverCrawledReserve = Math.min(
    neverCrawledCandidates.length,
    Math.max(1, Math.ceil(limit * 0.25)),
  );

  for (const candidate of priorityCandidates.slice(0, priorityReserve)) {
    selected.push(candidate);
    selectedIds.add(candidate.record._id);
  }

  for (const candidate of neverCrawledCandidates.slice(0, neverCrawledReserve)) {
    if (selected.length >= limit) {
      break;
    }

    if (selectedIds.has(candidate.record._id)) {
      continue;
    }

    selected.push(candidate);
    selectedIds.add(candidate.record._id);
  }

  for (const candidate of eligible) {
    if (selected.length >= limit) {
      break;
    }

    if (selectedIds.has(candidate.record._id)) {
      continue;
    }

    selected.push(candidate);
    selectedIds.add(candidate.record._id);
  }

  return selected;
}

export function resolveSourceNextEligibleAt(
  record: SourceInventoryRecord,
  input: {
    intervalMs: number;
  },
) {
  if (record.nextEligibleAt) {
    return record.nextEligibleAt;
  }

  const lastObservedAt = record.lastCrawledAt ?? record.lastSeenAt ?? record.firstSeenAt;
  const lastObservedMs = safeParseTime(lastObservedAt);
  if (lastObservedMs == null) {
    return new Date(0).toISOString();
  }

  if (!record.lastCrawledAt) {
    return new Date(lastObservedMs).toISOString();
  }

  const offsetMs = shouldBackOffSource(record)
    ? resolveFailureBackoffMs({
        intervalMs: input.intervalMs,
        consecutiveFailures: Math.max(1, record.consecutiveFailures),
        health: record.health,
      })
    : resolveSuccessRefreshMs(record, input.intervalMs);

  return new Date(lastObservedMs + offsetMs).toISOString();
}

export function resolveObservedSourceNextEligibleAt(input: {
  record: SourceInventoryRecord;
  observedAt: string;
  intervalMs: number;
  health: SourceInventoryRecord["health"];
  consecutiveFailures: number;
  succeeded?: boolean;
}) {
  const observedAtMs = safeParseTime(input.observedAt) ?? Date.now();
  const offsetMs = input.succeeded === false
    ? resolveFailureBackoffMs({
        intervalMs: input.intervalMs,
        consecutiveFailures: Math.max(1, input.consecutiveFailures),
        health: input.health,
      })
    : resolveSuccessRefreshMs(
        {
          ...input.record,
          health: input.health,
          consecutiveFailures: input.consecutiveFailures,
        },
        input.intervalMs,
      );

  return new Date(observedAtMs + offsetMs).toISOString();
}

function compareCandidates(
  left: { record: SourceInventoryRecord; overdueMs: number; nextEligibleAt: string },
  right: { record: SourceInventoryRecord; overdueMs: number; nextEligibleAt: string },
) {
  if (left.overdueMs !== right.overdueMs) {
    return right.overdueMs - left.overdueMs;
  }

  const leftHealthRank = healthRank(left.record.health);
  const rightHealthRank = healthRank(right.record.health);
  if (leftHealthRank !== rightHealthRank) {
    return leftHealthRank - rightHealthRank;
  }

  if (left.record.consecutiveFailures !== right.record.consecutiveFailures) {
    return left.record.consecutiveFailures - right.record.consecutiveFailures;
  }

  if (left.record.crawlPriority !== right.record.crawlPriority) {
    return left.record.crawlPriority - right.record.crawlPriority;
  }

  if (left.record.inventoryRank !== right.record.inventoryRank) {
    return left.record.inventoryRank - right.record.inventoryRank;
  }

  return left.nextEligibleAt.localeCompare(right.nextEligibleAt);
}

function classifyFreshnessBucket(
  record: SourceInventoryRecord,
  nextEligibleMs: number,
  nowMs: number,
): InventoryFreshnessBucket {
  if (!record.lastCrawledAt) {
    return "never_crawled";
  }

  if (nextEligibleMs <= nowMs) {
    return "eligible_due";
  }

  return shouldBackOffSource(record) ? "retry_backoff" : "cooling_down";
}

function shouldBackOffSource(record: SourceInventoryRecord) {
  return (
    record.consecutiveFailures > 0 ||
    record.health === "degraded" ||
    record.health === "failing"
  );
}

function resolveSuccessRefreshMs(record: SourceInventoryRecord, intervalMs: number) {
  const priorityBand =
    record.crawlPriority <= 100
      ? 2
      : record.crawlPriority <= 10_000
        ? 3
        : record.crawlPriority <= 30_000
          ? 4
          : 6;
  const sourceTypeAdjustment =
    record.sourceType === "ats_board" || record.sourceType === "feed"
      ? 0
      : record.sourceType === "career_site"
        ? 1
        : record.sourceType === "company_page"
          ? 2
          : 3;
  const confidenceAdjustment =
    record.confidence === "high" ? 0 : record.confidence === "medium" ? 1 : 2;
  const healthAdjustment = record.health === "healthy" ? 0 : record.health === "unknown" ? 1 : 2;

  return clamp(
    intervalMs * Math.max(1, priorityBand + sourceTypeAdjustment + confidenceAdjustment + healthAdjustment),
    intervalMs,
    dayMs,
  );
}

function resolveFailureBackoffMs(input: {
  intervalMs: number;
  consecutiveFailures: number;
  health: SourceInventoryRecord["health"];
}) {
  const failureMultiplier = Math.min(32, 2 ** Math.min(5, input.consecutiveFailures));
  const healthMultiplier = input.health === "failing" ? 2 : input.health === "degraded" ? 1.5 : 1;

  return clamp(
    Math.round(input.intervalMs * failureMultiplier * healthMultiplier),
    input.intervalMs * 2,
    7 * dayMs,
  );
}

function healthRank(health: SourceInventoryRecord["health"]) {
  switch (health) {
    case "healthy":
      return 0;
    case "unknown":
      return 1;
    case "degraded":
      return 2;
    case "failing":
      return 3;
    default:
      return 4;
  }
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

function incrementCount(counts: Record<string, number>, key: string) {
  counts[key] = (counts[key] ?? 0) + 1;
}

function pushSample(samples: string[], value: string) {
  if (samples.length >= 12) {
    return;
  }

  samples.push(value);
}
