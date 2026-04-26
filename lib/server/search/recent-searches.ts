import "server-only";

import { collectionNames } from "@/lib/server/db/collections";

type SortSpec = Record<string, 1 | -1>;

type RecentSearchDocument = Record<string, unknown> & {
  filters?: Record<string, unknown>;
  systemProfileId?: unknown;
};

type RecentSearchesCollection = {
  find(
    filter?: Record<string, unknown>,
    options?: { sort?: SortSpec; limit?: number },
  ): { toArray(): Promise<RecentSearchDocument[]> };
};

let hasWarnedRecentSearchMemoryFallback = false;

export async function listRecentSearchesForApi(limit = 6) {
  const collection = await resolveRecentSearchesCollection();
  const documents = await collection
    .find({}, { sort: { createdAt: -1 }, limit })
    .toArray();

  return documents.filter((document) => !isHiddenSystemSearch(document));
}

async function resolveRecentSearchesCollection(): Promise<RecentSearchesCollection> {
  try {
    const { getMongoDb } = await import("@/lib/server/mongodb");
    const db = await getMongoDb({ ensureIndexes: false });
    return db.collection<RecentSearchDocument>(collectionNames.searches);
  } catch (error) {
    if (!hasWarnedRecentSearchMemoryFallback) {
      hasWarnedRecentSearchMemoryFallback = true;
      console.warn(
        "[db:fallback] MongoDB is unavailable while listing recent searches; using in-memory persistence for this process.",
        error instanceof Error ? { message: error.message } : { error },
      );
    }

    const { getMemoryDb } = await import("@/lib/server/db/memory");
    return getMemoryDb().collection<RecentSearchDocument>(collectionNames.searches);
  }
}

function isHiddenSystemSearch(document: RecentSearchDocument) {
  return Boolean(document.systemProfileId) || isLegacyBackgroundIngestionSearch(document.filters);
}

function isLegacyBackgroundIngestionSearch(filters: unknown) {
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) {
    return false;
  }

  const candidate = filters as Record<string, unknown>;
  return (
    candidate.title === "Background Inventory Refresh" &&
    normalizeOptional(candidate.country) === "" &&
    normalizeOptional(candidate.state) === "" &&
    normalizeOptional(candidate.city) === "" &&
    (candidate.crawlMode ?? "balanced") === "deep" &&
    isEmptyStringArray(candidate.platforms) &&
    isEmptyStringArray(candidate.experienceLevels) &&
    normalizeOptional(candidate.experienceMatchMode) === "" &&
    !candidate.includeUnspecifiedExperience
  );
}

function normalizeOptional(value: unknown) {
  return typeof value === "string" ? value : "";
}

function isEmptyStringArray(value: unknown) {
  return !Array.isArray(value) || value.length === 0;
}
