import type { SearchFilters } from "@/lib/types";

export const backgroundIngestionOwnerKey = "system:background-ingestion";

export const backgroundIngestionSearchFilters: SearchFilters = {
  title: "Background Inventory Refresh",
  crawlMode: "deep",
};

export function isBackgroundIngestionSearchFilters(filters: SearchFilters) {
  return (
    filters.title === backgroundIngestionSearchFilters.title &&
    (filters.crawlMode ?? "balanced") === backgroundIngestionSearchFilters.crawlMode &&
    compareStringArrays(filters.platforms, backgroundIngestionSearchFilters.platforms)
  );
}

function compareStringArrays(left?: string[], right?: string[]) {
  const normalizedLeft = left ?? [];
  const normalizedRight = right ?? [];

  return (
    normalizedLeft.length === normalizedRight.length &&
    normalizedLeft.every((value, index) => value === normalizedRight[index])
  );
}
