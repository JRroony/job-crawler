import type { DiscoveredSource } from "@/lib/server/discovery/types";

/**
 * Cap sources to maxSources while preserving platform diversity.
 * Strategy: ensure at least 1 source per active platform first, then
 * fill remaining slots proportionally across platforms.
 */
export function capSourcesWithPlatformDiversity(
  sources: DiscoveredSource[],
  maxSources: number,
): DiscoveredSource[] {
  if (sources.length <= maxSources) {
    return sources;
  }

  // Group sources by platform
  const byPlatform = new Map<string, DiscoveredSource[]>();
  for (const source of sources) {
    const existing = byPlatform.get(source.platform) ?? [];
    existing.push(source);
    byPlatform.set(source.platform, existing);
  }

  const platforms = Array.from(byPlatform.keys());
  const result: DiscoveredSource[] = [];

  // Phase 1: ensure at least 1 source per platform (up to maxSources)
  for (const platform of platforms) {
    const platformSources = byPlatform.get(platform)!;
    if (platformSources.length > 0) {
      result.push(platformSources[0]);
    }
  }

  // Phase 2: fill remaining slots by round-robin across platforms
  const remainingSlots = maxSources - result.length;
  if (remainingSlots > 0) {
    const indices = new Map<string, number>();
    platforms.forEach((p) => indices.set(p, 1)); // start at index 1 (we already took index 0)

    let filled = 0;
    while (filled < remainingSlots) {
      let added = false;
      for (const platform of platforms) {
        if (filled >= remainingSlots) break;
        const platformSources = byPlatform.get(platform)!;
        const idx = indices.get(platform)!;
        if (idx < platformSources.length) {
          result.push(platformSources[idx]);
          indices.set(platform, idx + 1);
          filled += 1;
          added = true;
        }
      }
      if (!added) break; // all platforms exhausted
    }
  }

  return result.slice(0, maxSources);
}
