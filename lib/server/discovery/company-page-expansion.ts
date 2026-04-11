import "server-only";

import { runWithConcurrency } from "@/lib/server/crawler/helpers";
import { safeFetchText } from "@/lib/server/net/fetcher";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  isCompanyPageSource,
  type DiscoveredSource,
} from "@/lib/server/discovery/types";

const atsHostPattern =
  /(?:boards\.greenhouse\.io|job-boards\.greenhouse\.io|jobs\.lever\.co|jobs\.ashbyhq\.com|myworkdayjobs\.com)/i;

export async function expandCompanyPageSources(
  sources: DiscoveredSource[],
  fetchImpl?: typeof fetch,
) {
  const companyPages = sources.filter(
    (source) => isCompanyPageSource(source) && source.pageType !== "json_feed",
  );

  if (companyPages.length === 0) {
    return [] as DiscoveredSource[];
  }

  const expandedSources = await runWithConcurrency(
    companyPages,
    async (source) => {
      const result = await safeFetchText(source.url, {
        fetchImpl,
        method: "GET",
        headers: {
          Accept: "text/html,application/xhtml+xml",
        },
        cache: "no-store",
        timeoutMs: 5_000,
        retries: 1,
      });

      if (!result.ok || !result.data) {
        return [] as DiscoveredSource[];
      }

      return extractPublicAtsUrls(result.data, source.url).map((url) =>
        classifySourceCandidate({
          url,
          companyHint: source.companyHint,
          confidence: source.confidence,
          discoveryMethod: source.discoveryMethod,
        }),
      );
    },
    2,
  );

  return expandedSources.flat();
}

function extractPublicAtsUrls(html: string, sourcePageUrl: string) {
  const discovered = new Set<string>();

  Array.from(html.matchAll(/href=["']([^"']+)["']/gi)).forEach((match) => {
    const resolved = resolveUrlCandidate(match[1], sourcePageUrl);
    if (resolved && atsHostPattern.test(resolved)) {
      discovered.add(resolved);
    }
  });

  Array.from(
    html.matchAll(
      /https?:\\\/\\\/(?:www\\\/\.)?(?:boards\.greenhouse\.io|job-boards\.greenhouse\.io|jobs\.lever\.co|jobs\.ashbyhq\.com|[^"'\\<\s)]*myworkdayjobs\.com)[^"'\\<\s)]*/gi,
    ),
  ).forEach((match) => {
    const resolved = match[0].replace(/\\\//g, "/");
    const normalized = resolveUrlCandidate(resolved, sourcePageUrl);
    if (normalized && atsHostPattern.test(normalized)) {
      discovered.add(normalized);
    }
  });

  return Array.from(discovered);
}

function resolveUrlCandidate(value: string, sourcePageUrl: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  try {
    return new URL(trimmed, sourcePageUrl).toString();
  } catch {
    return undefined;
  }
}
