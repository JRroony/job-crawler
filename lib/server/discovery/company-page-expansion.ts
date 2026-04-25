import "server-only";

import { runWithConcurrency } from "@/lib/server/crawler/helpers";
import { safeFetchText } from "@/lib/server/net/fetcher";
import { classifySourceCandidate } from "@/lib/server/discovery/classify-source";
import {
  isCompanyPageSource,
  type DiscoveredSource,
} from "@/lib/server/discovery/types";

const atsHostPattern =
  /(?:boards\.greenhouse\.io|job-boards\.greenhouse\.io|jobs\.lever\.co|jobs\.ashbyhq\.com|(?:jobs|careers)\.smartrecruiters\.com|smartrecruiterscareers\.com|myworkdayjobs\.com)/i;
const maxSameOriginCareerLinksPerPage = 24;
const maxFeedLinksPerPage = 8;
const maxSitemapUrlsPerPage = 24;
const careerPathPattern =
  /(?:^|\/)(?:careers?|jobs?|open-roles?|openings?|positions?|opportunities|vacancies)(?:\/|$|-)/i;
const feedPathPattern =
  /(?:jobs?|careers?|openings?|positions?)[^?#]*(?:\.json|\/api\/|api\/|feed|rss|xml)|(?:api\/|\/api\/)(?:jobs?|positions?|postings?)/i;

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

      const extracted = await extractCompanyCareerExpansionUrls({
        html: result.data,
        sourcePageUrl: source.url,
        fetchImpl,
      });
      console.info("[discovery:company-page-expansion]", {
        sourceUrl: source.url,
        companyHint: source.companyHint,
        atsUrls: extracted.atsUrls.length,
        sameOriginCareerUrls: extracted.sameOriginCareerUrls.length,
        feedUrls: extracted.feedUrls.length,
        sitemapCareerUrls: extracted.sitemapCareerUrls.length,
        samples: {
          ats: extracted.atsUrls.slice(0, 3),
          companyPages: extracted.sameOriginCareerUrls.slice(0, 3),
          feeds: extracted.feedUrls.slice(0, 3),
          sitemap: extracted.sitemapCareerUrls.slice(0, 3),
        },
      });

      return [
        ...extracted.atsUrls.map((url) =>
          classifySourceCandidate({
            url,
            companyHint: source.companyHint,
            confidence: source.confidence,
            discoveryMethod: source.discoveryMethod,
          }),
        ),
        ...extracted.sameOriginCareerUrls.map((url) =>
          classifySourceCandidate({
            url,
            companyHint: source.companyHint,
            confidence: source.confidence === "high" ? "high" : "medium",
            discoveryMethod: source.discoveryMethod,
            pageType: "html_page",
          }),
        ),
        ...extracted.feedUrls.map((url) =>
          classifySourceCandidate({
            url,
            companyHint: source.companyHint,
            confidence: "medium",
            discoveryMethod: source.discoveryMethod,
            pageType: "json_feed",
          }),
        ),
        ...extracted.sitemapCareerUrls.map((url) =>
          classifySourceCandidate({
            url,
            companyHint: source.companyHint,
            confidence: "medium",
            discoveryMethod: source.discoveryMethod,
            pageType: url.toLowerCase().endsWith(".json") ? "json_feed" : "html_page",
          }),
        ),
      ];
    },
    2,
  );

  return expandedSources.flat();
}

async function extractCompanyCareerExpansionUrls(input: {
  html: string;
  sourcePageUrl: string;
  fetchImpl?: typeof fetch;
}) {
  const atsUrls = extractPublicAtsUrls(input.html, input.sourcePageUrl);
  const sameOriginCareerUrls = extractSameOriginCareerUrls(input.html, input.sourcePageUrl);
  const feedUrls = extractCompanyFeedUrls(input.html, input.sourcePageUrl);
  const sitemapCareerUrls = await extractSitemapCareerUrls(input.sourcePageUrl, input.fetchImpl);

  return {
    atsUrls,
    sameOriginCareerUrls,
    feedUrls,
    sitemapCareerUrls,
  };
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
      /https?:\\\/\\\/(?:www\\\/\.)?(?:boards\.greenhouse\.io|job-boards\.greenhouse\.io|jobs\.lever\.co|jobs\.ashbyhq\.com|(?:jobs|careers)\.smartrecruiters\.com|smartrecruiterscareers\.com|[^"'\\<\s)]*myworkdayjobs\.com)[^"'\\<\s)]*/gi,
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

function extractSameOriginCareerUrls(html: string, sourcePageUrl: string) {
  const base = safeUrl(sourcePageUrl);
  if (!base) {
    return [];
  }

  const discovered = new Map<string, number>();
  for (const candidate of extractHtmlUrlCandidates(html, sourcePageUrl)) {
    const url = safeUrl(candidate.url);
    if (!url || url.origin !== base.origin || url.toString() === base.toString()) {
      continue;
    }

    const score = scoreCompanyCareerUrl(url, candidate.context);
    if (score < 3) {
      continue;
    }

    const normalized = normalizeExpansionUrl(url);
    discovered.set(normalized, Math.max(discovered.get(normalized) ?? 0, score));
  }

  return Array.from(discovered.entries())
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, maxSameOriginCareerLinksPerPage)
    .map(([url]) => url);
}

function extractCompanyFeedUrls(html: string, sourcePageUrl: string) {
  const base = safeUrl(sourcePageUrl);
  if (!base) {
    return [];
  }

  const discovered = new Set<string>();
  for (const candidate of extractHtmlUrlCandidates(html, sourcePageUrl)) {
    const url = safeUrl(candidate.url);
    if (!url || url.origin !== base.origin) {
      continue;
    }

    const searchable = `${url.pathname}${url.search} ${candidate.context}`;
    if (!feedPathPattern.test(searchable)) {
      continue;
    }

    discovered.add(normalizeExpansionUrl(url));
  }

  return Array.from(discovered).slice(0, maxFeedLinksPerPage);
}

async function extractSitemapCareerUrls(sourcePageUrl: string, fetchImpl?: typeof fetch) {
  const base = safeUrl(sourcePageUrl);
  if (!base) {
    return [];
  }

  const sitemapUrls = [
    new URL("/sitemap.xml", base.origin).toString(),
    new URL("/sitemap_index.xml", base.origin).toString(),
  ];
  const discovered = new Set<string>();

  for (const sitemapUrl of sitemapUrls) {
    const result = await safeFetchText(sitemapUrl, {
      fetchImpl,
      method: "GET",
      headers: {
        Accept: "application/xml,text/xml,text/plain",
      },
      cache: "no-store",
      timeoutMs: 4_000,
      retries: 0,
    });

    if (!result.ok || !result.data) {
      continue;
    }

    for (const url of extractSitemapUrls(result.data, base.origin)) {
      const parsed = safeUrl(url);
      if (!parsed || parsed.origin !== base.origin) {
        continue;
      }

      if (careerPathPattern.test(parsed.pathname) || feedPathPattern.test(parsed.pathname)) {
        discovered.add(normalizeExpansionUrl(parsed));
      }

      if (discovered.size >= maxSitemapUrlsPerPage) {
        return Array.from(discovered);
      }
    }
  }

  return Array.from(discovered);
}

function extractHtmlUrlCandidates(html: string, sourcePageUrl: string) {
  const candidates: Array<{ url: string; context: string }> = [];

  Array.from(html.matchAll(/<a\b([^>]*?)href=["']([^"']+)["']([^>]*)>([\s\S]*?)<\/a>/gi)).forEach(
    (match) => {
      const resolved = resolveUrlCandidate(match[2], sourcePageUrl);
      if (!resolved) {
        return;
      }

      candidates.push({
        url: resolved,
        context: `${match[1]} ${match[3]} ${stripTags(match[4])}`,
      });
    },
  );

  Array.from(html.matchAll(/<(?:link|script)\b([^>]*?)(?:href|src)=["']([^"']+)["'][^>]*>/gi)).forEach(
    (match) => {
      const resolved = resolveUrlCandidate(match[2], sourcePageUrl);
      if (!resolved) {
        return;
      }

      candidates.push({
        url: resolved,
        context: match[1],
      });
    },
  );

  Array.from(html.matchAll(/https?:\\?\/\\?\/[^"'<\s)]+/gi)).forEach((match) => {
    const resolved = resolveUrlCandidate(match[0].replace(/\\\//g, "/"), sourcePageUrl);
    if (resolved) {
      candidates.push({ url: resolved, context: "" });
    }
  });

  return candidates;
}

function extractSitemapUrls(xml: string, origin: string) {
  return Array.from(xml.matchAll(/<loc>\s*([^<]+?)\s*<\/loc>/gi))
    .map((match) => resolveUrlCandidate(decodeXmlEntities(match[1] ?? ""), origin))
    .filter((url): url is string => Boolean(url));
}

function scoreCompanyCareerUrl(url: URL, context: string) {
  const searchable = `${url.hostname} ${url.pathname} ${url.search} ${context}`.toLowerCase();
  let score = 0;

  if (careerPathPattern.test(url.pathname)) {
    score += 3;
  }

  if (/\b(job|jobs|career|careers|opening|openings|role|roles|position|positions|vacancy|vacancies)\b/.test(searchable)) {
    score += 2;
  }

  if (/\b(apply|department|team|location|remote|hybrid|full.?time)\b/.test(searchable)) {
    score += 1;
  }

  if (/\b(blog|press|news|privacy|terms|cookie|login|sign-in|signin|docs?|support|investors?)\b/.test(searchable)) {
    score -= 4;
  }

  return score;
}

function normalizeExpansionUrl(url: URL) {
  url.hash = "";
  return url.toString();
}

function stripTags(value: string) {
  return value.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function decodeXmlEntities(value: string) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'");
}

function safeUrl(value: string) {
  try {
    return new URL(value);
  } catch {
    return undefined;
  }
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
