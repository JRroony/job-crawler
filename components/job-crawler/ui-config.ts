"use client";

import type {
  ActiveCrawlerPlatform,
  CrawlerPlatform,
  CrawlMode,
  CrawlSourceResult,
  ExperienceClassification,
  ExperienceMatchMode,
  ProviderPlatform,
  SearchFilters,
} from "@/lib/types";
import {
  activeCrawlerPlatforms,
  crawlerPlatforms,
  resolveOperationalCrawlerPlatforms,
} from "@/lib/types";

export const selectablePlatformOptions: Array<{
  platform: ActiveCrawlerPlatform;
  label: string;
  detail: string;
  availability: string;
}> = [
  {
    platform: "greenhouse",
    label: "Greenhouse",
    detail: "Registry-backed board crawl with public discovery as a supplement",
    availability: "MVP focus",
  },
  {
    platform: "lever",
    label: "Lever",
    detail: "Available, but not the reliability focus of this MVP",
    availability: "Secondary",
  },
  {
    platform: "ashby",
    label: "Ashby",
    detail: "Available, but not the reliability focus of this MVP",
    availability: "Secondary",
  },
  {
    platform: "smartrecruiters",
    label: "SmartRecruiters",
    detail: "Available through public board discovery and detail-page normalization",
    availability: "Secondary",
  },
  {
    platform: "company_page",
    label: "Company page",
    detail: "Available for configured feeds and pages, but not the focus of this MVP",
    availability: "Secondary",
  },
];

export const passivePlatformOptions = [
  {
    platform: "workday",
    label: "Workday",
    tone: "disabled",
    detail: "Not implemented yet.",
  },
  {
    label: "LinkedIn",
    tone: "limited",
    detail: "Limited to compliant public paths only; not an active crawler target.",
  },
  {
    label: "Indeed",
    tone: "limited",
    detail: "Limited to compliant public paths only; not an active crawler target.",
  },
] as const;

export const experienceModeOptions: Array<{
  value: ExperienceMatchMode;
  label: string;
  description: string;
}> = [
  {
    value: "strict",
    label: "Strict",
    description: "Only keep explicit or very direct experience matches.",
  },
  {
    value: "balanced",
    label: "Balanced",
    description: "Use the current default mix of explicit and stronger inferred matches.",
  },
  {
    value: "broad",
    label: "Broad",
    description: "Include looser inferred matches and unspecified roles.",
  },
];

export const crawlModeOptions: Array<{
  value: CrawlMode;
  label: string;
  description: string;
  validationSummary: string;
}> = [
  {
    value: "fast",
    label: "Fast",
    description: "Show baseline matches first, then continue heavier recall work in the background.",
    validationSummary: "Deferred validation",
  },
  {
    value: "balanced",
    label: "Balanced",
    description: "Start supplemental recall sooner while still keeping the first visible batch responsive.",
    validationSummary: "Inline newest 5, defer the rest",
  },
  {
    value: "deep",
    label: "Deep",
    description: "Spend more budget on supplemental recall and finish with full inline validation.",
    validationSummary: "Inline validation for all saved jobs",
  },
];

export function resolveSelectedPlatforms(
  platforms: SearchFilters["platforms"],
) {
  return resolveOperationalCrawlerPlatforms(platforms);
}

export function resolveRequestedPlatforms(
  platforms: SearchFilters["platforms"],
) {
  return platforms ?? activeCrawlerPlatforms;
}

export function togglePlatformSelection(
  platforms: SearchFilters["platforms"],
  platform: ActiveCrawlerPlatform,
) {
  const current = new Set(resolveRequestedPlatforms(platforms));

  if (current.has(platform)) {
    if (current.size === 1) {
      return Array.from(current);
    }

    current.delete(platform);
  } else {
    current.add(platform);
  }

  const normalized = crawlerPlatforms.filter((candidate) => current.has(candidate));
  const matchesDefaultImplementedScope =
    normalized.length === activeCrawlerPlatforms.length &&
    activeCrawlerPlatforms.every((candidate, index) => normalized[index] === candidate);

  return matchesDefaultImplementedScope ? undefined : normalized;
}

export function resolveExperienceMode(
  value: SearchFilters["experienceMatchMode"],
): ExperienceMatchMode {
  return value ?? "balanced";
}

export function resolveCrawlMode(
  value: SearchFilters["crawlMode"],
): CrawlMode {
  return value ?? "fast";
}

export function describeValidationMode(
  crawlMode: SearchFilters["crawlMode"],
) {
  return crawlModeOptions.find((option) => option.value === resolveCrawlMode(crawlMode))
    ?.validationSummary ?? "Deferred validation";
}

export function labelForProviderPlatform(provider: ProviderPlatform) {
  return labelForCrawlerPlatform(provider);
}

export function labelForCrawlerPlatform(platform: CrawlerPlatform | ProviderPlatform) {
  const labels: Record<CrawlerPlatform | ProviderPlatform, string> = {
    greenhouse: "Greenhouse",
    lever: "Lever",
    ashby: "Ashby",
    smartrecruiters: "SmartRecruiters",
    company_page: "Company page",
    workday: "Workday",
    linkedin_limited: "LinkedIn",
    indeed_limited: "Indeed",
  };

  return labels[platform];
}

export function labelForProviderStatus(status: CrawlSourceResult["status"]) {
  if (status === "running") {
    return "Running";
  }

  if (status === "success") {
    return "Healthy";
  }

  if (status === "partial") {
    return "Degraded";
  }

  if (status === "aborted") {
    return "Stopped";
  }

  if (status === "unsupported") {
    return "Limited";
  }

  return "Failed";
}

export function sourceStatusTone(status: CrawlSourceResult["status"]) {
  if (status === "running") {
    return {
      badge: "bg-tide/10 text-tide border border-tide/20",
      card: "border-tide/20 bg-[linear-gradient(180deg,rgba(63,114,175,0.08),rgba(255,255,255,0.95))]",
    };
  }

  if (status === "success") {
    return {
      badge: "bg-pine/10 text-pine border border-pine/20",
      card: "border-pine/20 bg-[linear-gradient(180deg,rgba(22,101,52,0.05),rgba(255,255,255,0.95))]",
    };
  }

  if (status === "partial") {
    return {
      badge: "bg-amber-100 text-amber-900 border border-amber-200",
      card: "border-amber-200 bg-[linear-gradient(180deg,rgba(251,191,36,0.10),rgba(255,255,255,0.95))]",
    };
  }

  if (status === "unsupported") {
    return {
      badge: "bg-tide/10 text-tide border border-tide/20",
      card: "border-tide/20 bg-[linear-gradient(180deg,rgba(63,114,175,0.06),rgba(255,255,255,0.95))]",
    };
  }

  if (status === "aborted") {
    return {
      badge: "bg-slate-100 text-slate-800 border border-slate-200",
      card: "border-slate-200 bg-[linear-gradient(180deg,rgba(148,163,184,0.10),rgba(255,255,255,0.95))]",
    };
  }

  return {
    badge: "bg-red-100 text-red-800 border border-red-200",
    card: "border-red-200 bg-[linear-gradient(180deg,rgba(239,68,68,0.06),rgba(255,255,255,0.95))]",
  };
}

export function summarizeExperienceConfidence(
  classification?: ExperienceClassification,
) {
  if (!classification || classification.isUnspecified) {
    return {
      label: "Unspecified",
      detail: "No explicit or inferred level",
    };
  }

  const confidence =
    classification.confidence === "none"
      ? "Low confidence"
      : `${capitalize(classification.confidence)} confidence`;

  const source =
    classification.source === "structured_metadata"
      ? "from metadata"
      : classification.source === "description"
        ? "from description"
        : classification.source === "title"
          ? "from title"
          : classification.source === "page_fetch"
            ? "from page content"
            : "from stored data";

  return {
    label: confidence,
    detail: source,
  };
}

export function isPassiveLimitedProvider(provider: ProviderPlatform) {
  return provider === "linkedin_limited" || provider === "indeed_limited";
}

function capitalize(value: string) {
  return value ? `${value[0].toUpperCase()}${value.slice(1)}` : value;
}
