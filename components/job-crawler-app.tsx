"use client";

import { useMemo, useState } from "react";

import { RecentSearchesPanel } from "@/components/job-crawler/recent-searches-panel";
import { SearchControlsPanel } from "@/components/job-crawler/search-controls-panel";
import { SourceCoveragePanel } from "@/components/job-crawler/source-coverage-panel";
import {
  LoadingPanel,
  MessageBanner,
  NoticeBanner,
  StatePanel,
} from "@/components/job-crawler/status-panels";
import {
  describeValidationMode,
  isPassiveLimitedProvider,
  labelForCrawlerPlatform,
  resolveCrawlMode,
  resolveRequestedPlatforms,
  resolveSelectedPlatforms,
} from "@/components/job-crawler/ui-config";
import { ResultsTable } from "@/components/results-table";
import type {
  CrawlDiagnostics,
  CrawlResponse,
  SearchDocument,
  SearchFilters,
} from "@/lib/types";
import {
  activeCrawlerPlatforms,
  crawlModes,
  crawlerPlatforms,
  experienceLevels,
  experienceMatchModes,
  normalizeCrawlerPlatforms,
  normalizeExperienceLevels,
  normalizeOptionalSearchString,
  sanitizeSearchFiltersInput,
  searchFiltersSchema,
} from "@/lib/types";
import { labelForExperience } from "@/lib/utils";

type JobCrawlerAppProps = {
  initialSearches: SearchDocument[];
  initialError?: string;
};

export type ViewState = "idle" | "loading" | "success" | "empty" | "partial" | "error";

type ZeroResultState = {
  title: string;
  description: string;
  highlights?: string[];
};

type AppErrorKind = "initial_load" | "validation" | "runtime";

type ValidationErrorDetails = {
  fieldErrors?: Record<string, string[] | undefined>;
  formErrors?: string[];
};

type ValidationErrorPayload = {
  details?: unknown;
  readableErrors?: unknown;
};

type BlockingErrorState = {
  title: string;
  description: string;
  actionLabel?: string;
  actionType?: "reload" | "retry";
};

type SearchPayloadResult =
  | {
      ok: true;
      payload: Record<string, unknown>;
    }
  | {
      ok: false;
      message: string;
    };

type ResultNotice = {
  title: string;
  description: string;
  tone: "amber" | "tide";
  highlights?: string[];
};

const initialFilters: SearchFilters = {
  title: "",
  country: "",
  state: "",
  city: "",
  experienceMatchMode: "balanced",
  crawlMode: "fast",
};

export function JobCrawlerApp({
  initialSearches,
  initialError,
}: JobCrawlerAppProps) {
  const [filters, setFilters] = useState<SearchFilters>(initialFilters);
  const [recentSearches, setRecentSearches] = useState<SearchDocument[]>(() =>
    initialSearches.map(normalizeSearchDocumentForClient),
  );
  const [activeResult, setActiveResult] = useState<CrawlResponse | null>(null);
  const [viewState, setViewState] = useState<ViewState>(initialError ? "error" : "idle");
  const [message, setMessage] = useState(initialError ?? "");
  const [errorKind, setErrorKind] = useState<AppErrorKind | null>(
    initialError ? "initial_load" : null,
  );
  const [revalidatingIds, setRevalidatingIds] = useState<string[]>([]);

  async function submitSearch(nextFilters: SearchFilters) {
    const payloadResult = buildSearchRequestPayload(nextFilters);

    if (!payloadResult.ok) {
      setViewState("error");
      setErrorKind("validation");
      setMessage(payloadResult.message);
      return;
    }

    setViewState("loading");
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch("/api/searches", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payloadResult.payload),
      });

      const payload = (await response.json()) as CrawlResponse & {
        error?: string;
        details?: unknown;
        readableErrors?: unknown;
      };
      if (!response.ok) {
        if (response.status === 400) {
          throw createClassifiedClientError(
            "validation",
            buildValidationErrorMessage(
              {
                details: payload.details,
                readableErrors: payload.readableErrors,
              },
              payload.error ?? "Invalid search filters.",
            ),
          );
        }

        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The crawl request failed.",
        );
      }

      applyLoadedResult(payload);
      setRecentSearches((current) =>
        dedupeSearches([normalizeSearchDocumentForClient(payload.search), ...current]),
      );
    } catch (error) {
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The crawl request failed.");
    }
  }

  async function rerunActiveSearch(searchId?: string) {
    const id = searchId ?? activeResult?.search._id;
    if (!id) {
      await submitSearch(filters);
      return;
    }

    setViewState("loading");
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch(`/api/searches/${id}/rerun`, {
        method: "POST",
      });
      const payload = (await response.json()) as CrawlResponse & { error?: string };
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The rerun request failed.",
        );
      }

      applyLoadedResult(payload);
      setRecentSearches((current) =>
        dedupeSearches([normalizeSearchDocumentForClient(payload.search), ...current]),
      );
    } catch (error) {
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The rerun request failed.");
    }
  }

  async function loadSearch(searchId: string) {
    setViewState("loading");
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch(`/api/searches/${searchId}`);
      const payload = (await response.json()) as CrawlResponse & { error?: string };
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The search could not be loaded.",
        );
      }

      applyLoadedResult(payload);
    } catch (error) {
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The search could not be loaded.");
    }
  }

  async function revalidateSingleJob(jobId: string) {
    setRevalidatingIds((current) => [...current, jobId]);

    try {
      const response = await fetch(`/api/jobs/${jobId}/revalidate`, {
        method: "POST",
      });
      const payload = (await response.json()) as { job?: CrawlResponse["jobs"][number]; error?: string };
      if (!response.ok || !payload.job) {
        throw new Error(payload.error ?? "The job could not be revalidated.");
      }

      setActiveResult((current) =>
        current
          ? {
              ...current,
              jobs: current.jobs
                .map((job) => (job._id === payload.job?._id ? payload.job : job))
                .sort(jobComparator),
            }
          : current,
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "The job could not be revalidated.");
    } finally {
      setRevalidatingIds((current) => current.filter((id) => id !== jobId));
    }
  }

  function applyLoadedResult(payload: CrawlResponse) {
    const normalizedPayload = normalizeCrawlResponseForClient(payload);

    setActiveResult(normalizedPayload);
    setFilters(normalizedPayload.search.filters);
    setViewState(resolveViewState(normalizedPayload));
    setErrorKind(null);
  }

  const visibleSourceResults = useMemo(
    () =>
      activeResult
        ? filterOperationalSourceResults(activeResult.sourceResults)
        : [],
    [activeResult],
  );
  const selectedFilters = buildFilterBadges(filters);
  const resultNotice = activeResult ? describeResultNotice(activeResult) : null;
  const zeroResultState =
    activeResult && activeResult.jobs.length === 0
      ? describeZeroResultState(activeResult)
      : null;
  const blockingErrorState =
    viewState === "error" && !activeResult
      ? describeBlockingErrorState(errorKind, message)
      : null;

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f5f2eb_0%,#eef3f8_100%)] text-ink">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <div className="grid gap-6 xl:grid-cols-[1.4fr_0.6fr]">
          <SearchControlsPanel
            filters={filters}
            selectedFilters={selectedFilters}
            isLoading={viewState === "loading"}
            setFilters={setFilters}
            onSubmit={(event) => {
              event.preventDefault();
              void submitSearch(filters);
            }}
            onReset={() => setFilters(initialFilters)}
          />

          <div className="space-y-6">
            {message && !blockingErrorState ? (
              <MessageBanner message={message} />
            ) : null}

            <RecentSearchesPanel
              searches={recentSearches}
              activeSearchId={activeResult?.search._id}
              onLoad={(searchId) => void loadSearch(searchId)}
              onRerun={(searchId) => void rerunActiveSearch(searchId)}
              describeSearchMeta={describeSearchMeta}
            />
          </div>
        </div>

        <div className="mt-8 space-y-6">
          {resultNotice ? (
            <NoticeBanner
              title={resultNotice.title}
              description={resultNotice.description}
              tone={resultNotice.tone}
              highlights={resultNotice.highlights}
            />
          ) : null}

          {visibleSourceResults.length > 0 ? (
            <SourceCoveragePanel sourceResults={visibleSourceResults} />
          ) : null}

          {viewState === "loading" ? (
            <LoadingPanel />
          ) : null}

          {viewState === "idle" ? (
            <StatePanel
              title="Start with a clear job target"
              description="Choose the role, location scope, experience policy, active platforms, and crawl mode before starting the crawl."
              tone="neutral"
            />
          ) : null}

          {viewState === "error" && !activeResult ? (
            <StatePanel
              title={blockingErrorState?.title ?? "The crawl could not complete"}
              description={
                blockingErrorState?.description ??
                "The crawl could not complete. Please retry."
              }
              actionLabel={blockingErrorState?.actionLabel}
              onAction={
                blockingErrorState?.actionType === "reload"
                  ? () => window.location.reload()
                  : blockingErrorState?.actionType === "retry"
                    ? () => void rerunActiveSearch()
                    : undefined
              }
              tone="red"
            />
          ) : null}

          {zeroResultState && viewState !== "loading" ? (
            <StatePanel
              title={zeroResultState.title}
              description={zeroResultState.description}
              highlights={zeroResultState.highlights}
              actionLabel="Rerun crawl"
              onAction={() => void rerunActiveSearch()}
              tone="amber"
            />
          ) : null}

          {activeResult && activeResult.jobs.length > 0 ? (
            <ResultsTable
              jobs={activeResult.jobs}
              onRevalidate={revalidateSingleJob}
              revalidatingIds={revalidatingIds}
            />
          ) : null}
        </div>
      </div>
    </main>
  );
}

export function resolveViewState(result: CrawlResponse): ViewState {
  if (result.crawlRun.status === "failed") {
    return "error";
  }

  if (result.crawlRun.status === "partial") {
    return "partial";
  }

  if (result.jobs.length === 0) {
    return "empty";
  }

  return "success";
}

export function describeZeroResultState(result: CrawlResponse): ZeroResultState {
  const diagnostics = result.diagnostics;

  if (diagnostics.discoveredSources === 0) {
    return {
      title: "No public sources were discovered",
      description:
        "The crawler did not find any configured public boards or company pages for the selected platform scope.",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

  if (diagnostics.providerFailures > 0 && result.crawlRun.status === "failed") {
    return {
      title: "Providers failed before jobs could be saved",
      description:
        "The run encountered provider-side failures and never reached a usable saved result set.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (diagnostics.providerFailures > 0) {
    return {
      title: "Provider issues left the run with no saved jobs",
      description:
        "One or more providers failed, and the remaining source coverage did not produce any saved jobs. Retry the crawl or broaden the filters.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (
    diagnostics.excludedByTitle > 0 ||
    diagnostics.excludedByLocation > 0 ||
    diagnostics.excludedByExperience > 0
  ) {
    return {
      title: "Filters were too narrow for the fetched jobs",
      description:
        "The crawler found public jobs, but the current title, location, or experience policy removed them before save.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (diagnostics.dedupedOut > 0) {
    return {
      title: "Matched jobs were merged away during dedupe",
      description:
        "The crawl found overlapping matches, but dedupe collapsed them before any new jobs were saved.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  return {
    title: "The crawl completed but nothing was saved",
    description:
      "The run finished without provider failure, but no deduped jobs remained to persist for this search.",
    highlights: buildOperationalHighlights(diagnostics),
  };
}

function describeResultNotice(result: CrawlResponse): ResultNotice | null {
  const diagnostics = result.diagnostics;

  if (result.jobs.length > 0 && diagnostics.validationDeferred > 0) {
    return {
      title: "Validation is still deferred for some saved jobs",
      description:
        "The saved result set is ready to review, but not every link has been checked inline for redirects or stale pages yet.",
      tone: "tide",
      highlights: [
        `${diagnostics.validationDeferred} saved job${diagnostics.validationDeferred === 1 ? "" : "s"} still rely on deferred validation.`,
        `This run used ${describeValidationMode(result.search.filters.crawlMode).toLowerCase()}.`,
      ],
    };
  }

  if (result.crawlRun.status === "partial" && result.jobs.length > 0) {
    return {
      title: "Partial result set: some jobs were saved, but coverage was degraded",
      description:
        "Review the provider cards and diagnostics before rerunning so you can tell whether the gap came from provider issues or tighter filters.",
      tone: "amber",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  return null;
}

function buildOperationalHighlights(
  diagnostics: CrawlDiagnostics,
  options: { includeFilters?: boolean } = {},
) {
  const highlights: string[] = [];

  highlights.push(
    `Discovered ${diagnostics.discoveredSources} source${diagnostics.discoveredSources === 1 ? "" : "s"} and crawled ${diagnostics.crawledSources}.`,
  );

  if (diagnostics.providerFailures > 0) {
    highlights.push(
      `${diagnostics.providerFailures} provider path${diagnostics.providerFailures === 1 ? "" : "s"} failed or returned degraded coverage.`,
    );
  }

  if (options.includeFilters !== false && diagnostics.excludedByTitle > 0) {
    highlights.push(
      `${diagnostics.excludedByTitle} fetched job${diagnostics.excludedByTitle === 1 ? "" : "s"} were excluded by title mismatch.`,
    );
  }

  if (options.includeFilters !== false && diagnostics.excludedByLocation > 0) {
    highlights.push(
      `${diagnostics.excludedByLocation} fetched job${diagnostics.excludedByLocation === 1 ? "" : "s"} were excluded by location mismatch.`,
    );
  }

  if (options.includeFilters !== false && diagnostics.excludedByExperience > 0) {
    highlights.push(
      `${diagnostics.excludedByExperience} title/location match${diagnostics.excludedByExperience === 1 ? "" : "es"} were excluded by experience policy.`,
    );
  }

  if (diagnostics.dedupedOut > 0) {
    highlights.push(
      `${diagnostics.dedupedOut} match${diagnostics.dedupedOut === 1 ? "" : "es"} were merged away during dedupe.`,
    );
  }

  return highlights.slice(0, 4);
}

function toUiFilters(filters: SearchFilters): SearchFilters {
  return normalizeSearchFiltersForClient(filters);
}

export function normalizeSearchFiltersForClient(rawFilters: unknown): SearchFilters {
  const filters = sanitizeSearchFiltersInput(rawFilters);
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) {
    return {
      ...initialFilters,
    };
  }

  const candidate = filters as Record<string, unknown>;
  const normalizedPlatforms = normalizeCrawlerPlatforms(
    normalizeEnumArray(
      Array.isArray(candidate.platforms)
        ? candidate.platforms.filter((value): value is string => typeof value === "string")
        : undefined,
      crawlerPlatforms,
    ),
  );
  const normalizedExperienceLevels = normalizeExperienceLevels(
    normalizeEnumArray(
      [
        ...(Array.isArray(candidate.experienceLevels)
          ? candidate.experienceLevels.filter((value): value is string => typeof value === "string")
          : []),
        ...(typeof candidate.experienceLevel === "string" ? [candidate.experienceLevel] : []),
      ],
      experienceLevels,
    ),
  );

  return {
    title: typeof candidate.title === "string" ? candidate.title.trim() : "",
    country: toUiOptionalString(candidate.country),
    state: toUiOptionalString(candidate.state),
    city: toUiOptionalString(candidate.city),
    ...(normalizedPlatforms
      ? {
          platforms: normalizedPlatforms,
        }
      : {}),
    ...(normalizedExperienceLevels
      ? {
          experienceLevels: normalizedExperienceLevels,
        }
      : {}),
    experienceMatchMode:
      normalizeEnumValue(
        typeof candidate.experienceMatchMode === "string"
          ? candidate.experienceMatchMode
          : undefined,
        experienceMatchModes,
      ) ?? "balanced",
    crawlMode:
      normalizeEnumValue(
        typeof candidate.crawlMode === "string" ? candidate.crawlMode : undefined,
        crawlModes,
      ) ?? "fast",
    ...(candidate.includeUnspecifiedExperience === true
      ? {
          includeUnspecifiedExperience: true,
        }
      : {}),
  };
}

function normalizeSearchDocumentForClient(search: SearchDocument): SearchDocument {
  return {
    ...search,
    filters: toUiFilters(search.filters),
  };
}

function normalizeCrawlResponseForClient(payload: CrawlResponse): CrawlResponse {
  return {
    ...payload,
    search: normalizeSearchDocumentForClient(payload.search),
  };
}

function buildFilterBadges(filters: SearchFilters) {
  const badges: string[] = [];

  if (filters.title.trim()) {
    badges.push(`Role: ${filters.title.trim()}`);
  }

  const location = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
  if (location) {
    badges.push(`Location: ${location}`);
  }

  const levels = describeExperienceLevels(filters.experienceLevels);
  if (levels) {
    badges.push(`Levels: ${levels}`);
  }

  badges.push(`Experience mode: ${filters.experienceMatchMode ?? "Balanced"}`);

  if (filters.includeUnspecifiedExperience || filters.experienceMatchMode === "broad") {
    badges.push("Unspecified levels included");
  }

  badges.push(`Crawl mode: ${resolveCrawlMode(filters.crawlMode)}`);

  badges.push(`Platforms: ${describePlatformScope(filters.platforms)}`);

  return badges;
}

function describeSearchMeta(filters: SearchFilters) {
  const parts: string[] = [];
  const location = [filters.city, filters.state, filters.country].filter(Boolean).join(", ");
  const levels = describeExperienceLevels(filters.experienceLevels);
  const requestedPlatforms = resolveRequestedPlatforms(filters.platforms);
  const selectedPlatforms = resolveSelectedPlatforms(filters.platforms);

  parts.push(location || "Any location");
  parts.push(levels || "Any level");
  parts.push(filters.experienceMatchMode ?? "balanced");
  parts.push(resolveCrawlMode(filters.crawlMode));

  if (filters.platforms) {
    if (selectedPlatforms.length === 0) {
      parts.push(
        `${requestedPlatforms.map((platform) => labelForCrawlerPlatform(platform)).join(", ")} disabled`,
      );
    } else if (selectedPlatforms.length < activeCrawlerPlatforms.length) {
      parts.push(`${selectedPlatforms.length} active platform${selectedPlatforms.length === 1 ? "" : "s"}`);
    }
  }

  return parts.join(" • ");
}

function describePlatformScope(platforms: SearchFilters["platforms"]) {
  if (!platforms) {
    return "All enabled paths";
  }

  const requestedPlatforms = resolveRequestedPlatforms(platforms);
  const labels = requestedPlatforms.map((platform) =>
    activeCrawlerPlatforms.includes(platform as (typeof activeCrawlerPlatforms)[number])
      ? labelForCrawlerPlatform(platform)
      : `${labelForCrawlerPlatform(platform)} (disabled)`,
  );

  return labels.join(", ");
}

function describeExperienceLevels(
  levels: SearchFilters["experienceLevels"],
  format: "default" | "lowercase" = "default",
) {
  if (!levels?.length) {
    return undefined;
  }

  const labels = levels.map((level) => {
    const label = labelForExperience(level);
    return format === "lowercase" ? label.toLowerCase() : label;
  });

  if (labels.length === 1) {
    return labels[0];
  }

  if (labels.length === 2) {
    return `${labels[0]} or ${labels[1]}`;
  }

  return `${labels.slice(0, -1).join(", ")}, or ${labels[labels.length - 1]}`;
}

function filterOperationalSourceResults(sourceResults: CrawlResponse["sourceResults"]) {
  return sourceResults.filter((sourceResult) => !isPassiveLimitedProvider(sourceResult.provider));
}

function dedupeSearches(searches: SearchDocument[]) {
  const map = new Map<string, SearchDocument>();
  searches.forEach((search) => {
    map.set(search._id, search);
  });
  return Array.from(map.values());
}

function jobComparator(left: CrawlResponse["jobs"][number], right: CrawlResponse["jobs"][number]) {
  if (left.postedAt && right.postedAt && left.postedAt !== right.postedAt) {
    return left.postedAt > right.postedAt ? -1 : 1;
  }

  if (left.postedAt && !right.postedAt) {
    return -1;
  }

  if (!left.postedAt && right.postedAt) {
    return 1;
  }

  const sourceComparison = left.sourcePlatform.localeCompare(right.sourcePlatform);
  if (sourceComparison !== 0) {
    return sourceComparison;
  }

  return left.title.localeCompare(right.title);
}

function buildValidationErrorMessage(
  validationPayload: ValidationErrorPayload | unknown,
  fallbackMessage: string,
) {
  const readableErrors = extractReadableErrors(validationPayload);
  if (readableErrors.length > 0) {
    return `${fallbackMessage.replace(/\.$/, "")} — ${readableErrors.join("; ")}`;
  }

  const details = extractValidationErrorDetails(validationPayload);
  if (!details) {
    return fallbackMessage;
  }

  const fieldMessages = Object.entries(details.fieldErrors ?? {})
    .flatMap(([field, messages]) =>
      (messages ?? [])
        .filter((message) => message.trim().length > 0)
        .map((message) => `${field}: ${message}`),
    );

  const formMessages = (details.formErrors ?? []).filter(
    (message) => message.trim().length > 0,
  );

  const messages = [...fieldMessages, ...formMessages];
  return messages.length > 0
    ? `${fallbackMessage.replace(/\.$/, "")} — ${messages.join("; ")}`
    : fallbackMessage;
}

function extractValidationErrorDetails(
  value: ValidationErrorPayload | unknown,
): ValidationErrorDetails | null {
  if (isValidationErrorPayload(value) && isValidationErrorDetails(value.details)) {
    return value.details;
  }

  return isValidationErrorDetails(value) ? value : null;
}

function extractReadableErrors(value: ValidationErrorPayload | unknown) {
  if (!isValidationErrorPayload(value) || !isStringArray(value.readableErrors)) {
    return [];
  }

  return value.readableErrors.filter((message) => message.trim().length > 0);
}

function isValidationErrorPayload(value: unknown): value is ValidationErrorPayload {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("readableErrors" in candidate && !isStringArray(candidate.readableErrors)) {
    return false;
  }

  if ("details" in candidate && !isValidationErrorDetails(candidate.details)) {
    return false;
  }

  return true;
}

function isValidationErrorDetails(value: unknown): value is ValidationErrorDetails {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("fieldErrors" in candidate && !isFieldErrorMap(candidate.fieldErrors)) {
    return false;
  }

  if ("formErrors" in candidate && !isStringArray(candidate.formErrors)) {
    return false;
  }

  return true;
}

function isFieldErrorMap(value: unknown): value is Record<string, string[] | undefined> {
  if (!value || typeof value !== "object") {
    return false;
  }

  return Object.values(value).every(
    (messages) => typeof messages === "undefined" || isStringArray(messages),
  );
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}

export function buildSearchRequestPayload(filters: SearchFilters): SearchPayloadResult {
  const normalizedFilters = normalizeSearchFiltersForClient(filters);
  const experienceMatchMode = normalizeEnumValue(
    normalizedFilters.experienceMatchMode,
    experienceMatchModes,
  );
  const candidate = {
    title: normalizedFilters.title.trim(),
    country: normalizeOptionalSearchString(normalizedFilters.country),
    state: normalizeOptionalSearchString(normalizedFilters.state),
    city: normalizeOptionalSearchString(normalizedFilters.city),
    platforms: normalizeCrawlerPlatforms(
      normalizeEnumArray(normalizedFilters.platforms, crawlerPlatforms),
    ),
    crawlMode: normalizeEnumValue(normalizedFilters.crawlMode, crawlModes),
    experienceLevels: normalizeExperienceLevels(
      normalizeEnumArray(normalizedFilters.experienceLevels, experienceLevels),
    ),
    experienceMatchMode,
    includeUnspecifiedExperience:
      experienceMatchMode === "broad"
        ? true
        : normalizedFilters.includeUnspecifiedExperience === true
          ? true
          : undefined,
  };

  const clientValidationMessage = validateSearchFiltersForClient(candidate);
  if (clientValidationMessage) {
    return {
      ok: false,
      message: clientValidationMessage,
    };
  }

  const parsed = searchFiltersSchema.safeParse(sanitizeSearchFiltersInput(candidate));
  if (!parsed.success) {
    return {
      ok: false,
      message: buildValidationErrorMessage(
        parsed.error.flatten(),
        "Invalid search filters.",
      ),
    };
  }

  return {
    ok: true,
    payload: parsed.data,
  };
}

function validateSearchFiltersForClient(filters: Pick<SearchFilters, "title">) {
  const title = filters.title.trim();

  if (!title) {
    return "Invalid search filters — title: Please enter a target title.";
  }

  if (title.length < 2) {
    return "Invalid search filters — title: Title must contain at least 2 characters.";
  }

  return null;
}

function toUiOptionalString(value: unknown) {
  const normalized = normalizeOptionalSearchString(value);
  return typeof normalized === "string" ? normalized : "";
}

function normalizeEnumValue<T extends string>(
  value: string | undefined,
  allowedValues: readonly T[],
) {
  return value && allowedValues.includes(value as T) ? (value as T) : undefined;
}

function normalizeEnumArray<T extends string>(
  values: readonly string[] | undefined,
  allowedValues: readonly T[],
) {
  if (!values?.length) {
    return undefined;
  }

  const normalized = allowedValues.filter((value) => values.includes(value));
  return normalized.length > 0 ? normalized : undefined;
}

type ClassifiedClientError = Error & {
  kind: Exclude<AppErrorKind, "initial_load">;
};

function createClassifiedClientError(
  kind: ClassifiedClientError["kind"],
  message: string,
) {
  const error = new Error(message) as ClassifiedClientError;
  error.kind = kind;
  return error;
}

function resolveErrorKind(error: unknown): Exclude<AppErrorKind, "initial_load"> {
  return isClassifiedClientError(error) ? error.kind : "runtime";
}

function isClassifiedClientError(error: unknown): error is ClassifiedClientError {
  return (
    error instanceof Error &&
    "kind" in error &&
    (error.kind === "validation" || error.kind === "runtime")
  );
}

function describeBlockingErrorState(
  errorKind: AppErrorKind | null,
  message: string,
): BlockingErrorState {
  if (errorKind === "validation") {
    return {
      title: "Search filters need attention",
      description:
        message || "One or more search filters are invalid. Update the form and try again.",
    };
  }

  if (errorKind === "initial_load") {
    return {
      title: "The crawler could not load",
      description:
        message || "Check that MongoDB is available, then reload the page and try again.",
      actionLabel: "Reload page",
      actionType: "reload",
    };
  }

  return {
    title: "The crawl could not complete",
    description:
      message || "The crawl request failed before results could be returned.",
    actionLabel: "Retry",
    actionType: "retry",
  };
}
