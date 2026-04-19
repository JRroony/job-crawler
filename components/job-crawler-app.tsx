"use client";

import { useMemo, useRef, useState } from "react";

import {
  BackgroundSupplementIndicator,
  LoadingPanel,
  MessageBanner,
  StatePanel,
} from "@/components/job-crawler/status-panels";
import {
  describeValidationMode,
  isPassiveLimitedProvider,
  labelForCrawlerPlatform,
  resolveCrawlMode,
  resolveRequestedPlatforms,
  resolveSelectedPlatforms,
  togglePlatformSelection,
} from "@/components/job-crawler/ui-config";
import { FilterBar } from "@/components/job-search/filter-bar";
import { DiagnosticsDrawer } from "@/components/job-search/diagnostics-drawer";
import {
  buildLocationInputValue,
  defaultClientResultFilters,
  filterJobsForDisplay,
  parseLocationInput,
} from "@/components/job-search/helpers";
import { SearchBar } from "@/components/job-search/search-bar";
import { buildResultsExportFilename, ResultsTable } from "@/components/results-table";
import { buildStableJobRenderIdentity } from "@/lib/job-identity";
import type {
  CrawlDeltaResponse,
  CrawlDiagnostics,
  CrawlResponse,
  ExperienceLevel,
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
import { formatRelativeMoment, labelForExperience } from "@/lib/utils";

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

type SearchRoutePayload = CrawlResponse & {
  queued?: boolean;
  aborted?: boolean;
  error?: string;
  details?: unknown;
  readableErrors?: unknown;
};

type SearchDeltaRoutePayload = CrawlDeltaResponse & {
  error?: string;
};

type ProgressiveSearchSnapshot = Pick<CrawlResponse, "crawlRun" | "jobs">;

const initialFilters: SearchFilters = {
  title: "",
  country: "",
  state: "",
  city: "",
  experienceMatchMode: "balanced",
  crawlMode: "fast",
};

const queuedSearchPollIntervalEmptyMs = 250;
const queuedSearchPollIntervalVisibleMs = 750;
const queuedSearchPollTimeoutMs = 90_000;

export function JobCrawlerApp({
  initialSearches,
  initialError,
}: JobCrawlerAppProps) {
  const [filters, setFilters] = useState<SearchFilters>(initialFilters);
  const [keywordInput, setKeywordInput] = useState(initialFilters.title);
  const [locationInput, setLocationInput] = useState(buildLocationInputValue(initialFilters));
  const [clientResultFilters, setClientResultFilters] = useState(defaultClientResultFilters);
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
  const pollSequenceRef = useRef(0);
  const activeDeliveryCursorRef = useRef(0);
  const activeIndexedDeliveryCursorRef = useRef(0);
  const clientRequestOwnerKeyRef = useRef(createClientRequestOwnerKey());
  const activeRequestControllerRef = useRef<AbortController | null>(null);

  function buildLiveFilters(baseFilters: SearchFilters = filters) {
    return normalizeSearchFiltersForClient({
      ...baseFilters,
      title: keywordInput,
      ...parseLocationInput(locationInput),
    });
  }

  function hydrateSearchForm(nextFilters: SearchFilters) {
    setFilters(nextFilters);
    setKeywordInput(nextFilters.title);
    setLocationInput(buildLocationInputValue(nextFilters));
  }

  function clearSearchForm() {
    hydrateSearchForm(initialFilters);
    setClientResultFilters(defaultClientResultFilters);
    setMessage("");
    setErrorKind(null);
  }

  function clearBrowseFilters() {
    setFilters((current) => ({
      ...current,
      platforms: undefined,
      experienceLevels: undefined,
    }));
    setClientResultFilters(defaultClientResultFilters);
  }

  function refreshRecentSearch(search: SearchDocument) {
    setRecentSearches((current) =>
      dedupeSearches([normalizeSearchDocumentForClient(search), ...current]),
    );
  }

  function applyQueuedResult(payload: CrawlResponse) {
    const normalizedPayload = normalizeCrawlResponseForClient(payload);

    setActiveResult(normalizedPayload);
    activeDeliveryCursorRef.current = normalizedPayload.delivery?.cursor ?? 0;
    activeIndexedDeliveryCursorRef.current = normalizedPayload.delivery?.indexedCursor ?? 0;
    hydrateSearchForm(normalizedPayload.search.filters);
    setViewState(resolveViewState(normalizedPayload));
    setErrorKind(null);
  }

  async function pollSearchUntilSettled(
    searchId: string,
    options: {
      updatedAfter?: string;
      pollToken: number;
      signal?: AbortSignal;
    },
  ) {
    const deadline = Date.now() + queuedSearchPollTimeoutMs;

    while (options.pollToken === pollSequenceRef.current && Date.now() < deadline) {
      await delay(
        resolveQueuedSearchPollIntervalMs(
          Math.max(activeDeliveryCursorRef.current, activeIndexedDeliveryCursorRef.current),
        ),
      );
      if (options.pollToken !== pollSequenceRef.current) {
        return;
      }

      const response = await fetch(
        `/api/searches/${searchId}?mode=delta&after=${activeDeliveryCursorRef.current}&indexedAfter=${activeIndexedDeliveryCursorRef.current}`,
        {
        signal: options.signal,
        },
      );
      const payload = (await response.json()) as SearchDeltaRoutePayload;
      if (!isLatestClientRequest(options.pollToken, pollSequenceRef.current)) {
        return;
      }
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The search could not be loaded.",
        );
      }

      applyDeltaResult(payload);
      refreshRecentSearch(payload.search);

      if (payload.crawlRun.status === "running") {
        continue;
      }

      const finalResponse = await fetch(`/api/searches/${searchId}`, {
        signal: options.signal,
      });
      const finalPayload = (await finalResponse.json()) as SearchRoutePayload;
      if (!isLatestClientRequest(options.pollToken, pollSequenceRef.current)) {
        return;
      }
      if (!finalResponse.ok) {
        throw createClassifiedClientError(
          "runtime",
          finalPayload.error ?? "The search could not be loaded.",
        );
      }

      if (options.updatedAfter && finalPayload.search.updatedAt <= options.updatedAfter) {
        applyLoadedResult(finalPayload);
        refreshRecentSearch(finalPayload.search);
        return;
      }

      applyLoadedResult(finalPayload);
      refreshRecentSearch(finalPayload.search);
      return;
    }

    if (options.pollToken !== pollSequenceRef.current) {
      return;
    }

    setMessage(
      "The crawl is still running in the background. Leave this page open or reopen the search from Recent searches in a moment.",
    );
    setErrorKind(null);
  }

  async function submitSearch(nextFilters: SearchFilters) {
    const pollToken = ++pollSequenceRef.current;
    const requestController = replaceActiveRequestController();
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
          "x-job-crawler-client-id": clientRequestOwnerKeyRef.current,
        },
        signal: requestController.signal,
        body: JSON.stringify(payloadResult.payload),
      });

      const payload = (await response.json()) as SearchRoutePayload;
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current)) {
        return;
      }
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

      if (payload.queued) {
        refreshRecentSearch(payload.search);
        if (shouldApplyQueuedResultImmediately(payload)) {
          applyLoadedResult(payload);
        } else {
          applyQueuedResult(payload);
        }
        if (payload.crawlRun.status === "running") {
          void pollSearchUntilSettled(payload.search._id, {
            updatedAfter: payload.search.updatedAt,
            pollToken,
            signal: requestController.signal,
          });
        }
        return;
      }

      applyLoadedResult(payload);
      refreshRecentSearch(payload.search);
    } catch (error) {
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current) || isAbortLikeClientError(error)) {
        return;
      }
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The crawl request failed.");
    }
  }

  async function rerunActiveSearch(searchId?: string) {
    const id = searchId ?? activeResult?.search._id;
    if (!id) {
      const nextFilters = buildLiveFilters();
      hydrateSearchForm(nextFilters);
      await submitSearch(nextFilters);
      return;
    }

    const pollToken = ++pollSequenceRef.current;
    const requestController = replaceActiveRequestController();
    setViewState("loading");
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch(`/api/searches/${id}/rerun`, {
        method: "POST",
        headers: {
          "x-job-crawler-client-id": clientRequestOwnerKeyRef.current,
        },
        signal: requestController.signal,
      });
      const payload = (await response.json()) as SearchRoutePayload;
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current)) {
        return;
      }
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The rerun request failed.",
        );
      }

      if (payload.queued) {
        refreshRecentSearch(payload.search);
        if (!shouldApplyQueuedResultImmediately(payload) && (payload.crawlRun.status === "running" || !activeResult)) {
          applyQueuedResult(payload);
        } else {
          applyLoadedResult(payload);
        }
        if (payload.crawlRun.status === "running") {
          await pollSearchUntilSettled(payload.search._id, {
            updatedAfter: payload.search.updatedAt,
            pollToken,
            signal: requestController.signal,
          });
        }
        return;
      }

      applyLoadedResult(payload);
      refreshRecentSearch(payload.search);
    } catch (error) {
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current) || isAbortLikeClientError(error)) {
        return;
      }
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The rerun request failed.");
    }
  }

  async function loadSearch(searchId: string) {
    const pollToken = ++pollSequenceRef.current;
    const requestController = replaceActiveRequestController();
    setViewState("loading");
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch(`/api/searches/${searchId}`, {
        signal: requestController.signal,
      });
      const payload = (await response.json()) as CrawlResponse & { error?: string };
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current)) {
        return;
      }
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The search could not be loaded.",
        );
      }

      applyLoadedResult(payload);
      if (payload.crawlRun.status === "running") {
        await pollSearchUntilSettled(searchId, {
          updatedAfter: payload.search.updatedAt,
          pollToken,
          signal: requestController.signal,
        });
      }
    } catch (error) {
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current) || isAbortLikeClientError(error)) {
        return;
      }
      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The search could not be loaded.");
    }
  }

  async function stopActiveSearch(searchId?: string) {
    const id = searchId ?? activeResult?.search._id;
    if (!id) {
      return;
    }

    const pollToken = ++pollSequenceRef.current;
    const requestController = replaceActiveRequestController();
    setMessage("");
    setErrorKind(null);

    try {
      const response = await fetch(`/api/searches/${id}`, {
        method: "DELETE",
        signal: requestController.signal,
      });
      const payload = (await response.json()) as SearchRoutePayload;
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current)) {
        return;
      }

      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "The crawl could not be stopped.",
        );
      }

      applyLoadedResult(payload);
      refreshRecentSearch(payload.search);
      setMessage(
        payload.aborted
          ? "Background work was stopped. Your visible results are preserved."
          : "That search already finished refining results.",
      );
    } catch (error) {
      if (!isLatestClientRequest(pollToken, pollSequenceRef.current) || isAbortLikeClientError(error)) {
        return;
      }

      setViewState("error");
      setErrorKind(resolveErrorKind(error));
      setMessage(error instanceof Error ? error.message : "The crawl could not be stopped.");
    }
  }

  function replaceActiveRequestController() {
    activeRequestControllerRef.current?.abort();
    const controller = new AbortController();
    activeRequestControllerRef.current = controller;
    return controller;
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
    activeDeliveryCursorRef.current = normalizedPayload.delivery?.cursor ?? 0;
    activeIndexedDeliveryCursorRef.current = normalizedPayload.delivery?.indexedCursor ?? 0;
    hydrateSearchForm(normalizedPayload.search.filters);
    setViewState(resolveViewState(normalizedPayload));
    setErrorKind(null);
  }

  function applyDeltaResult(payload: CrawlDeltaResponse) {
    const normalizedPayload = normalizeCrawlDeltaResponseForClient(payload);
    let mergedResult: CrawlResponse | null = null;

    activeDeliveryCursorRef.current = normalizedPayload.delivery.cursor;
    activeIndexedDeliveryCursorRef.current = normalizedPayload.delivery.indexedCursor ?? 0;
    setActiveResult((current) => {
      mergedResult = mergeCrawlDeltaIntoResult(current, normalizedPayload);
      return mergedResult;
    });
    hydrateSearchForm(normalizedPayload.search.filters);
    setViewState(
      mergedResult ? resolveViewState(mergedResult) : "loading",
    );
    setErrorKind(null);
  }

  async function startNewSearch() {
    const runningSearchId =
      activeResult?.crawlRun.status === "running" ? activeResult.search._id : null;

    activeRequestControllerRef.current?.abort();
    activeRequestControllerRef.current = null;
    setActiveResult(null);
    setViewState("idle");
    setMessage("");
    setErrorKind(null);
    pollSequenceRef.current += 1;
    activeDeliveryCursorRef.current = 0;
    activeIndexedDeliveryCursorRef.current = 0;
    clearSearchForm();

    if (runningSearchId) {
      void fetch(`/api/searches/${runningSearchId}`, {
        method: "DELETE",
      }).catch(() => {
        // Best-effort: keep the new search flow unblocked even if cancellation races.
      });
    }
  }

  const visibleJobs = useMemo(
    () =>
      activeResult
        ? filterJobsForDisplay(activeResult.jobs, filters, clientResultFilters)
        : [],
    [activeResult, clientResultFilters, filters],
  );
  const isBlockingSearchLoad = shouldShowBlockingSearchLoad(viewState, activeResult);
  const isRefreshingVisibleSession = isSupplementingSearchSession(activeResult);
  const zeroResultState =
    activeResult && activeResult.jobs.length === 0
      ? describeZeroResultState(activeResult)
      : null;
  const blockingErrorState =
    viewState === "error" && !activeResult
      ? describeBlockingErrorState(errorKind, message)
      : null;
  const resultNotice = activeResult ? describeResultNotice(activeResult) : null;
  const resultsLocation = activeResult
    ? buildLocationInputValue(activeResult.search.filters) || "All locations"
    : buildLocationInputValue(buildLiveFilters()) || "All locations";
  const activeSearchBadges = activeResult
    ? buildFilterBadges(activeResult.search.filters).slice(0, 4)
    : [];

  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,#ffffff_0%,#f4f7fb_40%,#edf2f7_100%)] text-ink">
      <div className="mx-auto max-w-[1360px] px-4 py-5 sm:px-6 lg:px-8">
        <div className="space-y-3">
          <SearchBar
            keyword={keywordInput}
            location={locationInput}
            isLoading={isBlockingSearchLoad}
            onKeywordChange={setKeywordInput}
            onLocationChange={setLocationInput}
            onSubmit={(event) => {
              event.preventDefault();
              const nextFilters = buildLiveFilters();
              hydrateSearchForm(nextFilters);
              void submitSearch(nextFilters);
            }}
            onReset={clearSearchForm}
          />

          <FilterBar
            filters={filters}
            resultFilters={clientResultFilters}
            onTogglePlatform={(platform) =>
              setFilters((current) => ({
                ...current,
                platforms: togglePlatformSelection(current.platforms, platform),
              }))
            }
            onToggleExperience={(level) =>
              setFilters((current) => ({
                ...current,
                experienceLevels: toggleExperienceLevel(current.experienceLevels, level),
              }))
            }
            onToggleRemoteOnly={() =>
              setClientResultFilters((current) => ({
                ...current,
                remoteOnly: !current.remoteOnly,
              }))
            }
            onToggleVisaFriendlyOnly={() =>
              setClientResultFilters((current) => ({
                ...current,
                visaFriendlyOnly: !current.visaFriendlyOnly,
              }))
            }
            onPostedDateChange={(postedDate) =>
              setClientResultFilters((current) => ({
                ...current,
                postedDate,
              }))
            }
            onClear={clearBrowseFilters}
          />

          {message && !blockingErrorState ? (
            <MessageBanner
              message={message}
              tone={errorKind ? "error" : "info"}
            />
          ) : null}
        </div>

        <div className="mt-4 space-y-4">
          {isBlockingSearchLoad ? (
            <LoadingPanel
              stage={activeResult?.crawlRun.stage}
              foundCount={activeResult?.jobs.length}
              fetchedCount={activeResult?.crawlRun.totalFetchedJobs}
              matchedCount={activeResult?.crawlRun.totalMatchedJobs}
              providerSummary={activeResult?.crawlRun.providerSummary}
              stopButton={
                activeResult?.crawlRun.status === "running" && activeResult?.search._id ? (
                  <button
                    type="button"
                    onClick={() => void stopActiveSearch(activeResult.search._id)}
                    className="inline-flex items-center justify-center rounded-full border border-ink/15 bg-white px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-red-300 hover:bg-red-50"
                  >
                    Stop
                  </button>
                ) : undefined
              }
              actionButton={
                <button
                  type="button"
                  onClick={() => void startNewSearch()}
                  className="inline-flex items-center justify-center rounded-full border border-ink/15 bg-white px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-sand hover:bg-sand/20"
                >
                  New Search
                </button>
              }
            />
          ) : null}

          {isRefreshingVisibleSession && activeResult ? (
            <div className="flex items-center justify-between gap-3">
              <BackgroundSupplementIndicator
                stage={activeResult.crawlRun.stage}
                foundCount={activeResult.jobs.length}
                onStop={() => void stopActiveSearch(activeResult.search._id)}
              />
              <button
                type="button"
                onClick={() => void startNewSearch()}
                className="inline-flex items-center justify-center rounded-full border border-ink/15 bg-white px-3 py-1.5 text-xs font-semibold text-ink transition hover:border-sand hover:bg-sand/20"
              >
                New Search
              </button>
            </div>
          ) : null}

          {viewState === "idle" ? (
            <StatePanel
              title="Search by role and location"
              description="Start with a role and location to load saved matches quickly, then let background refresh improve coverage without taking over the page."
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
            <>
              <section className="rounded-[20px] border border-ink/10 bg-white/94 px-5 py-4 shadow-[0_18px_48px_rgba(15,23,42,0.06)] backdrop-blur">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate/65">
                      Search results
                    </div>
                    <h2 className="mt-1 text-xl font-semibold tracking-tight text-ink">
                      {activeResult.search.filters.title}
                    </h2>
                    <p className="mt-1 text-sm text-slate">
                      {resultsLocation} • updated {formatRelativeMoment(activeResult.search.updatedAt)}
                    </p>
                    <p className="mt-2 text-sm text-slate">
                      {isRefreshingVisibleSession
                        ? "Saved matches are ready now. Background refresh is still adding coverage for this session."
                        : "Saved matches for this session are ready to review and refine."}
                    </p>
                    {activeSearchBadges.length > 0 ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {activeSearchBadges.map((badge, index) => (
                          <span
                            key={`active-search-badge-${index}`}
                            className="rounded-full border border-ink/10 bg-mist/35 px-3 py-1.5 text-xs text-slate"
                          >
                            {badge}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <div className="space-y-3 lg:min-w-[320px]">
                    {/* Stop button only shown while supplemental work is running */}
                    {activeResult.crawlRun.status === "running" ? (
                      <button
                        type="button"
                        onClick={() => void stopActiveSearch(activeResult.search._id)}
                        className="inline-flex w-full items-center justify-center rounded-full border border-ink/15 bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-red-300 hover:bg-red-50"
                      >
                        Stop background work
                      </button>
                    ) : null}
                    {/* New Search always available in results view */}
                    <button
                      type="button"
                      onClick={() => void startNewSearch()}
                      className="inline-flex w-full items-center justify-center rounded-full border border-ink/15 bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-sand hover:bg-sand/20"
                    >
                      New Search
                    </button>
                    <div className="grid gap-3 sm:grid-cols-2">
                    <div className="rounded-[18px] border border-ink/8 bg-mist/30 px-4 py-3">
                      <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate/60">
                        Visible results
                      </div>
                      <div className="mt-1 text-base font-semibold text-ink">
                        {visibleJobs.length === activeResult.jobs.length
                          ? `${visibleJobs.length} jobs`
                          : `${visibleJobs.length} of ${activeResult.jobs.length}`}
                      </div>
                    </div>
                    <div className="rounded-[18px] border border-ink/8 bg-mist/30 px-4 py-3">
                      <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate/60">
                        Source scope
                      </div>
                      <div className="mt-1 text-base font-semibold text-ink">
                        {describePlatformScope(activeResult.search.filters.platforms)}
                      </div>
                    </div>
                  </div>
                  </div>
                </div>
              </section>

              {resultNotice ? (
                <section className="rounded-[18px] border border-ink/10 bg-white/92 px-5 py-4 shadow-sm">
                  <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                      <div className="text-sm font-semibold text-ink">{resultNotice.title}</div>
                      <p className="mt-1 text-sm text-slate">{resultNotice.description}</p>
                    </div>
                    <div className="flex flex-wrap gap-2 text-sm text-slate">
                      {(resultNotice.highlights ?? []).slice(0, 2).map((highlight, index) => (
                        <span
                          key={`result-notice-${index}`}
                          className="rounded-full border border-ink/10 bg-mist/40 px-3 py-1.5"
                        >
                          {highlight}
                        </span>
                      ))}
                    </div>
                  </div>
                </section>
              ) : null}

              <ResultsTable
                jobs={visibleJobs}
                totalJobs={activeResult.jobs.length}
                exportFilename={buildResultsExportFilename(activeResult.search.filters)}
                emptyMessage="No jobs match the current browse filters. Clear a few filters or rerun the search."
                onRevalidate={revalidateSingleJob}
                revalidatingIds={revalidatingIds}
              />
            </>
          ) : null}

          {activeResult || recentSearches.length > 0 ? (
            <DiagnosticsDrawer
              activeResult={activeResult}
              recentSearches={recentSearches}
              filters={filters}
              onLoadSearch={(searchId) => void loadSearch(searchId)}
              onRerunSearch={(searchId) => void rerunActiveSearch(searchId)}
              onSetCrawlMode={(value) =>
                setFilters((current) => ({
                  ...current,
                  crawlMode: value,
                }))
              }
              onSetExperienceMatchMode={(value) =>
                setFilters((current) => ({
                  ...current,
                  experienceMatchMode: value,
                  includeUnspecifiedExperience:
                    value === "broad"
                      ? true
                      : current.includeUnspecifiedExperience,
                }))
              }
              onToggleIncludeUnspecified={() =>
                setFilters((current) => ({
                  ...current,
                  includeUnspecifiedExperience:
                    current.experienceMatchMode === "broad"
                      ? true
                      : !(current.includeUnspecifiedExperience === true),
                }))
              }
            />
          ) : null}
        </div>
      </div>
    </main>
  );
}

export function resolveViewState(result: CrawlResponse): ViewState {
  if (result.jobs.length > 0) {
    return "success";
  }

  if (result.crawlRun.status === "running") {
    return "loading";
  }

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

export function isSupplementingSearchSession(result: CrawlResponse | null | undefined) {
  return Boolean(result && result.crawlRun.status === "running" && result.jobs.length > 0);
}

export function shouldShowBlockingSearchLoad(
  viewState: ViewState,
  result: CrawlResponse | null | undefined,
) {
  return viewState === "loading" && !(result && result.jobs.length > 0);
}

export function describeZeroResultState(result: CrawlResponse): ZeroResultState {
  const diagnostics = result.diagnostics;

  if (result.crawlRun.status === "aborted") {
    return {
      title: "The crawl was stopped before more jobs were saved",
      description:
        "This run was canceled before completion, so the saved result set may be incomplete.",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

  if (diagnostics.discoveredSources === 0) {
    return {
      title: "No runnable sources were discovered",
      description:
        "The crawler did not find any registry-backed or publicly discovered sources for the selected platform scope.",
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

export function isLatestClientRequest(requestToken: number, currentToken: number) {
  return requestToken === currentToken;
}

export function shouldApplyQueuedResultImmediately(result: ProgressiveSearchSnapshot) {
  return result.crawlRun.status !== "running" || result.jobs.length > 0;
}

export function resolveQueuedSearchPollIntervalMs(visibleJobCount: number) {
  return visibleJobCount > 0
    ? queuedSearchPollIntervalVisibleMs
    : queuedSearchPollIntervalEmptyMs;
}

function createClientRequestOwnerKey() {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }

  return `job-crawler-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function isAbortLikeClientError(error: unknown) {
  return error instanceof Error && error.name === "AbortError";
}

export function describeResultNotice(result: CrawlResponse): ResultNotice | null {
  const diagnostics = result.diagnostics;

  if (result.crawlRun.status === "running" && result.jobs.length > 0) {
    const crawlMode = resolveCrawlMode(result.search.filters.crawlMode);
    const modeHighlight =
      crawlMode === "fast"
        ? "Fast mode shows your first results quickly while refinement continues."
        : crawlMode === "balanced"
          ? "Balanced mode refines results in the background while you review what's already visible."
          : "Deep mode keeps exploring more sources while your current results stay available.";

    return {
      title: "Results are arriving while background work continues",
      description:
        "Browse these jobs now — more may appear as the search finishes exploring additional sources.",
      tone: "tide",
      highlights: [
        modeHighlight,
        `${result.jobs.length} job${result.jobs.length === 1 ? "" : "s"} saved so far.`,
      ],
    };
  }

  if (result.crawlRun.status === "aborted") {
    return {
      title: "This search was stopped before refinement finished",
      description:
        "Your visible results are preserved. Some potential matches may not have been explored yet.",
      tone: "amber",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

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

  if (diagnostics.discovery?.publicSearch) {
    const publicSearch = diagnostics.discovery.publicSearch;
    highlights.push(
      `Public search generated ${publicSearch.generatedQueries} queries, executed ${publicSearch.executedQueries}, harvested ${publicSearch.rawResultsHarvested} raw results, and added ${publicSearch.sourcesAdded} sources.`,
    );

    if (publicSearch.sampleGeneratedRoleQueries.length > 0) {
      const preview = publicSearch.sampleGeneratedRoleQueries.slice(0, 4);
      highlights.push(
        `Title variants explored: ${preview.join(", ")}${publicSearch.roleQueryCount > preview.length ? ", ..." : ""}.`,
      );
    }
  }

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

function normalizeCrawlDeltaResponseForClient(payload: CrawlDeltaResponse): CrawlDeltaResponse {
  return {
    ...payload,
    search: normalizeSearchDocumentForClient(payload.search),
  };
}

export function mergeCrawlDeltaIntoResult(
  current: CrawlResponse | null,
  payload: CrawlDeltaResponse,
): CrawlResponse {
  const baseJobs = current?.search._id === payload.search._id ? current.jobs : [];
  const mergedJobs = dedupeJobsForSessionRender([...baseJobs, ...payload.jobs]).sort(jobComparator);

  return {
    search: payload.search,
    ...(payload.searchSession ? { searchSession: payload.searchSession } : {}),
    crawlRun: payload.crawlRun,
    sourceResults: payload.sourceResults,
    jobs: mergedJobs,
    diagnostics: payload.diagnostics,
    delivery: {
      mode: "full",
      cursor: payload.delivery.cursor,
      indexedCursor: payload.delivery.indexedCursor ?? current?.delivery?.indexedCursor,
    },
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

function dedupeJobsForSessionRender(jobs: CrawlResponse["jobs"]) {
  const map = new Map<string, CrawlResponse["jobs"][number]>();

  jobs.forEach((job) => {
    map.set(buildStableJobRenderIdentity(job), job);
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

function toggleExperienceLevel(
  selectedLevels: SearchFilters["experienceLevels"],
  level: ExperienceLevel,
) {
  const nextLevels = new Set(selectedLevels ?? []);

  if (nextLevels.has(level)) {
    nextLevels.delete(level);
  } else {
    nextLevels.add(level);
  }

  const normalized = experienceLevels.filter((candidate) => nextLevels.has(candidate));
  return normalized.length > 0 ? normalized : undefined;
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

function delay(durationMs: number) {
  return new Promise<void>((resolve) => {
    window.setTimeout(resolve, durationMs);
  });
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
