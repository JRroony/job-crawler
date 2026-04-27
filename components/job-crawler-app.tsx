"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

import { togglePlatformSelection } from "@/components/job-crawler/ui-config";
import { JobFilterSidebar } from "@/components/job-search/filter-bar";
import { DiagnosticsDrawer } from "@/components/job-search/diagnostics-drawer";
import { JobDetailPanel } from "@/components/job-search/job-detail-panel";
import { JobEmptyState } from "@/components/job-search/job-empty-state";
import { JobLoadingSkeleton } from "@/components/job-search/job-loading-skeleton";
import { JobResultsList } from "@/components/job-search/job-results-list";
import {
  buildLocationInputValue,
  buildStableJobRenderKeys,
  defaultClientResultFilters,
  filterJobsForDisplay,
  parseLocationInput,
  toggleEmploymentTypeSelection,
} from "@/components/job-search/helpers";
import { JobSearchHeader } from "@/components/job-search/search-bar";
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
  const [isLoadingMoreResults, setIsLoadingMoreResults] = useState(false);
  const [revalidatingIds, setRevalidatingIds] = useState<string[]>([]);
  const [selectedJobKey, setSelectedJobKey] = useState<string | undefined>();
  const [isFilterDrawerOpen, setIsFilterDrawerOpen] = useState(false);
  const [debugAvailable, setDebugAvailable] = useState(false);
  const [debugEnabled, setDebugEnabled] = useState(false);
  const hydratedUrlSearchRef = useRef(false);
  const pollSequenceRef = useRef(0);
  const loadMoreSequenceRef = useRef(0);
  const activeDeliveryCursorRef = useRef(0);
  const activeIndexedDeliveryCursorRef = useRef(0);
  const clientRequestOwnerKeyRef = useRef(createClientRequestOwnerKey());
  const activeRequestControllerRef = useRef<AbortController | null>(null);
  const loadMoreRequestControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (initialSearches.length > 0) {
      return;
    }

    let cancelled = false;

    async function loadRecentSearches() {
      try {
        const response = await fetch("/api/searches", { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload = (await response.json()) as { searches?: SearchDocument[] };
        if (!cancelled && Array.isArray(payload.searches)) {
          setRecentSearches(dedupeSearches(payload.searches.map(normalizeSearchDocumentForClient)));
        }
      } catch {
        // Recent search history is non-blocking; search itself remains available.
      }
    }

    void loadRecentSearches();

    return () => {
      cancelled = true;
    };
  }, [initialSearches.length]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const params = new URLSearchParams(window.location.search);
    const debugRequested = params.get("debug") === "1";
    const localHost =
      window.location.hostname === "localhost" ||
      window.location.hostname === "127.0.0.1" ||
      window.location.hostname === "";
    const storedDebugEnabled = window.localStorage.getItem("job-search-debug") === "1";

    setDebugAvailable(debugRequested || localHost || storedDebugEnabled);
    setDebugEnabled(debugRequested || storedDebugEnabled);
  }, []);

  useEffect(() => {
    if (hydratedUrlSearchRef.current || typeof window === "undefined") {
      return;
    }

    hydratedUrlSearchRef.current = true;
    const params = new URLSearchParams(window.location.search);
    const title = params.get("title")?.trim() ?? "";

    if (!title) {
      return;
    }

    const location = params.get("location")?.trim() ?? "";
    const nextFilters = normalizeSearchFiltersForClient({
      ...initialFilters,
      title,
      ...parseLocationInput(location),
    });

    hydrateSearchForm(nextFilters);
    void submitSearch(nextFilters, { updateUrl: false });
  }, []);

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
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "/jobs");
    }
  }

  function clearBrowseFilters() {
    setFilters((current) => ({
      ...current,
      platforms: undefined,
      experienceLevels: undefined,
    }));
    setClientResultFilters(defaultClientResultFilters);
  }

  function syncSearchUrl(nextFilters: SearchFilters) {
    if (typeof window === "undefined") {
      return;
    }

    const params = new URLSearchParams();
    if (nextFilters.title.trim()) {
      params.set("title", nextFilters.title.trim());
    }

    const location = buildLocationInputValue(nextFilters);
    if (location) {
      params.set("location", location);
    }

    const url = params.toString() ? `/jobs?${params.toString()}` : "/jobs";
    window.history.pushState(null, "", url);
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
      "We're updating results in the background. New matching jobs may appear shortly.",
    );
    setErrorKind(null);
  }

  async function submitSearch(
    nextFilters: SearchFilters,
    options: { updateUrl?: boolean } = {},
  ) {
    const pollToken = ++pollSequenceRef.current;
    const requestController = replaceActiveRequestController();
    const payloadResult = buildSearchRequestPayload(nextFilters);

    if (!payloadResult.ok) {
      setViewState("error");
      setErrorKind("validation");
      setMessage(payloadResult.message);
      return;
    }

    cancelLoadMoreRequest();
    setViewState("loading");
    setMessage("");
    setErrorKind(null);
    setActiveResult(null);
    activeDeliveryCursorRef.current = 0;
    activeIndexedDeliveryCursorRef.current = 0;
    setSelectedJobKey(undefined);
    if (options.updateUrl !== false) {
      syncSearchUrl(nextFilters);
    }

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
          payload.error ?? "The search request failed.",
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
      setMessage(error instanceof Error ? error.message : "The search request failed.");
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
    cancelLoadMoreRequest();
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
    cancelLoadMoreRequest();
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

  async function loadMoreResults() {
    const currentResult = activeResult;
    const nextCursor = currentResult?.nextCursor;

    if (!currentResult || currentResult.hasMore !== true || nextCursor == null) {
      return;
    }

    const searchId = currentResult.search._id;
    const searchSessionId = resolveResultSearchSessionId(currentResult);
    const pollToken = pollSequenceRef.current;
    const loadMoreToken = ++loadMoreSequenceRef.current;
    loadMoreRequestControllerRef.current?.abort();
    const requestController = new AbortController();
    loadMoreRequestControllerRef.current = requestController;
    setIsLoadingMoreResults(true);
    setMessage("");
    setErrorKind(null);

    const params = new URLSearchParams({
      cursor: String(nextCursor),
      pageSize: String(currentResult.pageSize ?? 50),
    });
    if (searchSessionId) {
      params.set("searchSessionId", searchSessionId);
    }

    try {
      const response = await fetch(`/api/searches/${searchId}?${params.toString()}`, {
        signal: requestController.signal,
      });
      const payload = (await response.json()) as SearchRoutePayload;
      if (
        !isLatestClientRequest(pollToken, pollSequenceRef.current) ||
        !isLatestClientRequest(loadMoreToken, loadMoreSequenceRef.current)
      ) {
        return;
      }
      if (!response.ok) {
        throw createClassifiedClientError(
          "runtime",
          payload.error ?? "More results could not be loaded.",
        );
      }

      const normalizedPayload = normalizeCrawlResponseForClient(payload);
      let mergedResult: CrawlResponse | null = null;
      setActiveResult((current) => {
        mergedResult = mergeSearchPageIntoResult(current, normalizedPayload);
        return mergedResult;
      });
      if (mergedResult) {
        setViewState(resolveViewState(mergedResult));
      }
      refreshRecentSearch(payload.search);
    } catch (error) {
      if (
        !isLatestClientRequest(pollToken, pollSequenceRef.current) ||
        !isLatestClientRequest(loadMoreToken, loadMoreSequenceRef.current) ||
        isAbortLikeClientError(error)
      ) {
        return;
      }
      setMessage(error instanceof Error ? error.message : "More results could not be loaded.");
      setErrorKind("runtime");
    } finally {
      if (isLatestClientRequest(loadMoreToken, loadMoreSequenceRef.current)) {
        setIsLoadingMoreResults(false);
      }
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
          payload.error ?? "The background update could not be stopped.",
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
      setMessage(error instanceof Error ? error.message : "The background update could not be stopped.");
    }
  }

  function replaceActiveRequestController() {
    activeRequestControllerRef.current?.abort();
    const controller = new AbortController();
    activeRequestControllerRef.current = controller;
    return controller;
  }

  function cancelLoadMoreRequest() {
    loadMoreRequestControllerRef.current?.abort();
    loadMoreRequestControllerRef.current = null;
    loadMoreSequenceRef.current += 1;
    setIsLoadingMoreResults(false);
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
      mergedResult = mergeCrawlDeltaIntoResult(current, normalizedPayload, normalizedPayload.search.filters);
      return mergedResult;
    });
    hydrateSearchForm(normalizedPayload.search.filters);
    setViewState(
      mergedResult ? resolveViewState(mergedResult) : "loading",
    );
    setErrorKind(null);
  }

  async function startNewSearch() {
    activeRequestControllerRef.current?.abort();
    activeRequestControllerRef.current = null;
    cancelLoadMoreRequest();
    setActiveResult(null);
    setViewState("idle");
    setMessage("");
    setErrorKind(null);
    pollSequenceRef.current += 1;
    activeDeliveryCursorRef.current = 0;
    activeIndexedDeliveryCursorRef.current = 0;
    setSelectedJobKey(undefined);
    clearSearchForm();
  }

  const visibleJobs = useMemo(
    () =>
      activeResult
        ? filterJobsForDisplay(activeResult.jobs, filters, clientResultFilters)
        : [],
    [activeResult, clientResultFilters, filters],
  );
  const jobRenderKeys = useMemo(() => buildStableJobRenderKeys(visibleJobs), [visibleJobs]);
  const totalMatchedCount = activeResult ? resolveTotalMatchedCount(activeResult) : 0;
  const loadedResultCount = activeResult?.jobs.length ?? 0;
  const returnedCount = activeResult?.returnedCount ?? loadedResultCount;
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
  const selectedJobKeyForRender = resolveVisibleJobSelection(selectedJobKey, jobRenderKeys);
  const selectedJobIndex = selectedJobKeyForRender
    ? jobRenderKeys.findIndex((key) => key === selectedJobKeyForRender)
    : -1;
  const selectedJob = selectedJobIndex === -1 ? undefined : visibleJobs[selectedJobIndex];

  useEffect(() => {
    setSelectedJobKey((current) => resolveVisibleJobSelection(current, jobRenderKeys));
  }, [jobRenderKeys]);

  return (
    <main className="min-h-screen bg-[#f3f2ef] text-ink">
      <JobSearchHeader
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
        onReset={() => void startNewSearch()}
      />

      <div className="mx-auto grid max-w-[1440px] gap-4 px-4 py-4 sm:px-6 lg:grid-cols-[280px_minmax(0,1fr)] lg:px-8 xl:grid-cols-[280px_minmax(0,1fr)_420px]">
        <JobFilterSidebar
          filters={filters}
          resultFilters={clientResultFilters}
          mobileOpen={isFilterDrawerOpen}
          onCloseMobile={() => setIsFilterDrawerOpen(false)}
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
          onWorkplaceChange={(workplace) =>
            setClientResultFilters((current) => ({
              ...current,
              workplace,
              remoteOnly: false,
            }))
          }
          onToggleEmploymentType={(employmentType) =>
            setClientResultFilters((current) => ({
              ...current,
              employmentTypes: toggleEmploymentTypeSelection(
                current.employmentTypes,
                employmentType,
              ),
            }))
          }
          onSponsorshipChange={(sponsorship) =>
            setClientResultFilters((current) => ({
              ...current,
              sponsorship,
              visaFriendlyOnly: false,
            }))
          }
          onCompanyChange={(company) =>
            setClientResultFilters((current) => ({
              ...current,
              company,
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

        <section className="min-w-0 space-y-3">
          <div className="flex items-center justify-between gap-3 lg:hidden">
            <button
              type="button"
              onClick={() => setIsFilterDrawerOpen(true)}
              className="rounded-md border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-ink shadow-sm"
            >
              Filters
            </button>
          </div>

          {viewState === "idle" ? (
            <section className="rounded-lg border border-slate-200 bg-white px-6 py-10 shadow-sm">
              <h1 className="text-2xl font-semibold text-ink">Search jobs by role and location</h1>
              <p className="mt-2 max-w-2xl text-sm leading-6 text-slate">
                Enter a title and location above to review matching jobs from the indexed database.
              </p>
            </section>
          ) : null}

          {message && !blockingErrorState ? (
            <StatusBanner message={message} tone={errorKind ? "error" : "info"} />
          ) : null}

          {isRefreshingVisibleSession ? (
            <StatusBanner
              message="We're updating results in the background."
              tone="info"
            />
          ) : null}

          {resultNotice ? (
            <StatusBanner message={resultNotice.description} tone="info" />
          ) : null}

          {viewState === "error" && !activeResult ? (
            <BlockingState
              title={blockingErrorState?.title ?? "Search could not complete"}
              description={
                blockingErrorState?.description ??
                "Some sources could not be refreshed. Try again in a moment."
              }
              actionLabel={blockingErrorState?.actionLabel}
              onAction={
                blockingErrorState?.actionType === "reload"
                  ? () => window.location.reload()
                  : blockingErrorState?.actionType === "retry"
                    ? () => void submitSearch(buildLiveFilters())
                    : undefined
              }
            />
          ) : null}

          {isBlockingSearchLoad ? <JobLoadingSkeleton /> : null}

          {zeroResultState && viewState !== "loading" ? (
            <JobEmptyState backgroundUpdating={activeResult?.crawlRun.status === "running"} />
          ) : null}

          {activeResult && activeResult.jobs.length > 0 ? (
            <>
              <section className="rounded-lg border border-slate-200 bg-white px-4 py-3 shadow-sm">
                <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
                  <div>
                    <h1 className="text-xl font-semibold text-ink">
                      {activeResult.search.filters.title}
                    </h1>
                    <p className="mt-1 text-sm text-slate">{resultsLocation}</p>
                  </div>
                  <p className="text-sm text-slate">
                    {visibleJobs.length === loadedResultCount
                      ? `${loadedResultCount} jobs indexed`
                      : `${visibleJobs.length} visible from ${loadedResultCount} indexed`}
                  </p>
                </div>
              </section>

              {visibleJobs.length > 0 ? (
                <JobResultsList
                  jobs={visibleJobs}
                  jobRenderKeys={jobRenderKeys}
                  selectedJobKey={selectedJobKeyForRender}
                  totalMatchedCount={totalMatchedCount}
                  returnedCount={returnedCount}
                  pageSize={activeResult.pageSize}
                  hasMore={activeResult.hasMore}
                  isLoadingMore={isLoadingMoreResults}
                  onSelect={setSelectedJobKey}
                  onLoadMore={() => void loadMoreResults()}
                />
              ) : (
                <JobEmptyState backgroundUpdating={isRefreshingVisibleSession} />
              )}

              <div className="xl:hidden">
                <JobDetailPanel job={selectedJob} />
              </div>
            </>
          ) : null}

          {debugAvailable ? (
            <div className="pt-2">
              <button
                type="button"
                onClick={() => {
                  const nextEnabled = !debugEnabled;
                  setDebugEnabled(nextEnabled);
                  if (typeof window !== "undefined") {
                    window.localStorage.setItem("job-search-debug", nextEnabled ? "1" : "0");
                  }
                }}
                className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate shadow-sm hover:bg-slate-50"
              >
                Developer {debugEnabled ? "on" : "off"}
              </button>
            </div>
          ) : null}

          {debugEnabled && (activeResult || recentSearches.length > 0) ? (
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
        </section>

        <aside className="hidden min-w-0 xl:block">
          <div className="sticky top-24">
            <JobDetailPanel job={selectedJob} />
          </div>
        </aside>
      </div>
    </main>
  );
}

function StatusBanner(props: { message: string; tone: "info" | "error" }) {
  return (
    <div
      className={
        props.tone === "error"
          ? "rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
          : "rounded-lg border border-[#0a66c2]/20 bg-[#e7f3ff] px-4 py-3 text-sm text-[#004182]"
      }
    >
      {props.message}
    </div>
  );
}

function BlockingState(props: {
  title: string;
  description: string;
  actionLabel?: string;
  onAction?: () => void;
}) {
  return (
    <section className="rounded-lg border border-red-200 bg-white px-6 py-10 text-center shadow-sm">
      <h2 className="text-xl font-semibold text-ink">{props.title}</h2>
      <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-slate">{props.description}</p>
      {props.actionLabel && props.onAction ? (
        <button
          type="button"
          onClick={props.onAction}
          className="mt-5 rounded-md bg-[#0a66c2] px-5 py-2.5 text-sm font-semibold text-white hover:bg-[#004182]"
        >
          {props.actionLabel}
        </button>
      ) : null}
    </section>
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
      title: "The update stopped before more jobs were indexed",
      description:
        "The current result set may still be incomplete. Try a broader title or location.",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

  if (diagnostics.discoveredSources === 0) {
    return {
      title: "No matching jobs found yet",
      description:
        "Try a broader title or location while results continue to refresh in the background.",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

  if (diagnostics.providerFailures > 0 && result.crawlRun.status === "failed") {
    return {
      title: "Some sources could not be refreshed",
      description:
        "No indexed jobs are available for this search yet. Try again or broaden the filters.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (diagnostics.providerFailures > 0) {
    return {
      title: "Some sources could not be refreshed",
      description:
        "No indexed jobs are available for this search yet. Try a broader title or location.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (
    diagnostics.excludedByTitle > 0 ||
    diagnostics.excludedByLocation > 0 ||
    diagnostics.excludedByExperience > 0
  ) {
    return {
      title: "No matching jobs found yet",
      description:
        "The current title, location, or filters are too narrow for the available indexed jobs.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  if (diagnostics.dedupedOut > 0) {
    return {
      title: "No matching jobs found yet",
      description:
        "The available matches were duplicates of jobs already represented in the index.",
      highlights: buildOperationalHighlights(diagnostics),
    };
  }

  return {
    title: "No matching jobs found yet",
    description:
      "Try a broader title or location while results continue to refresh in the background.",
    highlights: buildOperationalHighlights(diagnostics),
  };
}

export function isLatestClientRequest(requestToken: number, currentToken: number) {
  return requestToken === currentToken;
}

export function resolveVisibleJobSelection(
  selectedJobKey: string | undefined,
  visibleJobKeys: string[],
) {
  if (selectedJobKey && visibleJobKeys.includes(selectedJobKey)) {
    return selectedJobKey;
  }

  return visibleJobKeys[0];
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
    return {
      title: "Updating results",
      description:
        "Matching jobs are ready now. More may appear as background refresh finishes.",
      tone: "tide",
      highlights: [
        `${result.jobs.length} job${result.jobs.length === 1 ? "" : "s"} indexed so far.`,
      ],
    };
  }

  if (result.crawlRun.status === "aborted") {
    return {
      title: "Update stopped",
      description:
        "Your visible results are preserved. Some potential matches may not have been explored yet.",
      tone: "amber",
      highlights: buildOperationalHighlights(diagnostics, { includeFilters: false }),
    };
  }

  if (result.jobs.length > 0 && diagnostics.validationDeferred > 0) {
    return {
      title: "Some links are still being checked",
      description:
        "The result set is ready to review while link freshness updates continue.",
      tone: "tide",
      highlights: [
        `${diagnostics.validationDeferred} job${diagnostics.validationDeferred === 1 ? "" : "s"} still need link freshness checks.`,
      ],
    };
  }

  if (result.crawlRun.status === "partial" && result.jobs.length > 0) {
    return {
      title: "Some sources could not be refreshed",
      description:
        "The indexed results are available, but a few sources could not be updated.",
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

export function resolveTotalMatchedCount(result: CrawlResponse) {
  return (
    result.totalMatchedCount ??
    result.finalMatchedCount ??
    result.diagnostics.searchResponse?.totalMatchedCount ??
    result.diagnostics.searchResponse?.finalMatchedCount ??
    result.diagnostics.searchResponse?.matchedCount ??
    result.diagnostics.session?.totalVisibleResultsCount ??
    result.jobs.length
  );
}

export function resolveResultSearchSessionId(
  result: Pick<CrawlResponse, "search"> & {
    searchSession?: CrawlResponse["searchSession"];
    searchSessionId?: string | null;
  },
) {
  return (
    result.searchSession?._id ??
    result.searchSessionId ??
    result.search.latestSearchSessionId ??
    undefined
  );
}

export function isSameSearchSessionResult(
  current: CrawlResponse | null | undefined,
  payload: Pick<CrawlResponse, "search"> & {
    searchSession?: CrawlResponse["searchSession"];
    searchSessionId?: string | null;
  },
) {
  if (!current || current.search._id !== payload.search._id) {
    return false;
  }

  const currentSessionId = resolveResultSearchSessionId(current);
  const payloadSessionId = resolveResultSearchSessionId(payload);

  if (!currentSessionId && !payloadSessionId) {
    return true;
  }

  return Boolean(currentSessionId && payloadSessionId && currentSessionId === payloadSessionId);
}

export function mergeSearchPageIntoResult(
  current: CrawlResponse | null,
  payload: CrawlResponse,
): CrawlResponse | null {
  if (!current) {
    return payload;
  }

  if (!isSameSearchSessionResult(current, payload)) {
    return current;
  }

  const mergedJobs = dedupeJobsForSessionRender([...current.jobs, ...payload.jobs]).sort(jobComparator);
  const totalMatchedCount = resolveTotalMatchedCount(payload);

  return {
    ...payload,
    jobs: mergedJobs,
    returnedCount: mergedJobs.length,
    totalMatchedCount,
    finalMatchedCount: totalMatchedCount,
    pageSize: payload.pageSize ?? current.pageSize,
    nextCursor: payload.nextCursor,
    hasMore: payload.hasMore === true,
    delivery: {
      mode: "full",
      cursor: payload.delivery?.cursor ?? current.delivery?.cursor ?? 0,
      indexedCursor: payload.delivery?.indexedCursor ?? current.delivery?.indexedCursor,
    },
  };
}

export function mergeCrawlDeltaIntoResult(
  current: CrawlResponse | null,
  payload: CrawlDeltaResponse,
  activeFilters: SearchFilters = payload.search.filters,
): CrawlResponse {
  const sameSearch = current?.search._id === payload.search._id;
  const currentSessionId = current?.searchSession?._id;
  const payloadSessionId = payload.searchSession?._id;
  const sameSession =
    sameSearch && (!currentSessionId || !payloadSessionId || currentSessionId === payloadSessionId);
  const baseJobs = sameSession ? current?.jobs ?? [] : [];
  const guardedDeltaJobs = filterJobsForDisplay(payload.jobs, activeFilters, defaultClientResultFilters);
  const mergedJobs = dedupeJobsForSessionRender([...baseJobs, ...guardedDeltaJobs]).sort(jobComparator);

  return {
    searchId: payload.searchId ?? payload.search._id,
    searchSessionId: payload.searchSessionId ?? resolveResultSearchSessionId(payload),
    candidateCount: payload.candidateCount ?? current?.candidateCount,
    finalMatchedCount:
      payload.finalMatchedCount ??
      current?.finalMatchedCount ??
      current?.totalMatchedCount ??
      mergedJobs.length,
    totalMatchedCount:
      payload.totalMatchedCount ??
      current?.totalMatchedCount ??
      current?.finalMatchedCount ??
      mergedJobs.length,
    returnedCount: mergedJobs.length,
    pageSize: payload.pageSize ?? current?.pageSize,
    nextCursor: payload.nextCursor ?? current?.nextCursor,
    hasMore: payload.hasMore ?? current?.hasMore,
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
      title: "Job search could not load",
      description:
        message || "Check that MongoDB is available, then reload the page and try again.",
      actionLabel: "Reload page",
      actionType: "reload",
    };
  }

  return {
    title: "Search could not complete",
    description:
      message || "Some sources could not be refreshed before results were returned.",
    actionLabel: "Retry",
    actionType: "retry",
  };
}
