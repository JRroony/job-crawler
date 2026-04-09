#!/usr/bin/env node
"use strict";

const path = require("node:path");
const process = require("node:process");

const PROJECT_ROOT = path.resolve(__dirname, "..");

// Keep the default acceptance run stable by limiting discovery to configured and
// curated sources unless the shell already opted into broader discovery.
process.env.PUBLIC_SEARCH_DISCOVERY_ENABLED ??= "false";

const configuredPlatforms = parseCsv(process.env.VERIFY_CRAWL_PLATFORMS) ?? ["lever"];

const VERIFY_CRAWL_CONFIG = {
  // Configure the acceptance-test query here.
  query: {
    title: process.env.VERIFY_CRAWL_TITLE ?? "software engineer",
    country: process.env.VERIFY_CRAWL_COUNTRY ?? "United States",
    // Configure the platform/source scope here. Implemented families today:
    // greenhouse, lever, ashby, company_page
    platforms: configuredPlatforms,
    crawlMode: process.env.VERIFY_CRAWL_MODE ?? "fast",
  },
  sampleSize: parsePositiveInt(process.env.VERIFY_CRAWL_SAMPLE_SIZE, 5),
  // Configure which API endpoint the verifier calls here.
  // - "route_handler" calls the Next.js route modules directly.
  // - "http" calls a running app server at VERIFY_CRAWL_API_BASE_URL.
  api: {
    mode: process.env.VERIFY_CRAWL_API_MODE ?? "route_handler",
    baseUrl: process.env.VERIFY_CRAWL_API_BASE_URL ?? "",
    createSearchPath: process.env.VERIFY_CRAWL_CREATE_PATH ?? "/api/searches",
    detailsPathTemplate:
      process.env.VERIFY_CRAWL_DETAILS_PATH_TEMPLATE ?? "/api/searches/:id",
  },
  // Configure the expected UI response shape here. This app uses the shared
  // crawlResponseSchema contract for crawler responses and API payloads.
  expectedResponseShapeName: "crawlResponseSchema",
};

async function main() {
  const summary = {
    totalJobsFound: 0,
    sampledValidJobsCount: 0,
    invalidJobsCount: 0,
    duplicateCount: 0,
    apiValidationPassed: false,
    finalStatus: "FAIL",
  };
  const failures = [];
  const diagnostics = [];

  try {
    const sharedModules = loadSharedModules();
    const queryResult = sharedModules.searchFiltersSchema.safeParse(
      VERIFY_CRAWL_CONFIG.query,
    );

    if (!queryResult.success) {
      failures.push(
        `Configured query is invalid: ${formatZodIssues(queryResult.error.issues)}`,
      );
      return finish(summary, failures, diagnostics);
    }

    const query = queryResult.data;
    const apiClient = loadApiClient();
    const createOutcome = await withCapturedConsole(() => apiClient.createSearch(query));
    diagnostics.push(...createOutcome.logs);

    const createCheck = await validateEndpointResponse(
      {
        response: createOutcome.value.response,
        payload: createOutcome.value.payload,
      },
      sharedModules,
      "Create search",
      { requireJobs: true },
    );

    if (!createCheck.ok) {
      failures.push(...createCheck.failures);
      const createAnalysis = updateSummaryFromJobs(summary, createCheck.jobs, sharedModules);
      failures.push(...createAnalysis.sampleValidation.failures);
      if (createAnalysis.duplicateOnlyReason) {
        failures.push(createAnalysis.duplicateOnlyReason);
      }
      return finish(summary, failures, diagnostics);
    }

    const createAnalysis = updateSummaryFromJobs(summary, createCheck.jobs, sharedModules);
    failures.push(...createAnalysis.sampleValidation.failures);
    if (createAnalysis.duplicateOnlyReason) {
      failures.push(createAnalysis.duplicateOnlyReason);
    }
    failures.push(...createCheck.failures);

    if (!hasNonEmptyString(createCheck.searchId)) {
      failures.push("Create search response did not include a search id.");
      return finish(summary, failures, diagnostics);
    }

    const detailsOutcome = await withCapturedConsole(() =>
      apiClient.getSearchDetails(createCheck.searchId),
    );
    diagnostics.push(...detailsOutcome.logs);

    const detailsCheck = await validateEndpointResponse(
      {
        response: detailsOutcome.value.response,
        payload: detailsOutcome.value.payload,
      },
      sharedModules,
      "Search details",
      { requireJobs: true },
    );

    failures.push(...detailsCheck.failures);

    if (detailsCheck.ok) {
      if (detailsCheck.searchId !== createCheck.searchId) {
        failures.push(
          `Search details returned a different search id (${String(detailsCheck.searchId)}) than the create search response (${createCheck.searchId}).`,
        );
      }

      if (detailsCheck.jobs.length < 1) {
        failures.push("Search details endpoint returned zero jobs.");
      }
    }

    summary.apiValidationPassed =
      createCheck.ok &&
      detailsCheck.ok &&
      createCheck.jobs.length > 0 &&
      detailsCheck.jobs.length > 0;
  } catch (error) {
    failures.push(formatUnexpectedError(error));
  }

  return finish(summary, failures, diagnostics);
}

function loadSharedModules() {
  const load = createModuleLoader();
  const typesModule = load(projectPath("lib/types.ts"));
  const utilsModule = load(projectPath("lib/utils.ts"));
  const dedupeModule = load(projectPath("lib/server/crawler/dedupe.ts"));

  if (!typesModule?.crawlResponseSchema || !typesModule?.searchFiltersSchema) {
    throw new Error("Unable to load shared crawler schemas.");
  }

  if (typeof utilsModule?.jobPostingUrl !== "function") {
    throw new Error("Unable to load the shared job posting URL helper.");
  }

  if (typeof dedupeModule?.dedupeJobs !== "function") {
    throw new Error("Unable to load the shared dedupe helper.");
  }

  return {
    crawlResponseSchema: typesModule.crawlResponseSchema,
    searchFiltersSchema: typesModule.searchFiltersSchema,
    jobPostingUrl: utilsModule.jobPostingUrl,
    dedupeJobs: dedupeModule.dedupeJobs,
  };
}

function loadApiClient() {
  if (VERIFY_CRAWL_CONFIG.api.mode === "http") {
    return createHttpApiClient();
  }

  if (VERIFY_CRAWL_CONFIG.api.mode !== "route_handler") {
    throw new Error(
      `Unsupported VERIFY_CRAWL_API_MODE "${VERIFY_CRAWL_CONFIG.api.mode}". Expected "route_handler" or "http".`,
    );
  }

  const load = createModuleLoader();
  const searchesRoute = load(projectPath("app/api/searches/route.ts"));
  const searchDetailsRoute = load(projectPath("app/api/searches/[id]/route.ts"));

  if (typeof searchesRoute?.POST !== "function") {
    throw new Error("Unable to load POST /api/searches route handler.");
  }

  if (typeof searchDetailsRoute?.GET !== "function") {
    throw new Error("Unable to load GET /api/searches/[id] route handler.");
  }

  return {
    async createSearch(query) {
      const request = new Request("http://verify-crawl.local/api/searches", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(query),
      });
      const response = await searchesRoute.POST(request);
      const payload = await readResponsePayload(response);
      return { response, payload };
    },
    async getSearchDetails(searchId) {
      const request = new Request(
        `http://verify-crawl.local${buildDetailsPath(searchId)}`,
        { method: "GET" },
      );
      const response = await searchDetailsRoute.GET(request, {
        params: Promise.resolve({ id: searchId }),
      });
      const payload = await readResponsePayload(response);
      return { response, payload };
    },
  };
}

function createHttpApiClient() {
  if (!hasNonEmptyString(VERIFY_CRAWL_CONFIG.api.baseUrl)) {
    throw new Error(
      "VERIFY_CRAWL_API_BASE_URL is required when VERIFY_CRAWL_API_MODE=http.",
    );
  }

  return {
    async createSearch(query) {
      const response = await fetch(
        new URL(
          VERIFY_CRAWL_CONFIG.api.createSearchPath,
          VERIFY_CRAWL_CONFIG.api.baseUrl,
        ),
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(query),
        },
      );
      const payload = await readResponsePayload(response);
      return { response, payload };
    },
    async getSearchDetails(searchId) {
      const response = await fetch(
        new URL(buildDetailsPath(searchId), VERIFY_CRAWL_CONFIG.api.baseUrl),
        {
          method: "GET",
        },
      );
      const payload = await readResponsePayload(response);
      return { response, payload };
    },
  };
}

function createModuleLoader() {
  let jitiFactory;

  try {
    jitiFactory = require("jiti");
  } catch (error) {
    throw new Error(
      `Unable to load "jiti" to import the TypeScript crawler modules. ${formatUnexpectedError(error)}`,
    );
  }

  return jitiFactory(__filename, {
    alias: {
      "@/": `${PROJECT_ROOT}/`,
      "server-only": projectPath("scripts/server-only-shim.js"),
    },
  });
}

function projectPath(relativePath) {
  return path.join(PROJECT_ROOT, relativePath);
}

async function validateEndpointResponse(input, sharedModules, label, options) {
  const failures = [];
  const response = input.response;
  const payload = input.payload;

  if (!(response instanceof Response)) {
    return {
      ok: false,
      failures: [`${label} did not return a valid Response object.`],
      jobs: [],
      searchId: undefined,
    };
  }

  if (!response.ok) {
    failures.push(
      `${label} endpoint returned ${response.status}: ${extractErrorMessage(payload)}`,
    );
  }

  const shapeResult = sharedModules.crawlResponseSchema.safeParse(payload);

  if (!shapeResult.success) {
    failures.push(
      `${label} response failed ${VERIFY_CRAWL_CONFIG.expectedResponseShapeName} validation: ${formatZodIssues(shapeResult.error.issues)}`,
    );
  }

  const jobs = extractJobs(shapeResult.success ? shapeResult.data : payload);
  const searchId = extractSearchId(shapeResult.success ? shapeResult.data : payload);

  if (options.requireJobs && jobs.length < 1) {
    failures.push(`${label} endpoint returned zero jobs.`);
  }

  return {
    ok: failures.length === 0,
    failures,
    jobs,
    searchId,
  };
}

function updateSummaryFromJobs(summary, jobs, sharedModules) {
  summary.totalJobsFound = jobs.length;

  const sampleValidation = validateSampleJobs(
    jobs,
    Math.min(VERIFY_CRAWL_CONFIG.sampleSize, jobs.length || VERIFY_CRAWL_CONFIG.sampleSize),
    sharedModules.jobPostingUrl,
  );
  summary.sampledValidJobsCount = sampleValidation.validCount;
  summary.invalidJobsCount = sampleValidation.invalidCount;

  const duplicateAnalysis = analyzeDuplicates(jobs, sharedModules.dedupeJobs);
  summary.duplicateCount = duplicateAnalysis.count;

  return {
    sampleValidation,
    duplicateOnlyReason: duplicateAnalysis.duplicateOnlyReason,
  };
}

function finish(summary, failures, diagnostics) {
  if (summary.totalJobsFound < 1) {
    failures.push("Crawler returned zero jobs.");
  }

  const sampleValidation = summary.totalJobsFound
    ? summary.sampledValidJobsCount + summary.invalidJobsCount
    : 0;
  if (sampleValidation < 1 && summary.totalJobsFound > 0) {
    failures.push("No sampled jobs were available for validation.");
  }

  if (summary.invalidJobsCount > 0) {
    failures.push(`Found ${summary.invalidJobsCount} invalid sampled job(s).`);
  }

  if (!summary.apiValidationPassed) {
    failures.push("API validation did not pass.");
  }

  summary.finalStatus = failures.length === 0 ? "PASS" : "FAIL";

  printSummary(summary, failures, diagnostics);
  process.exitCode = summary.finalStatus === "PASS" ? 0 : 1;
}

function printSummary(summary, failures, diagnostics) {
  const sampledCount = summary.sampledValidJobsCount + summary.invalidJobsCount;
  console.log("verify-crawl");
  console.log(
    `query: ${VERIFY_CRAWL_CONFIG.query.title} | ${VERIFY_CRAWL_CONFIG.query.country ?? "anywhere"} | ${VERIFY_CRAWL_CONFIG.query.platforms.join(", ")}`,
  );
  console.log(`api mode: ${VERIFY_CRAWL_CONFIG.api.mode}`);
  console.log(`total jobs found: ${summary.totalJobsFound}`);
  console.log(
    `sampled valid jobs count: ${summary.sampledValidJobsCount}/${sampledCount || 0}`,
  );
  console.log(`invalid jobs count: ${summary.invalidJobsCount}`);
  console.log(`duplicate count: ${summary.duplicateCount}`);
  console.log(`API validation passed: ${summary.apiValidationPassed ? "yes" : "no"}`);

  if (failures.length > 0) {
    console.log("failure reasons:");
    for (const failure of dedupeMessages(failures)) {
      console.log(`- ${failure}`);
    }

    const recentDiagnostics = diagnostics.slice(-5);
    if (recentDiagnostics.length > 0) {
      console.log("recent diagnostics:");
      for (const entry of recentDiagnostics) {
        console.log(`- [${entry.level}] ${entry.message}`);
      }
    }
  }

  console.log(summary.finalStatus);
}

function validateSampleJobs(jobs, sampleSize, jobPostingUrl) {
  const sample = pickSample(jobs, sampleSize);
  const failures = [];
  let validCount = 0;

  sample.forEach((job, index) => {
    const problems = [];
    const title = readNonEmptyString(job?.title);
    const company = readNonEmptyString(job?.company);
    const applyUrl = readNonEmptyString(job?.applyUrl);
    const link =
      applyUrl ??
      readNonEmptyString(
        safeJobPostingUrl(jobPostingUrl, job),
      );

    if (!title) {
      problems.push("missing title");
    }

    if (!company) {
      problems.push("missing company");
    }

    if (!link) {
      problems.push("missing link");
    } else if (!isHttpUrl(link)) {
      problems.push(`invalid link "${link}"`);
    }

    if (problems.length > 0) {
      failures.push(
        `Sample job ${index + 1} (${describeJob(job)}) ${problems.join(", ")}.`,
      );
      return;
    }

    validCount += 1;
  });

  return {
    validCount,
    invalidCount: failures.length,
    failures,
  };
}

function pickSample(items, size) {
  if (!Array.isArray(items) || items.length === 0 || size <= 0) {
    return [];
  }

  if (items.length <= size) {
    return items;
  }

  const indexes = new Set();
  for (let index = 0; index < size; index += 1) {
    const position = Math.floor((index * (items.length - 1)) / Math.max(size - 1, 1));
    indexes.add(position);
  }

  return Array.from(indexes)
    .sort((left, right) => left - right)
    .map((index) => items[index]);
}

function countProjectDuplicates(jobs, dedupeJobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    return 0;
  }

  try {
    const candidates = jobs.map(stripJobForDedupe);
    return Math.max(0, candidates.length - dedupeJobs(candidates).length);
  } catch {
    const signatures = new Set(
      jobs.map((job) =>
        [
          normalizeComparableText(job?.title),
          normalizeComparableText(job?.company),
          normalizeComparableText(job?.locationText),
          normalizeComparableText(job?.applyUrl),
        ].join("|"),
      ),
    );
    return Math.max(0, jobs.length - signatures.size);
  }
}

function analyzeDuplicates(jobs, dedupeJobs) {
  const count = countProjectDuplicates(jobs, dedupeJobs);
  const obviousDuplicateSignatures = new Set(
    jobs.map((job) =>
      [
        normalizeComparableText(job?.title),
        normalizeComparableText(job?.company),
        normalizeComparableText(job?.locationText),
      ].join("|"),
    ),
  );
  const duplicateOnlyReason =
    jobs.length > 1 && obviousDuplicateSignatures.size === 1
      ? "Results look duplicate-only: every job shares the same normalized title, company, and location signature."
      : undefined;

  return {
    count,
    duplicateOnlyReason,
  };
}

function stripJobForDedupe(job) {
  const { _id, crawlRunIds, ...candidate } = job ?? {};
  return candidate;
}

async function withCapturedConsole(operation) {
  const logs = [];
  const originalConsole = {
    info: console.info,
    warn: console.warn,
    error: console.error,
  };

  console.info = (...args) => {
    logs.push({
      level: "info",
      message: formatConsoleArgs(args),
    });
  };
  console.warn = (...args) => {
    logs.push({
      level: "warn",
      message: formatConsoleArgs(args),
    });
  };
  console.error = (...args) => {
    logs.push({
      level: "error",
      message: formatConsoleArgs(args),
    });
  };

  try {
    return {
      value: await operation(),
      logs,
    };
  } finally {
    console.info = originalConsole.info;
    console.warn = originalConsole.warn;
    console.error = originalConsole.error;
  }
}

async function readResponsePayload(response) {
  const text = await response.text();

  if (!text) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch {
    return {
      error: text,
    };
  }
}

function buildDetailsPath(searchId) {
  return VERIFY_CRAWL_CONFIG.api.detailsPathTemplate.replace(
    ":id",
    encodeURIComponent(searchId),
  );
}

function extractJobs(payload) {
  return Array.isArray(payload?.jobs) ? payload.jobs : [];
}

function extractSearchId(payload) {
  return readNonEmptyString(payload?.search?._id);
}

function extractErrorMessage(payload) {
  const readableErrors = Array.isArray(payload?.readableErrors)
    ? payload.readableErrors.filter(hasNonEmptyString)
    : [];

  if (hasNonEmptyString(payload?.error) && readableErrors.length > 0) {
    return `${payload.error.trim()} ${readableErrors.join("; ")}`;
  }

  if (hasNonEmptyString(payload?.error)) {
    return payload.error.trim();
  }

  return "No error payload was returned.";
}

function safeJobPostingUrl(jobPostingUrl, job) {
  try {
    return typeof jobPostingUrl === "function" ? jobPostingUrl(job) : undefined;
  } catch {
    return undefined;
  }
}

function isHttpUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function describeJob(job) {
  const title = readNonEmptyString(job?.title) ?? "untitled";
  const company = readNonEmptyString(job?.company) ?? "unknown company";
  return `${title} @ ${company}`;
}

function normalizeComparableText(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseCsv(value) {
  if (!hasNonEmptyString(value)) {
    return undefined;
  }

  const items = value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  return items.length > 0 ? items : undefined;
}

function parsePositiveInt(value, fallback) {
  if (!hasNonEmptyString(value)) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function hasNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function readNonEmptyString(value) {
  return hasNonEmptyString(value) ? value.trim() : undefined;
}

function formatZodIssues(issues) {
  return issues
    .slice(0, 3)
    .map((issue) => {
      const pathLabel = issue.path.length > 0 ? issue.path.join(".") : "root";
      return `${pathLabel}: ${issue.message}`;
    })
    .join("; ");
}

function formatUnexpectedError(error) {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }

  return String(error);
}

function formatConsoleArgs(args) {
  return args
    .map((arg) => {
      if (typeof arg === "string") {
        return arg;
      }

      try {
        return JSON.stringify(arg);
      } catch {
        return String(arg);
      }
    })
    .join(" ");
}

function dedupeMessages(messages) {
  return Array.from(new Set(messages.filter(Boolean)));
}

main().catch((error) => {
  console.log("verify-crawl");
  console.log(`query: ${VERIFY_CRAWL_CONFIG.query.title} | ${VERIFY_CRAWL_CONFIG.query.country ?? "anywhere"} | ${VERIFY_CRAWL_CONFIG.query.platforms.join(", ")}`);
  console.log("total jobs found: 0");
  console.log("sampled valid jobs count: 0/0");
  console.log("invalid jobs count: 0");
  console.log("duplicate count: 0");
  console.log("API validation passed: no");
  console.log("failure reasons:");
  console.log(`- ${formatUnexpectedError(error)}`);
  console.log("FAIL");
  process.exitCode = 1;
});
