import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";

import { MongoClient, type Collection, type Document, type Filter } from "mongodb";

type SearchDiagnosticScenario = {
  title: string;
  location: string;
};

type CliOptions = {
  title: string;
  location: string;
  scenarios: SearchDiagnosticScenario[];
  usedDefaultScenarios: boolean;
};

type CoverageFilters = {
  strongUsFilter: Filter<Document>;
  rawUsLocationFallbackFilter: Filter<Document>;
  likelyUsFilter: Filter<Document>;
  exactAimlTitleFilter: Filter<Document>;
  aliasAimlTitleFilter: Filter<Document>;
  indexedAimlRoleFamilyFilter: Filter<Document>;
  rawAimlTitleFallbackFilter: Filter<Document>;
  likelyAimlFilter: Filter<Document>;
  likelyUsAndAimlFilter: Filter<Document>;
  hasSearchIndexFilter: Filter<Document>;
  missingSearchIndexFilter: Filter<Document>;
  hasTitleSearchKeysFilter: Filter<Document>;
  missingTitleSearchKeysFilter: Filter<Document>;
  hasLocationSearchKeysFilter: Filter<Document>;
  missingLocationSearchKeysFilter: Filter<Document>;
  likelyNonUsPassingRawFallbackFilter: Filter<Document>;
};

const prefix = "[search:coverage]";

const defaultMongoUri = "mongodb://127.0.0.1:27017/job_crawler";
const jobsCollectionName = "jobs";
const defaultSearchScenarios: SearchDiagnosticScenario[] = [
  { title: "software engineer", location: "United States" },
  { title: "data analyst", location: "United States" },
  { title: "business analyst", location: "United States" },
  { title: "product manager", location: "United States" },
];

const usStates = [
  ["AL", "Alabama"],
  ["AK", "Alaska"],
  ["AZ", "Arizona"],
  ["AR", "Arkansas"],
  ["CA", "California"],
  ["CO", "Colorado"],
  ["CT", "Connecticut"],
  ["DE", "Delaware"],
  ["FL", "Florida"],
  ["GA", "Georgia"],
  ["HI", "Hawaii"],
  ["ID", "Idaho"],
  ["IL", "Illinois"],
  ["IN", "Indiana"],
  ["IA", "Iowa"],
  ["KS", "Kansas"],
  ["KY", "Kentucky"],
  ["LA", "Louisiana"],
  ["ME", "Maine"],
  ["MD", "Maryland"],
  ["MA", "Massachusetts"],
  ["MI", "Michigan"],
  ["MN", "Minnesota"],
  ["MS", "Mississippi"],
  ["MO", "Missouri"],
  ["MT", "Montana"],
  ["NE", "Nebraska"],
  ["NV", "Nevada"],
  ["NH", "New Hampshire"],
  ["NJ", "New Jersey"],
  ["NM", "New Mexico"],
  ["NY", "New York"],
  ["NC", "North Carolina"],
  ["ND", "North Dakota"],
  ["OH", "Ohio"],
  ["OK", "Oklahoma"],
  ["OR", "Oregon"],
  ["PA", "Pennsylvania"],
  ["RI", "Rhode Island"],
  ["SC", "South Carolina"],
  ["SD", "South Dakota"],
  ["TN", "Tennessee"],
  ["TX", "Texas"],
  ["UT", "Utah"],
  ["VT", "Vermont"],
  ["VA", "Virginia"],
  ["WA", "Washington"],
  ["WV", "West Virginia"],
  ["WI", "Wisconsin"],
  ["WY", "Wyoming"],
  ["DC", "District of Columbia"],
] as const;

const usMetroSignals = [
  "Ann Arbor",
  "Arlington",
  "Atlanta",
  "Austin",
  "Bellevue",
  "Boston",
  "Boulder",
  "Cambridge",
  "Charlotte",
  "Chicago",
  "Cincinnati",
  "Columbus",
  "Dallas",
  "Denver",
  "Detroit",
  "Herndon",
  "Houston",
  "Irvine",
  "Jersey City",
  "Las Vegas",
  "Los Angeles",
  "Madison",
  "Miami",
  "Minneapolis",
  "Mountain View",
  "Nashville",
  "New York",
  "Oakland",
  "Palo Alto",
  "Philadelphia",
  "Phoenix",
  "Pittsburgh",
  "Portland",
  "Raleigh",
  "Redmond",
  "Redwood City",
  "Reston",
  "Sacramento",
  "Salt Lake City",
  "San Antonio",
  "San Diego",
  "San Francisco",
  "San Jose",
  "Santa Clara",
  "Seattle",
  "Sunnyvale",
  "Tampa",
  "Tempe",
  "Washington DC",
];

const aimlTitleAliases = [
  "AI Engineer",
  "Applied AI Engineer",
  "Applied ML Engineer",
  "Computer Vision Engineer",
  "Deep Learning Engineer",
  "Generative AI Engineer",
  "LLM Engineer",
  "Machine Learning Engineer",
  "ML Engineer",
  "NLP Engineer",
  "Research Engineer ML",
];

const exactAimlTitle = "Machine Learning Engineer";
const nonUsCountrySignals = [
  "Canada",
  "United Kingdom",
  "UK",
  "Germany",
  "India",
  "Israel",
  "Netherlands",
  "Australia",
  "Singapore",
  "France",
  "Ireland",
];

export function parseDiagnoseSearchArgs(argv: string[]): CliOptions {
  const options: Partial<CliOptions> = {};

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    const next = argv[index + 1];

    if (value === "--title" && next) {
      options.title = next;
      index += 1;
      continue;
    }

    if (value === "--location" && next) {
      options.location = next;
      index += 1;
    }
  }

  if (!options.title && !options.location) {
    const firstScenario = defaultSearchScenarios[0]!;
    return {
      title: firstScenario.title,
      location: firstScenario.location,
      scenarios: defaultSearchScenarios,
      usedDefaultScenarios: true,
    };
  }

  if (!options.title || !options.location) {
    throw new Error(
      'Usage: npm run diagnose:search -- --title "machine learning engineer" --location "United States"',
    );
  }

  return {
    title: options.title,
    location: options.location,
    scenarios: [
      {
        title: options.title,
        location: options.location,
      },
    ],
    usedDefaultScenarios: false,
  };
}

export function databaseNameFromMongoUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

export function normalizeDiagnosticText(value?: string) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function buildCoverageFilters(): CoverageFilters {
  const countryUnitedStatesFilter = exactTextFilter("country", "United States");
  const resolvedUnitedStatesFilter = {
    "resolvedLocation.isUnitedStates": true,
  };
  const searchIndexUnitedStatesFilter = {
    "searchIndex.locationSearchKeys": "country:united states",
  };
  const rawUsLocationFallbackFilter = buildRawUsLocationFallbackFilter();
  const strongUsFilter = {
    $or: [
      countryUnitedStatesFilter,
      resolvedUnitedStatesFilter,
      searchIndexUnitedStatesFilter,
    ],
  };
  const likelyUsFilter = {
    $or: [
      countryUnitedStatesFilter,
      resolvedUnitedStatesFilter,
      searchIndexUnitedStatesFilter,
      rawUsLocationFallbackFilter,
    ],
  };

  const exactAimlTitleFilter = buildExactTitleFilter(exactAimlTitle);
  const aliasAimlTitleFilter = buildAliasTitleFilter(
    aimlTitleAliases.filter(
      (alias) =>
        normalizeDiagnosticText(alias) !== normalizeDiagnosticText(exactAimlTitle),
    ),
  );
  const indexedAimlRoleFamilyFilter = {
    $or: [
      { "searchIndex.titleFamily": "ai_ml_science" },
      { "searchIndex.titleSearchKeys": "family:ai_ml_science" },
      {
        "searchIndex.titleConceptIds": {
          $in: [
            "ai_engineer",
            "applied_scientist",
            "data_scientist",
            "machine_learning_engineer",
            "mlops_engineer",
            "research_scientist",
          ],
        },
      },
    ],
  };
  const rawAimlTitleFallbackFilter = buildRawAimlTitleFallbackFilter();
  const likelyAimlFilter = {
    $or: [
      exactAimlTitleFilter,
      aliasAimlTitleFilter,
      indexedAimlRoleFamilyFilter,
      rawAimlTitleFallbackFilter,
    ],
  };
  const likelyUsAndAimlFilter = {
    $and: [likelyUsFilter, likelyAimlFilter],
  };
  const hasSearchIndexFilter = {
    searchIndex: { $exists: true, $ne: null },
  };
  const missingSearchIndexFilter = {
    $or: [{ searchIndex: { $exists: false } }, { searchIndex: null }],
  };
  const hasTitleSearchKeysFilter = {
    "searchIndex.titleSearchKeys.0": { $exists: true },
  };
  const missingTitleSearchKeysFilter = {
    $or: [
      { "searchIndex.titleSearchKeys": { $exists: false } },
      { "searchIndex.titleSearchKeys.0": { $exists: false } },
    ],
  };
  const hasLocationSearchKeysFilter = {
    "searchIndex.locationSearchKeys.0": { $exists: true },
  };
  const missingLocationSearchKeysFilter = {
    $or: [
      { "searchIndex.locationSearchKeys": { $exists: false } },
      { "searchIndex.locationSearchKeys.0": { $exists: false } },
    ],
  };
  const likelyNonUsPassingRawFallbackFilter = {
    $and: [
      rawUsLocationFallbackFilter,
      { $nor: [strongUsFilter] },
      buildNonUsEvidenceFilter(),
    ],
  };

  return {
    strongUsFilter,
    rawUsLocationFallbackFilter,
    likelyUsFilter,
    exactAimlTitleFilter,
    aliasAimlTitleFilter,
    indexedAimlRoleFamilyFilter,
    rawAimlTitleFallbackFilter,
    likelyAimlFilter,
    likelyUsAndAimlFilter,
    hasSearchIndexFilter,
    missingSearchIndexFilter,
    hasTitleSearchKeysFilter,
    missingTitleSearchKeysFilter,
    hasLocationSearchKeysFilter,
    missingLocationSearchKeysFilter,
    likelyNonUsPassingRawFallbackFilter,
  };
}

async function main() {
  installServerOnlyShim();
  const options = parseDiagnoseSearchArgs(process.argv.slice(2));
  const mongoUri = process.env.MONGODB_URI ?? defaultMongoUri;
  const serverSelectionTimeoutMS = Number(
    process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ?? "1500",
  );
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS });

  try {
    await client.connect();
    const dbName = databaseNameFromMongoUri(mongoUri);
    const db = client.db(dbName);
    const jobs = db.collection(jobsCollectionName);
    const filters = buildCoverageFilters();
    const [{ JobCrawlerRepository }, { getIndexedJobsForSearch }] = await Promise.all([
      import("@/lib/server/db/repository"),
      import("@/lib/server/search/indexed-jobs"),
    ]);
    const repository = new JobCrawlerRepository(db as never);

    const [
      indexedSearchScenarios,
      databaseCoverage,
      rawLocationCoverage,
      rawTitleCoverage,
      combinedCoverage,
      samples,
    ] = await Promise.all([
      buildIndexedSearchScenarioDiagnostics(
        repository,
        getIndexedJobsForSearch,
        options.scenarios,
      ),
      buildDatabaseCoverage(jobs, filters),
      buildRawLocationCoverage(jobs, filters),
      buildRawTitleCoverage(jobs, filters),
      buildCombinedCoverage(jobs, filters),
      buildSamples(jobs, filters),
    ]);
    const failures = indexedSearchScenarios
      .filter((scenario) => scenario.returnedCount <= 0)
      .map(
        (scenario) =>
          `No indexed jobs returned for ${scenario.title} / ${scenario.location}.`,
      );

    const report = {
      query: {
        title: options.title,
        location: options.location,
        usedDefaultScenarios: options.usedDefaultScenarios,
        scenarios: options.scenarios,
      },
      connection: {
        databaseName: dbName,
        collectionName: jobsCollectionName,
      },
      indexedSearchScenarios,
      databaseCoverage,
      rawLocationCoverage,
      rawTitleCoverage,
      combinedCoverage,
      samples,
      failures,
      pass: failures.length === 0,
      notes: [
        "Indexed search scenarios call the DB-backed search index directly and do not run providers.",
        "Counts are direct MongoDB diagnostics and do not call the application search pipeline.",
        "Raw fallback counts are explainability probes; they are intentionally not search behavior changes.",
      ],
    };

    const output = `${prefix} ${JSON.stringify(report, null, 2)}`;
    if (failures.length > 0) {
      console.error(output);
      process.exitCode = 1;
      return;
    }

    console.log(output);
  } finally {
    await client.close();
  }
}

async function buildIndexedSearchScenarioDiagnostics(
  repository: unknown,
  getIndexedJobsForSearch: (
    repository: never,
    filters: { title: string; country: string },
  ) => Promise<{
    candidateCount: number;
    matchedCount: number;
    timingsMs: {
      candidateQuery: number;
      requestTimeRefinement: number;
      total: number;
    };
    matches: Array<{
      job: {
        _id: string;
        title: string;
        company: string;
        locationText?: string;
        country?: string;
        sourcePlatform?: string;
      };
    }>;
  }>,
  scenarios: SearchDiagnosticScenario[],
) {
  return Promise.all(
    scenarios.map(async (scenario) => {
      const indexedSearch = await getIndexedJobsForSearch(repository as never, {
        title: scenario.title,
        country: scenario.location,
      });

      return {
        title: scenario.title,
        location: scenario.location,
        dbSearchMs: indexedSearch.timingsMs.total,
        providerCrawlMs: 0,
        returnedCount: indexedSearch.matches.length,
        candidateCount: indexedSearch.candidateCount,
        matchedCount: indexedSearch.matchedCount,
        timing: {
          dbSearchMs: indexedSearch.timingsMs.total,
          candidateQueryMs: indexedSearch.timingsMs.candidateQuery,
          requestTimeRefinementMs: indexedSearch.timingsMs.requestTimeRefinement,
          providerCrawlMs: 0,
        },
        sampleJobs: indexedSearch.matches.slice(0, 5).map(({ job }) => ({
          id: job._id,
          title: job.title,
          company: job.company,
          locationText: job.locationText,
          country: job.country,
          sourcePlatform: job.sourcePlatform,
        })),
      };
    }),
  );
}

function installServerOnlyShim() {
  const require = createRequire(import.meta.url);
  const id = require.resolve("server-only");
  require.cache[id] = {
    id,
    filename: id,
    loaded: true,
    exports: {},
  } as NodeJS.Module;
}

async function buildDatabaseCoverage(
  jobs: Collection<Document>,
  filters: CoverageFilters,
) {
  const totalJobs = await jobs.countDocuments({});
  const [
    activeJobs,
    inactiveJobs,
    jobsWithSearchIndex,
    jobsWithTitleSearchKeys,
    jobsWithLocationSearchKeys,
    jobsWithResolvedLocation,
  ] = await Promise.all([
    jobs.countDocuments({ isActive: { $ne: false } }),
    jobs.countDocuments({ isActive: false }),
    jobs.countDocuments(filters.hasSearchIndexFilter),
    jobs.countDocuments(filters.hasTitleSearchKeysFilter),
    jobs.countDocuments(filters.hasLocationSearchKeysFilter),
    jobs.countDocuments({ resolvedLocation: { $exists: true, $ne: null } }),
  ]);

  return {
    totalJobs,
    activeJobs,
    inactiveJobs,
    jobsWithSearchIndex,
    jobsMissingSearchIndex: totalJobs - jobsWithSearchIndex,
    jobsWithTitleSearchKeys,
    jobsWithLocationSearchKeys,
    jobsWithResolvedLocation,
    jobsMissingResolvedLocation: totalJobs - jobsWithResolvedLocation,
  };
}

async function buildRawLocationCoverage(
  jobs: Collection<Document>,
  filters: CoverageFilters,
) {
  const [
    usByCountryField,
    usByResolvedLocation,
    usBySearchIndex,
    usByRawLocationFallback,
    totalLikelyUSJobs,
  ] = await Promise.all([
    jobs.countDocuments(exactTextFilter("country", "United States")),
    jobs.countDocuments({ "resolvedLocation.isUnitedStates": true }),
    jobs.countDocuments({ "searchIndex.locationSearchKeys": "country:united states" }),
    jobs.countDocuments(filters.rawUsLocationFallbackFilter),
    jobs.countDocuments(filters.likelyUsFilter),
  ]);

  return {
    usByCountryField,
    usByResolvedLocation,
    usBySearchIndex,
    usByRawLocationFallback,
    totalLikelyUSJobs,
  };
}

async function buildRawTitleCoverage(
  jobs: Collection<Document>,
  filters: CoverageFilters,
) {
  const [
    exactTitleCount,
    aliasTitleCount,
    indexedRoleFamilyCount,
    rawRegexFallbackCount,
    totalLikelyAIMLJobs,
  ] = await Promise.all([
    jobs.countDocuments(filters.exactAimlTitleFilter),
    jobs.countDocuments(filters.aliasAimlTitleFilter),
    jobs.countDocuments(filters.indexedAimlRoleFamilyFilter),
    jobs.countDocuments(filters.rawAimlTitleFallbackFilter),
    jobs.countDocuments(filters.likelyAimlFilter),
  ]);

  return {
    exactTitleCount,
    aliasTitleCount,
    indexedRoleFamilyCount,
    rawRegexFallbackCount,
    totalLikelyAIMLJobs,
  };
}

async function buildCombinedCoverage(
  jobs: Collection<Document>,
  filters: CoverageFilters,
) {
  const [
    likelyUSAndAIMLJobs,
    likelyUSAndAIMLJobsWithSearchIndex,
    likelyUSAndAIMLJobsMissingSearchIndex,
    likelyUSAndAIMLJobsMissingLocationKeys,
    likelyUSAndAIMLJobsMissingTitleKeys,
  ] = await Promise.all([
    jobs.countDocuments(filters.likelyUsAndAimlFilter),
    jobs.countDocuments({
      $and: [filters.likelyUsAndAimlFilter, filters.hasSearchIndexFilter],
    }),
    jobs.countDocuments({
      $and: [filters.likelyUsAndAimlFilter, filters.missingSearchIndexFilter],
    }),
    jobs.countDocuments({
      $and: [filters.likelyUsAndAimlFilter, filters.missingLocationSearchKeysFilter],
    }),
    jobs.countDocuments({
      $and: [filters.likelyUsAndAimlFilter, filters.missingTitleSearchKeysFilter],
    }),
  ]);

  return {
    likelyUSAndAIMLJobs,
    likelyUSAndAIMLJobsWithSearchIndex,
    likelyUSAndAIMLJobsMissingSearchIndex,
    likelyUSAndAIMLJobsMissingLocationKeys,
    likelyUSAndAIMLJobsMissingTitleKeys,
  };
}

async function buildSamples(
  jobs: Collection<Document>,
  filters: CoverageFilters,
) {
  const [
    likelyRelevantMissingSearchIndex,
    likelyRelevantMissingLocationSearchKeys,
    likelyRelevantMissingTitleSearchKeys,
    likelyNonUsJobsThatMightPassLocationFilter,
  ] = await Promise.all([
    sampleJobs(jobs, {
      $and: [filters.likelyUsAndAimlFilter, filters.missingSearchIndexFilter],
    }),
    sampleJobs(jobs, {
      $and: [filters.likelyUsAndAimlFilter, filters.missingLocationSearchKeysFilter],
    }),
    sampleJobs(jobs, {
      $and: [filters.likelyUsAndAimlFilter, filters.missingTitleSearchKeysFilter],
    }),
    sampleJobs(jobs, filters.likelyNonUsPassingRawFallbackFilter),
  ]);

  return {
    likelyRelevantMissingSearchIndex,
    likelyRelevantMissingLocationSearchKeys,
    likelyRelevantMissingTitleSearchKeys,
    likelyNonUsJobsThatMightPassLocationFilter,
  };
}

async function sampleJobs(
  jobs: Collection<Document>,
  filter: Filter<Document>,
) {
  const documents = await jobs
    .find(filter, {
      projection: {
        _id: 1,
        title: 1,
        company: 1,
        country: 1,
        state: 1,
        city: 1,
        locationText: 1,
        locationRaw: 1,
        normalizedLocation: 1,
        isActive: 1,
        sourcePlatform: 1,
        sourceUrl: 1,
        applyUrl: 1,
        canonicalUrl: 1,
        lastSeenAt: 1,
        discoveredAt: 1,
        resolvedLocation: 1,
        searchIndex: 1,
      },
    })
    .sort({ lastSeenAt: -1, discoveredAt: -1, _id: 1 })
    .limit(10)
    .toArray();

  return documents.map(formatSampleJob);
}

function formatSampleJob(document: Document) {
  const searchIndex = asRecord(document.searchIndex);
  const resolvedLocation = asRecord(document.resolvedLocation);

  return {
    id: String(document._id ?? ""),
    title: document.title,
    company: document.company,
    isActive: document.isActive ?? true,
    location: {
      country: document.country,
      state: document.state,
      city: document.city,
      locationText: document.locationText,
      locationRaw: document.locationRaw,
      normalizedLocation: document.normalizedLocation,
    },
    resolvedLocation: resolvedLocation
      ? {
          country: resolvedLocation.country,
          state: resolvedLocation.state,
          stateCode: resolvedLocation.stateCode,
          city: resolvedLocation.city,
          isRemote: resolvedLocation.isRemote,
          isUnitedStates: resolvedLocation.isUnitedStates,
          confidence: resolvedLocation.confidence,
        }
      : undefined,
    searchIndex: {
      hasSearchIndex: Boolean(searchIndex),
      titleFamily: searchIndex?.titleFamily,
      titleConceptIds: searchIndex?.titleConceptIds,
      titleSearchKeysCount: Array.isArray(searchIndex?.titleSearchKeys)
        ? searchIndex.titleSearchKeys.length
        : 0,
      locationSearchKeysCount: Array.isArray(searchIndex?.locationSearchKeys)
        ? searchIndex.locationSearchKeys.length
        : 0,
    },
    source: {
      platform: document.sourcePlatform,
      url: document.canonicalUrl ?? document.applyUrl ?? document.sourceUrl,
    },
  };
}

function buildRawUsLocationFallbackFilter(): Filter<Document> {
  const stateNames = usStates.map(([, name]) => name);
  const stateCodes = usStates.map(([code]) => code);
  const phrases = [
    "United States",
    "United States of America",
    "USA",
    "U.S.",
    "U.S.A.",
    "Remote US",
    "Remote USA",
    "Remote United States",
    "US Remote",
    "USA Remote",
    ...stateNames,
    ...usMetroSignals,
  ];
  const phraseRegex = boundaryRegex(phrases);
  const stateCodeRegex = new RegExp(
    `(^|[\\s,(/-])(${stateCodes.map(escapeRegExp).join("|")})($|[\\s,)/-])`,
  );

  return {
    $or: [
      ...textFieldsRegex(
        ["locationText", "locationRaw", "normalizedLocation", "locationNormalized"],
        phraseRegex,
      ),
      ...textFieldsRegex(
        ["locationText", "locationRaw", "normalizedLocation", "locationNormalized"],
        stateCodeRegex,
      ),
    ],
  };
}

function buildExactTitleFilter(title: string): Filter<Document> {
  const normalized = normalizeDiagnosticText(title);

  return {
    $or: [
      exactTextFilter("title", title),
      exactTextFilter("normalizedTitle", normalized),
      exactTextFilter("titleNormalized", normalized),
      exactTextFilter("searchIndex.titleNormalized", normalized),
      exactTextFilter("searchIndex.titleStrippedNormalized", normalized),
      { "searchIndex.titleSearchTerms": normalized },
    ],
  };
}

function buildAliasTitleFilter(aliases: string[]): Filter<Document> {
  const normalizedAliases = aliases.map(normalizeDiagnosticText);
  const titleRegex = boundaryRegex(aliases);

  return {
    $or: [
      ...textFieldsRegex(["title", "normalizedTitle", "titleNormalized"], titleRegex),
      { "searchIndex.titleSearchTerms": { $in: normalizedAliases } },
    ],
  };
}

function buildRawAimlTitleFallbackFilter(): Filter<Document> {
  const phraseRegex = boundaryRegex([
    ...aimlTitleAliases,
    "AI Research Engineer",
    "Artificial Intelligence Engineer",
    "GenAI Engineer",
    "Large Language Model Engineer",
    "Machine Learning Infrastructure Engineer",
    "MLOps Engineer",
  ]);
  const compactRegex = /\b(ai|genai|llm|ml|mlops|nlp)\b/i;
  const conceptRegex =
    /\b(machine learning|deep learning|computer vision|generative ai|artificial intelligence|large language model)\b/i;

  return {
    $or: [
      ...textFieldsRegex(["title", "normalizedTitle", "titleNormalized"], phraseRegex),
      ...textFieldsRegex(["title", "normalizedTitle", "titleNormalized"], compactRegex),
      ...textFieldsRegex(["title", "normalizedTitle", "titleNormalized"], conceptRegex),
    ],
  };
}

function buildNonUsEvidenceFilter(): Filter<Document> {
  const nonUsRegex = boundaryRegex(nonUsCountrySignals);

  return {
    $or: [
      ...nonUsCountrySignals.flatMap((country) => [
        exactTextFilter("country", country),
        exactTextFilter("resolvedLocation.country", country),
      ]),
      ...textFieldsRegex(
        ["locationText", "locationRaw", "normalizedLocation", "locationNormalized"],
        nonUsRegex,
      ),
    ],
  };
}

function textFieldsRegex(fields: string[], regex: RegExp) {
  return fields.map((field) => ({
    [field]: { $regex: regex },
  }));
}

function exactTextFilter(field: string, value: string): Filter<Document> {
  return {
    [field]: { $regex: new RegExp(`^${escapeRegExp(value)}$`, "i") },
  };
}

function boundaryRegex(values: string[]) {
  const pattern = values
    .map(normalizeDiagnosticText)
    .filter(Boolean)
    .sort((left, right) => right.length - left.length || left.localeCompare(right))
    .map(escapeRegExp)
    .join("|");

  return new RegExp(`(^|[^a-z0-9])(${pattern})($|[^a-z0-9])`, "i");
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function asRecord(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          error: error instanceof Error ? error.message : "Unknown diagnostic failure",
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
