import "server-only";

import type { Db, MongoClient } from "mongodb";

import { getEnv } from "@/lib/server/env";

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerMongoClientPromise: Promise<MongoClient> | undefined;
  // eslint-disable-next-line no-var
  var __jobCrawlerMongoBootstrapState: MongoBootstrapState | undefined;
}

type MongoDbOptions = {
  ensureIndexes?: boolean;
  requireIndexes?: boolean;
  bootstrapRetryDelaysMs?: readonly number[];
};

function databaseNameFromUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

type MongoBootstrapStatus = "pending" | "running" | "succeeded" | "failed";
type MongoBootstrapFailureReason = "bootstrap_running" | "mongo_transient" | "bootstrap_failed";
type MongoBootstrapPhase = "connection" | "index_initialization";

type MongoBootstrapState = {
  status: MongoBootstrapStatus;
  promise?: Promise<void>;
  lastAttemptAt?: string;
  lastSuccessAt?: string;
  lastError?: string;
  lastErrorName?: string;
  lastErrorCode?: unknown;
  lastFailureReason?: MongoBootstrapFailureReason;
  lastPhase?: MongoBootstrapPhase;
};

export type MongoBootstrapStateSnapshot = Omit<MongoBootstrapState, "promise">;

const defaultBootstrapRetryDelaysMs = [500, 1500, 3000] as const;
let mongoUnavailableUntil = 0;

export async function getMongoDb(options: MongoDbOptions = {}): Promise<Db> {
  const env = getEnv();
  if (mongoUnavailableUntil > Date.now()) {
    throw new Error("MongoDB connection is in cooldown after a recent failure.");
  }

  const client = await getMongoClient(env);
  const dbName = databaseNameFromUri(env.MONGODB_URI);
  const db = client.db(dbName);
  const shouldEnsureIndexes = options.ensureIndexes ?? true;

  if (!shouldEnsureIndexes) {
    return db;
  }

  try {
    await ensureMongoDbBootstrap(db, {
      dbName,
      uri: env.MONGODB_URI,
      retryDelaysMs: options.bootstrapRetryDelaysMs,
    });
  } catch (error) {
    if (options.requireIndexes) {
      throw error;
    }
  }

  return db;
}

export async function ensureMongoDbBootstrap(
  db: Db,
  options: {
    uri?: string;
    dbName?: string;
    retryDelaysMs?: readonly number[];
  } = {},
) {
  const env = getEnv();
  const state = getMutableBootstrapState();
  if (state.status === "succeeded") {
    return;
  }

  if (state.status === "running" && state.promise) {
    return state.promise;
  }

  const uri = options.uri ?? env.MONGODB_URI;
  const dbName = options.dbName ?? databaseNameFromUri(uri);
  const retryDelaysMs = options.retryDelaysMs ?? defaultBootstrapRetryDelaysMs;
  const promise = runBootstrapWithRetry(db, {
    uri,
    dbName,
    retryDelaysMs,
    state,
  }).finally(() => {
    if (state.promise === promise) {
      state.promise = undefined;
    }
  });

  state.status = "running";
  state.promise = promise;
  state.lastAttemptAt = new Date().toISOString();
  state.lastError = undefined;
  state.lastErrorName = undefined;
  state.lastErrorCode = undefined;
  state.lastFailureReason = undefined;
  state.lastPhase = "index_initialization";

  return promise;
}

export function getMongoBootstrapState(): MongoBootstrapStateSnapshot {
  const { promise: _promise, ...snapshot } = getMutableBootstrapState();
  return { ...snapshot };
}

export function resetMongoBootstrapStateForTests() {
  globalThis.__jobCrawlerMongoBootstrapState = undefined;
  globalThis.__jobCrawlerMongoClientPromise = undefined;
  mongoUnavailableUntil = 0;
}

export class MongoBootstrapError extends Error {
  readonly reason: MongoBootstrapFailureReason;
  readonly phase: MongoBootstrapPhase;
  readonly retryable: boolean;
  readonly code?: unknown;

  constructor(
    message: string,
    input: {
      reason: MongoBootstrapFailureReason;
      phase: MongoBootstrapPhase;
      retryable: boolean;
      code?: unknown;
      cause?: unknown;
    },
  ) {
    super(message, { cause: input.cause });
    this.name = "MongoBootstrapError";
    this.reason = input.reason;
    this.phase = input.phase;
    this.retryable = input.retryable;
    this.code = input.code;
  }
}

export function isMongoBootstrapError(error: unknown): error is MongoBootstrapError {
  return error instanceof Error && error.name === "MongoBootstrapError";
}

async function runBootstrapWithRetry(
  db: Db,
  input: {
    uri: string;
    dbName: string;
    retryDelaysMs: readonly number[];
    state: MongoBootstrapState;
  },
) {
  const startedMs = Date.now();
  const maxAttempts = Math.max(1, input.retryDelaysMs.length);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    input.state.lastAttemptAt = new Date().toISOString();
    console.info("[db:bootstrap-start]", {
      attempt,
      ensureIndexes: true,
      uriHost: uriHostFromMongoUri(input.uri),
      dbName: input.dbName,
    });

    try {
      const { ensureDatabaseIndexes } = await import("@/lib/server/db/indexes");
      await ensureDatabaseIndexes(db);
      input.state.status = "succeeded";
      input.state.lastSuccessAt = new Date().toISOString();
      input.state.lastError = undefined;
      input.state.lastErrorName = undefined;
      input.state.lastErrorCode = undefined;
      input.state.lastFailureReason = undefined;
      input.state.lastPhase = "index_initialization";
      console.info("[db:bootstrap-succeeded]", {
        durationMs: Date.now() - startedMs,
      });
      return;
    } catch (error) {
      const retryable = isTransientMongoBootstrapError(error);
      const willRetry = retryable && attempt < maxAttempts;
      const diagnostics = mongoErrorDiagnostics(error);
      input.state.status = willRetry ? "running" : "failed";
      input.state.lastError = diagnostics.errorMessage;
      input.state.lastErrorName = diagnostics.errorName;
      input.state.lastErrorCode = diagnostics.code;
      input.state.lastFailureReason = retryable ? "mongo_transient" : "bootstrap_failed";
      input.state.lastPhase = "index_initialization";

      console.warn("[db:bootstrap-failed]", {
        phase: "index_initialization",
        errorName: diagnostics.errorName,
        errorMessage: diagnostics.errorMessage,
        code: diagnostics.code,
        retryable,
        attempt,
        willRetry,
      });

      if (!willRetry) {
        throw new MongoBootstrapError(
          `MongoDB bootstrap failed during migration/index initialization: ${diagnostics.errorMessage}`,
          {
            reason: retryable ? "mongo_transient" : "bootstrap_failed",
            phase: "index_initialization",
            retryable,
            code: diagnostics.code,
            cause: error,
          },
        );
      }

      await sleep(input.retryDelaysMs[attempt - 1] ?? input.retryDelaysMs[input.retryDelaysMs.length - 1] ?? 0);
    }
  }
}

function getMutableBootstrapState(): MongoBootstrapState {
  if (!globalThis.__jobCrawlerMongoBootstrapState) {
    globalThis.__jobCrawlerMongoBootstrapState = {
      status: "pending",
    };
  }

  return globalThis.__jobCrawlerMongoBootstrapState;
}

async function getMongoClient(env: ReturnType<typeof getEnv>): Promise<MongoClient> {
  const cachedPromise = globalThis.__jobCrawlerMongoClientPromise;
  if (cachedPromise) {
    return cachedPromise;
  }

  const { MongoClient } = await import("mongodb");
  const connectionPromise = new MongoClient(env.MONGODB_URI, {
    serverSelectionTimeoutMS: env.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
  })
    .connect()
    .catch((error) => {
      if (globalThis.__jobCrawlerMongoClientPromise === connectionPromise) {
        globalThis.__jobCrawlerMongoClientPromise = undefined;
      }

      mongoUnavailableUntil =
        Date.now() + env.MONGODB_UNAVAILABLE_COOLDOWN_MS;

      throw error;
    });

  globalThis.__jobCrawlerMongoClientPromise = connectionPromise;
  return connectionPromise;
}

function uriHostFromMongoUri(uri: string) {
  try {
    return new URL(uri).host;
  } catch {
    return "unknown";
  }
}

function mongoErrorDiagnostics(error: unknown) {
  const record = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const cause = record.cause && typeof record.cause === "object"
    ? record.cause as Record<string, unknown>
    : undefined;
  const rootCause = cause?.cause && typeof cause.cause === "object"
    ? cause.cause as Record<string, unknown>
    : undefined;

  return {
    errorName:
      error instanceof Error
        ? error.name
        : typeof record.name === "string"
          ? record.name
          : "UnknownError",
    errorMessage: error instanceof Error ? error.message : String(error),
    code: record.code ?? cause?.code ?? rootCause?.code,
    codeName: record.codeName ?? cause?.codeName ?? rootCause?.codeName,
    errorLabels: [
      ...asStringArray(record.errorLabels),
      ...asStringArray(cause?.errorLabels),
      ...asStringArray(rootCause?.errorLabels),
    ],
  };
}

function isTransientMongoBootstrapError(error: unknown) {
  if (isDuplicateKeyOrMigrationDataError(error)) {
    return false;
  }

  const diagnostics = mongoErrorDiagnostics(error);
  const haystack = [
    diagnostics.errorName,
    diagnostics.errorMessage,
    diagnostics.codeName,
    ...diagnostics.errorLabels,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return (
    haystack.includes("mongoserverselectionerror") ||
    haystack.includes("mongonetworkerror") ||
    haystack.includes("mongonetworktimeouterror") ||
    haystack.includes("server selection") ||
    haystack.includes("connection pool") ||
    haystack.includes("pool") && haystack.includes("cleared") ||
    haystack.includes("connection") && haystack.includes("timed out") ||
    haystack.includes("etimedout") ||
    haystack.includes("econnreset") ||
    haystack.includes("econnrefused") ||
    haystack.includes("retryablewriteerror") ||
    haystack.includes("transienttransactionerror")
  );
}

function isDuplicateKeyOrMigrationDataError(error: unknown) {
  const diagnostics = mongoErrorDiagnostics(error);
  const haystack = [
    diagnostics.errorName,
    diagnostics.errorMessage,
    diagnostics.codeName,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return (
    diagnostics.code === 11000 ||
    haystack.includes("duplicate key") ||
    haystack.includes("e11000") ||
    haystack.includes("jobscanonicalkeymigrationerror")
  );
}

function asStringArray(value: unknown) {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string")
    : [];
}

async function sleep(ms: number) {
  if (ms <= 0) {
    await Promise.resolve();
    return;
  }

  await new Promise<void>((resolve) => {
    const timeout = setTimeout(resolve, ms);
    timeout.unref?.();
  });
}
