import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const {
  MongoClientMock,
  clientDbMock,
  connectMock,
  ensureDatabaseIndexesMock,
} = vi.hoisted(() => ({
  MongoClientMock: vi.fn(),
  clientDbMock: vi.fn(),
  connectMock: vi.fn(),
  ensureDatabaseIndexesMock: vi.fn(),
}));

vi.mock("mongodb", () => ({
  MongoClient: MongoClientMock,
}));

vi.mock("@/lib/server/db/indexes", () => ({
  ensureDatabaseIndexes: ensureDatabaseIndexesMock,
}));

let originalServerSelectionTimeout: string | undefined;
let originalUnavailableCooldown: string | undefined;
let currentDb: { collection: ReturnType<typeof vi.fn> };

describe("MongoDB bootstrap", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.restoreAllMocks();
    MongoClientMock.mockReset();
    clientDbMock.mockReset();
    connectMock.mockReset();
    ensureDatabaseIndexesMock.mockReset();
    originalServerSelectionTimeout = process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS;
    originalUnavailableCooldown = process.env.MONGODB_UNAVAILABLE_COOLDOWN_MS;
    delete process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS;
    delete process.env.MONGODB_UNAVAILABLE_COOLDOWN_MS;
    (
      globalThis as typeof globalThis & {
        __jobCrawlerMongoClientPromise?: Promise<unknown>;
        __jobCrawlerMongoBootstrapState?: unknown;
      }
    ).__jobCrawlerMongoClientPromise = undefined;
    (
      globalThis as typeof globalThis & {
        __jobCrawlerMongoClientPromise?: Promise<unknown>;
        __jobCrawlerMongoBootstrapState?: unknown;
      }
    ).__jobCrawlerMongoBootstrapState = undefined;

    currentDb = { collection: vi.fn() };
    clientDbMock.mockReturnValue(currentDb);
    connectMock.mockResolvedValue({ db: clientDbMock });
    MongoClientMock.mockImplementation(() => ({ connect: connectMock }));
  });

  afterEach(() => {
    if (originalServerSelectionTimeout === undefined) {
      delete process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS;
    } else {
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS = originalServerSelectionTimeout;
    }

    if (originalUnavailableCooldown === undefined) {
      delete process.env.MONGODB_UNAVAILABLE_COOLDOWN_MS;
    } else {
      process.env.MONGODB_UNAVAILABLE_COOLDOWN_MS = originalUnavailableCooldown;
    }
  });

  it("concurrent getMongoDb({ ensureIndexes: true }) only runs ensureDatabaseIndexes once", async () => {
    let resolveBootstrap: (() => void) | undefined;
    ensureDatabaseIndexesMock.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          resolveBootstrap = resolve;
        }),
    );

    const { getMongoDb } = await import("@/lib/server/mongodb");
    const first = getMongoDb({
      ensureIndexes: true,
      requireIndexes: true,
      bootstrapRetryDelaysMs: [0, 0, 0],
    });
    const second = getMongoDb({
      ensureIndexes: true,
      requireIndexes: true,
      bootstrapRetryDelaysMs: [0, 0, 0],
    });

    await vi.waitFor(() => {
      expect(ensureDatabaseIndexesMock).toHaveBeenCalledTimes(1);
    });
    resolveBootstrap?.();

    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    expect(ensureDatabaseIndexesMock).toHaveBeenCalledTimes(1);
    expect(MongoClientMock).toHaveBeenCalledWith(
      "mongodb://127.0.0.1:27017/job_crawler",
      expect.objectContaining({ serverSelectionTimeoutMS: 30000 }),
    );
  });

  it("retries transient Mongo timeouts during ensureIndexes", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const transientError = Object.assign(
      new Error(
        'Connection pool for 127.0.0.1:27017 was cleared because another operation failed with: "connection <monitor> to 127.0.0.1:27017 timed out"',
      ),
      { name: "MongoServerSelectionError" },
    );
    ensureDatabaseIndexesMock
      .mockRejectedValueOnce(transientError)
      .mockResolvedValueOnce(undefined);

    const { getMongoBootstrapState, getMongoDb } = await import("@/lib/server/mongodb");
    await getMongoDb({
      ensureIndexes: true,
      requireIndexes: true,
      bootstrapRetryDelaysMs: [0, 0, 0],
    });

    expect(ensureDatabaseIndexesMock).toHaveBeenCalledTimes(2);
    expect(getMongoBootstrapState()).toMatchObject({ status: "succeeded" });
    expect(warnSpy).toHaveBeenCalledWith(
      "[db:bootstrap-failed]",
      expect.objectContaining({
        retryable: true,
        attempt: 1,
        willRetry: true,
      }),
    );
  });

  it("does not retry duplicate-key migration errors endlessly", async () => {
    const duplicateKeyError = Object.assign(
      new Error("E11000 duplicate key error collection: job_crawler.jobs"),
      { code: 11000 },
    );
    ensureDatabaseIndexesMock.mockRejectedValue(duplicateKeyError);

    const { getMongoBootstrapState, getMongoDb } = await import("@/lib/server/mongodb");
    await expect(
      getMongoDb({
        ensureIndexes: true,
        requireIndexes: true,
        bootstrapRetryDelaysMs: [0, 0, 0],
      }),
    ).rejects.toMatchObject({
      name: "MongoBootstrapError",
      reason: "bootstrap_failed",
      retryable: false,
    });

    expect(ensureDatabaseIndexesMock).toHaveBeenCalledTimes(1);
    expect(getMongoBootstrapState()).toMatchObject({
      status: "failed",
      lastFailureReason: "bootstrap_failed",
    });
  });

  it("can return a working Db connection when optional index bootstrap fails", async () => {
    const duplicateKeyError = Object.assign(
      new Error("E11000 duplicate key error collection: job_crawler.jobs"),
      { code: 11000 },
    );
    ensureDatabaseIndexesMock.mockRejectedValue(duplicateKeyError);

    const { getMongoBootstrapState, getMongoDb } = await import("@/lib/server/mongodb");
    await expect(
      getMongoDb({
        ensureIndexes: true,
        bootstrapRetryDelaysMs: [0, 0, 0],
      }),
    ).resolves.toBe(currentDb);

    expect(ensureDatabaseIndexesMock).toHaveBeenCalledTimes(1);
    expect(getMongoBootstrapState()).toMatchObject({
      status: "failed",
      lastFailureReason: "bootstrap_failed",
    });
  });
});
