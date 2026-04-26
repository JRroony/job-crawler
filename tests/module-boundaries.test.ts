import { beforeEach, describe, expect, it, vi } from "vitest";

import { FakeDb } from "@/tests/helpers/fake-db";

describe("module boundaries for Node-only persistence", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.restoreAllMocks();
    vi.doUnmock("mongodb");
    vi.doUnmock("@/lib/server/mongodb");
    vi.doUnmock("@/lib/server/background/recurring-ingestion");
    vi.doUnmock("@/lib/server/db/indexes");
    vi.unmock("mongodb");
    vi.unmock("@/lib/server/mongodb");
    vi.unmock("@/lib/server/background/recurring-ingestion");
    vi.unmock("@/lib/server/db/indexes");
    (
      globalThis as typeof globalThis & {
        __jobCrawlerMongoClientPromise?: Promise<unknown>;
      }
    ).__jobCrawlerMongoClientPromise = undefined;
  });

  it("keeps recurring ingestion out of instrumentation.node until runtime registration", async () => {
    const recurringModuleLoaded = vi.fn();
    const startSchedulerMock = vi.fn();

    vi.doMock("@/lib/server/background/recurring-ingestion", () => {
      recurringModuleLoaded();
      return {
        startRecurringBackgroundIngestionScheduler: startSchedulerMock,
      };
    });

    const instrumentationNode = await import("../instrumentation.node");

    expect(recurringModuleLoaded).not.toHaveBeenCalled();

    await instrumentationNode.registerNodeInstrumentation();

    expect(recurringModuleLoaded).toHaveBeenCalledTimes(1);
    expect(startSchedulerMock).toHaveBeenCalledTimes(1);
  });

  it("does not load MongoDB just by importing the repository module or using an injected db", async () => {
    const mongoModuleLoaded = vi.fn();
    const getMongoDbMock = vi.fn();

    vi.doMock("@/lib/server/mongodb", () => {
      mongoModuleLoaded();
      return {
        getMongoDb: getMongoDbMock,
      };
    });

    const repositoryModule = await import("@/lib/server/db/repository");

    expect(mongoModuleLoaded).not.toHaveBeenCalled();

    const repository = new repositoryModule.JobCrawlerRepository(new FakeDb());
    expect(repository).toBeInstanceOf(repositoryModule.JobCrawlerRepository);

    await repositoryModule.getRepository(new FakeDb());

    expect(mongoModuleLoaded).not.toHaveBeenCalled();
    expect(getMongoDbMock).not.toHaveBeenCalled();
  });

  it("loads MongoDB lazily when durable repository resolution is requested", async () => {
    const mongoModuleLoaded = vi.fn();
    const getMongoDbMock = vi.fn().mockRejectedValue(new Error("Mongo offline"));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    vi.doMock("@/lib/server/mongodb", () => {
      mongoModuleLoaded();
      return {
        getMongoDb: getMongoDbMock,
      };
    });

    const repositoryModule = await import("@/lib/server/db/repository");

    expect(mongoModuleLoaded).not.toHaveBeenCalled();

    const repository = await repositoryModule.getRepository();

    expect(mongoModuleLoaded).toHaveBeenCalledTimes(1);
    expect(getMongoDbMock).toHaveBeenCalledTimes(1);
    expect(repository).toBeInstanceOf(repositoryModule.JobCrawlerRepository);
    expect(warnSpy).toHaveBeenCalled();
  });

  it("can resolve MongoDB without loading index bootstrap when explicitly requested", async () => {
    const indexesModuleLoaded = vi.fn();
    const ensureDatabaseIndexesMock = vi.fn();
    const db = {
      collection: vi.fn(),
    };
    const client = {
      db: vi.fn(() => db),
    };
    const connectMock = vi.fn().mockResolvedValue(client);
    const MongoClientMock = vi.fn(() => ({
      connect: connectMock,
    }));

    vi.doMock("mongodb", () => ({
      MongoClient: MongoClientMock,
    }));
    vi.doMock("@/lib/server/db/indexes", () => {
      indexesModuleLoaded();
      return {
        ensureDatabaseIndexes: ensureDatabaseIndexesMock,
      };
    });

    const { getMongoDb } = await import("@/lib/server/mongodb");
    await getMongoDb({ ensureIndexes: false });

    expect(MongoClientMock).toHaveBeenCalledTimes(1);
    expect(connectMock).toHaveBeenCalledTimes(1);
    expect(client.db).toHaveBeenCalledTimes(1);
    expect(indexesModuleLoaded).not.toHaveBeenCalled();
    expect(ensureDatabaseIndexesMock).not.toHaveBeenCalled();
  });
});
