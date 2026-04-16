import { beforeEach, describe, expect, it, vi } from "vitest";

import { FakeDb } from "@/tests/helpers/fake-db";

describe("module boundaries for Node-only persistence", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.restoreAllMocks();
    vi.unmock("@/lib/server/mongodb");
    vi.unmock("@/lib/server/background/recurring-ingestion");
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
});
