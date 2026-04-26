import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  getMemoryDbMock,
  getMongoDbMock,
} = vi.hoisted(() => ({
  getMemoryDbMock: vi.fn(),
  getMongoDbMock: vi.fn(),
}));

vi.mock("@/lib/server/mongodb", () => ({
  getMongoDb: getMongoDbMock,
}));

vi.mock("@/lib/server/db/memory", () => ({
  getMemoryDb: getMemoryDbMock,
}));

import { listRecentSearchesForApi } from "@/lib/server/search/recent-searches";

describe("recent search API reads", () => {
  beforeEach(() => {
    getMemoryDbMock.mockReset();
    getMongoDbMock.mockReset();
  });

  it("reads recent searches without requesting Mongo index bootstrap", async () => {
    const userSearch = {
      _id: "search-1",
      filters: {
        title: "Software Engineer",
        country: "United States",
      },
      createdAt: "2026-04-15T12:00:00.000Z",
      updatedAt: "2026-04-15T12:00:00.000Z",
    };
    const systemSearch = {
      _id: "search-2",
      systemProfileId: "software_engineer__software_engineer__us_wa",
      filters: {
        title: "software engineer",
        country: "United States",
      },
    };
    const legacyBackgroundSearch = {
      _id: "search-3",
      filters: {
        title: "Background Inventory Refresh",
        crawlMode: "deep",
      },
    };
    const toArray = vi.fn().mockResolvedValue([
      userSearch,
      systemSearch,
      legacyBackgroundSearch,
    ]);
    const find = vi.fn(() => ({ toArray }));
    const collection = { find };
    const db = {
      collection: vi.fn(() => collection),
    };
    getMongoDbMock.mockResolvedValue(db);

    const searches = await listRecentSearchesForApi();

    expect(searches).toEqual([userSearch]);
    expect(getMongoDbMock).toHaveBeenCalledWith({ ensureIndexes: false });
    expect(db.collection).toHaveBeenCalledWith("searches");
    expect(find).toHaveBeenCalledWith({}, { sort: { createdAt: -1 }, limit: 6 });
    expect(getMemoryDbMock).not.toHaveBeenCalled();
  });
});
