import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  isInputValidationErrorMock,
  listRecentSearchesMock,
  startSearchFromFiltersMock,
} = vi.hoisted(() => ({
  isInputValidationErrorMock: vi.fn(() => false),
  listRecentSearchesMock: vi.fn(),
  startSearchFromFiltersMock: vi.fn(),
}));

vi.mock("@/lib/server/crawler/service", () => ({
  isInputValidationError: isInputValidationErrorMock,
  listRecentSearches: listRecentSearchesMock,
  startSearchFromFilters: startSearchFromFiltersMock,
}));

import { POST } from "@/app/api/searches/route";

describe("search API normalization", () => {
  beforeEach(() => {
    isInputValidationErrorMock.mockReturnValue(false);
    listRecentSearchesMock.mockReset();
    startSearchFromFiltersMock.mockReset();
  });

  it("strips null optional filters and legacy experienceClassification before starting the crawl", async () => {
    startSearchFromFiltersMock.mockResolvedValue({
      queued: true,
      result: {
        search: {
          _id: "search-1",
        },
      },
    });

    const request = new Request("http://localhost/api/searches", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        title: "Software Engineer",
        country: "United States",
        state: null,
        city: null,
        experienceClassification: null,
        platforms: ["greenhouse"],
      }),
    });

    const response = await POST(request);

    expect(response.status).toBe(201);
    expect(startSearchFromFiltersMock).toHaveBeenCalledWith({
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
  });
});
