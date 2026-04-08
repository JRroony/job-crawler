import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  isInputValidationErrorMock,
  listRecentSearchesMock,
  runSearchFromFiltersMock,
} = vi.hoisted(() => ({
  isInputValidationErrorMock: vi.fn(() => false),
  listRecentSearchesMock: vi.fn(),
  runSearchFromFiltersMock: vi.fn(),
}));

vi.mock("@/lib/server/crawler/service", () => ({
  isInputValidationError: isInputValidationErrorMock,
  listRecentSearches: listRecentSearchesMock,
  runSearchFromFilters: runSearchFromFiltersMock,
}));

import { POST } from "@/app/api/searches/route";

describe("search API normalization", () => {
  beforeEach(() => {
    isInputValidationErrorMock.mockReturnValue(false);
    listRecentSearchesMock.mockReset();
    runSearchFromFiltersMock.mockReset();
  });

  it("strips null optional filters and legacy experienceClassification before starting the crawl", async () => {
    runSearchFromFiltersMock.mockResolvedValue({
      search: {
        _id: "search-1",
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
    expect(runSearchFromFiltersMock).toHaveBeenCalledWith({
      title: "Software Engineer",
      country: "United States",
      platforms: ["greenhouse"],
    });
  });
});
