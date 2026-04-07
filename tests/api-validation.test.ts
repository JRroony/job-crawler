import { describe, expect, it } from "vitest";

import { POST } from "@/app/api/searches/route";

describe("search API validation", () => {
  it("returns 400 for invalid search payloads", async () => {
    const request = new Request("http://localhost/api/searches", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        title: "A",
      }),
    });

    const response = await POST(request);
    const payload = (await response.json()) as { error?: string };

    expect(response.status).toBe(400);
    expect(payload.error).toBe("Invalid search filters.");
  });
});
