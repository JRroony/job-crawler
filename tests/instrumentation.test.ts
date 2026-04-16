import { describe, expect, it, vi } from "vitest";

vi.mock("@/lib/server/background/recurring-ingestion", () => ({
  startRecurringBackgroundIngestionScheduler: vi.fn(),
}));

describe("instrumentation runtime wiring", () => {
  it("starts the recurring background ingestion scheduler during runtime registration", async () => {
    const module = await import("@/lib/server/background/recurring-ingestion");
    const instrumentation = await import("../instrumentation");

    await instrumentation.register();

    expect(module.startRecurringBackgroundIngestionScheduler).toHaveBeenCalledTimes(1);
  });
});
