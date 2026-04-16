import { beforeEach, describe, expect, it, vi } from "vitest";

const { registerNodeInstrumentationMock } = vi.hoisted(() => ({
  registerNodeInstrumentationMock: vi.fn(),
}));

vi.mock("../instrumentation.node", () => ({
  registerNodeInstrumentation: registerNodeInstrumentationMock,
}));

describe("instrumentation runtime wiring", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.unstubAllEnvs();
  });

  it("starts the recurring background ingestion scheduler during runtime registration when NEXT_RUNTIME=nodejs", async () => {
    vi.stubEnv("NEXT_RUNTIME", "nodejs");
    const instrumentation = await import("../instrumentation");

    await instrumentation.register();

    expect(registerNodeInstrumentationMock).toHaveBeenCalledTimes(1);
  });

  it("does not start the scheduler when NEXT_RUNTIME is not nodejs", async () => {
    vi.stubEnv("NEXT_RUNTIME", "edge");
    const instrumentation = await import("../instrumentation");

    await instrumentation.register();

    expect(registerNodeInstrumentationMock).not.toHaveBeenCalled();
  });

  it("does not start the scheduler when NEXT_RUNTIME is undefined", async () => {
    // Ensure NEXT_RUNTIME is not set
    delete process.env.NEXT_RUNTIME;
    const instrumentation = await import("../instrumentation");

    await instrumentation.register();

    expect(registerNodeInstrumentationMock).not.toHaveBeenCalled();
  });
});
