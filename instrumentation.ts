import "server-only";

export async function register() {
  // Only run in Node.js runtime (not edge)
  if (process.env.NEXT_RUNTIME !== "nodejs") {
    return;
  }

  try {
    const { registerNodeInstrumentation } = await import("./instrumentation.node");
    await registerNodeInstrumentation();
  } catch (error) {
    // Log but don't crash the instrumentation
    console.error("[instrumentation] Failed to start background ingestion scheduler", error);
  }
}
