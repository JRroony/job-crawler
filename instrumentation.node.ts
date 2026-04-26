import "server-only";

const developmentBackgroundIngestionInitialDelayMs = 120_000;

export async function registerNodeInstrumentation() {
  const { startRecurringBackgroundIngestionScheduler } = await import(
    "@/lib/server/background/recurring-ingestion"
  );
  startRecurringBackgroundIngestionScheduler({
    initialDelayMs:
      process.env.NODE_ENV === "development"
        ? developmentBackgroundIngestionInitialDelayMs
        : 0,
  });
}
