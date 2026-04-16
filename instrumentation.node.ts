import "server-only";

export async function registerNodeInstrumentation() {
  const { startRecurringBackgroundIngestionScheduler } = await import(
    "@/lib/server/background/recurring-ingestion"
  );
  startRecurringBackgroundIngestionScheduler();
}
