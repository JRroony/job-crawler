import { startRecurringBackgroundIngestionScheduler } from "@/lib/server/background/recurring-ingestion";

export async function register() {
  startRecurringBackgroundIngestionScheduler();
}
