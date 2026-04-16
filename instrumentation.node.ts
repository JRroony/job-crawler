import "server-only";

import { startRecurringBackgroundIngestionScheduler } from "@/lib/server/background/recurring-ingestion";

export function registerNodeInstrumentation() {
  startRecurringBackgroundIngestionScheduler();
}
