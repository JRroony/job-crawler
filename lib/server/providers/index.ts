import "server-only";

import { createAshbyProvider } from "@/lib/server/providers/ashby";
import { createCompanyPageProvider } from "@/lib/server/providers/company-page";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import { createLeverProvider } from "@/lib/server/providers/lever";
import { createWorkdayProvider } from "@/lib/server/providers/workday";
import type { CrawlProvider } from "@/lib/server/providers/types";

export function createDefaultProviders(): CrawlProvider[] {
  // Default provider registration only includes implemented source-driven families.
  return [
    createGreenhouseProvider(),
    createLeverProvider(),
    createAshbyProvider(),
    createWorkdayProvider(),
    createCompanyPageProvider(),
  ];
}
