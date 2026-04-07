import "server-only";

import { createAshbyProvider } from "@/lib/server/providers/ashby";
import { createCompanyPageProvider } from "@/lib/server/providers/company-page";
import { createGreenhouseProvider } from "@/lib/server/providers/greenhouse";
import { createIndeedLimitedProvider, createLinkedInLimitedProvider } from "@/lib/server/providers/limited";
import { createLeverProvider } from "@/lib/server/providers/lever";

export function createDefaultProviders() {
  return [
    createGreenhouseProvider(),
    createLeverProvider(),
    createAshbyProvider(),
    createCompanyPageProvider(),
    createLinkedInLimitedProvider(),
    createIndeedLimitedProvider(),
  ];
}
