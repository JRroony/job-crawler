import "server-only";

import type { CrawlProvider, ProviderResult } from "@/lib/server/providers/types";

function limitedResult(
  provider: ProviderResult["provider"],
  message: string,
): ProviderResult {
  return {
    provider,
    status: "unsupported",
    jobs: [],
    fetchedCount: 0,
    matchedCount: 0,
    errorMessage: message,
  };
}

export function createLinkedInLimitedProvider(): CrawlProvider {
  return {
    provider: "linkedin_limited",
    async crawl() {
      return limitedResult(
        "linkedin_limited",
        "LinkedIn is intentionally limited to public, compliant collection paths only. Login-only scraping, CAPTCHA bypass, bot evasion, and private APIs are not implemented.",
      );
    },
  };
}

export function createIndeedLimitedProvider(): CrawlProvider {
  return {
    provider: "indeed_limited",
    async crawl() {
      return limitedResult(
        "indeed_limited",
        "Indeed is intentionally limited to public, compliant collection paths only. Login-only scraping, CAPTCHA bypass, bot evasion, and hidden APIs are not implemented.",
      );
    },
  };
}
