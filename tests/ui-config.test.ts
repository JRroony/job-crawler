import { describe, expect, it } from "vitest";

import {
  passivePlatformOptions,
  selectablePlatformOptions,
  togglePlatformSelection,
} from "@/components/job-crawler/ui-config";
import { activeCrawlerPlatforms } from "@/lib/types";

describe("job crawler UI config", () => {
  it("keeps selectable platform cards aligned with active crawler platforms", () => {
    expect(selectablePlatformOptions.map((option) => option.platform)).toEqual(
      activeCrawlerPlatforms,
    );
    expect(selectablePlatformOptions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          platform: "workday",
          label: "Workday",
        }),
      ]),
    );
    expect(passivePlatformOptions.map((option) => option.label)).not.toContain(
      "Workday",
    );
  });

  it("treats all active platforms as the default scope after selection returns to full coverage", () => {
    const withoutWorkday = togglePlatformSelection(undefined, "workday");
    expect(withoutWorkday).toEqual(
      activeCrawlerPlatforms.filter((platform) => platform !== "workday"),
    );
    expect(togglePlatformSelection(withoutWorkday, "workday")).toBeUndefined();
  });
});
