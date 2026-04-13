import { describe, expect, it } from "vitest";

// eslint-disable-next-line @typescript-eslint/no-require-imports
const {
  hasRequiredProductionBuildFiles,
  isBrokenProductionBuild,
  resolveNextDistDir,
} = require("../scripts/next-runtime.js");

describe("next runtime guards", () => {
  it("defaults to .next when NEXT_DIST_DIR is unset or unsafe", () => {
    expect(resolveNextDistDir(undefined)).toBe(".next");
    expect(resolveNextDistDir("")).toBe(".next");
    expect(resolveNextDistDir("   ")).toBe(".next");
    expect(resolveNextDistDir("../tmp/next")).toBe(".next");
    expect(resolveNextDistDir("C:\\temp\\next-build")).toBe(".next");
  });

  it("keeps safe relative dist directories", () => {
    expect(resolveNextDistDir("next-cache")).toBe("next-cache");
    expect(resolveNextDistDir("./artifacts/next")).toBe("artifacts/next");
  });

  it("recognizes when the required production manifest set exists", () => {
    expect(
      hasRequiredProductionBuildFiles([
        "BUILD_ID",
        "routes-manifest.json",
        "required-server-files.json",
      ]),
    ).toBe(true);
  });

  it("treats partial production output as broken", () => {
    expect(isBrokenProductionBuild(["BUILD_ID"])).toBe(true);
    expect(
      isBrokenProductionBuild(["BUILD_ID", "required-server-files.json", "server"]),
    ).toBe(true);
    expect(isBrokenProductionBuild(["trace", "cache"])).toBe(false);
  });
});
