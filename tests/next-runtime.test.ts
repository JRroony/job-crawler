import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

// eslint-disable-next-line @typescript-eslint/no-require-imports
const {
  buildRecoveryDistDir,
  ensureTraceArtifactWritable,
  getTraceArtifactState,
  hasRequiredProductionBuildFiles,
  isBrokenProductionBuild,
  resolveNextDistDir,
  shouldRemoveDistDirBeforeRun,
} = require("../scripts/next-runtime.js");

const tempDirs: string[] = [];

afterEach(() => {
  for (const tempDir of tempDirs.splice(0)) {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

function createTempProjectDir() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "next-runtime-"));
  tempDirs.push(tempDir);
  return tempDir;
}

describe("next runtime guards", () => {
  it("defaults to .next when NEXT_DIST_DIR is unset or unsafe", () => {
    expect(resolveNextDistDir(undefined)).toBe(".next");
    expect(resolveNextDistDir("")).toBe(".next");
    expect(resolveNextDistDir("   ")).toBe(".next");
    expect(resolveNextDistDir("../tmp/next")).toBe(".next");
    expect(resolveNextDistDir("C:\\temp\\next-build")).toBe(".next");
    expect(resolveNextDistDir("\\\\server\\share\\next-build")).toBe(".next");
  });

  it("keeps safe relative dist directories", () => {
    expect(resolveNextDistDir("next-cache")).toBe("next-cache");
    expect(resolveNextDistDir("./artifacts/next")).toBe("artifacts/next");
  });

  it("derives a safe fallback dist directory for Windows recovery", () => {
    expect(buildRecoveryDistDir(".next")).toBe(".next-runtime-.next");
    expect(buildRecoveryDistDir("artifacts/next")).toBe(".next-runtime-artifacts_next");
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

  it("does not remove dev output before next dev starts", () => {
    expect(
      shouldRemoveDistDirBeforeRun("dev", [
        "build-manifest.json",
        "server",
        "static",
        "trace",
      ]),
    ).toBe(false);
  });

  it("still removes incomplete production output before build or start", () => {
    const incompleteProductionEntries = ["build-manifest.json", "server"];

    expect(shouldRemoveDistDirBeforeRun("build", incompleteProductionEntries)).toBe(true);
    expect(shouldRemoveDistDirBeforeRun("start", incompleteProductionEntries)).toBe(true);
  });

  it("repairs a non-file trace artifact without resetting the whole dist dir", () => {
    const projectDir = createTempProjectDir();
    const distDir = ".next";
    const distPath = path.join(projectDir, distDir);

    fs.mkdirSync(path.join(distPath, "cache"), { recursive: true });
    fs.mkdirSync(path.join(distPath, "trace"));

    expect(getTraceArtifactState(projectDir, distDir)).toBe("directory");

    const repair = ensureTraceArtifactWritable(projectDir, distDir);

    expect(repair).toEqual({
      beforeState: "directory",
      afterState: "missing",
      repaired: true,
      resetDistDir: false,
    });
    expect(fs.existsSync(distPath)).toBe(true);
    expect(fs.existsSync(path.join(distPath, "cache"))).toBe(true);
    expect(fs.existsSync(path.join(distPath, "trace"))).toBe(false);
  });
});
