const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const requiredProductionFiles = [
  "BUILD_ID",
  "routes-manifest.json",
  "required-server-files.json",
];
const traceArtifactName = "trace";

function resolveNextDistDir(rawValue) {
  const trimmed = typeof rawValue === "string" ? rawValue.trim() : "";
  if (!trimmed) {
    return ".next";
  }

  if (path.isAbsolute(trimmed) || isWindowsAbsolutePath(trimmed)) {
    return ".next";
  }

  const normalized = trimmed.replace(/\\/g, "/").replace(/^\.\/+/, "");
  if (!normalized || normalized === ".") {
    return ".next";
  }

  const segments = normalized.split("/").filter(Boolean);
  if (segments.some((segment) => segment === "..")) {
    return ".next";
  }

  return normalized;
}

function buildRecoveryDistDir(distDir) {
  const sanitized = distDir.replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+/, "");
  return `.next-runtime-${sanitized || "recovery"}`;
}

function isWindowsAbsolutePath(value) {
  return (
    /^[a-zA-Z]:[\\/]/.test(value) ||
    /^\\\\[^\\]+\\[^\\]+/.test(value) ||
    /^\/\/[^/]+\/[^/]+/.test(value)
  );
}

function readDistDirEntries(projectDir, distDir) {
  const distPath = path.resolve(projectDir, distDir);

  try {
    return fs.readdirSync(distPath);
  } catch (error) {
    if (error && typeof error === "object" && error.code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

function hasRequiredProductionBuildFiles(entries) {
  if (!entries || entries.length === 0) {
    return false;
  }

  const entrySet = new Set(entries);
  return requiredProductionFiles.every((file) => entrySet.has(file));
}

function isBrokenProductionBuild(entries) {
  if (!entries || entries.length === 0) {
    return false;
  }

  if (hasRequiredProductionBuildFiles(entries)) {
    return false;
  }

  const entrySet = new Set(entries);
  return (
    entrySet.has("BUILD_ID") ||
    entrySet.has("build-manifest.json") ||
    entrySet.has("required-server-files.json") ||
    entrySet.has("server")
  );
}

function shouldRemoveDistDirBeforeRun(mode, entries) {
  if (mode === "dev") {
    return false;
  }

  return isBrokenProductionBuild(entries);
}

function getTraceArtifactState(projectDir, distDir, entries) {
  const currentEntries = entries ?? readDistDirEntries(projectDir, distDir);
  if (!currentEntries || !currentEntries.includes(traceArtifactName)) {
    return "missing";
  }

  const tracePath = path.resolve(projectDir, distDir, traceArtifactName);

  try {
    const stats = fs.lstatSync(tracePath);
    if (stats.isFile()) {
      return "file";
    }

    if (stats.isDirectory()) {
      return "directory";
    }

    return "other";
  } catch (error) {
    if (error && typeof error === "object") {
      if (error.code === "ENOENT") {
        return "ghost";
      }

      if (error.code === "EPERM" || error.code === "EACCES") {
        return "unreadable";
      }
    }

    throw error;
  }
}

function ensureTraceArtifactWritable(projectDir, distDir) {
  const beforeState = getTraceArtifactState(projectDir, distDir);
  if (beforeState === "missing" || beforeState === "file") {
    return {
      beforeState,
      afterState: beforeState,
      repaired: false,
      resetDistDir: false,
    };
  }

  const tracePath = path.resolve(projectDir, distDir, traceArtifactName);

  if (beforeState === "directory" || beforeState === "other") {
    try {
      removePath(tracePath, beforeState === "directory" ? "directory" : "file");
      const afterDirectRepair = getTraceArtifactState(projectDir, distDir);
      if (afterDirectRepair === "missing" || afterDirectRepair === "file") {
        return {
          beforeState,
          afterState: afterDirectRepair,
          repaired: true,
          resetDistDir: false,
        };
      }
    } catch {
      // Fall back to replacing the dist directory below when the trace path
      // cannot be repaired in place on Windows.
    }
  }

  removeDistDir(projectDir, distDir);
  const afterState = getTraceArtifactState(projectDir, distDir);
  if (afterState !== "missing" && afterState !== "file") {
    throw new Error(
      `[next:runtime] Failed to repair "${distDir}/${traceArtifactName}" (state: ${beforeState} -> ${afterState}).`,
    );
  }

  return {
    beforeState,
    afterState,
    repaired: true,
    resetDistDir: true,
  };
}

function removeDistDir(projectDir, distDir) {
  const distPath = path.resolve(projectDir, distDir);
  removePath(distPath, "directory");
}

function removePath(targetPath, targetKind) {
  try {
    fs.rmSync(targetPath, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 100,
    });
    return;
  } catch (error) {
    if (process.platform !== "win32") {
      throw error;
    }

    const parentDir = path.dirname(targetPath);
    const baseName = path.basename(targetPath).replace(/[\\/]/g, "_");
    const renamedPath = path.resolve(parentDir, `${baseName}.stale-${Date.now()}`);
    let removablePath = targetPath;

    try {
      if (fs.existsSync(targetPath)) {
        fs.renameSync(targetPath, renamedPath);
        removablePath = renamedPath;
      }

      fs.rmSync(removablePath, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 100,
      });
      return;
    } catch {
      const shellResult = spawnSync(
        process.env.ComSpec || "cmd.exe",
        [
          "/d",
          "/s",
          "/c",
          targetKind === "directory"
            ? `rd /s /q "${removablePath}"`
            : `del /f /q "${removablePath}"`,
        ],
        { stdio: "ignore" },
      );

      if (shellResult.status === 0) {
        return;
      }

      if (removablePath !== targetPath && !fs.existsSync(targetPath)) {
        return;
      }
    }

    throw error;
  }
}

module.exports = {
  buildRecoveryDistDir,
  ensureTraceArtifactWritable,
  getTraceArtifactState,
  hasRequiredProductionBuildFiles,
  isBrokenProductionBuild,
  readDistDirEntries,
  removeDistDir,
  resolveNextDistDir,
  shouldRemoveDistDirBeforeRun,
};
