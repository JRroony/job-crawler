const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const requiredProductionFiles = [
  "BUILD_ID",
  "routes-manifest.json",
  "required-server-files.json",
];

function resolveNextDistDir(rawValue) {
  const trimmed = typeof rawValue === "string" ? rawValue.trim() : "";
  if (!trimmed) {
    return ".next";
  }

  if (path.isAbsolute(trimmed)) {
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

function removeDistDir(projectDir, distDir) {
  const distPath = path.resolve(projectDir, distDir);

  try {
    fs.rmSync(distPath, {
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

    const renamedPath = path.resolve(
      projectDir,
      `${distDir.replace(/[\\/]/g, "_")}.stale-${Date.now()}`,
    );

    try {
      if (fs.existsSync(distPath)) {
        fs.renameSync(distPath, renamedPath);
      }

      fs.rmSync(renamedPath, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 100,
      });
      return;
    } catch {
      const shellResult = spawnSync(
        process.env.ComSpec || "cmd.exe",
        ["/d", "/s", "/c", `rd /s /q "${distPath}"`],
        { stdio: "ignore" },
      );

      if (shellResult.status === 0) {
        return;
      }
    }

    throw error;
  }
}

module.exports = {
  hasRequiredProductionBuildFiles,
  isBrokenProductionBuild,
  readDistDirEntries,
  removeDistDir,
  resolveNextDistDir,
};
