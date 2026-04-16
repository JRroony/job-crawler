#!/usr/bin/env node

const path = require("node:path");
const { spawn, spawnSync } = require("node:child_process");

const {
  hasRequiredProductionBuildFiles,
  readDistDirEntries,
  removeDistDir,
  resolveNextDistDir,
  shouldRemoveDistDirBeforeRun,
} = require("./next-runtime");

const projectDir = path.resolve(__dirname, "..");
const nextBin = require.resolve("next/dist/bin/next");
const mode = process.argv[2];
const passthroughArgs = process.argv.slice(3);

if (!mode || !["dev", "build", "start"].includes(mode)) {
  console.error(
    "[next:runtime] Expected one of: dev, build, start. Received:",
    mode ?? "<missing>",
  );
  process.exit(1);
}

const rawDistDir = process.env.NEXT_DIST_DIR;
const distDir = resolveNextDistDir(rawDistDir);

if (rawDistDir && rawDistDir.trim() && rawDistDir.trim() !== distDir) {
  console.warn(
    `[next:runtime] Ignoring unsafe NEXT_DIST_DIR="${rawDistDir}". Falling back to "${distDir}".`,
  );
}

const existingEntries = readDistDirEntries(projectDir, distDir);

if (shouldRemoveDistDirBeforeRun(mode, existingEntries)) {
  console.warn(
    `[next:runtime] Removing incomplete Next build output from "${distDir}" before "${mode}".`,
  );
  removeDistDir(projectDir, distDir);
}

if (mode === "start") {
  const refreshedEntries = readDistDirEntries(projectDir, distDir);
  if (!hasRequiredProductionBuildFiles(refreshedEntries)) {
    console.warn(
      `[next:runtime] "${distDir}" is missing required Next build artifacts. Running "next build" first.`,
    );

    const buildResult = spawnSync(process.execPath, [nextBin, "build"], {
      cwd: projectDir,
      env: process.env,
      stdio: "inherit",
    });

    if (typeof buildResult.status === "number" && buildResult.status !== 0) {
      process.exit(buildResult.status);
    }

    if (buildResult.error) {
      throw buildResult.error;
    }
  }
}

const child = spawn(process.execPath, [nextBin, mode, ...passthroughArgs], {
  cwd: projectDir,
  env: process.env,
  stdio: "inherit",
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }

  process.exit(code ?? 0);
});

child.on("error", (error) => {
  console.error("[next:runtime] Failed to launch Next.js.", error);
  process.exit(1);
});
