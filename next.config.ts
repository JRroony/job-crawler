import path from "node:path";
import type { NextConfig } from "next";

const configuredDistDir = resolveSafeDistDir(process.env.NEXT_DIST_DIR);

const nextConfig: NextConfig = {
  reactStrictMode: true,
  ...(configuredDistDir
    ? {
        distDir: configuredDistDir,
      }
    : {}),
};

export default nextConfig;

function resolveSafeDistDir(rawValue?: string) {
  const trimmed = rawValue?.trim();
  if (!trimmed) {
    return undefined;
  }

  if (path.isAbsolute(trimmed)) {
    console.warn(
      `[next:config] Ignoring unsafe NEXT_DIST_DIR="${trimmed}". Use a relative directory inside the project root instead.`,
    );
    return undefined;
  }

  const normalized = trimmed.replace(/\\/g, "/").replace(/^\.\/+/, "");
  if (!normalized || normalized === ".") {
    return undefined;
  }

  const segments = normalized.split("/").filter(Boolean);
  if (segments.some((segment) => segment === "..")) {
    console.warn(
      `[next:config] Ignoring unsafe NEXT_DIST_DIR="${trimmed}". Relative parent segments are not allowed.`,
    );
    return undefined;
  }

  return normalized;
}
