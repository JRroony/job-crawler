import path from "node:path";
import { builtinModules } from "node:module";
import type { NextConfig } from "next";

const configuredDistDir = resolveSafeDistDir(process.env.NEXT_DIST_DIR);
const nodeExternals = buildNodeExternals();

const nextConfig: NextConfig = {
  reactStrictMode: true,
  serverExternalPackages: ["mongodb"],
  webpack: (config, { isServer }) => {
    if (isServer) {
      const externals = Array.isArray(config.externals)
        ? config.externals
        : config.externals
          ? [config.externals]
          : [];

      config.externals = [
        ...externals,
        nodeExternals,
      ];
    }

    return config;
  },
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

export function buildNodeExternals() {
  const builtinRequests = new Set(
    builtinModules.flatMap((moduleName) => {
      const normalizedModuleName = moduleName.replace(/^node:/, "");
      return [normalizedModuleName, `node:${normalizedModuleName}`];
    }),
  );

  return function externalizeNodeRuntimeModules(
    context: { request?: string },
    callback: (error?: Error | null, result?: string) => void,
  ) {
    const request = context.request;
    if (!request) {
      callback();
      return;
    }

    if (request === "mongodb") {
      callback(null, "commonjs mongodb");
      return;
    }

    if (builtinRequests.has(request)) {
      callback(null, `commonjs ${request}`);
      return;
    }

    callback();
  };
}
