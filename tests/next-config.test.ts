import { describe, expect, it, vi } from "vitest";

import { buildNodeExternals } from "@/next.config";

describe("next webpack node externals", () => {
  it("externalizes MongoDB for server runtime bundling", () => {
    const matcher = buildNodeExternals();
    const callback = vi.fn();

    matcher({ request: "mongodb" }, callback);

    expect(callback).toHaveBeenCalledWith(null, "commonjs mongodb");
  });

  it("externalizes bare Node builtin requests", () => {
    const matcher = buildNodeExternals();
    const callback = vi.fn();

    matcher({ request: "crypto" }, callback);

    expect(callback).toHaveBeenCalledWith(null, "commonjs crypto");
  });

  it("externalizes node:-prefixed builtin requests", () => {
    const matcher = buildNodeExternals();
    const callback = vi.fn();

    matcher({ request: "node:crypto" }, callback);

    expect(callback).toHaveBeenCalledWith(null, "commonjs node:crypto");
  });

  it("leaves application modules to webpack resolution", () => {
    const matcher = buildNodeExternals();
    const callback = vi.fn();

    matcher({ request: "@/lib/server/background/recurring-ingestion" }, callback);

    expect(callback).toHaveBeenCalledWith();
  });
});
