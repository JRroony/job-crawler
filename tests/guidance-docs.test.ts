import { readFileSync } from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

const repoRoot = path.resolve(__dirname, "..");

function readRepoFile(relativePath: string): string {
  return readFileSync(path.join(repoRoot, relativePath), "utf8");
}

describe("repository guidance docs", () => {
  it("keeps AGENTS.md aligned with architecture refactor guidance", () => {
    const agents = readRepoFile("AGENTS.md");

    expect(agents).toContain("## Architecture refactors");
    expect(agents).toContain("index-first search architecture, not a crawl-first search architecture");
    expect(agents).toContain("background ingestion responsible for source discovery and source crawling");
    expect(agents).toContain("search requests query indexed jobs first");
    expect(agents).toContain("request-time crawl only as supplemental recovery, never as the primary path");
    expect(agents).toContain("search sessions, incremental delivery, durable cancellation, and durable background control as required behavior");
    expect(agents).toContain("do not claim completion for code movement alone; validate actual behavior improvement");
    expect(agents).toContain("`docs/skills/index-first-search-refactor.md`");
  });

  it("defines enforceable index-first refactor rules in the new skill doc", () => {
    const skill = readRepoFile("docs/skills/index-first-search-refactor.md");

    expect(skill).toContain("The target architecture is not crawl-first request handling.");
    expect(skill).toContain("query indexed jobs first");
    expect(skill).toContain("Background ingestion is responsible for:");
    expect(skill).toContain("Request-time crawl may be used for:");
    expect(skill).toContain("Search sessions must be explicit system concepts.");
    expect(skill).toContain("Incremental delivery is first-class");
    expect(skill).toContain("Durable cancellation and durable background control are required");
    expect(skill).toContain("1. audit");
    expect(skill).toContain("9. rerun until green");
    expect(skill).toContain("Completion requires evidence of actual behavior improvement");
  });
});
