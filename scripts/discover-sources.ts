import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { getRepository } from "@/lib/server/db/repository";
import { runRecurringSourceDiscovery } from "@/lib/server/discovery/source-discovery";
import { getEnv } from "@/lib/server/env";

const prefix = "[source-discovery:cli]";

async function main() {
  installServerOnlyShim();

  const env = getEnv();
  const repository = await getRepository(undefined, { ensureIndexes: true });
  const maxSources = parsePositiveInteger(
    readArgValue(process.argv.slice(2), "--max-sources"),
    env.PUBLIC_SEARCH_DISCOVERY_MAX_SOURCES,
  );
  const before = await repository.listSourceInventory();
  const result = await runRecurringSourceDiscovery({
    repository,
    now: new Date(),
    maxSources,
  });
  const summary = {
    sourceInventoryBeforeCount: before.length,
    sourceInventoryAfterCount: result.inventory.length,
    newSourceCount: result.stats.newSourceCount,
    updatedSourceCount: result.stats.updatedSourceCount,
    duplicateSourceCount: result.stats.duplicateSourceCount,
    invalidSourceCount: result.stats.invalidSourceCount,
    discoveredSourceCount: result.stats.discoveredSourceCount,
    platformCounts: result.stats.platformCounts,
    newSourceIds: result.stats.newSourceIds.slice(0, 20),
    updatedSourceIds: result.stats.updatedSourceIds.slice(0, 20),
    skippedReason: result.stats.skippedReason,
  };

  const output = `${prefix} ${JSON.stringify(summary, null, 2)}`;
  if (
    env.SOURCE_DISCOVERY_ENABLED &&
    result.stats.discoveredSourceCount === 0
  ) {
    console.error(output);
    process.exitCode = 1;
    return;
  }

  console.log(output);
}

function readArgValue(argv: string[], name: string) {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

function parsePositiveInteger(value: string | undefined, fallback: number) {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function installServerOnlyShim() {
  const require = createRequire(import.meta.url);
  const id = require.resolve("server-only");
  require.cache[id] = {
    id,
    filename: id,
    loaded: true,
    exports: {},
  } as NodeJS.Module;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(
      `${prefix} ${JSON.stringify(
        {
          status: "failed",
          error: error instanceof Error ? error.message : String(error),
        },
        null,
        2,
      )}`,
    );
    process.exitCode = 1;
  });
}
