import "server-only";

import { MongoClient } from "mongodb";

import { ensureDatabaseIndexes } from "@/lib/server/db/indexes";
import { getEnv } from "@/lib/server/env";

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerMongoClientPromise: Promise<MongoClient> | undefined;
}

function databaseNameFromUri(uri: string) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, "");
    return pathname || "job_crawler";
  } catch {
    return "job_crawler";
  }
}

const env = getEnv();

export async function getMongoDb() {
  const client = await getMongoClient();
  const db = client.db(databaseNameFromUri(env.MONGODB_URI));
  await ensureDatabaseIndexes(db);
  return db;
}

async function getMongoClient() {
  const cachedPromise = globalThis.__jobCrawlerMongoClientPromise;
  if (cachedPromise) {
    return cachedPromise;
  }

  const connectionPromise = new MongoClient(env.MONGODB_URI)
    .connect()
    .catch((error) => {
      if (globalThis.__jobCrawlerMongoClientPromise === connectionPromise) {
        globalThis.__jobCrawlerMongoClientPromise = undefined;
      }

      throw error;
    });

  globalThis.__jobCrawlerMongoClientPromise = connectionPromise;
  return connectionPromise;
}
