import "server-only";

import type { Db, MongoClient } from "mongodb";

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
let mongoUnavailableUntil = 0;

export async function getMongoDb(): Promise<Db> {
  if (mongoUnavailableUntil > Date.now()) {
    throw new Error("MongoDB connection is in cooldown after a recent failure.");
  }

  const client = await getMongoClient();
  const db = client.db(databaseNameFromUri(env.MONGODB_URI));
  await ensureDatabaseIndexes(db);
  return db;
}

async function getMongoClient(): Promise<MongoClient> {
  const cachedPromise = globalThis.__jobCrawlerMongoClientPromise;
  if (cachedPromise) {
    return cachedPromise;
  }

  const { MongoClient } = await import("mongodb");
  const connectionPromise = new MongoClient(env.MONGODB_URI, {
    serverSelectionTimeoutMS: env.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
  })
    .connect()
    .catch((error) => {
      if (globalThis.__jobCrawlerMongoClientPromise === connectionPromise) {
        globalThis.__jobCrawlerMongoClientPromise = undefined;
      }

      mongoUnavailableUntil =
        Date.now() + env.MONGODB_UNAVAILABLE_COOLDOWN_MS;

      throw error;
    });

  globalThis.__jobCrawlerMongoClientPromise = connectionPromise;
  return connectionPromise;
}
