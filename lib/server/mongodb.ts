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
const clientPromise =
  globalThis.__jobCrawlerMongoClientPromise ??
  new MongoClient(env.MONGODB_URI).connect();

if (process.env.NODE_ENV !== "production") {
  globalThis.__jobCrawlerMongoClientPromise = clientPromise;
}

export async function getMongoDb() {
  const client = await clientPromise;
  const db = client.db(databaseNameFromUri(env.MONGODB_URI));
  await ensureDatabaseIndexes(db);
  return db;
}
