import "server-only";

import type { CollectionAdapter, DatabaseAdapter } from "@/lib/server/db/repository";
import { compareWithSort, matchesQuery } from "@/lib/server/db/query-utils";

type SortSpec = Record<string, 1 | -1>;

class MemoryCollection<TDocument extends Record<string, unknown>>
  implements CollectionAdapter<TDocument>
{
  private documents: TDocument[] = [];

  async findOne(
    filter: Record<string, unknown>,
    options?: { sort?: SortSpec },
  ): Promise<TDocument | null> {
    const results = this.filter(filter, options?.sort);
    return clone(results[0] ?? null);
  }

  async insertOne(document: TDocument) {
    this.documents.push(clone(document));
    return { insertedId: document._id };
  }

  async bulkWrite(
    operations: Array<
      | { insertOne: { document: TDocument } }
      | {
          updateOne: {
            filter: Record<string, unknown>;
            update: Record<string, unknown>;
            options?: Record<string, unknown>;
          };
        }
    >,
  ) {
    let insertedCount = 0;
    let matchedCount = 0;

    for (const operation of operations) {
      if ("insertOne" in operation) {
        await this.insertOne(operation.insertOne.document);
        insertedCount += 1;
        continue;
      }

      const result = await this.updateOne(
        operation.updateOne.filter,
        operation.updateOne.update,
        operation.updateOne.options,
      );
      matchedCount += Number((result as { matchedCount?: number }).matchedCount ?? 0);
    }

    return {
      insertedCount,
      matchedCount,
      modifiedCount: matchedCount,
    };
  }

  async updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ) {
    const target = this.documents.find((document) => matches(document, filter));
    if (!target && !options?.upsert) {
      return { matchedCount: 0, modifiedCount: 0 };
    }

    if (!target && options?.upsert) {
      const inserted = {
        ...clone((update.$setOnInsert as Record<string, unknown>) ?? {}),
        ...clone((update.$set as Record<string, unknown>) ?? {}),
      } as TDocument;
      this.documents.push(inserted);
      return { matchedCount: 0, modifiedCount: 0, upsertedCount: 1 };
    }

    const nextValues = update.$set && typeof update.$set === "object" ? update.$set : update;
    if (Object.prototype.hasOwnProperty.call(nextValues, "_id")) {
      throw new Error("MemoryDb does not allow updates to the immutable _id field.");
    }

    Object.assign(target as TDocument, clone(nextValues as Record<string, unknown>));
    return { matchedCount: 1, modifiedCount: 1 };
  }

  find(filter: Record<string, unknown> = {}, options?: { sort?: SortSpec; limit?: number }) {
    const results = this.filter(filter, options?.sort).slice(
      0,
      options?.limit ?? Number.MAX_SAFE_INTEGER,
    );

    return {
      toArray: async () => clone(results),
    };
  }

  private filter(filter: Record<string, unknown>, sort?: SortSpec) {
    const results = this.documents.filter((document) => matches(document, filter));
    if (!sort) {
      return results;
    }

    return results.sort((left, right) => compareWithSort(left, right, sort));
  }
}

class MemoryDb implements DatabaseAdapter {
  private readonly collections = new Map<string, MemoryCollection<Record<string, unknown>>>();

  collection<TDocument extends Record<string, unknown>>(name: string): CollectionAdapter<TDocument> {
    if (!this.collections.has(name)) {
      this.collections.set(name, new MemoryCollection<Record<string, unknown>>());
    }

    return this.collections.get(name) as unknown as CollectionAdapter<TDocument>;
  }
}

declare global {
  // eslint-disable-next-line no-var
  var __jobCrawlerMemoryDb: MemoryDb | undefined;
}

export function getMemoryDb() {
  if (!globalThis.__jobCrawlerMemoryDb) {
    globalThis.__jobCrawlerMemoryDb = new MemoryDb();
  }

  return globalThis.__jobCrawlerMemoryDb;
}

function matches(document: Record<string, unknown>, filter: Record<string, unknown>) {
  return matchesQuery(document, filter);
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
