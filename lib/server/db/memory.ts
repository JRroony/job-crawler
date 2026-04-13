import "server-only";

import type { CollectionAdapter, DatabaseAdapter } from "@/lib/server/db/repository";

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
  ) {
    const target = this.documents.find((document) => matches(document, filter));
    if (!target) {
      return { matchedCount: 0, modifiedCount: 0 };
    }

    const nextValues = update.$set && typeof update.$set === "object" ? update.$set : update;
    if (Object.prototype.hasOwnProperty.call(nextValues, "_id")) {
      throw new Error("MemoryDb does not allow updates to the immutable _id field.");
    }

    Object.assign(target, clone(nextValues as Record<string, unknown>));
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
  return Object.entries(filter).every(([key, expected]) => {
    const actual = document[key];

    if (expected && typeof expected === "object" && !Array.isArray(expected)) {
      if ("$in" in expected && Array.isArray((expected as { $in: unknown[] }).$in)) {
        const values = (expected as { $in: unknown[] }).$in;
        return Array.isArray(actual)
          ? actual.some((item) => values.includes(item))
          : values.includes(actual);
      }

      return false;
    }

    if (Array.isArray(actual)) {
      return actual.includes(expected);
    }

    return actual === expected;
  });
}

function compareWithSort(
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  sort: SortSpec,
) {
  for (const [key, direction] of Object.entries(sort)) {
    const leftValue = left[key];
    const rightValue = right[key];

    if (leftValue === rightValue) {
      continue;
    }

    if (leftValue == null) {
      return direction === -1 ? 1 : -1;
    }

    if (rightValue == null) {
      return direction === -1 ? -1 : 1;
    }

    if (leftValue > rightValue) {
      return direction === -1 ? -1 : 1;
    }

    if (leftValue < rightValue) {
      return direction === -1 ? 1 : -1;
    }
  }

  return 0;
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
