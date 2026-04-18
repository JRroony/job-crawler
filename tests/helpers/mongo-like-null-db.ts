import type { CollectionAdapter, DatabaseAdapter } from "@/lib/server/db/repository";
import { compareWithSort, matchesQuery } from "@/lib/server/db/query-utils";

type SortSpec = Record<string, 1 | -1>;

type IndexSpec = {
  key: Record<string, 1 | -1>;
  name: string;
  unique?: boolean;
  sparse?: boolean;
};

export class MongoLikeNullCollection<TDocument extends Record<string, unknown>>
  implements CollectionAdapter<TDocument>
{
  documents: TDocument[] = [];
  indexes: IndexSpec[] = [];

  async findOne(
    filter: Record<string, unknown>,
    options?: { sort?: SortSpec },
  ): Promise<TDocument | null> {
    const results = this.filter(filter, options?.sort);
    return clone(results[0] ?? null);
  }

  async insertOne(document: TDocument) {
    const nextDocuments = [...this.documents, clone(toMongoLikeValue(document))];
    assertIndexesAllowDocuments(this.indexes, nextDocuments);
    this.documents = nextDocuments;
    return { insertedId: document._id };
  }

  async bulkWrite(
    operations: Array<
      | { insertOne: { document: TDocument } }
      | { deleteOne: { filter: Record<string, unknown> } }
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

      if ("deleteOne" in operation) {
        const targetIndex = this.documents.findIndex((document) =>
          matches(document, operation.deleteOne.filter),
        );
        if (targetIndex >= 0) {
          const nextDocuments = clone(this.documents);
          nextDocuments.splice(targetIndex, 1);
          this.documents = nextDocuments;
        }
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
        ...clone(toMongoLikeValue((update.$setOnInsert as Record<string, unknown>) ?? {})),
        ...clone(toMongoLikeValue((update.$set as Record<string, unknown>) ?? {})),
      } as TDocument;
      const nextDocuments = [...this.documents, inserted];
      assertIndexesAllowDocuments(this.indexes, nextDocuments);
      this.documents = nextDocuments;
      return { matchedCount: 0, modifiedCount: 0, upsertedCount: 1 };
    }

    const nextValues = update.$set && typeof update.$set === "object" ? update.$set : update;
    if (Object.prototype.hasOwnProperty.call(nextValues, "_id")) {
      throw new Error("MongoDB does not allow updates to the immutable _id field.");
    }

    const nextDocuments = clone(this.documents);
    const nextTarget = nextDocuments.find((document) => matches(document, filter));
    Object.assign(
      nextTarget as TDocument,
      clone(toMongoLikeValue(nextValues as Record<string, unknown>)),
    );
    assertIndexesAllowDocuments(this.indexes, nextDocuments);
    this.documents = nextDocuments;
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

  async createIndexes(indexes: IndexSpec[]) {
    assertIndexesAllowDocuments([...this.indexes, ...indexes], this.documents);
    this.indexes.push(...indexes);
    return indexes.map((index) => index.name);
  }

  private filter(filter: Record<string, unknown>, sort?: SortSpec) {
    const results = this.documents.filter((document) => matches(document, filter));
    if (!sort) {
      return results;
    }

    return results.sort((left, right) => compareWithSort(left, right, sort));
  }
}

export class MongoLikeNullDb implements DatabaseAdapter {
  private readonly collections = new Map<
    string,
    MongoLikeNullCollection<Record<string, unknown>>
  >();

  collection<TDocument extends Record<string, unknown>>(
    name: string,
  ): MongoLikeNullCollection<TDocument> {
    if (!this.collections.has(name)) {
      this.collections.set(
        name,
        new MongoLikeNullCollection<Record<string, unknown>>(),
      );
    }

    return this.collections.get(name) as unknown as MongoLikeNullCollection<TDocument>;
  }

  snapshot<TDocument extends Record<string, unknown>>(name: string) {
    return clone(this.collection<TDocument>(name).documents);
  }
}

function matches(document: Record<string, unknown>, filter: Record<string, unknown>) {
  return matchesQuery(document, filter);
}

function toMongoLikeValue<T>(value: T): T {
  if (value === undefined) {
    return null as T;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => toMongoLikeValue(entry)) as T;
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  return Object.entries(value).reduce<Record<string, unknown>>((record, [key, entry]) => {
    record[key] = toMongoLikeValue(entry);
    return record;
  }, {}) as T;
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function assertIndexesAllowDocuments<TDocument extends Record<string, unknown>>(
  indexes: IndexSpec[],
  documents: TDocument[],
) {
  for (const index of indexes.filter((candidate) => candidate.unique)) {
    const seen = new Set<string>();
    for (const document of documents) {
      const key = buildUniqueIndexKey(document, index);
      if (!key) {
        continue;
      }

      if (seen.has(key)) {
        throw new Error(
          `E11000 duplicate key error collection: mongo-like index: ${index.name} dup key: ${key}`,
        );
      }

      seen.add(key);
    }
  }
}

function buildUniqueIndexKey(document: Record<string, unknown>, index: IndexSpec) {
  const entries = Object.keys(index.key).map((field) => {
    const value = getValueByPath(document, field);
    if (index.sparse && typeof value === "undefined") {
      return undefined;
    }

    return `${field}:${JSON.stringify(typeof value === "undefined" ? null : value)}`;
  });

  if (entries.some((entry) => typeof entry === "undefined")) {
    return undefined;
  }

  return entries.join("|");
}

function getValueByPath(document: Record<string, unknown>, path: string): unknown {
  return path.split(".").reduce<unknown>((value, segment) => {
    if (!value || typeof value !== "object") {
      return undefined;
    }

    return (value as Record<string, unknown>)[segment];
  }, document);
}
