import "server-only";

type SortSpec = Record<string, 1 | -1>;

function isOperatorRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function getValueAtPath(document: Record<string, unknown>, path: string) {
  if (!path.includes(".")) {
    return document[path];
  }

  return path.split(".").reduce<unknown>((current, segment) => {
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }

    return (current as Record<string, unknown>)[segment];
  }, document);
}

function matchesFieldValue(actual: unknown, expected: unknown): boolean {
  if (expected instanceof RegExp) {
    if (Array.isArray(actual)) {
      return actual.some((value) => typeof value === "string" && expected.test(value));
    }

    return typeof actual === "string" && expected.test(actual);
  }

  if (isOperatorRecord(expected)) {
    if ("$in" in expected && Array.isArray(expected.$in)) {
      const values = expected.$in;
      return Array.isArray(actual)
        ? actual.some((value) => values.includes(value))
        : values.includes(actual);
    }

    if ("$gte" in expected && expected.$gte != null) {
      if (actual == null || actual < expected.$gte) {
        return false;
      }
    }

    if ("$lte" in expected && expected.$lte != null) {
      if (actual == null || actual > expected.$lte) {
        return false;
      }
    }

    if ("$exists" in expected && typeof expected.$exists === "boolean") {
      const exists = typeof actual !== "undefined";
      if (exists !== expected.$exists) {
        return false;
      }
    }

    if ("$regex" in expected) {
      const regex =
        expected.$regex instanceof RegExp
          ? expected.$regex
          : typeof expected.$regex === "string"
            ? new RegExp(expected.$regex, typeof expected.$options === "string" ? expected.$options : "")
            : undefined;

      if (!regex) {
        return false;
      }

      if (Array.isArray(actual)) {
        return actual.some((value) => typeof value === "string" && regex.test(value));
      }

      return typeof actual === "string" && regex.test(actual);
    }

    return Object.keys(expected).every((key) => key.startsWith("$"));
  }

  if (Array.isArray(actual)) {
    return actual.includes(expected);
  }

  return actual === expected;
}

export function matchesQuery(
  document: Record<string, unknown>,
  filter: Record<string, unknown>,
): boolean {
  return Object.entries(filter).every(([key, expected]) => {
    if (key === "$and") {
      return Array.isArray(expected) &&
        expected.every((clause) =>
          isOperatorRecord(clause) ? matchesQuery(document, clause) : false,
        );
    }

    if (key === "$or") {
      return Array.isArray(expected) &&
        expected.some((clause) =>
          isOperatorRecord(clause) ? matchesQuery(document, clause) : false,
        );
    }

    const actual = getValueAtPath(document, key);
    return matchesFieldValue(actual, expected);
  });
}

export function compareWithSort(
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  sort: SortSpec,
) {
  for (const [key, direction] of Object.entries(sort)) {
    const leftValue = getValueAtPath(left, key);
    const rightValue = getValueAtPath(right, key);

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
