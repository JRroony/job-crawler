import "server-only";

const seniorityPhrases = [
  "new grad",
  "new graduate",
  "recent grad",
  "recent graduate",
  "entry level",
  "early career",
  "mid level",
  "mid level",
  "associate level",
] as const;

const seniorityTokens = new Set([
  "associate",
  "distinguished",
  "fellow",
  "intern",
  "internship",
  "jr",
  "junior",
  "lead",
  "mid",
  "principal",
  "senior",
  "sr",
  "staff",
  "trainee",
  "apprentice",
  "ii",
  "iii",
  "iv",
  "v",
]);

const stopWords = new Set([
  "a",
  "an",
  "and",
  "for",
  "in",
  "of",
  "on",
  "the",
  "to",
    "with",
]);

const roleHeadWords = new Set([
  "analyst",
  "architect",
  "consultant",
  "coordinator",
  "designer",
  "developer",
  "engineer",
  "manager",
  "recruiter",
  "specialist",
  "sourcer",
  "writer",
]);

export function normalizeTitleText(value?: string) {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[^\x00-\x7F]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/\bfront[\s-]*end\b/g, "frontend")
    .replace(/\bback[\s-]*end\b/g, "backend")
    .replace(/\bfull[\s-]*stack\b/g, "full stack")
    .replace(/\bpre[\s-]*sales\b/g, "pre sales")
    .replace(/\bsite reliability engineer\b/g, "sre")
    .replace(/\bquality assurance\b/g, "quality assurance")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function tokenizeTitle(value?: string) {
  const normalized = normalizeTitleText(value);
  return normalized ? normalized.split(" ") : [];
}

export function stripTitleSeniority(value?: string) {
  const normalized = normalizeTitleText(value);
  if (!normalized) {
    return "";
  }

  let stripped = normalized;
  for (const phrase of seniorityPhrases) {
    stripped = removeNormalizedPhrase(stripped, phrase);
  }

  return stripped
    .split(" ")
    .filter((token) => token && !seniorityTokens.has(token))
    .join(" ");
}

export function removeNormalizedPhrase(value: string, phrase: string) {
  if (!value || !phrase) {
    return value;
  }

  return value
    .replace(new RegExp(`(^| )${escapeRegExp(phrase)}(?= |$)`, "g"), " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function containsNormalizedPhrase(haystack: string, phrase: string) {
  if (!haystack || !phrase) {
    return false;
  }

  return new RegExp(`(^| )${escapeRegExp(phrase)}(?= |$)`).test(haystack);
}

export function extractHeadWord(value: string) {
  const tokens = tokenizeTitle(value);

  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const token = tokens[index];
    if (roleHeadWords.has(token)) {
      return token;
    }
  }

  return undefined;
}

export function extractMeaningfulTokens(value: string) {
  const headWord = extractHeadWord(value);

  return tokenizeTitle(value).filter(
    (token) => token && token !== headWord && !stopWords.has(token),
  );
}

export function replaceHeadWord(
  value: string,
  fromWord: string,
  toWord: string,
) {
  const normalized = normalizeTitleText(value);
  if (!normalized || !fromWord || !toWord || fromWord === toWord) {
    return normalized;
  }

  const tokens = normalized.split(" ");
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    if (tokens[index] === fromWord) {
      tokens[index] = toWord;
      return tokens.join(" ");
    }
  }

  return normalized;
}

export function dedupeNormalizedValues(values: Array<string | undefined>) {
  const deduped: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    const normalized = normalizeTitleText(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    deduped.push(normalized);
  }

  return deduped;
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
