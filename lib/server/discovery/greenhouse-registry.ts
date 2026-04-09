import "server-only";

export type GreenhouseRegistryEntry = {
  token: string;
  companyHint: string;
};

const defaultGreenhouseRegistryEntries: GreenhouseRegistryEntry[] = [
  { token: "openai", companyHint: "OpenAI" },
  { token: "stripe", companyHint: "Stripe" },
  { token: "coinbase", companyHint: "Coinbase" },
  { token: "affirm", companyHint: "Affirm" },
  { token: "airbnb", companyHint: "Airbnb" },
  { token: "alarmcom", companyHint: "Alarm.com" },
  { token: "asana", companyHint: "Asana" },
  { token: "benchling", companyHint: "Benchling" },
  { token: "bottomlinetechnologies", companyHint: "Bottomline" },
  { token: "brex", companyHint: "Brex" },
  { token: "canva", companyHint: "Canva" },
  { token: "checkr", companyHint: "Checkr" },
  { token: "chalkinc", companyHint: "Chalk" },
  { token: "chime", companyHint: "Chime" },
  { token: "coursera", companyHint: "Coursera" },
  { token: "datadog", companyHint: "Datadog" },
  { token: "discord", companyHint: "Discord" },
  { token: "doordashusa", companyHint: "DoorDash" },
  { token: "dropbox", companyHint: "Dropbox" },
  { token: "duolingo", companyHint: "Duolingo" },
  { token: "figma", companyHint: "Figma" },
  { token: "flexport", companyHint: "Flexport" },
  { token: "greenhouse", companyHint: "Greenhouse" },
  { token: "graphcore", companyHint: "Graphcore" },
  { token: "gusto", companyHint: "Gusto" },
  { token: "instacart", companyHint: "Instacart" },
  { token: "loom", companyHint: "Loom" },
  { token: "lyft", companyHint: "Lyft" },
  { token: "mongodb", companyHint: "MongoDB" },
  { token: "reddit", companyHint: "Reddit" },
  { token: "samsara", companyHint: "Samsara" },
  { token: "scaleai", companyHint: "Scale AI" },
  { token: "sentry", companyHint: "Sentry" },
  { token: "squarespace", companyHint: "Squarespace" },
  { token: "thumbtack", companyHint: "Thumbtack" },
  { token: "turo", companyHint: "Turo" },
  { token: "wayfair", companyHint: "Wayfair" },
];

const greenhouseRegistryCompanyHints = new Map(
  defaultGreenhouseRegistryEntries.map((entry) => [entry.token, entry.companyHint]),
);

export function resolveGreenhouseRegistryTokens(
  ...tokenLists: Array<readonly string[] | undefined>
) {
  const deduped = new Set<string>();

  for (const entry of defaultGreenhouseRegistryEntries) {
    deduped.add(entry.token);
  }

  for (const tokenList of tokenLists) {
    for (const token of tokenList ?? []) {
      const normalizedToken = normalizeGreenhouseBoardToken(token);
      if (normalizedToken) {
        deduped.add(normalizedToken);
      }
    }
  }

  return Array.from(deduped);
}

export function lookupGreenhouseCompanyHint(token?: string) {
  const normalizedToken = normalizeGreenhouseBoardToken(token);
  return normalizedToken ? greenhouseRegistryCompanyHints.get(normalizedToken) : undefined;
}

export function normalizeGreenhouseBoardToken(value?: string) {
  const trimmed = value?.trim().toLowerCase();
  return trimmed ? trimmed : undefined;
}

export function getDefaultGreenhouseRegistryEntries() {
  return [...defaultGreenhouseRegistryEntries];
}
