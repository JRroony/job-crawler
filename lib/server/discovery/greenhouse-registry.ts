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
  { token: "contentful", companyHint: "Contentful" },
  { token: "coursera", companyHint: "Coursera" },
  { token: "crunchyroll", companyHint: "Crunchyroll" },
  { token: "current81", companyHint: "Current" },
  { token: "datadog", companyHint: "Datadog" },
  { token: "discord", companyHint: "Discord" },
  { token: "divergent", companyHint: "Divergent" },
  { token: "doordashusa", companyHint: "DoorDash" },
  { token: "dropbox", companyHint: "Dropbox" },
  { token: "duolingo", companyHint: "Duolingo" },
  { token: "eosfitness", companyHint: "EoS Fitness" },
  { token: "figma", companyHint: "Figma" },
  { token: "flexport", companyHint: "Flexport" },
  { token: "forafinancial", companyHint: "Fora Financial" },
  { token: "gitlab", companyHint: "GitLab" },
  { token: "gmmb", companyHint: "GMMB" },
  { token: "govini", companyHint: "Govini" },
  { token: "greenhouse", companyHint: "Greenhouse" },
  { token: "graphcore", companyHint: "Graphcore" },
  { token: "gusto", companyHint: "Gusto" },
  { token: "instacart", companyHint: "Instacart" },
  { token: "kc3", companyHint: "KC3" },
  { token: "loom", companyHint: "Loom" },
  { token: "lyft", companyHint: "Lyft" },
  { token: "mongodb", companyHint: "MongoDB" },
  { token: "omadahealth", companyHint: "Omada Health" },
  { token: "parloa", companyHint: "Parloa" },
  { token: "proshares", companyHint: "ProShares" },
  { token: "public", companyHint: "Public" },
  { token: "purestorage", companyHint: "Pure Storage" },
  { token: "reddit", companyHint: "Reddit" },
  { token: "samsara", companyHint: "Samsara" },
  { token: "scaleai", companyHint: "Scale AI" },
  { token: "sentry", companyHint: "Sentry" },
  { token: "smarterdx", companyHint: "SmarterDx" },
  { token: "squarespace", companyHint: "Squarespace" },
  { token: "techholding", companyHint: "Tech Holding" },
  { token: "thumbtack", companyHint: "Thumbtack" },
  { token: "turo", companyHint: "Turo" },
  { token: "wayfair", companyHint: "Wayfair" },
  { token: "xometry", companyHint: "Xometry" },
  { token: "zinnia", companyHint: "Zinnia" },
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
