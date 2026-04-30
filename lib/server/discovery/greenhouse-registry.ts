import "server-only";

export type GreenhouseRegistryEntry = {
  token: string;
  companyHint: string;
  coverageTags?: readonly string[];
};

const defaultGreenhouseRegistryEntries: GreenhouseRegistryEntry[] = [
  { token: "openai", companyHint: "OpenAI", coverageTags: ["ai", "software"] },
  { token: "stripe", companyHint: "Stripe", coverageTags: ["fintech", "data", "analytics", "global"] },
  { token: "coinbase", companyHint: "Coinbase", coverageTags: ["fintech", "data", "analytics", "remote"] },
  { token: "affirm", companyHint: "Affirm" },
  { token: "airbnb", companyHint: "Airbnb", coverageTags: ["marketplace", "data", "analytics", "united_states"] },
  { token: "alarmcom", companyHint: "Alarm.com", coverageTags: ["united_states", "data", "analytics"] },
  { token: "asana", companyHint: "Asana", coverageTags: ["productivity", "data", "analytics"] },
  { token: "benchling", companyHint: "Benchling" },
  { token: "bottomlinetechnologies", companyHint: "Bottomline", coverageTags: ["fintech", "analytics"] },
  { token: "brex", companyHint: "Brex", coverageTags: ["fintech", "data", "analytics", "united_states"] },
  { token: "canva", companyHint: "Canva" },
  { token: "checkr", companyHint: "Checkr", coverageTags: ["united_states", "analytics"] },
  { token: "chalkinc", companyHint: "Chalk" },
  { token: "chime", companyHint: "Chime", coverageTags: ["fintech", "data", "analytics", "analytics_high_yield", "united_states"] },
  { token: "contentful", companyHint: "Contentful", coverageTags: ["data", "analytics", "united_states"] },
  { token: "coursera", companyHint: "Coursera" },
  { token: "crunchyroll", companyHint: "Crunchyroll", coverageTags: ["media", "data", "analytics", "united_states"] },
  { token: "current81", companyHint: "Current", coverageTags: ["fintech", "data", "analytics", "analytics_high_yield", "united_states"] },
  { token: "datadog", companyHint: "Datadog", coverageTags: ["developer_tools", "infrastructure", "data"] },
  { token: "discord", companyHint: "Discord", coverageTags: ["consumer", "data", "analytics", "united_states"] },
  { token: "divergent", companyHint: "Divergent" },
  { token: "doordashusa", companyHint: "DoorDash", coverageTags: ["marketplace", "data", "analytics", "united_states"] },
  { token: "dropbox", companyHint: "Dropbox", coverageTags: ["productivity", "data", "analytics", "analytics_high_yield", "remote"] },
  { token: "duolingo", companyHint: "Duolingo" },
  { token: "eosfitness", companyHint: "EoS Fitness" },
  { token: "figma", companyHint: "Figma", coverageTags: ["productivity", "data", "analytics", "united_states"] },
  { token: "flexport", companyHint: "Flexport" },
  { token: "forafinancial", companyHint: "Fora Financial" },
  { token: "gitlab", companyHint: "GitLab", coverageTags: ["developer_tools", "data", "analytics", "remote"] },
  { token: "gmmb", companyHint: "GMMB" },
  { token: "govini", companyHint: "Govini", coverageTags: ["united_states", "data", "analytics", "analytics_high_yield"] },
  { token: "greenhouse", companyHint: "Greenhouse" },
  { token: "graphcore", companyHint: "Graphcore" },
  { token: "gusto", companyHint: "Gusto", coverageTags: ["hr_tech", "data", "analytics", "united_states"] },
  { token: "instacart", companyHint: "Instacart", coverageTags: ["marketplace", "data", "analytics", "analytics_high_yield", "remote"] },
  { token: "kc3", companyHint: "KC3" },
  { token: "loom", companyHint: "Loom" },
  { token: "lyft", companyHint: "Lyft", coverageTags: ["marketplace", "data", "analytics", "analytics_high_yield", "united_states"] },
  { token: "mongodb", companyHint: "MongoDB", coverageTags: ["data", "analytics", "united_states"] },
  { token: "omadahealth", companyHint: "Omada Health", coverageTags: ["healthcare", "data", "analytics", "remote"] },
  { token: "parloa", companyHint: "Parloa" },
  { token: "proshares", companyHint: "ProShares" },
  { token: "public", companyHint: "Public" },
  { token: "purestorage", companyHint: "Pure Storage", coverageTags: ["data", "analytics", "infrastructure"] },
  { token: "reddit", companyHint: "Reddit", coverageTags: ["consumer", "data", "analytics", "analytics_high_yield", "remote"] },
  { token: "samsara", companyHint: "Samsara", coverageTags: ["iot", "data", "analytics"] },
  { token: "scaleai", companyHint: "Scale AI", coverageTags: ["ai", "data", "analytics", "united_states"] },
  { token: "sentry", companyHint: "Sentry" },
  { token: "smarterdx", companyHint: "SmarterDx", coverageTags: ["healthcare", "data", "analytics", "remote"] },
  { token: "squarespace", companyHint: "Squarespace" },
  { token: "techholding", companyHint: "Tech Holding", coverageTags: ["consulting", "analytics", "remote"] },
  { token: "thumbtack", companyHint: "Thumbtack", coverageTags: ["marketplace", "data", "analytics", "analytics_high_yield", "remote"] },
  { token: "turo", companyHint: "Turo" },
  { token: "wayfair", companyHint: "Wayfair", coverageTags: ["ecommerce", "data", "analytics"] },
  { token: "xometry", companyHint: "Xometry", coverageTags: ["marketplace", "data", "analytics", "united_states"] },
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
