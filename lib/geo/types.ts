export type GeoConfidence = "high" | "medium" | "low" | "ambiguous";

export type GeoEntity = {
  code?: string;
  name: string;
  aliases: string[];
};

export type GeoIntentScope =
  | "none"
  | "global_remote"
  | "country"
  | "region"
  | "city"
  | "city_region"
  | "city_country"
  | "remote_country"
  | "remote_region"
  | "remote_city"
  | "ambiguous";

export type GeoIntent = {
  rawInput: string;
  normalizedInput: string;
  scope: GeoIntentScope;
  country?: GeoEntity;
  region?: GeoEntity;
  city?: Omit<GeoEntity, "code">;
  isRemote: boolean;
  isCountryWide: boolean;
  confidence: GeoConfidence;
  ambiguityReason?: string;
  searchKeys: string[];
  discoveryClauses: string[];
};

export type GeoLocationPoint = {
  country?: string;
  countryCode?: string;
  region?: string;
  regionCode?: string;
  city?: string;
  searchKeys: string[];
  confidence: Exclude<GeoConfidence, "ambiguous">;
  evidence: string[];
};

export type GeoLocation = {
  rawText: string;
  normalizedText: string;
  physicalLocations: GeoLocationPoint[];
  remoteEligibility: GeoLocationPoint[];
  workplaceType: "remote" | "hybrid" | "onsite" | "unknown";
  isGlobalRemote: boolean;
  unresolvedTerms: string[];
  conflicts: string[];
  searchKeys: string[];
};

export type GeoLocationMatchResult = {
  matches: boolean;
  explanation: string;
  matchedKeys: string[];
  intent: GeoIntent;
};
