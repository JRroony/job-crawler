import { normalizeGeoText, uniqueStrings } from "@/lib/geo/normalize";

export type GeoCatalogRegion = {
  code?: string;
  name: string;
  aliases?: string[];
  discoveryAliases?: string[];
};

export type GeoCatalogCity = {
  name: string;
  regionCode?: string;
  regionName?: string;
  aliases?: string[];
};

export type GeoCatalogCountry = {
  code: string;
  name: string;
  aliases?: string[];
  discoveryAliases?: string[];
  remoteDiscoveryAliases?: string[];
  regions?: GeoCatalogRegion[];
  cities?: GeoCatalogCity[];
};

export const geoCatalog: GeoCatalogCountry[] = [
  {
    code: "US",
    name: "United States",
    aliases: ["United States of America", "USA", "US", "U.S.", "U.S.A."],
    discoveryAliases: ["United States", "USA", "US"],
    remoteDiscoveryAliases: ["United States", "USA", "US"],
    regions: [
      ["CA", "California"], ["TX", "Texas"], ["NY", "New York", "New York State"], ["WA", "Washington", "Washington State"],
      ["MA", "Massachusetts"], ["IL", "Illinois"], ["DC", "District of Columbia"], ["CO", "Colorado"],
      ["GA", "Georgia"], ["FL", "Florida"], ["OR", "Oregon"], ["VA", "Virginia"],
    ].map(([code, name, alias]) => ({ code, name, aliases: alias ? [alias] : undefined, discoveryAliases: alias ? [alias] : undefined })),
    cities: [
      ["Seattle", "WA", "Washington"], ["Bellevue", "WA", "Washington"], ["San Francisco", "CA", "California"], ["San Jose", "CA", "California"],
      ["Mountain View", "CA", "California"], ["New York", "NY", "New York"], ["Austin", "TX", "Texas"],
      ["Boston", "MA", "Massachusetts"], ["Chicago", "IL", "Illinois"], ["Washington", "DC", "District of Columbia"],
      ["Denver", "CO", "Colorado"], ["Atlanta", "GA", "Georgia"], ["Miami", "FL", "Florida"],
    ].map(([name, regionCode, regionName]) => ({ name, regionCode, regionName })),
  },
  {
    code: "CA",
    name: "Canada",
    aliases: ["CA Canada"],
    discoveryAliases: ["Canada", "CA Canada"],
    remoteDiscoveryAliases: ["Canada"],
    regions: [
      ["ON", "Ontario"], ["BC", "British Columbia"], ["QC", "Quebec"], ["AB", "Alberta"],
      ["MB", "Manitoba"], ["SK", "Saskatchewan"], ["NS", "Nova Scotia"], ["NB", "New Brunswick"],
      ["NL", "Newfoundland and Labrador"], ["PE", "Prince Edward Island"], ["NT", "Northwest Territories"],
      ["YT", "Yukon"], ["NU", "Nunavut"],
    ].map(([code, name]) => ({ code, name })),
    cities: [
      ["Toronto", "ON", "Ontario"], ["Vancouver", "BC", "British Columbia"], ["Montreal", "QC", "Quebec"],
      ["Calgary", "AB", "Alberta"], ["Waterloo", "ON", "Ontario"], ["Kitchener", "ON", "Ontario"],
      ["Ottawa", "ON", "Ontario"], ["Markham", "ON", "Ontario"], ["Mississauga", "ON", "Ontario"],
      ["Edmonton", "AB", "Alberta"], ["Victoria", "BC", "British Columbia"], ["Halifax", "NS", "Nova Scotia"],
      ["Quebec City", "QC", "Quebec"], ["London", "ON", "Ontario"],
    ].map(([name, regionCode, regionName]) => ({ name, regionCode, regionName })),
  },
  { code: "GB", name: "United Kingdom", aliases: ["UK", "Great Britain", "Britain"], cities: [{ name: "London" }, { name: "Manchester" }, { name: "Edinburgh" }] },
  { code: "DE", name: "Germany", aliases: ["Deutschland"], cities: [{ name: "Berlin" }, { name: "Munich", aliases: ["Muenchen"] }, { name: "Hamburg" }, { name: "Frankfurt" }] },
  {
    code: "IN",
    name: "India",
    cities: [
      { name: "Bengaluru", aliases: ["Bangalore"] },
      { name: "Hyderabad" },
      { name: "Mumbai" },
      { name: "Delhi" },
      { name: "Pune" },
      { name: "Noida" },
      { name: "Chennai" },
      { name: "Gurugram", aliases: ["Gurgaon"] },
    ],
  },
  { code: "JP", name: "Japan", cities: [{ name: "Tokyo" }, { name: "Osaka" }, { name: "Kyoto" }] },
  { code: "KR", name: "South Korea", aliases: ["Korea", "Republic of Korea"], cities: [{ name: "Seoul" }] },
  { code: "SG", name: "Singapore", cities: [{ name: "Singapore" }] },
  { code: "AU", name: "Australia", cities: [{ name: "Sydney" }, { name: "Melbourne" }, { name: "Brisbane" }] },
  { code: "FR", name: "France", cities: [{ name: "Paris" }, { name: "Lyon" }] },
  { code: "NL", name: "Netherlands", aliases: ["Holland"], cities: [{ name: "Amsterdam" }, { name: "Rotterdam" }] },
  { code: "IE", name: "Ireland", cities: [{ name: "Dublin" }] },
  { code: "IL", name: "Israel", cities: [{ name: "Tel Aviv" }, { name: "Jerusalem" }] },
  { code: "CN", name: "China", cities: [{ name: "Beijing" }, { name: "Shanghai" }, { name: "Shenzhen" }] },
  { code: "HK", name: "Hong Kong", cities: [{ name: "Hong Kong" }] },
  { code: "TW", name: "Taiwan", cities: [{ name: "Taipei" }] },
  { code: "CH", name: "Switzerland", cities: [{ name: "Zurich" }, { name: "Geneva" }] },
  { code: "SE", name: "Sweden", cities: [{ name: "Stockholm" }] },
  { code: "ES", name: "Spain", cities: [{ name: "Madrid" }, { name: "Barcelona" }] },
  { code: "IT", name: "Italy", cities: [{ name: "Milan" }, { name: "Rome" }] },
  { code: "PL", name: "Poland", cities: [{ name: "Warsaw" }, { name: "Krakow" }] },
  { code: "BR", name: "Brazil", cities: [{ name: "Sao Paulo" }, { name: "Rio de Janeiro" }] },
  { code: "MX", name: "Mexico", cities: [{ name: "Mexico City" }, { name: "Guadalajara" }] },
  { code: "PH", name: "Philippines", aliases: ["PH"], cities: [{ name: "Manila" }] },
];

export function countryAliases(country: GeoCatalogCountry) {
  return uniqueStrings([country.name, country.code, ...(country.aliases ?? [])]);
}

export function regionAliases(country: GeoCatalogCountry, region: GeoCatalogRegion) {
  return uniqueStrings([
    region.name,
    region.code,
    ...(region.aliases ?? []),
    `${region.name} ${country.name}`,
    region.code ? `${region.code} ${country.name}` : undefined,
  ]);
}

export function cityAliases(country: GeoCatalogCountry, city: GeoCatalogCity) {
  return uniqueStrings([
    city.name,
    ...(city.aliases ?? []),
    `${city.name} ${country.name}`,
    city.regionCode ? `${city.name} ${city.regionCode}` : undefined,
    city.regionName ? `${city.name} ${city.regionName}` : undefined,
  ]);
}

export function countryLocationAliases(country: GeoCatalogCountry) {
  return uniqueStrings([
    ...countryAliases(country),
    ...(country.regions ?? []).flatMap((region) => regionAliases(country, region)),
    ...(country.cities ?? []).flatMap((city) => cityAliases(country, city)),
  ]);
}

export function findCountryByAlias(value?: string) {
  const normalized = normalizeGeoText(value);
  return geoCatalog.find((country) =>
    countryAliases(country).some((alias) => normalizeGeoText(alias) === normalized),
  );
}

export function findRegionByAlias(value?: string, countryCode?: string) {
  const normalized = normalizeGeoText(value);
  return geoCatalog.flatMap((country) =>
    country.regions?.map((region) => ({ country, region })) ?? [],
  ).filter(({ country, region }) =>
    (!countryCode || country.code === countryCode) &&
    regionAliases(country, region).some((alias) => normalizeGeoText(alias) === normalized),
  );
}

export function findCityByAlias(value?: string, countryCode?: string, regionCode?: string) {
  const normalized = normalizeGeoText(value);
  return geoCatalog.flatMap((country) =>
    country.cities?.map((city) => ({ country, city })) ?? [],
  ).filter(({ country, city }) =>
    (!countryCode || country.code === countryCode) &&
    (!regionCode || city.regionCode === regionCode) &&
    cityAliases(country, city).some((alias) => normalizeGeoText(alias) === normalized),
  );
}
