import { describe, expect, it } from "vitest";

import { parseGeoIntent } from "@/lib/geo/parse";
import { normalizeJobGeoLocation } from "@/lib/geo/location";
import { matchJobLocationAgainstGeoIntent } from "@/lib/geo/match";
import { buildIndexedJobCandidateQuery } from "@/lib/server/search/job-search-index";
import { buildDiscoveryClausesFromGeoIntent } from "@/lib/geo/discovery";

function match(locationText: string, query: string) {
  return matchJobLocationAgainstGeoIntent(
    normalizeJobGeoLocation({ locationText, locationRaw: locationText }),
    parseGeoIntent(query),
  );
}

function matchedLocations(locations: string[], query: string) {
  return locations.filter((location) => match(location, query).matches);
}

describe("generic geo location engine", () => {
  it("keeps Canada country searches in Canada", () => {
    expect(
      matchedLocations(
        ["Toronto, Canada", "Vancouver, Canada", "Tokyo, Japan", "Seoul, South Korea"],
        "Canada",
      ),
    ).toEqual(["Toronto, Canada", "Vancouver, Canada"]);
  });

  it("keeps Germany country searches in Germany", () => {
    expect(
      matchedLocations(
        ["Berlin, Germany", "Munich, Germany", "Paris, France", "London, UK"],
        "Germany",
      ),
    ).toEqual(["Berlin, Germany", "Munich, Germany"]);
  });

  it("keeps city searches at city scope", () => {
    expect(
      matchedLocations(["Toronto, Canada", "Vancouver, Canada", "Tokyo, Japan"], "Toronto"),
    ).toEqual(["Toronto, Canada"]);
  });

  it("keeps region searches at region scope", () => {
    expect(
      matchedLocations(
        [
          "Toronto, Ontario, Canada",
          "Vancouver, British Columbia, Canada",
          "Montreal, Quebec, Canada",
        ],
        "Ontario",
      ),
    ).toEqual(["Toronto, Ontario, Canada"]);
  });

  it("does not let remote-country searches leak other countries or physical jobs", () => {
    expect(
      matchedLocations(
        ["Remote Canada", "Remote United States", "Remote Europe", "Toronto, Canada"],
        "Remote Canada",
      ),
    ).toEqual(["Remote Canada"]);
  });

  it("global remote searches match remote jobs but not onsite jobs", () => {
    expect(
      matchedLocations(
        ["Remote Canada", "Remote United States", "Remote Worldwide", "Tokyo, Japan onsite"],
        "Remote",
      ),
    ).toEqual(["Remote Canada", "Remote United States", "Remote Worldwide"]);
  });

  it("ambiguous cities do not broaden to unrelated places", () => {
    const intent = parseGeoIntent("London");
    expect(intent.scope).toBe("ambiguous");
    expect(
      matchedLocations(["London, UK", "London, Ontario, Canada", "Berlin, Germany"], "London"),
    ).toEqual(["London, UK", "London, Ontario, Canada"]);
  });

  it("legacy text-only locations resolve through generic fallback", () => {
    const locations = ["Toronto, ON", "Berlin, Germany", "Tokyo, Japan", "Remote Canada"];
    expect(matchedLocations(locations, "Canada")).toEqual(["Toronto, ON", "Remote Canada"]);
    expect(matchedLocations(locations, "Germany")).toEqual(["Berlin, Germany"]);
    expect(matchedLocations(locations, "Japan")).toEqual(["Tokyo, Japan"]);
  });

  it("builds generic indexed candidate queries from GeoIntent keys", () => {
    const query = buildIndexedJobCandidateQuery({
      title: "machine learning engineer",
      country: "Canada",
    });
    const serialized = JSON.stringify(query.filter);
    expect(serialized).toContain("searchIndex.locationSearchKeys");
    expect(serialized).toContain("geoLocation.searchKeys");
    expect(serialized).toContain("country:canada");
    expect(serialized).not.toContain("resolvedLocation.isUnitedStates");
  });

  it("builds discovery clauses from catalog data", () => {
    expect(buildDiscoveryClausesFromGeoIntent(parseGeoIntent("Germany"))).toEqual(
      expect.arrayContaining([
        "germany",
        "remote germany",
        "remote in germany",
        "berlin",
        "berlin germany",
      ]),
    );
    expect(buildDiscoveryClausesFromGeoIntent(parseGeoIntent("Tokyo"))).toContain("Tokyo Japan");
  });
});
