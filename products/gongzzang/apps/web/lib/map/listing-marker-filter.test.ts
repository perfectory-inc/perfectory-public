// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { buildListingMarkerLayerFilter } from "@/lib/map/listing-marker-filter";
import { LISTING_MARKER_ID_PROPERTY } from "@/lib/map/marker-tile-contract";

// The tile writers alias the marker id to this Rust constant; the web must read the same name.
const rustMarkerIdProperty = readFileSync(
  resolve(__dirname, "../../../../crates/listing-domain/src/repository/marker_tile.rs"),
  "utf8",
).match(/pub const LISTING_MARKER_ID_PROPERTY: &str = "([^"]+)";/)?.[1];

describe("listing marker id property", () => {
  it("is the name the Rust tile writers emit", () => {
    expect(rustMarkerIdProperty).toBeDefined();
    expect(LISTING_MARKER_ID_PROPERTY).toBe(rustMarkerIdProperty);
  });
});

describe("buildListingMarkerLayerFilter", () => {
  it("returns an all expression when fast filters are empty", () => {
    expect(
      buildListingMarkerLayerFilter({
        types: [],
        transactions: [],
        minAreaM2: undefined,
        maxAreaM2: undefined,
        minPriceKrw: undefined,
        maxPriceKrw: undefined,
        sort: "created_at_desc",
        adminCode: undefined,
        landUseType: undefined,
      }),
    ).toEqual(["all"]);
  });

  it("builds type transaction price and area predicates", () => {
    expect(
      buildListingMarkerLayerFilter({
        types: ["factory", "industrial_land"],
        transactions: ["sale"],
        minAreaM2: 300,
        maxAreaM2: 1000,
        minPriceKrw: 100_000_000,
        maxPriceKrw: 5_000_000_000,
        sort: "created_at_desc",
        adminCode: undefined,
        landUseType: undefined,
      }),
    ).toEqual([
      "all",
      ["in", ["get", "listing_type"], ["literal", ["factory", "industrial_land"]]],
      ["in", ["get", "transaction_type"], ["literal", ["sale"]]],
      [">=", ["to-number", ["get", "area_m2"]], 300],
      ["<=", ["to-number", ["get", "area_m2"]], 1000],
      [">=", ["to-number", ["get", "price_krw"]], 100_000_000],
      ["<=", ["to-number", ["get", "price_krw"]], 5_000_000_000],
    ]);
  });

  it("adds tombstone ids as a hide predicate before listing predicates", () => {
    expect(
      buildListingMarkerLayerFilter(
        {
          types: ["factory"],
          transactions: [],
          minAreaM2: undefined,
          maxAreaM2: undefined,
          minPriceKrw: undefined,
          maxPriceKrw: undefined,
          sort: "created_at_desc",
          adminCode: undefined,
          landUseType: undefined,
        },
        ["lm_lst_01HXY3NK0Z9F6S1B2C3D4E5F6G"],
      ),
    ).toEqual([
      "all",
      ["!", ["in", ["get", "id"], ["literal", ["lm_lst_01HXY3NK0Z9F6S1B2C3D4E5F6G"]]]],
      ["in", ["get", "listing_type"], ["literal", ["factory"]]],
    ]);
  });
});
