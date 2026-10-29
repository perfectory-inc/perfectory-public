// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { env } from "@/lib/env";
import { EdgeBuildingProfileSchema } from "./building-edge";
import { PublicAuctionDocumentSchema } from "./public-auction";
import { UnitPriceDocumentSchema } from "./unit-price-edge";

// The web's copies of what the foundation publishes are checked against their sources: the
// connection contract for the unit price and public auction lanes, the Rust constant for the
// building document.
const FOUNDATION = resolve(__dirname, "../../../../../../platforms/foundation-platform");
const lanes = JSON.parse(
  readFileSync(resolve(FOUNDATION, "config/r2-connections.contract.json"), "utf8"),
).by_pnu_lanes;
const lane = lanes["unit-price"];
const publicAuction = lanes["parcel-public-auction"];
const buildingVersion = readFileSync(
  resolve(FOUNDATION, "crates/foundation-contracts/src/building_panel.rs"),
  "utf8",
).match(/pub const BUILDING_DOCUMENT_SCHEMA_VERSION: &str = "([^"]+)";/)?.[1];

describe("the web reads what the foundation publishes", () => {
  it("accepts the building document version the publisher writes", () => {
    expect(buildingVersion).toBeDefined();
    expect(EdgeBuildingProfileSchema.shape.schema_version.options).toContain(buildingVersion);
  });

  it("reads the unit price lane's document version, table and host from the contract", () => {
    expect(UnitPriceDocumentSchema.shape.schema_version.value).toBe(lane.document_schema_version);
    expect(UnitPriceDocumentSchema.shape.source.shape.table.value).toBe(lane.gold_table);
    expect(new URL(env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL).host).toBe(lane.public_hostname);
  });

  // 2026-10-29: a web build that refused the public auction v2 document showed no auction at all.
  // A lane serves its current version and every version it expands (root ADR-0320 §5), so the web
  // must take each of them.
  it("accepts every public auction document version the lane can serve", () => {
    const servable = [
      publicAuction.document_schema_version,
      ...(publicAuction.document_schema_expands ?? []),
    ];
    expect(servable[0]).toBeDefined();
    for (const version of servable) {
      expect(PublicAuctionDocumentSchema.shape.schema_version.options).toContain(version);
    }
  });
});
