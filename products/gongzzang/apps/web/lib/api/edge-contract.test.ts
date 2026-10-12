// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { env } from "@/lib/env";
import { EdgeBuildingProfileSchema } from "./building-edge";
import { UnitPriceDocumentSchema } from "./unit-price-edge";

// The web's copies of what the foundation publishes are checked against their sources: the
// connection contract for the unit price lane, the Rust constant for the building document.
const FOUNDATION = resolve(__dirname, "../../../../../../platforms/foundation-platform");
const lane = JSON.parse(
  readFileSync(resolve(FOUNDATION, "config/r2-connections.contract.json"), "utf8"),
).by_pnu_lanes["unit-price"];
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
});
