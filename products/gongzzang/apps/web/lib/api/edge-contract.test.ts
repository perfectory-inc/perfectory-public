// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { env } from "@/lib/env";
import { EdgeBuildingProfileSchema } from "./building-edge";
import { CourtAuctionDocumentSchema } from "./court-auction";
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
const registry = JSON.parse(
  readFileSync(resolve(FOUNDATION, "contracts/schema-registry/registry.json"), "utf8"),
) as {
  subjects: {
    subject: string;
    owner: { lane: string };
    revisions: { major: number; schema_version: string }[];
  }[];
};
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
  // The lane's versions live in the schema registry (root ADR-0336), never on its contract line: the
  // web takes every string of every major the registry lists for the lane's subject.
  it.each([
    ["parcel-public-auction", PublicAuctionDocumentSchema],
    ["parcel-court-auction", CourtAuctionDocumentSchema],
  ] as const)("accepts every %s document version the lane can serve", (name, schema) => {
    expect(lanes[name].document_schema_version).toBeUndefined();
    const entry = registry.subjects.find((subject) => subject.owner.lane === name);
    expect(entry).toBeDefined();
    const servable = new Set<string>();
    for (const revision of entry?.revisions ?? []) {
      servable.add(`${entry?.subject}.v${revision.major}`);
      servable.add(revision.schema_version);
    }
    for (const version of servable) {
      expect(schema.shape.schema_version.options).toContain(version);
    }
  });
});
