// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { env } from "@/lib/env";
import { PHOTO_RENDITIONS } from "@/lib/media/photo-ladder";
import { EdgeBuildingProfileSchema } from "./building-edge";
import { COMPLEX_INDEX_PATH } from "./complexes";
import { CourtAuctionDocumentSchema } from "./court-auction";
import { PublicAuctionDocumentSchema } from "./public-auction";
import { UnitPriceDocumentSchema } from "./unit-price-edge";

// The web's copies of what the foundation publishes are checked against their sources: the
// schema registry for the pack lanes, the Rust constant for the building document.
const FOUNDATION = resolve(__dirname, "../../../../../../platforms/foundation-platform");
const connections = JSON.parse(
  readFileSync(resolve(FOUNDATION, "config/r2-connections.contract.json"), "utf8"),
);
const lanes = connections.by_pnu_lanes;
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
  // Root ADR-0456: the complex index is on the parcel edge, the host the web reads parcels from.
  it("reads the industrial-complex index where the contract puts it, on the parcel edge", () => {
    const documents = connections.industrial_complex_documents;
    expect(COMPLEX_INDEX_PATH).toBe(documents.index_object);
    expect(documents.lane).toBe("parcel");
    expect(new URL(env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL).host).toBe(
      connections.parcel_by_pnu_gateway.public_hostname,
    );
  });

  it("accepts the building document version the publisher writes", () => {
    expect(buildingVersion).toBeDefined();
    expect(EdgeBuildingProfileSchema.shape.schema_version.options).toContain(buildingVersion);
  });

  it("reads the unit price lane's host from the contract", () => {
    expect(new URL(env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL).host).toBe(lane.public_hostname);
  });

  // 2026-10-29: a web build that refused the public auction v2 document showed no auction at all.
  // The lane's versions live in the schema registry (root ADR-0336), never on its contract line: the
  // web takes every string of every major the registry lists for the lane's subject.
  it.each([
    ["parcel-public-auction", PublicAuctionDocumentSchema],
    ["parcel-court-auction", CourtAuctionDocumentSchema],
    ["unit-price", UnitPriceDocumentSchema],
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

  // Root ADR-0427: the web builds the photo ladder's URLs the collector writes and the edge serves.
  it("builds the photo rendition ladder the contract names", () => {
    const ladder = connections.parcel_media.renditions as {
      version: string;
      widths: number[];
      formats: { format: string }[];
    };
    expect(PHOTO_RENDITIONS.version).toBe(ladder.version);
    expect([...PHOTO_RENDITIONS.widths]).toEqual(ladder.widths);
    expect([...PHOTO_RENDITIONS.formats]).toEqual(ladder.formats.map((entry) => entry.format));
    const served = new RegExp(`^(?:${connections.parcel_media.path_pattern})$`);
    for (const width of PHOTO_RENDITIONS.widths) {
      for (const format of PHOTO_RENDITIONS.formats) {
        expect(
          `onbid/media/renditions/${ladder.version}/${"a".repeat(64)}/${width}.${format}`,
        ).toMatch(served);
      }
    }
  });
});
