// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { findParcelZoningAreas, interiorsOverlap } from "@/lib/map/zoning-areas";
import {
  ZONING_FAMILIES,
  ZONING_PENDING_LAYER_CODES,
  zoningColorExpression,
  zoningFamilyOf,
  zoningFamilyPending,
  zoningLayerFilter,
} from "@/lib/map/zoning-families";

describe("zoning families", () => {
  it("puts every served and pending layer under exactly one toggle", () => {
    const codes = ZONING_FAMILIES.flatMap((family) => [...family.layerCodes]);
    expect(new Set(codes).size).toBe(codes.length);
    expect(codes).toHaveLength(17);
    for (const pending of ZONING_PENDING_LAYER_CODES) expect(codes).toContain(pending);
    expect(zoningFamilyOf("D032")).toBe("greenbelt");
    expect(zoningFamilyOf("D327")).toBe("factoryLocation");
    expect(zoningFamilyOf("D125")).toBe("useDistrict");
    expect(zoningFamilyOf("D999")).toBeUndefined();
  });

  it("says which toggles still wait for collection", () => {
    expect(ZONING_PENDING_LAYER_CODES).toEqual(["D054", "D125"]);
    expect(zoningFamilyPending("useDistrict")).toBe("partial");
    expect(zoningFamilyPending("forest")).toBe("all");
    expect(zoningFamilyPending("greenbelt")).toBe("none");
  });

  it("filters to the enabled families' layers, and to nothing with none enabled", () => {
    expect(zoningLayerFilter(new Set())).toEqual(["in", ["get", "layer_code"], ["literal", []]]);
    expect(zoningLayerFilter(new Set(["factoryLocation", "greenbelt"]))).toEqual([
      "in",
      ["get", "layer_code"],
      ["literal", ["D032", "D335", "D336", "D327"]],
    ]);
  });

  it("colours each family by its layer codes", () => {
    const expression = zoningColorExpression();
    expect(expression.slice(0, 2)).toEqual(["match", ["get", "layer_code"]]);
    // a label array and a colour per family, then the fallback
    expect(expression).toHaveLength(2 + ZONING_FAMILIES.length * 2 + 1);
    expect(expression[2]).toEqual(["D032"]);
  });
});

// Synthetic geometry around (0°, 0°), written in metres: no real place.
const M_PER_DEG_X = 111_320;
const M_PER_DEG_Y = 110_540;
type Rect = [number, number, number, number];
const ring = ([x0, y0, x1, y1]: Rect) =>
  [
    [x0, y0],
    [x1, y0],
    [x1, y1],
    [x0, y1],
    [x0, y0],
  ].map(([x = 0, y = 0]) => [x / M_PER_DEG_X, y / M_PER_DEG_Y]);
const polygon = (outer: Rect, ...holes: Rect[]) => ({
  type: "Polygon",
  coordinates: [ring(outer), ...holes.map(ring)],
});
const zone = (id: string, layerCode: string, geometry: unknown) => ({
  properties: {
    zone_id: `${layerCode}:${id}`,
    layer_code: layerCode,
    layer_name: `시험층/${layerCode}`,
    zone_name: `시험구역 ${id}`,
  },
  geometry,
});

const PNU = "9999900000100120003";

function fakeMap(parcel: unknown[], zones: unknown[], hasZoning = true) {
  return {
    getSource: (id: string) => (id === "zoning" && !hasZoning ? undefined : { id }),
    querySourceFeatures: (sourceId: string) => (sourceId === "parcels" ? parcel : zones),
  };
}

describe("zones over a parcel", () => {
  const parcel = [{ properties: { pnu: PNU }, geometry: polygon([0, 0, 100, 100]) }];

  it("keeps overlapping zones once each, and drops neighbours that only share a line", () => {
    const overlapping = polygon([50, 50, 200, 200]);
    const areas = findParcelZoningAreas(
      fakeMap(parcel, [
        zone("A", "D032", overlapping),
        // the same zone cut by a tile edge comes back twice
        zone("A", "D032", overlapping),
        // next door, the shared edge 0.3 m apart: the sources disagreeing, not an overlap
        zone("B", "D335", polygon([100.3, 0, 200, 100])),
        zone("C", "D124", polygon([-300, -300, 400, 400])),
      ]),
      PNU,
    );
    expect(areas?.map((area) => area.zoneId)).toEqual(["D032:A", "D124:C"]);
    expect(areas?.[0]).toEqual({
      zoneId: "D032:A",
      layerCode: "D032",
      layerName: "시험층/D032",
      zoneName: "시험구역 A",
      family: "greenbelt",
    });
  });

  it("finds a narrow road strip across the parcel with no corner inside either", () => {
    const road = zone("R", "D142", polygon([47, -50, 53, 150]));
    expect(findParcelZoningAreas(fakeMap(parcel, [road]), PNU)?.map((a) => a.zoneId)).toEqual([
      "D142:R",
    ]);
  });

  it("leaves out a zone whose hole the parcel sits in", () => {
    const ringed = zone("H", "D036", polygon([-100, -100, 200, 200], [-10, -10, 110, 110]));
    expect(findParcelZoningAreas(fakeMap(parcel, [ringed]), PNU)).toEqual([]);
  });

  it("tells a map without the zoning unit from a parcel with no zone", () => {
    expect(findParcelZoningAreas(fakeMap(parcel, [], false), PNU)).toBeNull();
    expect(findParcelZoningAreas(fakeMap([], []), PNU)).toEqual([]);
  });

  it("counts an identical shape and a zone inside the parcel as overlapping", () => {
    const square = polygon([0, 0, 100, 100]).coordinates.map((r) =>
      r.map(([x = 0, y = 0]) => [x * M_PER_DEG_X, y * M_PER_DEG_Y]),
    );
    const inner = polygon([40, 40, 60, 60]).coordinates.map((r) =>
      r.map(([x = 0, y = 0]) => [x * M_PER_DEG_X, y * M_PER_DEG_Y]),
    );
    expect(interiorsOverlap(square, square)).toBe(true);
    expect(interiorsOverlap(square, inner)).toBe(true);
  });
});

describe("zoning pending layers (root ADR-0373 §4)", () => {
  // The Foundation lane contract is the one list of layers it does not load yet; the map's list
  // must be the same, or a collected layer would still say 수집 대기 (or a missing one would not).
  const contract = JSON.parse(
    readFileSync(
      resolve(
        __dirname,
        "../../../../../../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-use-zone-area-source-objects.json",
      ),
      "utf8",
    ),
  ) as { pending_regions?: { region: string }[] };
  // shortcut: skipped until the Foundation change that adds pending_regions (PR #999) is on main.
  it.skipIf(!contract.pending_regions)("matches the lane contract's pending_regions", () => {
    const fromContract = (contract.pending_regions ?? []).map((item) => `D${item.region}`).sort();
    expect([...ZONING_PENDING_LAYER_CODES].sort()).toEqual(fromContract);
  });
});
