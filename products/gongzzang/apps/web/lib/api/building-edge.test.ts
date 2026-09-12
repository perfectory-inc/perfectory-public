import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BuildingNotServedError,
  EdgeBuildingProfileSchema,
  EdgeUnitSchema,
  fetchBuildingProfile,
} from "./building-edge";
import { toBuildingUnitsResponse } from "./building-units";
import { toBuildingsResponse } from "./buildings";
import { toFloorsResponse } from "./floors";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_BUILDING_EDGE_BASE_URL: "https://buildings.example.test/" },
}));

const pnu = "9999900000100000000";
const parcelId = "00000000-0000-8000-8000-000000000000";
const buildingId = "00000000-0000-8000-8000-000000000001";
const unit = (id: string, parent: string | null) => ({
  id,
  parcel_id: parcelId,
  building_id: parent,
  building_name: "",
  dong_name: "101동",
  ho_name: "101호",
  floor_label: "1층",
  exclusive_area_m2: null,
  usage_name: "",
  structure_name: "",
  official_price_history: [
    { base_date: "20100601", price_won: 35000000 },
    { base_date: "20100101", price_won: 36000000 },
  ],
});
const document = () => ({
  schema_version: "foundation-platform.building_by_pnu_profile.v2",
  pnu,
  source: { table: "gold.building_panel", iceberg_snapshot_id: "999990000000000001" },
  buildings: [
    {
      id: buildingId,
      parcel_id: parcelId,
      register_pk: "BLDG-1",
      purpose_code: null,
      structure_code: null,
      floor_area_m2: null,
      stories: null,
      below_ground_floors: 0,
      has_rooftop: false,
      rooftop_usage: "",
      built_year: 2020,
      floors: [
        {
          floor_row_id: "FLOOR-1",
          floor_kind: "unknown",
          floor_number: null,
          floor_index: null,
          floor_display_ko: null,
        },
      ],
      units: [unit("00000000-0000-8000-8000-000000000002", buildingId)],
    },
  ],
  unlinked_units: [unit("00000000-0000-8000-8000-000000000003", null)],
});

afterEach(() => vi.unstubAllGlobals());

describe("building edge document mappings", () => {
  it("rejects annual or malformed reference dates and negative assessments", () => {
    const value = unit("00000000-0000-8000-8000-000000000002", buildingId);
    for (const price of [
      { base_year: 2010, price_won: 36000000 },
      { base_date: "2010-01-01", price_won: 36000000 },
      { base_date: "20100101", price_won: -1 },
    ]) {
      expect(EdgeUnitSchema.safeParse({ ...value, official_price_history: [price] }).success).toBe(
        false,
      );
    }
  });
  it("preserves absent facts, nested floors, unit prices and unlinked units without mutation", () => {
    const profile = EdgeBuildingProfileSchema.parse(document());
    const before = structuredClone(profile);
    const buildings = toBuildingsResponse(profile);
    expect(buildings.buildings[0]).toMatchObject({
      id: buildingId,
      name: "",
      purpose: null,
      total_area_m2: null,
      approved_at: null,
    });
    expect(buildings.buildings[0]?.units[0]?.official_price_history).toEqual([
      { base_date: "20100601", price_won: 35000000 },
      { base_date: "20100101", price_won: 36000000 },
    ]);
    expect(buildings.unlinked_units).toEqual(profile.unlinked_units);
    const floors = toFloorsResponse(profile).buildings[0];
    expect(floors).toMatchObject({ above_ground: null, below_ground: 0, has_rooftop: false });
    expect(floors?.floors).toEqual(profile.buildings[0]?.floors);
    expect(toBuildingUnitsResponse(profile).units).toHaveLength(2);
    expect(toBuildingUnitsResponse(profile, buildingId).units).toHaveLength(1);
    expect(toBuildingUnitsResponse(profile, null).units).toEqual(profile.unlinked_units);
    expect(profile).toEqual(before);
  });

  it("keeps an explicitly empty served document distinct from a missing object", () => {
    const profile = EdgeBuildingProfileSchema.parse({
      ...document(),
      buildings: [],
      unlinked_units: [],
    });
    expect(toBuildingsResponse(profile)).toEqual({ buildings: [], unlinked_units: [] });
    expect(toFloorsResponse(profile)).toEqual({ buildings: [] });
  });

  it("rejects a different schema and malformed required sections", () => {
    expect(() =>
      EdgeBuildingProfileSchema.parse({ ...document(), schema_version: "parcel" }),
    ).toThrow();
    expect(() => EdgeBuildingProfileSchema.parse({ ...document(), buildings: null })).toThrow();
  });
});

describe("building edge transport", () => {
  it("requests the edge directly and forwards cancellation", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    expect(await fetchBuildingProfile(pnu, signal)).toEqual(
      EdgeBuildingProfileSchema.parse(document()),
    );
    expect(fetcher).toHaveBeenCalledWith(
      `https://buildings.example.test/buildings/by-pnu/${pnu}?schema=2`,
      {
        signal,
        headers: { accept: "application/json" },
      },
    );
  });

  it("turns 404 into a typed not-served error and propagates outages", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchBuildingProfile(pnu)).rejects.toBeInstanceOf(BuildingNotServedError);
    await expect(fetchBuildingProfile(pnu)).rejects.toThrow("503");
  });

  it("refuses a noncanonical request and a response for another parcel", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValue(Response.json({ ...document(), pnu: "9999900000200000000" }));
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchBuildingProfile("../manifest.json")).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
    await expect(fetchBuildingProfile(pnu)).rejects.toThrow("PNU");
  });
});
