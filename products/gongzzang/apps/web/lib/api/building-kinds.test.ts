import { afterEach, describe, expect, it, vi } from "vitest";
import { BuildingNotServedError } from "./building-edge";
import { fetchBuildingProfile } from "./building-kinds";
import { toBuildingUnitsResponse } from "./building-units";
import { toBuildingsResponse } from "./buildings";
import { toFloorsResponse } from "./floors";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_BUILDING_EDGE_BASE_URL: "https://buildings.example.test/" },
}));

// Repository-reserved synthetic PNU and ids.
const pnu = "9999900000100000000";
const parcelId = "00000000-0000-8000-8000-000000000000";
const buildingId = "00000000-0000-8000-8000-000000000001";
const unit = (id: string, parent: string | null, ho = "101호") => ({
  id,
  parcel_id: parcelId,
  building_id: parent,
  building_name: "",
  dong_name: "101동",
  ho_name: ho,
  floor_label: "1층",
  exclusive_area_m2: null,
  usage_name: "",
  structure_name: "",
});
const title = (purpose: string | null) => ({
  id: buildingId,
  parcel_id: parcelId,
  register_pk: "BLDG-1",
  purpose_code: purpose,
  structure_code: null,
  floor_area_m2: null,
  stories: 3,
  below_ground_floors: 0,
  has_rooftop: false,
  rooftop_usage: "",
  built_year: 2020,
});
const floor = (id: string) => ({
  floor_row_id: id,
  floor_kind: "above_ground",
  floor_number: 1,
  floor_index: 1,
  floor_display_ko: "1층",
});
const bundle = () => ({
  schema_version: "foundation-platform.building_by_pnu_profile.v3",
  pnu,
  source: { table: "gold.building_panel", iceberg_snapshot_id: "999990000000000001" },
  buildings: [
    {
      ...title("bundle"),
      floors: [floor("BUNDLE-FLOOR")],
      units: [unit("00000000-0000-8000-8000-000000000002", buildingId, "번들호")],
    },
  ],
  unlinked_units: [unit("00000000-0000-8000-8000-000000000003", null, "번들미연결")],
});
const envelope = (lane: string) => ({
  schema_version: `foundation-platform.${lane.replaceAll("-", "_")}_by_pnu.v1`,
  pnu,
  source: { table: `gold.${lane.replaceAll("-", "_")}`, iceberg_snapshot_id: "999990000000000002" },
});
const lanes = {
  titles: { ...envelope("building-titles"), buildings: [title("lane")] },
  floors: {
    ...envelope("building-floors"),
    floors: [{ register_pk: "BLDG-1", id: buildingId, floors: [floor("LANE-FLOOR")] }],
  },
  units: {
    ...envelope("building-units"),
    units: [
      {
        register_pk: "BLDG-1",
        id: buildingId,
        units: [unit("00000000-0000-8000-8000-000000000004", buildingId, "레인호")],
      },
    ],
    unlinked_units: [],
  },
};

/** The edge: the bundle at `/buildings/by-pnu/`, each kind at `/buildings/<kind>/by-pnu/`. */
function edge(answers: Record<string, unknown>) {
  return vi.fn(async (url: string) => {
    const segment = new URL(url).pathname.split("/")[2] ?? "";
    const answer = answers[segment === "by-pnu" ? "bundle" : segment];
    if (typeof answer === "number") return new Response(null, { status: answer });
    return answer === undefined ? new Response(null, { status: 404 }) : Response.json(answer);
  });
}

afterEach(() => vi.unstubAllGlobals());

describe("the building views read the three kinds beside the bundle (root ADR-0193 §5)", () => {
  it("takes each kind from its lane when the lane has a document, in one round", async () => {
    const fetcher = edge({ bundle: bundle(), ...lanes });
    vi.stubGlobal("fetch", fetcher);
    const profile = await fetchBuildingProfile(pnu);
    expect(toBuildingsResponse(profile).buildings[0]?.purpose).toBe("lane");
    expect(toFloorsResponse(profile).buildings[0]?.floors.map((f) => f.floor_row_id)).toEqual([
      "LANE-FLOOR",
    ]);
    expect(toBuildingUnitsResponse(profile).units.map((u) => u.ho_name)).toEqual(["레인호"]);
    // The lanes' word wins: the units lane holds no unlinked unit, so none is drawn.
    expect(profile.unlinked_units).toEqual([]);
    expect(fetcher).toHaveBeenCalledTimes(4);
  });

  it("keeps the bundle's values for an unpublished lane (503), asking each lane once", async () => {
    const fetcher = edge({ bundle: bundle(), titles: lanes.titles, floors: 503, units: 503 });
    vi.stubGlobal("fetch", fetcher);
    const profile = await fetchBuildingProfile(pnu);
    expect(toBuildingsResponse(profile).buildings[0]?.purpose).toBe("lane");
    expect(toFloorsResponse(profile).buildings[0]?.floors.map((f) => f.floor_row_id)).toEqual([
      "BUNDLE-FLOOR",
    ]);
    expect(toBuildingUnitsResponse(profile).units.map((u) => u.ho_name)).toEqual([
      "번들호",
      "번들미연결",
    ]);
    expect(fetcher).toHaveBeenCalledTimes(4);
  });

  it("draws the lanes for a parcel the bundle does not serve, and none when nothing does", async () => {
    vi.stubGlobal("fetch", edge({ bundle: 404, ...lanes }));
    expect(toBuildingsResponse(await fetchBuildingProfile(pnu)).buildings).toHaveLength(1);
    vi.stubGlobal("fetch", edge({}));
    await expect(fetchBuildingProfile(pnu)).rejects.toBeInstanceOf(BuildingNotServedError);
  });

  it("falls back to the bundle for a lane document it cannot read", async () => {
    vi.stubGlobal(
      "fetch",
      edge({ bundle: bundle(), ...lanes, floors: { ...lanes.floors, floors: "not a list" } }),
    );
    const profile = await fetchBuildingProfile(pnu);
    expect(toFloorsResponse(profile).buildings[0]?.floors.map((f) => f.floor_row_id)).toEqual([
      "BUNDLE-FLOOR",
    ]);
  });

  it("is an outage when the bundle fails and a kind has no lane answer to stand in", async () => {
    vi.stubGlobal("fetch", edge({ bundle: 500, titles: lanes.titles, floors: 503, units: 503 }));
    await expect(fetchBuildingProfile(pnu)).rejects.toThrow("500");
  });
});
