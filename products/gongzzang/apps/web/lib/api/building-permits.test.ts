import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchBuildingPermits } from "./building-permits";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU.
const pnu = "9999900000100000000";
const permit = {
  arch_kind_name: "신축",
  main_purpose_name: "공장",
  plot_area_m2: "1000.5",
  building_area_m2: "400.25",
  total_floor_area_m2: "800",
  permit_date: "20980101",
  start_scheduled_date: null,
  start_delayed_date: null,
  start_actual_date: "20980301",
  use_approval_date: "20990101",
};
const document = () => ({
  schema_version: "foundation-platform.parcel_building_permits_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_building_permits", iceberg_snapshot_id: "999990000000000001" },
  building_permits: { records: [permit] },
  building_permit_total: 2,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchBuildingPermits", () => {
  it("reads the parcel's 건축인허가 from the building-permits edge path and keeps the full count", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const permits = await fetchBuildingPermits(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/building-permits/by-pnu/${pnu}`,
      expect.anything(),
    );
    expect(permits?.total).toBe(2);
    expect(permits?.records[0]?.main_purpose_name).toBe("공장");
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchBuildingPermits(pnu)).toBeNull();
  });

  it("refuses a document for another parcel", async () => {
    const other = { ...document(), pnu: "9999900000100000001" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(other), { status: 200 })),
    );
    await expect(fetchBuildingPermits(pnu)).rejects.toThrow("disagrees");
  });
});
