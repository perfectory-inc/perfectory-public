import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchRegisterMasters, filled } from "./register-masters";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU.
const pnu = "9999900000100000000";
const site = {
  plot_area_m2: "1000.5",
  building_area_m2: "400.25",
  total_floor_area_m2: "800",
  main_purpose_name: "공장",
  main_building_count: "2",
  annex_building_count: "0",
  parking_count: "10",
  permit_date: "20980101",
  start_date: null,
  use_approval_date: "20990101",
};
const document = () => ({
  schema_version: "foundation-platform.parcel_register_masters_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_register_masters", iceberg_snapshot_id: "999990000000000001" },
  register_masters: { records: [site] },
  register_master_total: 2,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchRegisterMasters", () => {
  it("reads the parcel's 총괄표제부 from the register-masters edge path and keeps the full count", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const masters = await fetchRegisterMasters(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/register-masters/by-pnu/${pnu}`,
      expect.anything(),
    );
    expect(masters?.total).toBe(2);
    expect(masters?.records[0]?.main_purpose_name).toBe("공장");
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchRegisterMasters(pnu)).toBeNull();
  });

  it("refuses a document for another parcel", async () => {
    const other = { ...document(), pnu: "9999900000100000001" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(other), { status: 200 })),
    );
    await expect(fetchRegisterMasters(pnu)).rejects.toThrow("disagrees");
  });
});

describe("filled", () => {
  it("drops the register's empty and zero figures and keeps the rest as written", () => {
    expect([null, undefined, "", "0", "0.00", "12.5"].map(filled)).toEqual([
      false,
      false,
      false,
      false,
      false,
      true,
    ]);
  });
});
