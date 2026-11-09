import { afterEach, describe, expect, it, vi } from "vitest";
import { distanceLabel, fetchNearby } from "./nearby";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU.
const pnu = "9999900000100000000";
const document = () => ({
  schema_version: "foundation-platform.parcel_nearby_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_nearby", iceberg_snapshot_id: "999990000000000001" },
  nearby: {
    airport: { name: "공항", distance_m: 12_345 },
    railway_station: null,
    subway_station: null,
    school: null,
    university: null,
    commercial_store_count: 3,
    highway_access_points: null,
  },
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchNearby", () => {
  it("reads the parcel's 주변 from the nearby edge path", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const nearby = await fetchNearby(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/nearby/by-pnu/${pnu}`,
      expect.objectContaining({ headers: { accept: "application/json" } }),
    );
    expect(nearby?.airport?.distance_m).toBe(12_345);
    expect(nearby?.commercial_store_count).toBe(3);
  });

  it("treats a parcel without a document as none", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchNearby(pnu)).toBeNull();
  });

  it("refuses a document for another parcel", async () => {
    const other = { ...document(), pnu: "9999900000100000001" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(other), { status: 200 })),
    );
    await expect(fetchNearby(pnu)).rejects.toThrow("disagrees");
  });

  it("says metres below a kilometre and kilometres to one decimal above", () => {
    expect(distanceLabel(850)).toBe("850m");
    expect(distanceLabel(12_345)).toBe("12.3km");
  });
});
