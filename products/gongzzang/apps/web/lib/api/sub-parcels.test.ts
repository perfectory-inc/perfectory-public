import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchSubParcels, jibunLabel } from "./sub-parcels";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): canonical-but-unused PNUs, no person or place.
const pnu = "9999900000100000000";
const other = "9999900000100020000";
const document = () => ({
  schema_version: "foundation-platform.parcel_sub_parcels_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_sub_parcels", iceberg_snapshot_id: "999990000000000001" },
  sub_parcels: {
    on_other_parcels: [
      {
        mgmt_key: "synthetic-1",
        register_kind_name: "일반건축물",
        other_pnu: other,
        other_jibun: "99999-00001-0-0002-0000",
      },
    ],
    on_other_total: 1,
    from_other_parcels: [],
    from_other_total: 0,
  },
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchSubParcels", () => {
  it("reads the parcel's links from the sub-parcels edge path", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const links = await fetchSubParcels(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/sub-parcels/by-pnu/${pnu}`,
      expect.anything(),
    );
    expect(links?.on_other_parcels[0]?.other_pnu).toBe(other);
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchSubParcels(pnu)).toBeNull();
  });
});

describe("jibunLabel", () => {
  it("reads the provider's 지번 as 번-지, 산 for 대지구분 1, and keeps text it cannot read", () => {
    expect(jibunLabel("99999-00001-0-0002-0000")).toBe("2");
    expect(jibunLabel("99999-00001-1-0012-0003")).toBe("산 12-3");
    expect(jibunLabel("99999-00001-0-12a-0")).toBe("99999-00001-0-12a-0");
    expect(jibunLabel(null)).toBeNull();
  });
});
