import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchOwnership, latestChange, ownerKinds } from "./ownership";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, no person or place.
const pnu = "9999900000100000000";
const owner = (serial: string, kind: string, changed: string | null) => ({
  co_owner_serial_no: serial,
  ownership_kind_code: kind === "법인" ? "06" : "01",
  ownership_kind_name: kind,
  residence_kind_code: "ZZ",
  residence_kind_name: "구분없음",
  state_agency_kind_code: "ZZ",
  state_agency_kind_name: "구분없음",
  ownership_change_reason_code: "03",
  ownership_change_reason: "소유권이전",
  ownership_changed_on: changed,
  co_owner_count: 2,
});
const document = () => ({
  schema_version: "foundation-platform.parcel_ownership_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_ownership", iceberg_snapshot_id: "999990000000000001" },
  ownership: {
    owners: [owner("000001", "법인", "2019-11-26"), owner("000002", "개인", "2024-01-02")],
    unit_owner_total: 0,
    unit_owner_kinds: [],
    latest_changed_on: "2024-01-02",
    base_year_month: "2024-12",
    data_reference_date: "2025-08-26",
  },
  owner_total: 2,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("ownership edge", () => {
  it("requests the lane directly and returns the parcel's owners with their total", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const ownership = await fetchOwnership(pnu, signal);
    expect(ownership?.ownerTotal).toBe(2);
    expect(ownership?.owners.map((o) => o.co_owner_serial_no)).toEqual(["000001", "000002"]);
    expect(fetcher).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/ownership/by-pnu/${pnu}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads a parcel without a document as none, and refuses another parcel's document", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 404 })));
    expect(await fetchOwnership(pnu)).toBeNull();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ...document(), pnu: "9999900000100000001" })),
    );
    await expect(fetchOwnership(pnu)).rejects.toThrow(/disagrees/);
  });

  it("counts owners by the source's 소유구분 and finds the newest change", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(document())));
    const ownership = await fetchOwnership(pnu);
    if (!ownership) throw new Error("expected a document");
    expect(ownerKinds(ownership)).toEqual([
      ["법인", 1],
      ["개인", 1],
    ]);
    expect(latestChange(ownership)?.co_owner_serial_no).toBe("000002");
  });
});
