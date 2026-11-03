import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchRegulationZones } from "./regulation-zones";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU and layer codes no
// provider uses.
const pnu = "9999900000100000000";
const zone = (id: string, layerCode: string) => ({
  zone_id: `${layerCode}:${id}`,
  layer_code: layerCode,
  layer_name: `시험층/${layerCode}`,
  zone_code: "UQZ999",
  zone_name: `시험구역 ${id}`,
  release_date: "2026-10-09",
});
const document = () => ({
  schema_version: "foundation-platform.parcel_regulation_zones_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_regulation_zones", iceberg_snapshot_id: "999990000000000001" },
  regulation_zones: { zones: [zone("A", "D998"), zone("B", "D999")] },
  zone_total: 3,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("regulation zones edge", () => {
  it("requests the lane directly and returns the parcel's zones with their total", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const zones = await fetchRegulationZones(pnu, signal);
    expect(zones?.zoneTotal).toBe(3);
    expect(zones?.zones.map((z) => z.zone_id)).toEqual(["D998:A", "D999:B"]);
    expect(fetcher).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/regulation-zones/by-pnu/${pnu}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads a parcel without a document as none, and refuses another parcel's document", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 404 })));
    expect(await fetchRegulationZones(pnu)).toBeNull();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ...document(), pnu: "9999900000100000001" })),
    );
    await expect(fetchRegulationZones(pnu)).rejects.toThrow(/disagrees/);
  });

  it("counts the listed zones when the document carries no total, and fails a server error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ...document(), zone_total: null })),
    );
    expect((await fetchRegulationZones(pnu))?.zoneTotal).toBe(2);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 503 })));
    await expect(fetchRegulationZones(pnu)).rejects.toThrow(/503/);
  });
});
