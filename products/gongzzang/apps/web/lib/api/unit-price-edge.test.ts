import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchUnitPrices, UNLINKED_BUILDING_ID, UnitPriceDocumentSchema } from "./unit-price-edge";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL: "https://unit-prices.example.test/" },
}));

const { prefix } = JSON.parse(
  readFileSync(
    resolve(
      __dirname,
      "../../../../../../platforms/foundation-platform/config/r2-connections.contract.json",
    ),
    "utf8",
  ),
).by_pnu_lanes["unit-price"].request_path;
const pnu = "9999900000100000000";
const buildingId = "00000000-0000-8000-8000-000000000003";
const unitId = "00000000-0000-8000-8000-000000000002";
const document = (building_id = buildingId) => ({
  schema_version: "foundation-platform.unit_price_by_pnu.v1",
  pnu,
  building_id,
  source: { table: "gold.unit_price", iceberg_snapshot_id: "999990000000000001" },
  units: [
    {
      id: unitId,
      official_price_history: [
        { base_date: "20100601", price_won: 35000000 },
        { base_date: "20100101", price_won: 36000000 },
      ],
    },
  ],
});

afterEach(() => vi.unstubAllGlobals());

describe("unit price edge", () => {
  it("requests one building's document of the PNU and keys each history by unit id", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const prices = await fetchUnitPrices(pnu, buildingId, signal);
    expect(prices.get(unitId)).toEqual(document().units[0]?.official_price_history);
    expect(fetcher).toHaveBeenCalledWith(
      `https://unit-prices.example.test${prefix}${pnu}/${buildingId}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads the unlinked units from the nil UUID document", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document(UNLINKED_BUILDING_ID)));
    vi.stubGlobal("fetch", fetcher);
    expect((await fetchUnitPrices(pnu, UNLINKED_BUILDING_ID)).size).toBe(1);
    expect(fetcher.mock.calls[0]?.[0]).toBe(
      `https://unit-prices.example.test${prefix}${pnu}/00000000-0000-0000-0000-000000000000`,
    );
  });

  it("reads 404 as no prices and propagates outages", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    expect((await fetchUnitPrices(pnu, buildingId)).size).toBe(0);
    await expect(fetchUnitPrices(pnu, buildingId)).rejects.toThrow("503");
  });

  it("refuses annual or malformed dates, negative prices and a document without its building", () => {
    for (const price of [
      { base_year: 2010, price_won: 36000000 },
      { base_date: "2010-01-01", price_won: 36000000 },
      { base_date: "20100101", price_won: -1 },
    ]) {
      const bad = { ...document(), units: [{ id: unitId, official_price_history: [price] }] };
      expect(UnitPriceDocumentSchema.safeParse(bad).success).toBe(false);
    }
    const { building_id: _, ...withoutBuilding } = document();
    expect(UnitPriceDocumentSchema.safeParse(withoutBuilding).success).toBe(false);
  });

  it("refuses a noncanonical PNU or building id before fetching", async () => {
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchUnitPrices("../manifest.json", buildingId)).rejects.toThrow();
    await expect(fetchUnitPrices(pnu, "../manifest.json")).rejects.toThrow();
    await expect(fetchUnitPrices(pnu, `${buildingId}/x`)).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("refuses a document of another parcel or another building", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ...document(), pnu: "9999900000200000000" })),
    );
    await expect(fetchUnitPrices(pnu, buildingId)).rejects.toThrow("PNU");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json(document(UNLINKED_BUILDING_ID))),
    );
    await expect(fetchUnitPrices(pnu, buildingId)).rejects.toThrow("building");
  });
});
