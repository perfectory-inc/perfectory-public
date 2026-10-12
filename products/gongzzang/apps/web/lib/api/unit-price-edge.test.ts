import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchUnitPrices, UnitPriceDocumentSchema } from "./unit-price-edge";

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
const unitId = "00000000-0000-8000-8000-000000000002";
const document = () => ({
  schema_version: "foundation-platform.unit_price_by_pnu.v1",
  pnu,
  source: { table: "gold.unit_price_panel", iceberg_snapshot_id: "999990000000000001" },
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
  it("requests the lane directly and keys each history by unit id", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const prices = await fetchUnitPrices(pnu, signal);
    expect(prices.get(unitId)).toEqual(document().units[0]?.official_price_history);
    expect(fetcher).toHaveBeenCalledWith(`https://unit-prices.example.test${prefix}${pnu}`, {
      signal,
      headers: { accept: "application/json" },
    });
  });

  it("reads 404 as no prices and propagates outages", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    expect((await fetchUnitPrices(pnu)).size).toBe(0);
    await expect(fetchUnitPrices(pnu)).rejects.toThrow("503");
  });

  it("refuses annual or malformed dates, negative prices, another parcel and a noncanonical PNU", async () => {
    for (const price of [
      { base_year: 2010, price_won: 36000000 },
      { base_date: "2010-01-01", price_won: 36000000 },
      { base_date: "20100101", price_won: -1 },
    ]) {
      const bad = { ...document(), units: [{ id: unitId, official_price_history: [price] }] };
      expect(UnitPriceDocumentSchema.safeParse(bad).success).toBe(false);
    }
    const fetcher = vi
      .fn()
      .mockResolvedValue(Response.json({ ...document(), pnu: "9999900000200000000" }));
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchUnitPrices("../manifest.json")).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
    await expect(fetchUnitPrices(pnu)).rejects.toThrow("PNU");
  });
});
