import { afterEach, describe, expect, it, vi } from "vitest";
import { chronological, electricityPeak, fetchEnergy, monthLabel } from "./energy";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, no person or place.
const pnu = "9999900000100000000";
const document = () => ({
  schema_version: "foundation-platform.parcel_energy_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_energy", iceberg_snapshot_id: "999990000000000001" },
  energy: {
    months: [
      { use_ym: "209908", electricity_usage: 90, electricity_rows: 1, gas_usage: 12, gas_rows: 1 },
      {
        use_ym: "209907",
        electricity_usage: 350.5,
        electricity_rows: 2,
        gas_usage: null,
        gas_rows: 0,
      },
    ],
    electricity_unit: "kWh",
    latest_use_ym: "209908",
    unparsed_usage_rows: 0,
  },
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchEnergy", () => {
  it("reads the parcel's document from the energy edge path", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const energy = await fetchEnergy(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/energy/by-pnu/${pnu}`,
      expect.objectContaining({ headers: { accept: "application/json" } }),
    );
    expect(energy?.electricity_unit).toBe("kWh");
    expect(energy?.months[1]?.gas_usage).toBeNull();
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchEnergy(pnu)).toBeNull();
  });

  it("refuses a document for another parcel", async () => {
    const other = { ...document(), pnu: "9999900000100000001" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(other), { status: 200 })),
    );
    await expect(fetchEnergy(pnu)).rejects.toThrow("disagrees");
  });
});

describe("energy helpers", () => {
  it("orders months oldest first, labels them and finds the 전기 peak", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 })),
    );
    const energy = await fetchEnergy(pnu);
    if (!energy) throw new Error("expected a document");
    expect(chronological(energy).map((m) => monthLabel(m.use_ym))).toEqual(["2099.07", "2099.08"]);
    expect(electricityPeak(energy)).toBe(350.5);
  });
});
