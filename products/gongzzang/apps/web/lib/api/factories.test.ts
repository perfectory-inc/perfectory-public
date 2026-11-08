import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchFactories } from "./factories";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, no company or place.
const pnu = "9999900000100000000";
const document = () => ({
  schema_version: "foundation-platform.parcel_factories_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_factories", iceberg_snapshot_id: "999990000000000001" },
  factories: {
    records: [
      { company_name: "가정밀", complex_name: "가산업단지", products: "금형" },
      { company_name: "나공업", complex_name: null, products: null },
    ],
  },
  factory_total: 3,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchFactories", () => {
  it("reads the parcel's factories from the factories edge path and keeps the full count", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const factories = await fetchFactories(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/factories/by-pnu/${pnu}`,
      expect.anything(),
    );
    expect(factories?.total).toBe(3);
    expect(factories?.records[0]?.company_name).toBe("가정밀");
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchFactories(pnu)).toBeNull();
  });

  it("refuses a document for another parcel", async () => {
    const other = { ...document(), pnu: "9999900000100000001" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(other), { status: 200 })),
    );
    await expect(fetchFactories(pnu)).rejects.toThrow("disagrees");
  });
});
