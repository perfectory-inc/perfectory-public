import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchCourtAuction } from "./court-auction";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, a made-up case, no real
// court numbers or place names.
const pnu = "9999900000100000000";
const document = () => ({
  schema_version: "foundation-platform.parcel_court_auction_by_pnu.v1",
  pnu,
  source: { table: "gold.parcel_court_auction", iceberg_snapshot_id: "999990000000000001" },
  court_auction: {
    source_attribution: "대한민국법원 법원경매정보",
    rounds: [
      {
        building_name: "테스트 물건 1",
        court_name: "테스트지방법원",
        case_no: "9999타경00001",
        user_case_no: "2099타경1",
        appraisal_amount_krw: 100000000,
        minimum_bid_price_krw: 70000000,
        sale_amount_krw: null,
        sale_date: "2099-01-05",
        sale_time_raw: "10:00",
        sale_place: "제1호 법정",
        failed_bid_count: 1,
      },
    ],
  },
});

afterEach(() => vi.unstubAllGlobals());

describe("court auction edge", () => {
  it("requests the lane directly and returns its court auction", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const result = await fetchCourtAuction(pnu, signal);
    expect(result).toEqual(document().court_auction);
    expect(fetcher).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/court-auction/by-pnu/${pnu}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads 404 as no auctions and propagates outages", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    expect(await fetchCourtAuction(pnu)).toBeNull();
    await expect(fetchCourtAuction(pnu)).rejects.toThrow("503");
  });

  it("refuses a noncanonical PNU before fetching and a response for another parcel", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValue(Response.json({ ...document(), pnu: "9999900000200000000" }));
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchCourtAuction("../manifest.json")).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
    await expect(fetchCourtAuction(pnu)).rejects.toThrow("PNU");
  });
});
