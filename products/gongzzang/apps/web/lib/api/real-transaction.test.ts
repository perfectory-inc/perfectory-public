import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchRealTransaction,
  formatManwon,
  industrialRegion,
  isInferred,
  isMainLotGroup,
  lotOfPnu,
} from "./real-transaction";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, no person or place.
const pnu = "9999900000100050000";
const deal = (date: string, extra: Record<string, unknown> = {}) => ({
  dataset_slug: "real_transaction_industrial_trade",
  deal_date: date,
  deal_amount_10k_krw: 52300,
  deposit_10k_krw: null,
  monthly_rent_10k_krw: null,
  rent_kind: null,
  building_type: "일반",
  building_use: "공장",
  building_name: null,
  building_area_m2: 400.5,
  building_area_kind: "total_floor",
  land_area_m2: 800,
  land_area_kind: "plottage",
  floor: null,
  build_year: 2001,
  land_use: "계획관리",
  jimok: null,
  jibun_raw: "5",
  share_dealing_type: null,
  dealing_gbn: "중개거래",
  cancelled: false,
  cancel_date: null,
  place_identity_grade: "code_exact",
  attached_via: null,
  ...extra,
});
const document = () => ({
  schema_version: "foundation-platform.parcel_real_transaction_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_real_transaction", iceberg_snapshot_id: "999990000000000001" },
  real_transaction: {
    as_of_month: "2098-05-01",
    transactions: [
      deal("2098-02-01"),
      deal("2097-11-03", { cancelled: true, cancel_date: "2097-12-01" }),
    ],
    transactions_omitted: 0,
    main_lot_group_pnus: [] as string[],
    comparables: [
      {
        deal_date: "2098-04-02",
        deal_amount_10k_krw: 80000,
        land_area_m2: 1000,
        building_area_m2: 500,
        land_use: "계획관리",
        building_use: "공장",
        build_year: 2001,
        jibun_raw: "3**",
        pnu: null,
        place_identity_grade: null,
        main_lot_group_size: null,
      },
      {
        deal_date: "2098-03-02",
        deal_amount_10k_krw: 8000,
        land_area_m2: 300,
        building_area_m2: 120,
        land_use: "계획관리",
        building_use: "창고",
        build_year: 2010,
        jibun_raw: "4**",
        pnu,
        place_identity_grade: "inferred_attributes",
        main_lot_group_size: null,
      },
    ],
    comparables_from: "2095-05-01",
    region: {
      legal_dong_code: "9999900000",
      from: "2097-05-01",
      datasets: [
        {
          dataset_slug: "real_transaction_industrial_trade",
          deal_count: 3,
          cancelled_count: 1,
          land_price_per_m2_10k_krw: { p25: 50, median: 56.3, p75: 62.5 },
          building_price_per_m2_10k_krw: { p25: 100, median: 110, p75: 125 },
        },
      ],
    },
  },
  transaction_total: 2,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("real transaction edge", () => {
  it("requests the lane directly and returns the parcel's deals with their total", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const found = await fetchRealTransaction(pnu, signal);
    expect(found?.transactionTotal).toBe(2);
    expect(found?.transactions.map((d) => d.deal_date)).toEqual(["2098-02-01", "2097-11-03"]);
    expect(found && industrialRegion(found)?.land_price_per_m2_10k_krw?.median).toBe(56.3);
    expect(fetcher).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/real-transaction/by-pnu/${pnu}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads a parcel without a document as none, and refuses another parcel's document", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 404 })));
    expect(await fetchRealTransaction(pnu)).toBeNull();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ...document(), pnu: "9999900000100050001" })),
    );
    await expect(fetchRealTransaction(pnu)).rejects.toThrow(/disagrees/);
  });

  it("writes the provider's 만원 as 억·만원 and tells an inferred link apart", () => {
    expect(formatManwon(52300)).toBe("5억 2,300만원");
    expect(formatManwon(50000)).toBe("5억원");
    expect(formatManwon(8000)).toBe("8,000만원");
    expect(formatManwon(0)).toBe("0원");
    expect(isInferred("inferred_ownership_change")).toBe(true);
    expect(isInferred("code_exact")).toBe(false);
    expect(isInferred(null)).toBe(false);
  });

  it("names a 본번 group link and the group's other parcels as 지번 (root ADR-0405)", async () => {
    const neighbour = "9999900000100050001";
    const grouped = document();
    grouped.real_transaction.transactions = [
      deal("2098-02-01", {
        jibun_raw: "5**",
        place_identity_grade: "inferred_same_main_lot_group",
      }),
    ];
    grouped.real_transaction.main_lot_group_pnus = [pnu, neighbour];
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(grouped)));
    const found = await fetchRealTransaction(pnu);
    expect(found?.mainLotGroupOthers).toEqual([neighbour]);
    expect(isMainLotGroup("inferred_same_main_lot_group")).toBe(true);
    expect(isMainLotGroup("inferred_attributes")).toBe(false);
    expect(isInferred("inferred_same_main_lot_group")).toBe(true);
    expect(lotOfPnu(neighbour)).toBe("5-1");
    expect(lotOfPnu("9999900000203000000")).toBe("산 300");
  });
});
