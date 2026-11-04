import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/panel/use-panel-stack", () => ({ usePanelStack: () => ({ push: vi.fn() }) }));

const reads = vi.hoisted(() => ({
  buildings: { buildings: [] as unknown[], unlinked_units: [] as unknown[] },
  listings: { listings: [] as unknown[], total: 0 },
  publicAuction: [] as unknown[],
  ownership: null as unknown,
  subParcels: null as unknown,
  energy: null as unknown,
  demolitions: null as unknown,
  realTransaction: null as unknown,
}));
// The regulation zones read is the real one (its fetch stubbed below): the panel must answer from
// the lane, never from a map.
vi.mock("@/components/panels/parcel/queries", async (importOriginal) => ({
  parcelRegulationZonesQuery: (
    await importOriginal<typeof import("@/components/panels/parcel/queries")>()
  ).parcelRegulationZonesQuery,
  parcelBuildingsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "buildings", pnu],
    queryFn: async () => reads.buildings,
  }),
  parcelListingsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "listings", pnu],
    queryFn: async () => reads.listings,
  }),
  parcelPublicAuctionQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "public-auction", pnu],
    queryFn: async () => reads.publicAuction,
  }),
  parcelOwnershipQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "ownership", pnu],
    queryFn: async () => reads.ownership,
  }),
  parcelSubParcelsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "sub-parcels", pnu],
    queryFn: async () => reads.subParcels,
  }),
  parcelEnergyQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "energy", pnu],
    queryFn: async () => reads.energy,
  }),
  parcelDemolitionsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "demolitions", pnu],
    queryFn: async () => reads.demolitions,
  }),
  parcelRealTransactionQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "real-transaction", pnu],
    queryFn: async () => reads.realTransaction,
  }),
}));

import { ParcelSummaryCard } from "@/components/panels/parcel/summary";
import { type ParcelDocument, parseParcelDocument } from "@/lib/api/parcels";
import koMessages from "@/lib/i18n/ko.json";

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100120003";
const entry = { kind: "parcel", id: PNU, view: "summary" } as const;

function parcel(extra: Record<string, unknown> = {}): ParcelDocument {
  return parseParcelDocument({
    schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
    pnu: PNU,
    ...extra,
  });
}

function renderPanel(data: ParcelDocument): void {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={client}>
      <NextIntlClientProvider locale="ko" messages={koMessages} onError={() => {}}>
        {children}
      </NextIntlClientProvider>
    </QueryClientProvider>
  );
  render(<ParcelSummaryCard entry={entry} data={data} />, { wrapper });
}

// The real regulation zones read goes to the parcel edge: answered here, never by the network.
beforeEach(() =>
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(null, { status: 404 })),
  ),
);
afterEach(() => vi.unstubAllGlobals());

const anchors = () =>
  within(screen.queryByRole("navigation", { name: "구획" }) ?? document.createElement("nav"))
    .queryAllByRole("button")
    .map((button) => button.textContent);

describe("parcel panel header", () => {
  it("names the legal dong from the document and spells the lot from the PNU", () => {
    renderPanel(parcel({ legal_dong: { name: "가가시 나나구 다다동", lowest_name: "다다동" } }));
    expect(screen.getByText("가가시 나나구 다다동")).toBeTruthy();
    expect(screen.getByRole("heading", { level: 2 }).textContent).toBe("다다동 12-3");
  });

  it("shows the legal dong code, never an invented name, until the document names it", () => {
    renderPanel(parcel());
    expect(screen.getByText("법정동코드 9999900000")).toBeTruthy();
    expect(screen.getByRole("heading", { level: 2 }).textContent).toBe("12-3");
  });
});

describe("parcel panel sections", () => {
  it("shows the source's zone names with 포함/저촉, and a 규제 anchor for them", async () => {
    renderPanel(
      parcel({
        zonings: [
          { zone_code: "UQA122", zone_name: "제2종일반주거지역", inclusion_code: "1" },
          { zone_code: "UQA220", zone_name: "일반상업지역", inclusion_code: "2" },
        ],
      }),
    );
    expect(screen.getAllByText("제2종일반주거지역").length).toBeGreaterThan(0);
    expect(screen.getByText("저촉")).toBeTruthy();
    await waitFor(() => expect(anchors()).toEqual(["토지", "규제"]));
  });

  it("lists the lane's zones for a parcel no map ever loaded, by family, with the layers not collected", async () => {
    // Root ADR-0375 acceptance: no map instance and no tile exist in this test; only the parcel
    // edge answers, with zones the Foundation computed for this PNU.
    const zone = (id: string, layerCode: string, layerName: string, zoneName: string) => ({
      zone_id: `${layerCode}:${id}`,
      layer_code: layerCode,
      layer_name: layerName,
      zone_code: "UQZ999",
      zone_name: zoneName,
      release_date: id === "A" ? "2026-06-09" : "2026-10-09",
    });
    const fetcher = vi.fn(async (url: string) =>
      url.endsWith(`/parcels/regulation-zones/by-pnu/${PNU}`)
        ? Response.json({
            schema_version: "foundation-platform.parcel_regulation_zones_by_pnu.v1",
            schema_revision: 1,
            pnu: PNU,
            source: {
              table: "gold.parcel_regulation_zones",
              iceberg_snapshot_id: "999990000000000001",
            },
            regulation_zones: {
              zones: [
                zone("A", "D032", "시험층/개발제한구역", "개발제한구역"),
                zone("B", "D335", "시험층/공장설립제한지역", "공장설립제한지역"),
                zone("C", "D998", "시험층/목록 밖", "시험구역"),
              ],
            },
            zone_total: 5,
          })
        : new Response(null, { status: 404 }),
    );
    vi.stubGlobal("fetch", fetcher);
    renderPanel(parcel());
    await waitFor(() => expect(anchors()).toEqual(["토지", "규제"]));
    expect(screen.getByText("지도 규제 구역")).toBeTruthy();
    expect(screen.getByText("기준일 2026-10-09")).toBeTruthy();
    expect(screen.getByRole("heading", { level: 4, name: "개발제한구역" })).toBeTruthy();
    expect(screen.getByRole("heading", { level: 4, name: "공장입지 규제" })).toBeTruthy();
    expect(screen.getByRole("heading", { level: 4, name: "기타" })).toBeTruthy();
    expect(screen.getByText("시험층/공장설립제한지역")).toBeTruthy();
    expect(screen.getByText("외 2개 구역")).toBeTruthy();
    expect(screen.getByText("일부 구역 수집 대기: 보전·준보전산지·관리지역")).toBeTruthy();
  });

  it("leaves the map zones out when the lane has no document for the parcel", async () => {
    renderPanel(parcel({ zonings: [{ zone_code: "UQA122", zone_name: "제2종일반주거지역" }] }));
    await waitFor(() => expect(anchors()).toEqual(["토지", "규제"]));
    expect(screen.queryByText("지도 규제 구역")).toBeNull();
  });

  it("never shows a code where the source's name is missing", () => {
    renderPanel(parcel({ forest_ledger: { ownership_kind: "06", co_owner_count: 2 } }));
    expect(screen.queryByText("06")).toBeNull();
    expect(screen.getByText("2인")).toBeTruthy();
  });

  it("shows the 토지소유 lane's owners in the source's words instead of the forest ledger", async () => {
    reads.ownership = {
      owners: [
        {
          co_owner_serial_no: "000001",
          ownership_kind_name: "법인",
          residence_kind_name: "관외",
          ownership_change_reason: "소유권이전",
          ownership_changed_on: "2019-11-26",
        },
        {
          co_owner_serial_no: "000002",
          ownership_kind_name: "법인",
          residence_kind_name: "관외",
          ownership_change_reason: "소유권이전",
          ownership_changed_on: "2024-01-02",
        },
      ],
      unit_owner_total: 0,
      unit_owner_kinds: [],
      base_year_month: "2024-12",
      ownerTotal: 2,
    };
    try {
      renderPanel(
        parcel({ forest_ledger: { ownership_kind_name: "임야소유", co_owner_count: 9 } }),
      );
      expect(await screen.findByText("법인 2")).toBeTruthy();
      expect(screen.getByText("2024-01-02 소유권이전")).toBeTruthy();
      expect(screen.getByText("토지소유정보 · 2024-12 기준")).toBeTruthy();
      expect(screen.queryByText("임야소유")).toBeNull();
    } finally {
      reads.ownership = null;
    }
  });

  it("shows the 지번's monthly 전기 in kWh and 가스 as the source's 사용량, a missing month as -", async () => {
    reads.energy = {
      months: [
        {
          use_ym: "209908",
          electricity_usage: 1234.5,
          electricity_rows: 2,
          gas_usage: 12,
          gas_rows: 1,
        },
        {
          use_ym: "209907",
          electricity_usage: 900,
          electricity_rows: 1,
          gas_usage: null,
          gas_rows: 0,
        },
      ],
      electricity_unit: "kWh",
      latest_use_ym: "209908",
    };
    try {
      renderPanel(parcel());
      expect(await screen.findByText("건물 에너지 사용량")).toBeTruthy();
      expect(anchors()).toContain("에너지");
      expect(screen.getByText("전기 사용량(kWh)")).toBeTruthy();
      expect(screen.getByText("건축HUB 건물에너지 · 2099.08 사용분까지")).toBeTruthy();
      const july = screen.getByText("2099.07").closest("tr");
      expect(july?.textContent).toContain("900");
      expect(july?.textContent).toContain("-");
      expect(screen.getByText("2099.08").closest("tr")?.textContent).toContain("1,234.5");
      expect(screen.getByText(/산업·수송·발전·열병합·충전용/)).toBeTruthy();
    } finally {
      reads.energy = null;
    }
  });

  it("shows no 에너지 anchor for a parcel without a usage document", async () => {
    renderPanel(parcel());
    // The lane's read settles (null: no document) before the sections are counted.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(screen.queryByText("건물 에너지 사용량")).toBeNull();
    expect(anchors()).not.toContain("에너지");
  });

  it("shows the parcel's 멸실된 건물 in the source's words, newest first", async () => {
    reads.demolitions = {
      records: [
        {
          demolition_kind_name: "멸실",
          demolition_loss_date: "20990505",
          main_purpose_name: "창고시설",
          structure_name: "일반철골구조",
          total_floor_area_m2: "1200.5",
          building_count: "2",
        },
        {
          demolition_kind_name: "철거",
          demolition_start_date: "20980101",
          demolition_end_date: "20980301",
          main_purpose_name: "공장",
        },
      ],
      total: 2,
    };
    try {
      renderPanel(parcel());
      expect(await screen.findByText("멸실된 건물 2건")).toBeTruthy();
      expect(anchors()).toContain("멸실 건물");
      expect(screen.getByText("멸실 · 2099.05.05")).toBeTruthy();
      expect(
        screen.getByText("창고시설 · 일반철골구조 · 연면적 1200.5㎡ · 건축물수 2"),
      ).toBeTruthy();
      expect(screen.getByText("철거 · 2098.01.01~2098.03.01")).toBeTruthy();
    } finally {
      reads.demolitions = null;
    }
  });

  it("shows 부속지번 both ways, each other parcel by its 지번 and 대장종류", async () => {
    reads.subParcels = {
      on_other_parcels: [
        {
          mgmt_key: "synthetic-1",
          register_kind_name: "일반건축물",
          other_pnu: "9999900000100020000",
          other_jibun: "99999-00001-0-0002-0000",
        },
      ],
      on_other_total: 1,
      from_other_parcels: [
        {
          mgmt_key: "synthetic-2",
          register_kind_name: "표제부",
          other_pnu: null,
          other_jibun: "99999-00001-1-0012-0003",
        },
      ],
      from_other_total: 1,
    };
    try {
      renderPanel(parcel());
      expect(await screen.findByText("이 필지 건물이 함께 선 필지 1건")).toBeTruthy();
      expect(anchors()).toContain("부속지번");
      expect(screen.getByText("2 · 일반건축물")).toBeTruthy();
      expect(screen.getByText("이 필지에 걸친 다른 필지의 건물 1건")).toBeTruthy();
      expect(screen.getByText("산 12-3 · 표제부")).toBeTruthy();
      // A parcel Foundation has no PNU for is named, not linked.
      expect(screen.getAllByRole("button", { name: /필지 보기/ })).toHaveLength(1);
    } finally {
      reads.subParcels = null;
    }
  });

  it("adds a 건물 anchor once the parcel's buildings are read", async () => {
    reads.buildings = {
      buildings: [
        {
          id: "b1",
          name: "",
          purpose: "업무시설",
          structure: "철골철근콘크리트구조",
          total_area_m2: 1200,
          stories: 5,
          below_ground_floors: 1,
          built_year: 2001,
          approved_at: null,
          units: [],
        },
      ],
      unlinked_units: [],
    };
    try {
      renderPanel(parcel());
      await waitFor(() => expect(anchors()).toEqual(["토지", "건물"]));
      expect(screen.getByText("업무시설")).toBeTruthy();
      expect(screen.getByText(/지상 5층 \/ 지하 1층/)).toBeTruthy();
    } finally {
      reads.buildings = { buildings: [], unlinked_units: [] };
    }
  });
});

describe("parcel panel categories", () => {
  it("shows no category bar while 필지정보 is the only category", async () => {
    renderPanel(parcel());
    await waitFor(() => expect(screen.getByText("12-3")).toBeTruthy());
    expect(screen.queryByRole("navigation", { name: "필지 정보 갈래" })).toBeNull();
  });

  it("adds 매물 with its count when the parcel has listings", async () => {
    reads.listings = { listings: [], total: 3 };
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      expect(
        within(bar)
          .getAllByRole("button")
          .map((b) => b.textContent),
      ).toEqual(["필지정보", "매물 3"]);
    } finally {
      reads.listings = { listings: [], total: 0 };
    }
  });

  it("adds 공매 with its count when the parcel has public auctions", async () => {
    reads.publicAuction = [{ item_name: "테스트 물건" }, { item_name: "테스트 물건" }];
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      expect(
        within(bar)
          .getAllByRole("button")
          .map((b) => b.textContent),
      ).toEqual(["필지정보", "공매 2"]);
    } finally {
      reads.publicAuction = [];
    }
  });

  it("adds 실거래 with its count, and labels a deal linked by inference 추정", async () => {
    const deal = {
      dataset_slug: "real_transaction_industrial_trade",
      deal_date: "2098-02-01",
      deal_amount_10k_krw: 52300,
      cancelled: false,
      place_identity_grade: "code_exact",
    };
    reads.realTransaction = {
      as_of_month: "2098-05-01",
      transactions: [deal],
      transactions_omitted: 0,
      main_lot_group_pnus: [],
      comparables: [
        {
          deal_date: "2098-03-02",
          deal_amount_10k_krw: 8000,
          jibun_raw: "4**",
          pnu: PNU,
          place_identity_grade: "inferred_attributes",
        },
      ],
      comparables_from: "2095-05-01",
      region: { datasets: [] },
      transactionTotal: 1,
      mainLotGroupOthers: [],
    };
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      const realTransaction = within(bar).getByRole("button", { name: "실거래 1" });
      fireEvent.click(realTransaction);
      expect(await screen.findByText("5억 2,300만원")).toBeTruthy();
      expect(anchors()).toEqual(["이 필지의 실거래", "같은 법정동 공장·창고 거래"]);
      expect(screen.getByText("지번 4**")).toBeTruthy();
      expect(screen.getByText("추정")).toBeTruthy();
    } finally {
      reads.realTransaction = null;
    }
  });

  it("labels a 본번 group deal 추정 · 같은 본번 and lists the group's other parcels", async () => {
    reads.realTransaction = {
      as_of_month: "2098-05-01",
      transactions: [
        {
          dataset_slug: "real_transaction_industrial_trade",
          deal_date: "2098-02-01",
          deal_amount_10k_krw: 52300,
          jibun_raw: "3**",
          cancelled: false,
          place_identity_grade: "inferred_same_main_lot_group",
        },
      ],
      transactions_omitted: 0,
      main_lot_group_pnus: [],
      comparables: [],
      comparables_from: "2095-05-01",
      region: { datasets: [] },
      transactionTotal: 1,
      mainLotGroupOthers: ["9999900000103000001", "9999900000103000002"],
    };
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      fireEvent.click(within(bar).getByRole("button", { name: "실거래 1" }));
      expect(await screen.findByText("추정 · 같은 본번")).toBeTruthy();
      expect(screen.getByText(/같은 본번의 다른 필지: 300-1, 300-2/)).toBeTruthy();
    } finally {
      reads.realTransaction = null;
    }
  });
});
