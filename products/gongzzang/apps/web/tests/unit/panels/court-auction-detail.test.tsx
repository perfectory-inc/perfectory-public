import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, expect, it } from "vitest";
import { ParcelCourtAuctionCard } from "@/components/panels/parcel/court-auction";
import type { CourtAuction } from "@/lib/api/court-auction";
import ko from "@/lib/i18n/ko.json";

// Made-up case and values (root ADR-0540): an item in progress with its 물건상세.
const round = {
  court_office_code: "B999999",
  case_no: "20990130000001",
  property_seq: 1,
  item_seq: 1,
  user_case_no: "2099타경1",
  court_name: "합성지방법원",
  sale_date: "2099-04-15",
  photos: [],
  detail: {
    case: {
      case_name: "부동산임의경매",
      claim_amount_krw: 150_000_000,
      dividend_demand_deadline: "2099-03-15",
    },
    spec: {
      first_rank_setting: "2099.1.5. 근저당권",
      assumed_rights: "해당사항없음",
      deposit_rate_percent: 10,
    },
    schedule: [
      {
        schedule_date: "2099-03-15",
        schedule_time_raw: "1000",
        schedule_kind_name: "합성매각기일",
        schedule_place: "제1호 법정",
        minimum_bid_price_krw: 100_000_000,
        result_name: "합성유찰",
      },
    ],
    objects: [{ object_seq: 1, realty_kind: "토지", land_category: "공장용지", area_raw: "330㎡" }],
    annexes: [{ object_seq: 1, usage_raw: "창고", structure_raw: "판넬조" }],
    appraisal_points: [{ content: "합성도 가나동 소재" }],
    related_cases: [
      { list: "related", related_user_case_no: "2099타경9", relation_kind_name: "중복" },
    ],
    parties: [
      { kind_name: "채권자", count: 1 },
      { kind_name: "임차인", count: 2 },
    ],
    survey: {
      round_seq: 1,
      surveyed_raw: "2099년01월02일^^",
      lease_text: "임차인 ○○○ 점유",
      objects: [{ object_seq: 1, possession_text: "합성 점유", lease_count: 1 }],
      tenants: [
        {
          object_seq: 1,
          usage_raw: "사무실",
          deposit_raw: "10,000,000원",
          monthly_rent_raw: "500,000원",
          move_in_raw: "2099.01.02",
          fixed_date_raw: "미상",
          leased_part_raw: "전부",
        },
      ],
    },
    appraisal: {
      round_seq: 1,
      price_basis_date: "2099-01-10",
      opinion: "합성 의견",
      report: { sha256: "a".repeat(64), media_format: "pdf", size_bytes: 1000 },
    },
    notice_changes: [
      { list: "correction", change_date: "2099-01-04", change_text: "최저매각가격 정정" },
    ],
    filings: [
      {
        received_date: "2099-02-02",
        filer_role: "채권자",
        document_kind: "보정서",
        action: "제출",
        result: null,
      },
    ],
    deliveries: [
      {
        registered_date: "2099-01-28",
        reached_date: "2099-01-30",
        delivery_result: "도달",
        recipient_role: "채권자대리인",
        document_kind: "보정명령등본",
        action: "발송",
      },
    ],
  },
};

function renderCard(data: CourtAuction) {
  return render(
    <NextIntlClientProvider locale="ko" messages={ko}>
      <ParcelCourtAuctionCard data={data} />
    </NextIntlClientProvider>,
  );
}

describe("court auction detail", () => {
  it("shows the 물건상세 in the site's words", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    for (const word of [
      "청구금액",
      "배당요구종기",
      "매각물건명세서",
      "최선순위 설정",
      "기일내역",
      "목록내역",
      "제시외건물",
      "감정평가요항표",
    ]) {
      expect(screen.getByText(word)).toBeTruthy();
    }
    expect(screen.getByText("제1호 법정")).toBeTruthy();
    expect(screen.getByText("최저매각가격의 10%")).toBeTruthy();
    expect(screen.getByText("합성도 가나동 소재")).toBeTruthy();
  });

  it("shows the name the site gives a code, and nothing for a code without one (root ADR-0540 §5)", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    expect(screen.getByText("기일결과")).toBeTruthy();
    expect(screen.getByText("합성유찰")).toBeTruthy();
    expect(screen.getByText("합성매각기일")).toBeTruthy();
    expect(screen.queryByText("물건종류"), "no name, no row").toBeNull();
  });

  it("shows the case's own documents: 관련사건·당사자·현황조사서·감정평가 (root ADR-0540 §1)", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    for (const word of [
      "관련사건내역",
      "당사자내역",
      "현황조사서",
      "임대차관계",
      "감정평가",
      "가격시점",
    ]) {
      expect(screen.getByText(word)).toBeTruthy();
    }
    expect(screen.getByText("2099타경9 · 중복")).toBeTruthy();
    expect(screen.getByText("채권자 1 · 임차인 2")).toBeTruthy();
    expect(screen.getByText("2099년01월02일")).toBeTruthy();
    expect(screen.getByText("임차인 ○○○ 점유")).toBeTruthy();
    expect(screen.getByText("합성 의견")).toBeTruthy();
    const report = screen.getByRole("link", { name: "감정평가서" });
    expect(report.getAttribute("href")).toContain(`sha256/${"a".repeat(64)}.pdf`);
  });

  it("shows the case's 문건·송달 as the role and kind, never a name (root ADR-0575 §5)", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    expect(screen.getByText("문건처리내역")).toBeTruthy();
    expect(screen.getByText("채권자 보정서 제출")).toBeTruthy();
    expect(screen.getByText("채권자대리인 보정명령등본 발송")).toBeTruthy();
    expect(screen.getByText("2099-01-30 도달")).toBeTruthy();
  });

  it("shows the 매각공고's 정정·취소 of the item (root ADR-0575 §6)", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    expect(screen.getByText("매각공고 정정·취소")).toBeTruthy();
    expect(screen.getByText("최저매각가격 정정")).toBeTruthy();
  });

  it("shows the 현황조사서's 임대차관계 without a name (root ADR-0575 §6)", () => {
    renderCard({ rounds: [round], source_attribution: "x" } as CourtAuction);
    expect(screen.getByText("보증(전세)금")).toBeTruthy();
    expect(screen.getByText("10,000,000원")).toBeTruthy();
    expect(screen.getByText("2099.01.02")).toBeTruthy();
  });

  it("shows an item whose 물건상세 is not read yet without a detail", () => {
    renderCard({ rounds: [{ ...round, detail: null }], source_attribution: "x" } as CourtAuction);
    expect(screen.queryByText("매각물건명세서")).toBeNull();
    expect(screen.getByText("2099타경1")).toBeTruthy();
  });
});
