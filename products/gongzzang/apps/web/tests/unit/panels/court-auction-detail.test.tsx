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
      bid_kind_name: "기일입찰",
      deposit_rate_percent: 10,
    },
    schedule: [
      {
        schedule_date: "2099-03-15",
        schedule_time_raw: "1000",
        schedule_kind_name: "매각기일",
        result_name: "유찰",
        minimum_bid_price_krw: 100_000_000,
      },
    ],
    objects: [
      { object_seq: 1, list_kind_name: "토지", land_category: "공장용지", area_raw: "330㎡" },
    ],
    annexes: [{ object_seq: 1, usage_raw: "창고", structure_raw: "판넬조" }],
    appraisal_points: [{ item_name: "위치 및 주위환경", content: "합성도 가나동 소재" }],
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
    expect(screen.getByText("유찰")).toBeTruthy();
    expect(screen.getByText("최저매각가격의 10%")).toBeTruthy();
    expect(screen.getByText("합성도 가나동 소재")).toBeTruthy();
  });

  it("shows an item whose 물건상세 is not read yet without a detail", () => {
    renderCard({ rounds: [{ ...round, detail: null }], source_attribution: "x" } as CourtAuction);
    expect(screen.queryByText("매각물건명세서")).toBeNull();
    expect(screen.getByText("2099타경1")).toBeTruthy();
  });
});
