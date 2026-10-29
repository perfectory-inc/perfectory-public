import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { describe, expect, it } from "vitest";
import { ParcelPublicAuctionCard } from "@/components/panels/parcel/public-auction";
import type { PublicAuctionRound } from "@/lib/api/public-auction";
import koMessages from "@/lib/i18n/ko.json";

// Synthetic 64-hex content addresses only (public-fixture-safety): no real onbid media.
const sha = (c: string) => c.repeat(64);

function renderCard(rounds: PublicAuctionRound[]): void {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <NextIntlClientProvider locale="ko" messages={koMessages} onError={() => {}}>
      {children}
    </NextIntlClientProvider>
  );
  render(<ParcelPublicAuctionCard rounds={rounds} />, { wrapper });
}

describe("public auction gallery", () => {
  it("renders photos as a thumbnail grid and appraisals as labeled PDF links, in source order", () => {
    renderCard([
      {
        cltr_mng_no: "X-0001",
        item_name: "테스트 물건 1",
        bid_ends_at: "2099-01-01T00:00:00.000Z",
        photos: [
          {
            sha256: sha("a"),
            media_kind: "photo",
            sequence: 1,
            title: "정면 사진",
            media_format: "jpg",
            size_bytes: 10,
          },
          {
            sha256: sha("c"),
            media_kind: "photo",
            sequence: 2,
            title: null,
            media_format: "png",
            size_bytes: 20,
          },
          {
            sha256: sha("b"),
            media_kind: "appraisal",
            sequence: null,
            title: "감정평가서 1",
            media_format: "pdf",
            size_bytes: 30,
          },
          {
            sha256: sha("d"),
            media_kind: "appraisal",
            sequence: null,
            title: null,
            media_format: "pdf",
            size_bytes: 40,
          },
        ],
      },
      {
        cltr_mng_no: "X-0002",
        item_name: "테스트 물건 2",
        bid_ends_at: "2099-01-01T00:00:00.000Z",
        photos: [],
      },
    ]);

    // Only the two photos are images, in the order the source gave them; title becomes alt,
    // and the untitled one falls back to the generic label.
    const images = screen.getAllByRole("img");
    expect(images.map((img) => img.getAttribute("alt"))).toEqual(["정면 사진", "공매 물건 사진"]);
    expect(screen.getByRole("img", { name: "정면 사진" }).getAttribute("src")).toBe(
      `https://catalog.perfectory.io/onbid/media/sha256/${sha("a")}.jpg`,
    );

    // Appraisals are links: titled one keeps its title, the untitled one uses the fallback label.
    const titled = screen.getByRole("link", { name: "감정평가서 1" });
    expect(titled.getAttribute("href")).toBe(
      `https://catalog.perfectory.io/onbid/media/sha256/${sha("b")}.pdf`,
    );
    expect(titled.getAttribute("target")).toBe("_blank");
    expect(screen.getByRole("link", { name: "감정평가서" }).getAttribute("href")).toBe(
      `https://catalog.perfectory.io/onbid/media/sha256/${sha("d")}.pdf`,
    );
  });

  it("renders no gallery when a round has no photos", () => {
    renderCard([
      {
        cltr_mng_no: "X-0001",
        item_name: "테스트 물건",
        bid_ends_at: "2099-01-01T00:00:00.000Z",
        photos: [],
      },
    ]);
    expect(screen.queryByRole("img")).toBeNull();
    expect(screen.queryByRole("link")).toBeNull();
  });

  // Root ADR-0340 §8: the provider's names of its codes and its own item names as labels, the
  // item's facts and the announcement under their sections.
  it("shows the round's conditions and the item's and announcement's facts in the provider's words", () => {
    renderCard([
      {
        cltr_mng_no: "X-0001",
        item_name: "테스트 물건",
        bid_ends_at: "2099-01-01T00:00:00.000Z",
        photos: [],
        prpt_div_nm: "압류재산",
        cptn_mthod_nm: "일반경쟁",
        usbd_nft: 2,
        eltr_grpr_use_yn: "Y",
        collb_bid_psbl_yn: "N",
        item: {
          cltr_mng_no: "X-0001",
          detail: {
            icdl_cdtn_cont: "현 상태대로 매각",
            leas_inf_list: [
              { irst_div_nm: "주거", bid_grtee_amt: "10000000", mvin_ymd: "20990101" },
            ],
            apsl_evl_clg_list: [
              {
                apsl_evl_org_nm: "합성 감정평가법인",
                apsl_evl_amt: "100000000",
                sha256: sha("e"),
                media_format: "pdf",
              },
            ],
          },
          history: [{ pbct_nsq: "001", pbct_stat_nm: "유찰", valid_bidder_count: 0 }],
          media: [
            { sha256: sha("f"), media_kind: "location_map", sequence: 0, media_format: "png" },
          ],
        },
        announcement: {
          onbid_pbanc_no: 1,
          pbct_no: 1,
          org_nm: "합성 기관",
          anncm_al_cont: "합성 공고문",
          atch_file_list: [
            { btbd_atch_file_nm: "공고문.pdf", sha256: sha("9"), media_format: "pdf" },
          ],
        },
      },
    ]);
    expect(screen.getByText("재산유형")).toBeTruthy();
    expect(screen.getByText("압류재산")).toBeTruthy();
    expect(screen.getByText("일반경쟁")).toBeTruthy();
    expect(screen.getByText("전자보증서가능여부")).toBeTruthy(); // Y: shown as a tag
    expect(screen.queryByText("공동입찰가능여부")).toBeNull(); // N: not
    expect(screen.getByText("현 상태대로 매각")).toBeTruthy();
    expect(screen.getByText("1,000만원")).toBeTruthy(); // a deposit of won, as won
    expect(screen.getByText("2099-01-01")).toBeTruthy(); // the provider's yyyyMMdd, as a date
    expect(screen.getByText("유찰")).toBeTruthy();
    expect(screen.getByRole("img", { name: "위치도" }).getAttribute("src")).toBe(
      `https://catalog.perfectory.io/onbid/media/sha256/${sha("f")}.png`,
    );
    expect(screen.getByRole("link", { name: "합성 감정평가법인" }).getAttribute("href")).toBe(
      `https://catalog.perfectory.io/onbid/media/sha256/${sha("e")}.pdf`,
    );
    expect(screen.getByText("합성 기관")).toBeTruthy();
    expect(screen.getByRole("link", { name: "공고문.pdf" })).toBeTruthy();
  });
});
