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
      { item_name: "테스트 물건 2", bid_ends_at: "2099-01-01T00:00:00.000Z", photos: [] },
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
    renderCard([{ item_name: "테스트 물건", bid_ends_at: "2099-01-01T00:00:00.000Z", photos: [] }]);
    expect(screen.queryByRole("img")).toBeNull();
    expect(screen.queryByRole("link")).toBeNull();
  });
});
