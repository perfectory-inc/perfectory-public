// apps/web/components/panels/parcel/court-auction.tsx
"use client";
import { useTranslations } from "next-intl";
import type { CourtAuction, CourtAuctionRound } from "@/lib/api/court-auction";
import { formatPriceKrw } from "@/lib/listings/format";

/** The parcel's 법원경매 (court auctions) as a category page, one card per item.
 *
 * 법원경매 데이터는 출처 표시가 필요한 공공저작물이다(저작권법 제24조의2, 루트 ADR-0281 §6):
 * 문서가 담아 준 출처(`source_attribution`)를 지어내지 않고 그대로 표시한다. */
export function ParcelCourtAuctionCard({ data }: { data: CourtAuction }) {
  const t = useTranslations("panels.parcel.courtAuction");
  if (data.rounds.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      <header>
        <h2 className="text-[length:var(--text-title-md)] font-semibold">
          {t("title", { count: data.rounds.length })}
        </h2>
      </header>
      <ul className="flex flex-col gap-2">
        {data.rounds.map((r, i) => (
          <li
            // biome-ignore lint/suspicious/noArrayIndexKey: a parcel's items carry no stable display id; their order is their identity.
            key={i}
            className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]"
          >
            <div className="flex items-baseline justify-between gap-2">
              <span className="font-semibold text-[var(--color-ink)]">
                {r.building_name ?? r.court_name ?? t("fallbackName")}
              </span>
              {(r.user_case_no ?? r.case_no) != null && (
                <span className="whitespace-nowrap text-[length:var(--text-caption)] text-[var(--color-muted)]">
                  {r.user_case_no ?? r.case_no}
                </span>
              )}
            </div>
            <dl className="mt-2 grid grid-cols-[6em_1fr] gap-x-3 gap-y-1 text-[var(--color-ink)] tabular-nums">
              {rowsOf(t, r).map(([label, value]) => (
                <div key={label} className="contents">
                  <dt className="text-[var(--color-muted)]">{label}</dt>
                  <dd>{value}</dd>
                </div>
              ))}
            </dl>
          </li>
        ))}
      </ul>
      <footer className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {t("source", { attribution: data.source_attribution })}
      </footer>
    </div>
  );
}

type Translate = ReturnType<typeof useTranslations<"panels.parcel.courtAuction">>;

function rowsOf(t: Translate, r: CourtAuctionRound): [string, string][] {
  const rows: [string, string][] = [];
  if (r.court_name) rows.push([t("court"), r.court_name]);
  if (r.appraisal_amount_krw != null)
    rows.push([t("appraisal"), formatPriceKrw(r.appraisal_amount_krw)]);
  if (r.minimum_bid_price_krw != null)
    rows.push([t("minimumBid"), formatPriceKrw(r.minimum_bid_price_krw)]);
  if (r.sale_amount_krw != null) rows.push([t("saleAmount"), formatPriceKrw(r.sale_amount_krw)]);
  if (r.sale_date)
    rows.push([t("saleDate"), [r.sale_date, r.sale_time_raw].filter(Boolean).join(" ")]);
  if (r.sale_place) rows.push([t("salePlace"), r.sale_place]);
  if (r.failed_bid_count != null && r.failed_bid_count > 0)
    rows.push([t("failedBids"), t("failedBidsCount", { count: r.failed_bid_count })]);
  return rows;
}
