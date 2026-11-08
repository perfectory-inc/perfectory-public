// apps/web/components/panels/parcel/court-auction.tsx
"use client";
import { useTranslations } from "next-intl";
import { PhotoGrid } from "@/components/media/photo-grid";
import type {
  CourtAuction,
  CourtAuctionRound,
  CourtAuctionSaleResult,
} from "@/lib/api/court-auction";
import { parcelMediaStore } from "@/lib/api/public-auction";
import { formatPriceKrw } from "@/lib/listings/format";

/** The parcel's 법원경매 (court auctions) as a category page: one card per item in progress, then
 * the parcel's 매각결과 (root ADR-0454), newest first.
 * No source line on screen; `source_attribution` stays in the document (root ADR-0428). */
export function ParcelCourtAuctionCard({ data }: { data: CourtAuction }) {
  const t = useTranslations("panels.parcel.courtAuction");
  const results = data.sale_results ?? [];
  if (data.rounds.length === 0 && results.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      {data.rounds.length > 0 && (
        <section className="flex flex-col gap-3">
          <h2 className="text-[length:var(--text-title-md)] font-semibold">
            {t("title", { count: data.rounds.length })}
          </h2>
          <ItemList
            t={t}
            items={data.rounds.map((r) => ({
              named: r,
              rows: roundRows(t, r),
              photos: r.photos ?? [],
            }))}
          />
        </section>
      )}
      {results.length > 0 && (
        <section className="flex flex-col gap-3">
          <h2 className="text-[length:var(--text-title-md)] font-semibold">
            {t("saleResults", { count: data.sale_result_total ?? results.length })}
          </h2>
          <ItemList t={t} items={results.map((r) => ({ named: r, rows: resultRows(t, r) }))} />
        </section>
      )}
    </div>
  );
}

type Translate = ReturnType<typeof useTranslations<"panels.parcel.courtAuction">>;
type Named = Pick<CourtAuctionRound, "building_name" | "court_name" | "user_case_no" | "case_no">;

type Photos = NonNullable<CourtAuctionRound["photos"]>;
type Item = { named: Named; rows: [string, string][]; photos?: Photos };

function ItemList({ t, items }: { t: Translate; items: Item[] }) {
  return (
    <ul className="flex flex-col gap-2">
      {items.map(({ named: r, rows, photos }, i) => (
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
            {rows.map(([label, value]) => (
              <div key={label} className="contents">
                <dt className="text-[var(--color-muted)]">{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          {/* 경매 photos are in the 공매 store, each with its ladder (root ADR-0384, ADR-0427 §7). */}
          {photos && (
            <PhotoGrid
              store={parcelMediaStore()}
              photos={photos}
              altFallback={t("photoAltFallback")}
              viewOriginal={t("viewOriginal")}
            />
          )}
        </li>
      ))}
    </ul>
  );
}

type Shared = Pick<
  CourtAuctionRound,
  | "print_address_raw"
  | "court_name"
  | "department_name"
  | "appraisal_amount_krw"
  | "minimum_bid_price_krw"
  | "sale_amount_krw"
  | "failed_bid_count"
>;

function sharedRows(t: Translate, r: Shared, saleDate: string | null): [string, string][] {
  const rows: [string, string][] = [];
  if (r.print_address_raw) rows.push([t("address"), r.print_address_raw]);
  if (r.court_name)
    rows.push([t("court"), [r.court_name, r.department_name].filter(Boolean).join(" ")]);
  if (r.appraisal_amount_krw) rows.push([t("appraisal"), formatPriceKrw(r.appraisal_amount_krw)]);
  if (r.minimum_bid_price_krw)
    rows.push([t("minimumBid"), formatPriceKrw(r.minimum_bid_price_krw)]);
  if (r.sale_amount_krw) rows.push([t("saleAmount"), formatPriceKrw(r.sale_amount_krw)]);
  if (saleDate) rows.push([t("saleDate"), saleDate]);
  return rows;
}

function failedBids(t: Translate, r: Shared): [string, string][] {
  return r.failed_bid_count != null && r.failed_bid_count > 0
    ? [[t("failedBids"), t("failedBidsCount", { count: r.failed_bid_count })]]
    : [];
}

function roundRows(t: Translate, r: CourtAuctionRound): [string, string][] {
  const date = [r.sale_date, r.sale_time_raw].filter(Boolean).join(" ");
  return [
    ...sharedRows(t, r, date),
    ...(r.sale_place ? ([[t("salePlace"), r.sale_place]] as [string, string][]) : []),
    ...failedBids(t, r),
  ];
}

function resultRows(t: Translate, r: CourtAuctionSaleResult): [string, string][] {
  return [...sharedRows(t, r, r.sale_date ?? null), ...failedBids(t, r)];
}
