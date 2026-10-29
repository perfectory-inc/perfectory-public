// apps/web/components/panels/parcel/public-auction.tsx
"use client";
import { partition } from "es-toolkit";
import { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import { onbidMediaSrc, type PublicAuctionRound } from "@/lib/api/public-auction";
import { formatAreaM2, formatPriceKrw } from "@/lib/listings/format";
import { RoundDetails, shown } from "./public-auction-details";

/** The parcel's on-bid public auctions as a category page, one card per auction round. */
export function ParcelPublicAuctionCard({ rounds }: { rounds: PublicAuctionRound[] }) {
  const t = useTranslations("panels.parcel.publicAuction");
  if (rounds.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      <header>
        <h2 className="text-[length:var(--text-title-md)] font-semibold">
          {t("title", { count: rounds.length })}
        </h2>
      </header>
      <ul className="flex flex-col gap-2">
        {rounds.map((r, i) => (
          <li
            // biome-ignore lint/suspicious/noArrayIndexKey: a parcel's rounds carry no stable display id; their order is their identity.
            key={i}
            className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]"
          >
            <div className="flex items-baseline justify-between gap-2">
              <span className="font-semibold text-[var(--color-ink)]">
                {r.item_name ?? t("fallbackName")}
              </span>
              {r.pbct_nsq != null && (
                <span className="whitespace-nowrap text-[length:var(--text-caption)] text-[var(--color-muted)]">
                  {t("round", { sequence: Number(r.pbct_nsq) })}
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
            <div className="mt-2 flex flex-wrap gap-1">
              {r.share_item_yn === "Y" && <Badge>{t("share")}</Badge>}
              {FLAGS.filter((flag) => r[flag] === "Y").map((flag) => (
                <Badge key={flag}>{t(`fields.round.${flag}`)}</Badge>
              ))}
            </div>
            <RoundMedia t={t} photos={r.photos} />
            <RoundDetails t={t} round={r} />
          </li>
        ))}
      </ul>
    </div>
  );
}

/** A round's onbid photos as a thumbnail grid and its appraisal reports as labeled PDF links. */
function RoundMedia({ t, photos }: { t: Translate; photos: PublicAuctionRound["photos"] }) {
  // Order is the source's own (root ADR-0070); partition preserves it within each group.
  const [images, appraisals] = partition(photos, (p) => p.media_kind === "photo");
  if (images.length === 0 && appraisals.length === 0) return null;
  return (
    <>
      {images.length > 0 && (
        <ul className="mt-2 grid grid-cols-3 gap-2">
          {images.map((photo) => (
            <li
              key={photo.sha256}
              className="relative aspect-[4/3] w-full overflow-hidden rounded-md bg-[var(--color-surface-cream-strong)]"
            >
              {/* biome-ignore lint/performance/noImgElement: onbid media is immutable,
                  content-addressed CDN output (sha256); the Next image optimizer would add a
                  server hop with no gain, so a plain img is correct here. */}
              <img
                src={onbidMediaSrc(photo)}
                alt={photo.title ?? t("photoAltFallback")}
                loading="lazy"
                decoding="async"
                className="h-full w-full object-cover"
              />
            </li>
          ))}
        </ul>
      )}
      {appraisals.length > 0 && (
        <ul className="mt-2 flex flex-col gap-1">
          {appraisals.map((doc) => (
            <li key={doc.sha256}>
              <a
                href={onbidMediaSrc(doc)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--color-accent)]"
              >
                {doc.title ?? t("appraisalDocument")}
              </a>
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

type Translate = ReturnType<typeof useTranslations<"panels.parcel.publicAuction">>;

function rowsOf(t: Translate, r: PublicAuctionRound): [string, string][] {
  const rows: [string, string][] = [];
  if (r.appraisal_amount_krw != null)
    rows.push([t("appraisal"), formatPriceKrw(r.appraisal_amount_krw)]);
  if (r.lowest_bid_price_krw != null)
    rows.push([t("lowestBid"), formatPriceKrw(r.lowest_bid_price_krw)]);
  if (r.bid_begins_at && r.bid_ends_at)
    rows.push([t("bidPeriod"), `${kst(r.bid_begins_at)} ~ ${kst(r.bid_ends_at)}`]);
  const area = [
    r.land_area_m2 ? t("landArea", { area: formatAreaM2(r.land_area_m2) }) : null,
    r.building_area_m2 ? t("buildingArea", { area: formatAreaM2(r.building_area_m2) }) : null,
  ].filter((v): v is string => Boolean(v));
  if (area.length > 0) rows.push([t("area"), area.join(" · ")]);
  const usage = r.usg_scls_ctgr_name ?? r.usg_mcls_ctgr_name ?? r.usg_lcls_ctgr_name;
  if (usage) rows.push([t("usage"), usage]);
  // Root ADR-0340 §8: the rest of the round as the provider gives it, under its own item names.
  for (const key of ROUND_ROWS) {
    const value = shown(key, r[key]);
    if (value != null) rows.push([t(`fields.round.${key}`), value]);
  }
  const rate = r.apsl_prc_ctrs_lowst_bid_rto;
  if (rate != null) rows.push([t("fields.round.apsl_prc_ctrs_lowst_bid_rto"), `${rate}%`]);
  const fromFirst = r.frst_ctrs_lowst_bid_prc_rto;
  if (fromFirst != null)
    rows.push([t("fields.round.frst_ctrs_lowst_bid_prc_rto"), `${fromFirst}%`]);
  if (r.fee_rate != null) rows.push([t("fields.round.fee_rate"), `${r.fee_rate}%`]);
  return rows;
}

// The round's values a buyer reads beside the money and the window, in the provider's words.
const ROUND_ROWS = [
  "prpt_div_nm",
  "dsps_mthod_nm",
  "cptn_mthod_nm",
  "bid_mthod_nm",
  "bid_div_nm",
  "pbct_stat_nm",
  "usbd_nft",
  "first_bid_price_krw",
  "org_name",
  "rqst_org_nm",
  "dtbt_rqr_edtm_cont",
  "evc_rsby_trgt_cont",
  "rent_mthod_nm",
  "rent_perd_cont",
  "totalamt_unpc_div_nm",
  "bid_prgn_nft",
  "cltr_mng_no",
] as const;
// The provider's Y/N conditions shown as a tag when they hold.
const FLAGS = [
  "pvct_trgt_yn",
  "batc_bid_yn",
  "crtn_yn",
  "eltr_grpr_use_yn",
  "collb_bid_psbl_yn",
  "subt_bid_psbl_yn",
  "twtm_gthr_bid_psbl_yn",
] as const;

function Badge({ children }: { children: ReactNode }) {
  return (
    <span className="inline-block rounded border border-[var(--color-hairline)] px-1.5 text-[length:var(--text-caption)] text-[var(--color-muted)]">
      {children}
    </span>
  );
}

// 입찰 일시는 UTC(Z)로 발행되므로 한국 시간으로 보여준다.
const kstFormat = new Intl.DateTimeFormat("ko-KR", {
  timeZone: "Asia/Seoul",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});
function kst(iso: string): string {
  return kstFormat.format(new Date(iso));
}
