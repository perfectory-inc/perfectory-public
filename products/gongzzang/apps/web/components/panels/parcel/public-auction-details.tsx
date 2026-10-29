// apps/web/components/panels/parcel/public-auction-details.tsx
"use client";
import type { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import {
  onbidMediaSrc,
  type PublicAuctionAnnouncement,
  type PublicAuctionItem,
  type PublicAuctionRound,
} from "@/lib/api/public-auction";
import { formatPriceKrw } from "@/lib/listings/format";

// What a buyer reads about a round beyond its card (root ADR-0340 §8): the item's detail, the
// 공매재산명세서 and rights, the bid conditions and every round's schedule, earlier rounds' results,
// appraisals, maps and the announcement. Every label is the provider's own item name (data.go.kr
// 활용가이드 항목명, `panels.parcel.publicAuction.fields`), every coded value the name the provider
// sent with it (`*_nm`); a field the provider left empty is not shown.

type Translate = ReturnType<typeof useTranslations<"panels.parcel.publicAuction">>;
type Values = Record<string, unknown>;

const DATE_KEY = /(_ymd|_dt|_dudt)$/;
const MONEY_KEY = /(_amt|_espc|_tdps|_cmsn|_prc|_krw|_indct_cont)$/;
const PERCENT_KEY = /(_rto|_rate|_crrt|_cfct)$/;

/** A provider value for the screen: `yyyyMMdd[HHmm]` as a date, a sum of won as won, the rest as given. */
export function shown(key: string, value: unknown): string | null {
  if (value == null || value === "") return null;
  const text = String(value).trim();
  if (text === "") return null;
  if (DATE_KEY.test(key) && /^\d{8}(\d{4}(\d{2})?)?$/.test(text)) {
    const day = `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    return text.length >= 12 ? `${day} ${text.slice(8, 10)}:${text.slice(10, 12)}` : day;
  }
  if (MONEY_KEY.test(key) && /^-?\d+(\.\d+)?$/.test(text)) return formatPriceKrw(Number(text));
  if (PERCENT_KEY.test(key) && /^-?\d+(\.\d+)?$/.test(text)) return `${text}%`;
  return text;
}

function rowsOf(
  t: Translate,
  section: string,
  values: Values | null | undefined,
  keys: readonly string[],
) {
  if (!values) return [];
  return keys.flatMap((key) => {
    const value = shown(key, values[key]);
    return value == null ? [] : [[t(`fields.${section}.${key}`), value] as [string, string]];
  });
}

function Fields({ rows }: { rows: [string, ReactNode][] }) {
  if (rows.length === 0) return null;
  return (
    <dl className="mt-1 grid grid-cols-[8em_1fr] gap-x-3 gap-y-1">
      {rows.map(([label, value], index) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: two sources may share a label; the order is fixed.
        <div key={index} className="contents">
          <dt className="text-[var(--color-muted)]">{label}</dt>
          <dd className="whitespace-pre-wrap break-words">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

/** A provider list as a table of the keys any of its rows fills, in the order given. */
function ListTable({
  t,
  section,
  rows,
  keys,
}: {
  t: Translate;
  section: string;
  rows: readonly Values[] | null | undefined;
  keys: readonly string[];
}) {
  const filled = keys.filter((key) => (rows ?? []).some((row) => shown(key, row[key]) != null));
  if (!rows || rows.length === 0 || filled.length === 0) return null;
  return (
    <div className="mt-1 overflow-x-auto">
      <table className="w-full text-left text-[length:var(--text-caption)]">
        <thead>
          <tr>
            {filled.map((key) => (
              <th key={key} className="pr-2 font-normal text-[var(--color-muted)]">
                {t(`fields.${section}.${key}`)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: provider rows carry no id; their order is the provider's.
            <tr key={index} className="align-top">
              {filled.map((key) => (
                <td key={key} className="whitespace-pre-wrap pr-2">
                  {shown(key, row[key]) ?? ""}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <details className="mt-2">
      <summary className="cursor-pointer text-[var(--color-accent)]">{title}</summary>
      <div className="mt-1">{children}</div>
    </details>
  );
}

const DETAIL = [
  "zadr_nm",
  "cltr_radr",
  "loc_vnty_pscd_cont",
  "utlz_pscd_cont",
  "icdl_cdtn_cont",
  "pytn_mtrs_cont",
  "dspl_vld_cont",
  "purr_qlfc_cont",
  "cltr_etc_cont",
  "frst_pbanc_ymd",
] as const;
const AREAS = ["cland_cont", "sqms_cont", "purs_alc_cont", "dtl_cltr_nm"] as const;
const STATEMENT = [
  "dlgt_org_nm",
  "mng_no",
  "pbanc_ymd",
  "dtbt_rqr_edtm_ymd",
  "pbct_tdps",
  "zadr",
  "alc_cont",
  "pbct_espc",
  "bid_perd",
  "opbd_ymd",
  "dodisp_dudt",
  "ersr_excl_rgt_cont",
  "stty_ebr_vld_cont",
  "pytn_mtrs_cont",
  "prcv_ymd",
  "etc_smry_cont",
  "szr_prpt_indct_cont",
] as const;
const LEASES = [
  "irst_div_nm",
  "bid_grtee_amt",
  "mthr_amt",
  "conv_grtee_amt",
  "cfmtn_ymd",
  "mvin_ymd",
] as const;
const OCCUPANCY = [
  "ocpy_rel_cd_nm",
  "ctrt_ymd",
  "mvin_ymd",
  "cfmtn_ymd",
  "acpm_grtee_amt",
  "rnt_amt",
  "lsd_part_cont",
] as const;
const REGISTRY = ["irst_div_nm", "rgst_ymd", "inpr_stng_amt"] as const;
const DISTRIBUTION = [
  "rgt_rel_cd_nm",
  "stng_ymd",
  "bond_dclr_stng_amt",
  "dtbt_rqr_yn",
  "dtbt_rqr_ymd",
  "dtbt_rqr_amt",
  "ersr_psbl_yn",
  "etc_cont",
] as const;
const BATCH = [
  "cltr_mng_no",
  "onbid_cltr_nm",
  "prpt_div_nm",
  "dsps_mthod_nm",
  "cltr_usg_scls_ctgr_nm",
  "usbd_nft",
  "apsl_evl_amt",
  "land_sqms",
  "bld_sqms",
] as const;
const CORRECTIONS = ["crtn_ymd", "crtn_item_cont", "bfmdf_lst_cont", "afmdf_lst_cont"] as const;
const BID_INFO = [
  "pbct_tdps_cont",
  "pcmt_pay_mtd_cont",
  "pcmt_pay_term_cont",
  "bid_vld_crtr_cont",
  "qlfc_lmt_cdtn_cont",
  "rgn_lmt_cdtn_cont",
  "etc_lmt_cdtn_cont",
  "ptct_cmsn",
  "bid_rltn_dcmt_cont",
  "bid_rltn_dcmt_sbmsn_term_nm",
  "bid_rltn_dcmt_sbmsn_mthod_nm",
  "nrnk_aply_psbl_yn",
  "tdps_sbtn_dcmt_yn",
  "tdps_sbtn_dcmt_cont",
  "tdps_sbtn_dcmt_sbmsn_term_nm",
  "tdps_sbtn_dcmt_sbmsn_mthod_nm",
  "bid_ptct_aply_yn",
  "bid_ptct_aply_dcmt_cont",
  "bid_ptct_aply_term_nm",
  "bid_ptct_dcmt_sbmsn_mthod_nm",
  "collb_mthod_dcmt_cont",
  "collb_dcmt_sbmsn_mthod_nm",
  "collb_dcmt_sbmsn_term_nm",
  "subt_bid_dcmt_cont",
  "subt_bid_dcmt_sbmsn_mthod_nm",
  "subt_bid_dcmt_sbmsn_term_nm",
  "smns_ip_dpcn_bid_blckt_yn",
  "opsn_cltr_bid_psbl_yn",
  "bid_psbl_cltr_qnty",
  "prcn_nsq_bid_rslt_nm",
  "prcn_nsq_lowst_bid_prc",
  "pytn_mtrs_cont",
] as const;
// The evaluation and reasonable-highest-price terms, when the round is sold by them.
const EVALUATION = [
  "evl_ptdb_clg_cont",
  "evl_perd_cont",
  "bid_prc_asmc_calcw_cont",
  "crtr_amt_bid_nm",
  "min_esure_amt",
  "min_esure_rate",
  "crtr_amt",
  "crtr_rate",
  "rsv_prc_rng_cont",
  "rsv_prc_chc_num",
  "rsv_prc_etmt_num",
  "tplm_crrt",
  "rsnb_hghprc_cfct",
] as const;
const EVALUATION_ITEMS = [
  "evl_item_cont",
  "evl_item_ptdb_scr",
  "evl_item_desc_cont",
  "evl_item_rmk_cont",
] as const;
const SCHEDULE = [
  "pbct_nsq",
  "bid_div_nm",
  "cltr_bid_bgng_dt",
  "cltr_bid_end_dt",
  "cltr_opbd_dt",
  "pbct_opbd_plc_cont",
  "lowst_bid_prc_indct_cont",
  "cltr_dodisp_dt",
] as const;
const EARLIER = [
  "pbct_nsq",
  "cltr_opbd_dt",
  "pbct_stat_nm",
  "lowst_bid_prc_indct_cont",
  "scfb_amt",
  "apsl_prc_ctrs_lowst_bid_rto",
  "frst_ctrs_lowst_bid_prc_rto",
] as const;
const HISTORY = [
  "pbct_nsq",
  "pbct_stat_nm",
  "opened_at",
  "lowest_bid_price_krw",
  "winning_bid_amount_krw",
  "valid_bidder_count",
  "invalid_bidder_count",
  "apsl_prc_ctrs_scfb_prc_rto",
  "lowst_bid_ctrs_scfb_prc_rto",
  "rtrcn_rsn_cont",
  "afsb_rtrcn_rsn_cont",
] as const;
const APPRAISALS = ["apsl_evl_org_nm", "apsl_appr_nm", "apsl_evl_ymd", "apsl_evl_amt"] as const;
const ANNOUNCEMENT = [
  "onbid_pbanc_nm",
  "pbanc_mng_no",
  "pbanc_kind_nm",
  "org_nm",
  "rspb_sbr_nm",
  "pbanc_ymd",
  "cltr_opbd_dt",
  "exct_stat_nm",
  "pbanc_org_nm",
  "pbanc_nsq_nm",
  "bid_amt_rls_yn",
  "pbanc_ptct_qlfc_cont",
  "ptct_cmsn",
  "pbanc_rtrcn_rsn_cont",
  "rltn_pbanc_mng_no",
  "rltn_pbanc_nm",
] as const;
// The item's media beyond its photos (root ADR-0340 §4): images open in place, the rest link.
const IMAGE_FORMATS = new Set(["jpg", "png", "gif"]);

/** The bid conditions, the item, its rights, appraisals, history and announcement of one round. */
export function RoundDetails({ t, round }: { t: Translate; round: PublicAuctionRound }) {
  const item = round.item;
  const announcement = round.announcement;
  return (
    <>
      <ItemSections t={t} item={item} />
      {announcement && <AnnouncementSection t={t} announcement={announcement} />}
    </>
  );
}

function ItemSections({ t, item }: { t: Translate; item: PublicAuctionItem | undefined }) {
  if (!item) return null;
  return (
    <>
      <ItemMaps t={t} item={item} />
      <BidSections t={t} item={item} />
      <DetailSections t={t} item={item} />
    </>
  );
}

function ItemMaps({ t, item }: { t: Translate; item: PublicAuctionItem }) {
  const maps = item.media ?? [];
  return (
    <>
      {maps.length > 0 && (
        <ul className="mt-2 grid grid-cols-3 gap-2" aria-label={t("sections.maps")}>
          {maps.map((media) =>
            IMAGE_FORMATS.has(media.media_format ?? "") ? (
              <li key={media.sha256} className="aspect-[4/3] overflow-hidden rounded-md">
                {/* biome-ignore lint/performance/noImgElement: content-addressed onbid media, as the gallery. */}
                <img
                  src={onbidMediaSrc({
                    sha256: media.sha256 ?? "",
                    media_format: media.media_format ?? "",
                  })}
                  alt={t(`mediaKind.${media.media_kind}`)}
                  loading="lazy"
                  decoding="async"
                  className="h-full w-full object-cover"
                />
              </li>
            ) : null,
          )}
        </ul>
      )}
    </>
  );
}

function BidSections({ t, item }: { t: Translate; item: PublicAuctionItem }) {
  const bidInfo = item.bid_info;
  const history = item.history ?? [];
  return (
    <>
      {bidInfo && (
        <Section title={t("sections.bidInfo")}>
          <Fields rows={rowsOf(t, "bidInfo", bidInfo, BID_INFO)} />
          <ListTable t={t} section="cseq" rows={bidInfo.cseq_bid_inf_clg_list} keys={SCHEDULE} />
          <Fields rows={rowsOf(t, "bidInfo", bidInfo, EVALUATION)} />
          <ListTable
            t={t}
            section="evl"
            rows={bidInfo.prpsl_evl_item_clg_list}
            keys={EVALUATION_ITEMS}
          />
        </Section>
      )}
      {(history.length > 0 || (bidInfo?.prcn_bid_clg_list ?? []).length > 0) && (
        <Section title={t("sections.history")}>
          {history.length > 0 ? (
            <ListTable t={t} section="history" rows={history} keys={HISTORY} />
          ) : (
            <ListTable t={t} section="prcn" rows={bidInfo?.prcn_bid_clg_list} keys={EARLIER} />
          )}
        </Section>
      )}
    </>
  );
}

function DetailSections({ t, item }: { t: Translate; item: PublicAuctionItem }) {
  const detail = item.detail;
  const appraisals = detail?.apsl_evl_clg_list ?? [];
  return (
    <>
      {detail && (
        <Section title={t("sections.detail")}>
          <Fields rows={rowsOf(t, "detail", detail, DETAIL)} />
          <ListTable t={t} section="sqms" rows={detail.sqms_list} keys={AREAS} />
          <ListTable t={t} section="batc" rows={detail.batc_bid_cltr_clg_list} keys={BATCH} />
        </Section>
      )}
      {detail &&
        [
          detail.paps_inf,
          detail.leas_inf_list,
          detail.ocpy_rel_list,
          detail.rgst_prmr_inf_list,
          detail.dtbt_rqr_mtrs_list,
        ].some((list) => (list ?? []).length > 0) && (
          <Section title={t("sections.rights")}>
            {(detail.paps_inf ?? []).map((statement, index) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: the provider's 0..1 statement has no id.
              <Fields key={index} rows={rowsOf(t, "paps", statement, STATEMENT)} />
            ))}
            <ListTable t={t} section="leas" rows={detail.leas_inf_list} keys={LEASES} />
            <ListTable t={t} section="ocpy" rows={detail.ocpy_rel_list} keys={OCCUPANCY} />
            <ListTable t={t} section="rgst" rows={detail.rgst_prmr_inf_list} keys={REGISTRY} />
            <ListTable t={t} section="dtbt" rows={detail.dtbt_rqr_mtrs_list} keys={DISTRIBUTION} />
          </Section>
        )}
      {appraisals.length > 0 && (
        <Section title={t("sections.appraisals")}>
          <ListTable t={t} section="apsl" rows={appraisals} keys={APPRAISALS} />
          <ul className="mt-1 flex flex-col gap-1">
            {appraisals.map((report, index) =>
              report.sha256 ? (
                // biome-ignore lint/suspicious/noArrayIndexKey: one report per provider row, in its order.
                <li key={index}>
                  <a
                    href={onbidMediaSrc({
                      sha256: report.sha256,
                      media_format: report.media_format ?? "pdf",
                    })}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-[var(--color-accent)]"
                  >
                    {report.apsl_evl_org_nm ?? t("appraisalDocument")}
                  </a>
                </li>
              ) : null,
            )}
          </ul>
        </Section>
      )}
      {(detail?.crtn_lst_clg_list ?? []).length > 0 && (
        <Section title={t("sections.corrections")}>
          <ListTable t={t} section="crtn" rows={detail?.crtn_lst_clg_list} keys={CORRECTIONS} />
        </Section>
      )}
    </>
  );
}

function AnnouncementSection({
  t,
  announcement,
}: {
  t: Translate;
  announcement: PublicAuctionAnnouncement;
}) {
  const files = (announcement.atch_file_list ?? []).filter((file) => file.sha256);
  return (
    <Section title={t("sections.announcement")}>
      <Fields rows={rowsOf(t, "announcement", announcement, ANNOUNCEMENT)} />
      {announcement.anncm_al_cont && (
        <Section title={t("fields.announcement.anncm_al_cont")}>
          <p className="whitespace-pre-wrap break-words">{announcement.anncm_al_cont}</p>
        </Section>
      )}
      {announcement.anncm_al_cont_omitted && (
        <p className="mt-1 text-[var(--color-muted)]">{t("announcementTextOmitted")}</p>
      )}
      {files.length > 0 && (
        <ul className="mt-1 flex flex-col gap-1" aria-label={t("sections.attachments")}>
          {files.map((file) => (
            <li key={file.sha256}>
              <a
                href={onbidMediaSrc({
                  sha256: file.sha256 ?? "",
                  media_format: file.media_format ?? "pdf",
                })}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--color-accent)]"
              >
                {file.btbd_atch_file_nm ?? t("sections.attachments")}
              </a>
            </li>
          ))}
        </ul>
      )}
    </Section>
  );
}
