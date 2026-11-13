// apps/web/components/panels/parcel/court-auction.tsx
"use client";
import { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import { PhotoGrid } from "@/components/media/photo-grid";
import type {
  CourtAuction,
  CourtAuctionRound,
  CourtAuctionSaleResult,
} from "@/lib/api/court-auction";
import { onbidMediaSrc, parcelMediaStore } from "@/lib/api/public-auction";
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
              detail: r.detail,
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
type Detail = NonNullable<CourtAuctionRound["detail"]>;
type Item = { named: Named; rows: [string, string][]; photos?: Photos; detail?: Detail | null };

function ItemList({ t, items }: { t: Translate; items: Item[] }) {
  return (
    <ul className="flex flex-col gap-2">
      {items.map(({ named: r, rows, photos, detail }, i) => (
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
          <Rows rows={rows} />
          {detail && <RoundDetail t={t} detail={detail} />}
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

function Rows({ rows }: { rows: [string, string][] }) {
  if (rows.length === 0) return null;
  return (
    <dl className="mt-2 grid grid-cols-[6em_1fr] gap-x-3 gap-y-1 text-[var(--color-ink)] tabular-nums">
      {rows.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-[var(--color-muted)]">{label}</dt>
          <dd className="whitespace-pre-line">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mt-3 border-t border-[var(--color-hairline)] pt-2">
      <h3 className="font-semibold text-[var(--color-ink)]">{title}</h3>
      {children}
    </section>
  );
}

/** Pairs whose value is present, in order. */
function present(pairs: [string, string | number | null | undefined][]): [string, string][] {
  return pairs.flatMap(([label, value]) =>
    value == null || value === "" ? [] : [[label, String(value)] as [string, string]],
  );
}

/** An item's 물건상세 (root ADR-0540): its case, its 매각물건명세서, 기일내역, 목록내역, 제시외건물 and
 * 감정평가요항표, in the site's words; a code shows the name the site gives it (its 공통코드, §5).
 * Names of people are not in the document. */
function RoundDetail({ t, detail }: { t: Translate; detail: Detail }) {
  const c = detail.case;
  const s = detail.spec;
  const money = (v: number | null | undefined) => (v ? formatPriceKrw(v) : null);
  const caseRows = present([
    [t("detail.caseName"), c?.case_name],
    [t("detail.caseReceived"), c?.case_received_date],
    [t("detail.commencement"), c?.commencement_date],
    [t("detail.claim"), money(c?.claim_amount_krw)],
    [t("detail.dividendDeadline"), c?.dividend_demand_deadline],
    [t("detail.saleDecisionDate"), s?.sale_decision_date],
    [
      t("detail.depositRate"),
      s?.deposit_rate_percent
        ? t("detail.depositRateValue", { rate: s.deposit_rate_percent })
        : null,
    ],
    [t("detail.itemKind"), s?.item_usage_name],
    [t("detail.bidKind"), s?.bid_kind_name],
    [t("detail.itemRemarks"), s?.item_remarks],
  ]);
  const specRows = present([
    [t("detail.specWritten"), s?.spec_written_date],
    [t("detail.firstRank"), s?.first_rank_setting],
    [t("detail.assumedRights"), s?.assumed_rights],
    [t("detail.superficies"), s?.superficies],
    [t("detail.specRemarks"), s?.spec_remarks],
  ]);
  return (
    <div className="mt-1">
      <Rows rows={caseRows} />
      {specRows.length > 0 && (
        <Section title={t("detail.spec")}>
          <Rows rows={specRows} />
        </Section>
      )}
      {detail.schedule.length > 0 && (
        <Section title={t("detail.schedule")}>
          <table className="mt-1 w-full text-left tabular-nums">
            <thead className="text-[var(--color-muted)]">
              <tr>
                <th className="font-normal">{t("detail.scheduleDate")}</th>
                <th className="font-normal">{t("detail.scheduleKind")}</th>
                <th className="font-normal">{t("detail.schedulePlace")}</th>
                <th className="font-normal">{t("detail.scheduleMinimum")}</th>
                <th className="font-normal">{t("detail.scheduleResult")}</th>
              </tr>
            </thead>
            <tbody>
              {detail.schedule.map((step, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: the site's list order is the row's identity.
                <tr key={i}>
                  <td>{[step.schedule_date, step.schedule_time_raw].filter(Boolean).join(" ")}</td>
                  <td>{step.schedule_kind_name ?? ""}</td>
                  <td>{step.schedule_place ?? ""}</td>
                  <td>{money(step.minimum_bid_price_krw) ?? ""}</td>
                  <td>{step.result_name ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      )}
      {detail.objects.length > 0 && (
        <Section title={t("detail.objects")}>
          <ul className="mt-1 flex flex-col gap-1">
            {detail.objects.map((o) => (
              <li key={o.object_seq} className="whitespace-pre-line">
                <span className="text-[var(--color-muted)]">
                  {t("detail.objectNumber", { n: o.object_seq })}
                </span>{" "}
                {[
                  o.list_kind_name,
                  o.realty_kind,
                  o.land_category,
                  o.area_raw,
                  money(o.appraisal_amount_krw),
                ]
                  .filter(Boolean)
                  .join(" · ")}
                {o.building_structure ? `\n${o.building_structure}` : ""}
                {o.closure_kind_name ? `\n${t("detail.closure")} ${o.closure_kind_name}` : ""}
              </li>
            ))}
          </ul>
        </Section>
      )}
      {detail.annexes.length > 0 && (
        <Section title={t("detail.annexes")}>
          <ul className="mt-1 flex flex-col gap-1">
            {detail.annexes.map((a, i) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: the site's list order is the row's identity.
              <li key={i}>
                <span className="text-[var(--color-muted)]">
                  {t("detail.objectNumber", { n: a.object_seq })}
                </span>{" "}
                {[a.usage_raw, a.structure_raw, a.area_raw].filter(Boolean).join(" · ")}
              </li>
            ))}
          </ul>
        </Section>
      )}
      {(detail.related_cases?.length ?? 0) > 0 && (
        <Section title={t("detail.relatedCases")}>
          <ul className="mt-1 flex flex-col gap-1">
            {detail.related_cases?.map((r, i) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: the site's list order is the row's identity.
              <li key={i}>
                {[r.related_user_case_no, r.relation_kind_name].filter(Boolean).join(" · ")}
              </li>
            ))}
          </ul>
        </Section>
      )}
      {(detail.parties?.length ?? 0) > 0 && (
        <Section title={t("detail.parties")}>
          <p className="mt-1">
            {detail.parties
              ?.map((p) => t("detail.partyCount", { kind: p.kind_name ?? "", count: p.count }))
              .join(" · ")}
          </p>
        </Section>
      )}
      {detail.survey && <SurveySection t={t} survey={detail.survey} />}
      {detail.appraisal && (
        <Section title={t("detail.appraisal")}>
          <Rows
            rows={present([
              [t("detail.priceBasis"), detail.appraisal.price_basis_date],
              [t("detail.written"), detail.appraisal.written_date],
              [t("detail.opinion"), detail.appraisal.opinion],
            ])}
          />
          {/* The 감정평가서 PDF sits once in the shared store the parcel edge serves (root ADR-0540 §1). */}
          {detail.appraisal.report && (
            <a
              href={onbidMediaSrc(detail.appraisal.report)}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-1 inline-block text-[var(--color-accent)]"
            >
              {t("detail.appraisalReport")}
            </a>
          )}
        </Section>
      )}
      {detail.appraisal_points.length > 0 && (
        <Section title={t("detail.appraisalPoints")}>
          <ul className="mt-1 flex flex-col gap-1">
            {detail.appraisal_points.map((p, i) =>
              p.content ? (
                // biome-ignore lint/suspicious/noArrayIndexKey: the site's list order is the row's identity.
                <li key={i} className="whitespace-pre-line">
                  {p.item_name && <span className="text-[var(--color-muted)]">{p.item_name} </span>}
                  {p.content}
                </li>
              ) : null,
            )}
          </ul>
        </Section>
      )}
    </div>
  );
}

type Survey = NonNullable<Detail["survey"]>;

/** The case's newest 현황조사서 (root ADR-0540 §1): when it was surveyed, the possession and lease
 * narrative and each 목록's possession, names masked. */
function SurveySection({ t, survey }: { t: Translate; survey: Survey }) {
  const surveyed = (survey.surveyed_raw ?? "")
    .split("^")
    .filter((part) => part.trim() !== "")
    .join(", ");
  return (
    <Section title={t("detail.survey")}>
      <Rows
        rows={present([
          [t("detail.surveyedAt"), surveyed],
          [t("detail.possession"), survey.possession_summary],
          [t("detail.lease"), survey.lease_text],
        ])}
      />
      {(survey.objects?.length ?? 0) > 0 && (
        <ul className="mt-1 flex flex-col gap-1">
          {survey.objects?.map((o) => (
            <li key={o.object_seq} className="whitespace-pre-line">
              <span className="text-[var(--color-muted)]">
                {t("detail.objectNumber", { n: o.object_seq })}
              </span>{" "}
              {[
                o.possession_text,
                o.lease_count ? t("detail.leaseCount", { n: o.lease_count }) : null,
              ]
                .filter(Boolean)
                .join(" · ")}
            </li>
          ))}
        </ul>
      )}
    </Section>
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
