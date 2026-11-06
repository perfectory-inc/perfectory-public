// apps/web/components/panels/parcel/real-transaction.tsx
"use client";
import { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import {
  type Comparable,
  type Deal,
  formatManwon,
  industrialRegion,
  isInferred,
  isShareDeal,
  type RealTransaction,
} from "@/lib/api/real-transaction";
import { formatAreaM2 } from "@/lib/listings/format";

type T = ReturnType<typeof useTranslations>;

// The provider's name of each area column, per dataset (rt.molit.go.kr 조건별 자료제공 headers), as
// message keys; any other dataset's building area is 전용면적 and its land area 대지권면적.
const BUILDING_AREA: Record<string, string> = {
  real_transaction_industrial_trade: "totalFloorArea",
  real_transaction_commercial_trade: "totalFloorArea",
  real_transaction_detached_house_trade: "totalFloorArea",
  real_transaction_detached_house_rent: "contractArea",
};
const LAND_AREA: Record<string, string> = {
  real_transaction_land_trade: "contractArea",
  real_transaction_industrial_trade: "plottage",
  real_transaction_commercial_trade: "plottage",
  real_transaction_detached_house_trade: "plottage",
};

/** The parcel's own deals, newest contract first; a cancelled one stays and says so. */
export function ParcelRealTransactionsCard({ data }: { data: RealTransaction }) {
  const t = useTranslations("panels.parcel.realTransaction");
  if (data.transactions.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      <h2 className="text-[length:var(--text-title-md)] font-semibold">
        {t("ownTitle", { count: data.transactionTotal })}
      </h2>
      <ul className="flex flex-col gap-2">
        {data.transactions.map((deal, i) => (
          <DealItem
            // biome-ignore lint/suspicious/noArrayIndexKey: a deal carries no display id; its order (newest first) is its identity.
            key={i}
            title={t(`datasets.${deal.dataset_slug}`)}
            rows={dealRows(t, deal)}
            badges={[
              deal.cancelled ? t("cancelled", { date: deal.cancel_date ?? "" }) : null,
              isShareDeal(deal) ? t("share") : null,
              isInferred(deal.place_identity_grade) ? t("inferred") : null,
            ]}
          />
        ))}
      </ul>
      {data.transactions_omitted > 0 && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("omitted", { count: data.transactions_omitted })}
        </p>
      )}
      <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">{t("source")}</p>
    </div>
  );
}

/** The newest 공장창고 deals of the parcel's 법정동, masked lots as the provider writes them. */
export function ParcelComparablesCard({ data }: { data: RealTransaction }) {
  const t = useTranslations("panels.parcel.realTransaction");
  const region = industrialRegion(data);
  const median = region?.land_price_per_m2_10k_krw?.median;
  return (
    <div className="flex flex-col gap-3 p-6">
      <h2 className="text-[length:var(--text-title-md)] font-semibold">{t("comparablesTitle")}</h2>
      <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {t("comparablesNote", { from: data.comparables_from.slice(0, 7) })}
      </p>
      {median != null && region && (
        <p className="text-[length:var(--text-body-sm)] tabular-nums">
          {t("median", { price: formatManwon(Math.round(median)), count: region.deal_count })}
        </p>
      )}
      <ul className="flex flex-col gap-2">
        {data.comparables.map((deal, i) => (
          <DealItem
            // biome-ignore lint/suspicious/noArrayIndexKey: a deal carries no display id; its order (newest first) is its identity.
            key={i}
            title={
              deal.jibun_raw
                ? t("lot", { jibun: deal.jibun_raw })
                : t("datasets.real_transaction_industrial_trade")
            }
            rows={comparableRows(t, deal)}
            badges={[isInferred(deal.place_identity_grade) ? t("inferred") : null]}
          />
        ))}
      </ul>
    </div>
  );
}

function DealItem({
  title,
  rows,
  badges,
}: {
  title: string;
  rows: [string, ReactNode][];
  badges: (string | null)[];
}) {
  return (
    <li className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="font-semibold text-[var(--color-ink)]">{title}</span>
        {badges
          .filter((badge): badge is string => badge !== null)
          .map((badge) => (
            <span
              key={badge}
              className="rounded border border-[var(--color-hairline)] px-1.5 text-[length:var(--text-caption)] text-[var(--color-muted)]"
            >
              {badge}
            </span>
          ))}
      </div>
      <dl className="mt-2 grid grid-cols-[6em_1fr] gap-x-3 gap-y-1 text-[var(--color-ink)] tabular-nums">
        {rows.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-[var(--color-muted)]">{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </li>
  );
}

function dealRows(t: T, deal: Deal): [string, ReactNode][] {
  const rows: [string, ReactNode][] = [];
  if (deal.deal_date) rows.push([t("fields.dealDate"), deal.deal_date]);
  if (deal.deal_amount_10k_krw != null)
    rows.push([t("fields.dealAmount"), formatManwon(deal.deal_amount_10k_krw)]);
  if (deal.deposit_10k_krw != null)
    rows.push([t("fields.deposit"), formatManwon(deal.deposit_10k_krw)]);
  if (deal.monthly_rent_10k_krw != null)
    rows.push([t("fields.monthlyRent"), formatManwon(deal.monthly_rent_10k_krw)]);
  if (deal.building_area_m2 != null)
    rows.push([
      t(`fields.${BUILDING_AREA[deal.dataset_slug] ?? "exclusiveArea"}`),
      formatAreaM2(deal.building_area_m2),
    ]);
  if (deal.land_area_m2 != null)
    rows.push([
      t(`fields.${LAND_AREA[deal.dataset_slug] ?? "landRightArea"}`),
      formatAreaM2(deal.land_area_m2),
    ]);
  if (deal.floor) rows.push([t("fields.floor"), deal.floor]);
  if (deal.build_year != null) rows.push([t("fields.buildYear"), String(deal.build_year)]);
  if (deal.land_use) rows.push([t("fields.landUse"), deal.land_use]);
  return rows;
}

function comparableRows(t: T, deal: Comparable): [string, ReactNode][] {
  const rows: [string, ReactNode][] = [];
  if (deal.deal_date) rows.push([t("fields.dealDate"), deal.deal_date]);
  if (deal.deal_amount_10k_krw != null)
    rows.push([t("fields.dealAmount"), formatManwon(deal.deal_amount_10k_krw)]);
  if (deal.land_area_m2 != null) rows.push([t("fields.plottage"), formatAreaM2(deal.land_area_m2)]);
  if (deal.building_area_m2 != null)
    rows.push([t("fields.totalFloorArea"), formatAreaM2(deal.building_area_m2)]);
  if (deal.land_use) rows.push([t("fields.landUse"), deal.land_use]);
  return rows;
}
