// apps/web/components/panels/parcel/buildings.tsx
"use client";
import { useQueries } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useState } from "react";
import type { BuildingUnit } from "@/lib/api/building-units";
import type { BuildingsResponse } from "@/lib/api/buildings";
import { unitFloorText } from "@/lib/api/unit-floor";
import type { UnitPrices } from "@/lib/api/unit-price-edge";
import type { PanelStackEntry } from "@/lib/panel/types";
import { NO_BUILDING, unitPricesQuery } from "./unit-prices";

export function ParcelBuildingsCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "parcel" }>;
  data: BuildingsResponse;
}) {
  const t = useTranslations("panels.parcel.buildings");
  const floorText = useTranslations("panels.parcel.floors");
  // Each building's unit prices are its own document (root ADR-0190, ADR-0204), joined to the
  // units by id: one request per building with units (and one for the units no building holds),
  // all at once.
  const priced = data.buildings.filter((b) => b.units.length > 0).map((b) => b.id);
  const keys = data.unlinked_units.length > 0 ? [...priced, NO_BUILDING] : priced;
  const prices = useQueries({ queries: keys.map((id) => unitPricesQuery(entry.id, id)) });
  const priceState = (buildingId: string): PriceState => {
    const query = prices[keys.indexOf(buildingId)];
    return {
      prices: query?.data,
      loading: query?.isFetching === true && query.data === undefined,
      failed: query?.isError ?? false,
    };
  };

  if (data.buildings.length === 0 && data.unlinked_units.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      <header className="flex items-baseline gap-2">
        <h2 className="text-[length:var(--text-title-md)] font-semibold">{t("title")}</h2>
        <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {data.buildings.length} {t("count")}
        </span>
      </header>
      <ul className="flex flex-col gap-2">
        {data.buildings.map((b) => {
          return (
            <li
              key={b.id}
              className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]"
            >
              <div className="font-semibold text-[var(--color-ink)]">
                {b.name || floorText("buildingFallback")}
              </div>
              <div className="text-[var(--color-muted)]">
                {/* 대장이 말하지 않은 값은 "정보 없음" — 0 으로 지어내지 않는다 (root ADR-0078). */}
                {b.purpose ?? t("unknown")} ·{" "}
                {b.total_area_m2 != null
                  ? `${b.total_area_m2.toLocaleString("ko-KR")} ㎡`
                  : t("unknown")}
                {b.approved_at && ` · ${b.approved_at}`}
              </div>
              <details className="group mt-2">
                <summary className="cursor-pointer text-[length:var(--text-caption)] text-[var(--color-accent)]">
                  <span className="group-open:hidden">{t("units.show")}</span>
                  <span className="hidden group-open:inline">{t("units.hide")}</span>
                </summary>
                <UnitList units={b.units} price={priceState(b.id)} />
              </details>
            </li>
          );
        })}
      </ul>
      {data.unlinked_units.length > 0 && (
        <section className="rounded-md border border-[var(--color-hairline)] p-3">
          <h3 className="font-semibold">
            {floorText("buildingFallback")} · {t("unknown")}
          </h3>
          <UnitList units={data.unlinked_units} price={priceState(NO_BUILDING)} />
        </section>
      )}
      {/* PNU 의 entry.id 는 i18n 라벨 표시 외 미사용 — 본 view 는 list-only */}
      <span className="hidden">{entry.id}</span>
    </div>
  );
}

type PriceState = { prices: UnitPrices | undefined; loading: boolean; failed: boolean };

function UnitList({ units, price }: { units: BuildingUnit[]; price: PriceState }) {
  const t = useTranslations("panels.parcel.buildings");
  const floorText = useTranslations("panels.parcel.floors");
  const [visibleCount, setVisibleCount] = useState(100);
  // Whatever the lane says wins; until it answers, or when it fails or has no document, a v2
  // building document's own prices are drawn, so no merge order leaves a gap (root ADR-0190).
  const historyOf = (unit: BuildingUnit) =>
    price.prices?.get(unit.id) ?? unit.official_price_history ?? [];
  const lost = price.failed && !units.some((unit) => historyOf(unit).length > 0);
  return (
    <div className="mt-2 border-t border-[var(--color-hairline)] pt-2">
      {units.length === 0 && <div className="text-[var(--color-muted)]">{t("units.none")}</div>}
      {lost && (
        <div role="status" className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("units.priceError")}
        </div>
      )}
      <ul className="flex flex-col gap-1">
        {units.slice(0, visibleCount).map((unit) => {
          const history = historyOf(unit);
          return (
            <li key={unit.id} className="flex flex-wrap items-baseline justify-between gap-2">
              <span className="text-[var(--color-ink)]">
                {unit.dong_name} {unit.ho_name}
              </span>
              <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
                {unitFloorText(unit, floorText) ?? floorText("unknownFloor")}
                {unit.exclusive_area_m2 != null &&
                  ` · ${unit.exclusive_area_m2.toLocaleString("ko-KR")} ㎡`}
                {unit.usage_name && ` · ${unit.usage_name}`}
              </span>
              {history.length > 0 && (
                <details className="w-full text-[length:var(--text-caption)]">
                  <summary className="cursor-pointer text-[var(--color-accent)]">
                    {t("units.officialPriceHistory")}
                  </summary>
                  <dl className="mt-1 grid grid-cols-2 gap-x-3 gap-y-1">
                    {history.map((price) => {
                      const date = `${price.base_date.slice(0, 4)}-${price.base_date.slice(4, 6)}-${price.base_date.slice(6, 8)}`;
                      return (
                        <div key={price.base_date} className="contents">
                          <dt>
                            <time dateTime={date}>{date}</time>
                          </dt>
                          <dd className="text-right">
                            {t("units.priceWon", {
                              value: price.price_won.toLocaleString("ko-KR"),
                            })}
                          </dd>
                        </div>
                      );
                    })}
                  </dl>
                </details>
              )}
              {history.length === 0 && price.loading && (
                // Holds the price row's height so the list does not move when the prices arrive.
                <span
                  aria-hidden="true"
                  className="invisible w-full text-[length:var(--text-caption)]"
                >
                  {t("units.officialPriceHistory")}
                </span>
              )}
            </li>
          );
        })}
      </ul>
      {visibleCount < units.length && (
        <button
          type="button"
          className="mt-2 text-[var(--color-accent)]"
          onClick={() => setVisibleCount((count) => count + 100)}
        >
          {t("units.more")}
        </button>
      )}
    </div>
  );
}
