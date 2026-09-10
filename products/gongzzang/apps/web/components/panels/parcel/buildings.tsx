// apps/web/components/panels/parcel/buildings.tsx
"use client";
import { useTranslations } from "next-intl";
import { useState } from "react";
import type { BuildingUnit } from "@/lib/api/building-units";
import type { BuildingsResponse } from "@/lib/api/buildings";
import type { PanelStackEntry } from "@/lib/panel/types";

export function ParcelBuildingsCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "parcel" }>;
  data: BuildingsResponse;
}) {
  const t = useTranslations("panels.parcel.buildings");
  const floorText = useTranslations("panels.parcel.floors");

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
                <UnitList units={b.units} />
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
          <UnitList units={data.unlinked_units} />
        </section>
      )}
      {/* PNU 의 entry.id 는 i18n 라벨 표시 외 미사용 — 본 view 는 list-only */}
      <span className="hidden">{entry.id}</span>
    </div>
  );
}

function UnitList({ units }: { units: BuildingUnit[] }) {
  const t = useTranslations("panels.parcel.buildings");
  const [visibleCount, setVisibleCount] = useState(100);
  return (
    <div className="mt-2 border-t border-[var(--color-hairline)] pt-2">
      {units.length === 0 && <div className="text-[var(--color-muted)]">{t("units.none")}</div>}
      <ul className="flex flex-col gap-1">
        {units.slice(0, visibleCount).map((unit) => (
          <li key={unit.id} className="flex items-baseline justify-between gap-2">
            <span className="text-[var(--color-ink)]">
              {unit.dong_name} {unit.ho_name}
            </span>
            <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
              {unit.floor_label}
              {unit.exclusive_area_m2 != null &&
                ` · ${unit.exclusive_area_m2.toLocaleString("ko-KR")} ㎡`}
              {unit.usage_name && ` · ${unit.usage_name}`}
            </span>
          </li>
        ))}
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
