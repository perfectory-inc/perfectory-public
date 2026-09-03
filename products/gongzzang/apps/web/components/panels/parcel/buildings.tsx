// apps/web/components/panels/parcel/buildings.tsx
"use client";
import { useTranslations } from "next-intl";
import { useCallback, useState } from "react";
import { type BuildingUnit, fetchBuildingUnits } from "@/lib/api/building-units";
import type { BuildingsResponse } from "@/lib/api/buildings";
import type { PanelStackEntry } from "@/lib/panel/types";

type UnitsState =
  | { status: "closed" }
  | { status: "loading"; units: BuildingUnit[]; cursor: string | null }
  | { status: "open"; units: BuildingUnit[]; cursor: string | null }
  | { status: "error"; units: BuildingUnit[]; cursor: string | null };

export function ParcelBuildingsCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "parcel" }>;
  data: BuildingsResponse;
}) {
  const t = useTranslations("panels.parcel.buildings");
  const [unitsByBuilding, setUnitsByBuilding] = useState<Record<string, UnitsState>>({});

  const loadPage = useCallback(async (buildingId: string, cursor: string | null) => {
    setUnitsByBuilding((prev) => {
      const current = prev[buildingId];
      const kept = current && current.status !== "closed" ? current.units : [];
      const keptCursor = current && current.status !== "closed" ? current.cursor : null;
      return {
        ...prev,
        [buildingId]: { status: "loading", units: kept, cursor: cursor ?? keptCursor },
      };
    });
    try {
      const page = await fetchBuildingUnits(buildingId, cursor ? { cursor } : undefined);
      setUnitsByBuilding((prev) => {
        const current = prev[buildingId];
        const kept = current && current.status !== "closed" ? current.units : [];
        return {
          ...prev,
          [buildingId]: {
            status: "open",
            units: cursor ? [...kept, ...page.units] : page.units,
            cursor: page.next_cursor ?? null,
          },
        };
      });
    } catch {
      setUnitsByBuilding((prev) => {
        const current = prev[buildingId];
        const kept = current && current.status !== "closed" ? current.units : [];
        const keptCursor = current && current.status !== "closed" ? current.cursor : null;
        return { ...prev, [buildingId]: { status: "error", units: kept, cursor: keptCursor } };
      });
    }
  }, []);

  const toggleUnits = useCallback(
    (buildingId: string) => {
      const current = unitsByBuilding[buildingId];
      if (current && current.status !== "closed") {
        setUnitsByBuilding((prev) => ({ ...prev, [buildingId]: { status: "closed" } }));
        return;
      }
      void loadPage(buildingId, null);
    },
    [loadPage, unitsByBuilding],
  );

  if (data.buildings.length === 0) {
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
          const units = unitsByBuilding[b.id] ?? { status: "closed" as const };
          const opened = units.status !== "closed";
          return (
            <li
              key={b.id}
              className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]"
            >
              <div className="font-semibold text-[var(--color-ink)]">{b.name}</div>
              <div className="text-[var(--color-muted)]">
                {/* 대장이 말하지 않은 값은 "정보 없음" — 0 으로 지어내지 않는다 (root ADR-0078). */}
                {b.purpose ?? t("unknown")} ·{" "}
                {b.total_area_m2 != null
                  ? `${b.total_area_m2.toLocaleString("ko-KR")} ㎡`
                  : t("unknown")}
                {b.approved_at && ` · ${b.approved_at}`}
              </div>
              <button
                type="button"
                className="mt-2 text-[length:var(--text-caption)] text-[var(--color-accent)] underline-offset-2 hover:underline"
                onClick={() => toggleUnits(b.id)}
              >
                {opened ? t("units.hide") : t("units.show")}
              </button>
              {opened && (
                <div className="mt-2 border-t border-[var(--color-hairline)] pt-2">
                  {units.units.length === 0 && units.status === "open" && (
                    <div className="text-[var(--color-muted)]">{t("units.none")}</div>
                  )}
                  {units.units.length > 0 && (
                    <ul className="flex flex-col gap-1">
                      {units.units.map((u) => (
                        <li key={u.id} className="flex items-baseline justify-between gap-2">
                          <span className="text-[var(--color-ink)]">
                            {u.dong_name && `${u.dong_name} `}
                            {u.ho_name}
                          </span>
                          <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
                            {u.floor_label}
                            {u.exclusive_area_m2 != null &&
                              ` · ${u.exclusive_area_m2.toLocaleString("ko-KR")} ㎡`}
                            {u.usage_name && ` · ${u.usage_name}`}
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                  {units.status === "loading" && (
                    <div className="mt-1 text-[var(--color-muted)]">{t("units.loading")}</div>
                  )}
                  {units.status === "error" && (
                    <div className="mt-1 text-[var(--color-danger)]">{t("units.error")}</div>
                  )}
                  {units.status === "open" && units.cursor && (
                    <button
                      type="button"
                      className="mt-2 text-[length:var(--text-caption)] text-[var(--color-accent)] underline-offset-2 hover:underline"
                      onClick={() => void loadPage(b.id, units.cursor)}
                    >
                      {t("units.more")}
                    </button>
                  )}
                </div>
              )}
            </li>
          );
        })}
      </ul>
      {/* PNU 의 entry.id 는 i18n 라벨 표시 외 미사용 — 본 view 는 list-only */}
      <span className="hidden">{entry.id}</span>
    </div>
  );
}
