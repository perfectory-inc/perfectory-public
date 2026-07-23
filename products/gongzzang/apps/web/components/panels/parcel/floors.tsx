// apps/web/components/panels/parcel/floors.tsx
"use client";
import { useTranslations } from "next-intl";
import type { FloorsResponse } from "@/lib/api/floors";
import type { PanelStackEntry } from "@/lib/panel/types";

export function ParcelFloorsCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "parcel" }>;
  data: FloorsResponse;
}) {
  const t = useTranslations("panels.parcel.floors");
  if (data.buildings.length === 0) {
    return <div className="p-6 text-center text-[var(--color-muted)]">{t("none")}</div>;
  }
  return (
    <div className="flex flex-col gap-3 p-6">
      <header className="flex items-baseline gap-2">
        <h2 className="text-[length:var(--text-title-md)] font-semibold">{t("title")}</h2>
      </header>
      <ul className="flex flex-col gap-2">
        {data.buildings.map((b) => (
          <li
            key={b.id}
            className="rounded-md border border-[var(--color-hairline)] p-3 text-[length:var(--text-body-sm)]"
          >
            <div className="font-semibold text-[var(--color-ink)]">
              {b.name.trim() || t("buildingFallback")}
            </div>
            <div className="text-[var(--color-muted)]">
              {t("aboveGround", { count: b.above_ground })}
              {b.below_ground > 0 && ` · ${t("belowGround", { count: b.below_ground })}`}
              {b.has_rooftop && ` · ${t("rooftop")}`}
            </div>
          </li>
        ))}
      </ul>
      {/* PNU 는 i18n/telemetry 용도 외 미사용 */}
      <span className="hidden">{entry.id}</span>
    </div>
  );
}
