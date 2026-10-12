// apps/web/components/panels/parcel/summary.tsx
"use client";
import { useQueries } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import { byPnuLaneQuery, isByPnuLane, type KindState } from "@/lib/api/by-pnu-lane";
import { type EdgeParcelProfile, FIRST_PAINT_KINDS, parcelSummary } from "@/lib/api/parcels";
import type { PanelStackEntry } from "@/lib/panel/types";
import { usePanelStack } from "@/lib/panel/use-panel-stack";

// The kinds whose lane the contract names, read in parallel (root ADR-0193 §5).
const SERVED_LANES = FIRST_PAINT_KINDS.map((kind) => kind.lane).filter(isByPnuLane);

export function ParcelSummaryCard({
  entry,
  data: bundle,
}: {
  entry: Extract<PanelStackEntry, { kind: "parcel" }>;
  data: EdgeParcelProfile;
}) {
  const t = useTranslations("panels.parcel.summary");
  const { push } = usePanelStack();
  const lanes = useQueries({
    queries: SERVED_LANES.map((lane) => byPnuLaneQuery(lane, entry.id)),
  });
  const { info: data, states } = parcelSummary(
    bundle,
    entry.id,
    Object.fromEntries(SERVED_LANES.map((lane, i) => [lane, lanes[i]])),
  );
  // Each row draws its own kind's answer: a value, none, still coming, or failed.
  const row = (state: KindState<unknown>["state"], value: ReactNode) =>
    state === "loading" ? (
      <span aria-busy="true">…</span>
    ) : state === "error" ? (
      <span className="text-[var(--color-error)]">{t("loadFailed")}</span>
    ) : (
      (value ?? t("unknown"))
    );

  // T3 P1 gap: backend returns Korean names as empty strings until a
  // code → name lookup table lands (shared_kernel admin_division has codes only).
  // Render names if present, else fall back to code-based heading.
  const names = [data.sido_name, data.sigungu_name, data.eupmyeondong_name]
    .filter((s) => s.trim() !== "")
    .join(" ");
  const heading = names || t("codeFallback", { code: data.eupmyeondong_code });

  return (
    <div className="flex flex-col gap-4 p-6">
      <header>
        <div className="font-mono text-[length:var(--text-caption)] text-[var(--color-muted)]">
          PNU {entry.id}
        </div>
        <h2 className="text-[length:var(--text-title-lg)] font-semibold text-[var(--color-ink)]">
          {heading}
        </h2>
      </header>
      <dl className="grid grid-cols-2 gap-y-2 text-[length:var(--text-body-sm)]">
        <dt className="text-[var(--color-muted)]">{t("landUse")}</dt>
        {/* 원천이 말하지 않은 값은 "정보 없음" — 지어내지 않는다 (root ADR-0070/0078). */}
        <dd className="text-[var(--color-ink)]">
          {row(states.characteristics, data.land_use_type || null)}
        </dd>
        <dt className="text-[var(--color-muted)]">{t("zoning")}</dt>
        {/* Wire values are the shared-kernel Zoning enum (closed set of five). */}
        <dd className="text-[var(--color-ink)]">
          {row(states.zonings, data.zoning ? t(`zoningValues.${data.zoning}`) : null)}
        </dd>
        <dt className="text-[var(--color-muted)]">{t("officialPrice")}</dt>
        <dd className="text-[var(--color-ink)]">
          {row(
            states.price,
            data.official_land_price_per_m2 != null
              ? t("officialPricePerM2", {
                  value: data.official_land_price_per_m2.toLocaleString("ko-KR"),
                })
              : null,
          )}
        </dd>
      </dl>
      <nav className="mt-4 flex flex-col gap-2">
        <button
          type="button"
          onClick={() => push({ kind: "parcel", id: entry.id, view: "buildings" })}
          className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-left hover:bg-[var(--color-surface-cream-strong)]"
        >
          {t("viewBuildings")} ›
        </button>
        <button
          type="button"
          onClick={() => push({ kind: "parcel", id: entry.id, view: "floors" })}
          className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-left hover:bg-[var(--color-surface-cream-strong)]"
        >
          {t("viewFloors")} ›
        </button>
        <button
          type="button"
          onClick={() => push({ kind: "parcel", id: entry.id, view: "listings" })}
          className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-left hover:bg-[var(--color-surface-cream-strong)]"
        >
          {t("viewListings")} ›
        </button>
      </nav>
    </div>
  );
}
