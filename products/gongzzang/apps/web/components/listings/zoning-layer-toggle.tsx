"use client";
import { useTranslations } from "next-intl";
import { useEffect, useState } from "react";
import { foundationVectorUnitLayerIds } from "@/lib/map/foundation-vector-fill-layers";
import type { MapboxGLLike } from "@/lib/map/listing-map-runtime";
import {
  ZONING_FAMILIES,
  type ZoningFamilyKey,
  zoningFamilyPending,
  zoningLayerFilter,
} from "@/lib/map/zoning-families";

/**
 * One button per regulation family (root ADR-0373), all off at first. Pressing one widens the
 * zoning layers' filter; the layers stay visible so their tiles keep loading for the parcel panel.
 * Rendered only when the map carries the `zoning` layers: an environment that has not promoted
 * the unit shows no control at all.
 */
export function ZoningLayerToggle({ mapbox }: { mapbox: MapboxGLLike }) {
  const t = useTranslations("map.zoning");
  const [enabled, setEnabled] = useState<ReadonlySet<ZoningFamilyKey>>(() => new Set());

  useEffect(() => {
    const filter = zoningLayerFilter(enabled);
    for (const layerId of foundationVectorUnitLayerIds(mapbox, "zoning")) {
      mapbox.setFilter?.(layerId, filter);
    }
  }, [mapbox, enabled]);

  const toggle = (key: ZoningFamilyKey) =>
    setEnabled((previous) => {
      const next = new Set(previous);
      if (!next.delete(key)) next.add(key);
      return next;
    });

  return (
    <fieldset className="pointer-events-auto absolute top-2 left-2 z-10 flex max-w-[16rem] flex-wrap gap-1 rounded bg-white/85 p-1.5 text-[11px] leading-tight shadow-sm backdrop-blur-sm dark:bg-black/70">
      <legend className="sr-only">{t("legend")}</legend>
      {ZONING_FAMILIES.map(({ key }) => {
        const pending = zoningFamilyPending(key);
        const pressed = enabled.has(key);
        return (
          <button
            key={key}
            type="button"
            aria-pressed={pressed}
            disabled={pending === "all"}
            onClick={() => toggle(key)}
            className={`rounded border px-2 py-1 text-left disabled:opacity-60 ${
              pressed
                ? "border-[var(--color-ink)] bg-[var(--color-ink)] text-[var(--color-canvas)]"
                : "border-[var(--color-hairline)] text-[var(--color-ink)]"
            }`}
          >
            {t(`families.${key}`)}
            {pending !== "none" && (
              <span className="block text-[10px] text-[var(--color-muted)]">{t("pending")}</span>
            )}
          </button>
        );
      })}
    </fieldset>
  );
}
