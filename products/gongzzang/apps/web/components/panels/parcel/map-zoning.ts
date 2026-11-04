"use client";
import { isEqual } from "es-toolkit";
import { useEffect, useState } from "react";
import { findParcelZoningAreas, type ZoningArea } from "@/lib/map/zoning-areas";
import { useMapStore } from "@/stores/map";

type IdleEvents = {
  on?: (event: string, handler: () => void) => void;
  off?: (event: string, handler: () => void) => void;
};

/**
 * The regulation zones the map draws over this parcel, or `null` while the map carries no `zoning`
 * source. Re-read whenever the map settles (`idle`), because the tiles under a parcel opened from a
 * link arrive after the panel does.
 */
export function useParcelMapZoningAreas(pnu: string): ZoningArea[] | null {
  const mapbox = useMapStore((state) => state.mapbox);
  const [areas, setAreas] = useState<ZoningArea[] | null>(null);
  useEffect(() => {
    if (!mapbox) {
      setAreas(null);
      return;
    }
    const read = () => {
      const next = findParcelZoningAreas(mapbox, pnu);
      setAreas((previous) => (isEqual(previous, next) ? previous : next));
    };
    read();
    const events = mapbox as IdleEvents;
    events.on?.("idle", read);
    return () => events.off?.("idle", read);
  }, [mapbox, pnu]);
  return areas;
}
