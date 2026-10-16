import { env } from "@/lib/env";
import {
  resolveVectorTileAllowedOrigins,
  resolveVectorTileRuntimeManifestUrl,
} from "@/lib/map/vector-tile-manifest";

/**
 * The cross-origin hosts the browser reads map and panel data from directly. proxy.ts's CSP
 * `connect-src` and the root layout's preconnect hints both read this one list, so a hint can
 * never name an origin the page is not allowed to fetch.
 */
export function browserDataOrigins() {
  return {
    // ADR 0036 / foundation-platform ADR 0004: the manifest's host; tile URLs follow the manifest.
    tiles: resolveVectorTileAllowedOrigins(),
    // The admin edit overlay (root ADR-0112) is read alongside the tiles.
    mapEdits: new URL(env.NEXT_PUBLIC_MAP_EDIT_OVERLAY_BASE_URL).origin,
    // The panels' by-PNU edges, each its own lane (root ADR-0190).
    parcels: new URL(env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL).origin,
    buildings: new URL(env.NEXT_PUBLIC_BUILDING_EDGE_BASE_URL).origin,
    unitPrices: new URL(env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL).origin,
  };
}

/** Every data origin, for `connect-src`. */
export function connectSrcDataOrigins(): string[] {
  const o = browserDataOrigins();
  return [...o.tiles, o.mapEdits, o.parcels, o.buildings, o.unitPrices];
}

/**
 * The few origins worth a connection before the first request (web.dev: preconnect only the 2-4
 * critical ones): the runtime manifest the map reads first, and the panel edges a click reads.
 * map-edits is left out: it is read in the same tick as the manifest, so a hint gains nothing.
 */
export function preconnectDataOrigins(): string[] {
  const o = browserDataOrigins();
  const manifest = resolveVectorTileRuntimeManifestUrl();
  const first = manifest ? [new URL(manifest).origin] : [];
  return [...new Set([...first, o.parcels, o.buildings, o.unitPrices])];
}
