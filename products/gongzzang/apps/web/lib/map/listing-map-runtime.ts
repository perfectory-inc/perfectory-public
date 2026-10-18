import { env } from "@/lib/env";
import {
  COMPLEX_CLICK_YIELDS_TO_LAYER_IDS,
  foundationVectorFillLayerId,
  registerFoundationVectorRuntimeUnit,
} from "@/lib/map/foundation-vector-fill-layers";
import { validateFoundationVectorManifest } from "@/lib/map/foundation-vector-layer-registry";
import {
  refreshFoundationVectorSources,
  startFoundationVectorManifestPolling,
} from "@/lib/map/foundation-vector-source-refresh";
import {
  applyMapEditOverlay,
  fetchMapEditOverlay,
  type MapEditOverlay,
  mapEditOverlayFillLayerId,
  registerMapEditOverlayLayers,
} from "@/lib/map/map-edit-overlay";
import {
  ALL_ACTIVE_MARKER_FILTER_HASH,
  LISTING_MARKER_ID_PROPERTY,
} from "@/lib/map/marker-tile-contract";
import {
  buildListingMarkerSourceRegistrations,
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
} from "@/lib/map/marker-tile-style";
import {
  fetchVectorTileRuntimeManifest,
  type VectorTileRuntimeManifest,
} from "@/lib/map/vector-tile-manifest";

export type MapboxGLLike = {
  addSource?: (id: string, src: Record<string, unknown>) => void;
  addLayer?: (layer: Record<string, unknown>, beforeId?: string) => void;
  getSource?: (id: string) => unknown;
  getLayer?: (id: string) => unknown;
  getCanvas?: () => HTMLCanvasElement | null;
  on?: (event: string, layer: string, handler: (e: unknown) => void) => void;
  isStyleLoaded?: () => boolean;
  once?: (event: string, handler: () => void) => void;
  setFilter?: (layerId: string, filter: unknown[] | null) => void;
  queryRenderedFeatures?: (
    point: unknown,
    options?: { layers?: readonly string[] },
  ) => readonly unknown[];
};

type MapboxLayerApi = MapboxGLLike & {
  addSource: NonNullable<MapboxGLLike["addSource"]>;
  addLayer: NonNullable<MapboxGLLike["addLayer"]>;
};

const MAPBOX_POLL_INTERVAL_MS = 100;
const MAPBOX_POLL_TIMEOUT_MS = 6_000;
const MAPBOX_MAX_ATTEMPTS = MAPBOX_POLL_TIMEOUT_MS / MAPBOX_POLL_INTERVAL_MS;

export async function setupMapboxRuntime(
  map: naver.maps.Map,
  cleanups: Array<() => void>,
  isCancelled: () => boolean,
  onParcelClick: (pnu: string) => void,
  onListingClick: (listingId: string) => void,
  onMapboxReady: (mb: MapboxGLLike) => void,
  onComplexClick: (lakehouseComplexId: string) => void,
): Promise<void> {
  const mb = await waitForMapbox(map);
  if (isCancelled()) return;
  if (process.env.NODE_ENV !== "production") {
    (window as unknown as Record<string, unknown>).__listingMb = mb;
  }

  await waitForMapboxStyle(mb, isCancelled);
  if (isCancelled()) return;

  // The runtime manifest (v2) is the one answer to what the map draws (root ADR-0225).
  const runtimeManifest = await loadFoundationPlatformVectorTileRuntimeManifest();
  setupPolygonLayers(mb, onParcelClick, runtimeManifest, onComplexClick);
  await setupListingMarkerTileLayers(mb, onListingClick);
  if (isCancelled()) return;
  for (const unit of ["complex", "admin"]) {
    const stopMapEditOverlay = startMapEditOverlay(mb, unit);
    if (stopMapEditOverlay) cleanups.push(stopMapEditOverlay);
  }
  if (runtimeManifest) {
    let activeRuntimeManifest = runtimeManifest;
    const stopRuntimeManifestPolling = startFoundationVectorManifestPolling({
      initialManifest: runtimeManifest,
      intervalMs: (latest) => latest.refresh_after_seconds * 1_000,
      startImmediately: false,
      fetchManifest: async (signal, previous, etag) => {
        const result = await fetchVectorTileRuntimeManifest(fetch, undefined, {
          signal,
          previous,
          etag,
        });
        return { manifest: result.manifest, etag: result.etag };
      },
      onManifest: (next) => {
        try {
          refreshFoundationVectorSources(
            mb as unknown as Parameters<typeof refreshFoundationVectorSources>[0],
            activeRuntimeManifest,
            next,
          );
          activeRuntimeManifest = next;
        } catch (error) {
          logMapLayerFailure("vector-tile-runtime-manifest-refresh", error, {
            kind: "core",
            source: "foundation-platform",
          });
        }
      },
      onError: (error) =>
        logMapLayerFailure("vector-tile-runtime-manifest-poll", error, {
          kind: "core",
          source: "foundation-platform",
        }),
      visible: () => typeof document === "undefined" || document.visibilityState === "visible",
    });
    cleanups.push(stopRuntimeManifestPolling);
  }
  onMapboxReady(mb);
  const recovery = setupWebGlRecovery(mb);
  if (recovery) cleanups.push(recovery);
}

async function waitForMapbox(naverMap: naver.maps.Map): Promise<MapboxGLLike> {
  for (let i = 0; i < MAPBOX_MAX_ATTEMPTS; i++) {
    const mb = (naverMap as unknown as { _mapbox?: MapboxGLLike })._mapbox;
    if (mb && typeof mb.addSource === "function") return mb;
    await new Promise((r) => setTimeout(r, MAPBOX_POLL_INTERVAL_MS));
  }
  throw new Error(`mapbox-gl instance polling timeout (${MAPBOX_POLL_TIMEOUT_MS / 1000}s)`);
}

async function waitForMapboxStyle(mb: MapboxGLLike, isCancelled: () => boolean): Promise<void> {
  for (let i = 0; i < MAPBOX_MAX_ATTEMPTS && !mb.isStyleLoaded?.(); i++) {
    await new Promise((r) => setTimeout(r, MAPBOX_POLL_INTERVAL_MS));
    if (isCancelled()) return;
  }
}

/**
 * Registers every Foundation polygon unit the runtime manifest describes.
 *
 * Exported because this is where "which polygons does a manifest put on the map" is decided, and
 * the answers that matter — an unpromoted unit draws nothing and breaks nothing, a unit this client
 * does not own is skipped while the others draw, a drifted owned unit draws none of them — are not
 * observable from {@link setupMapboxRuntime}, which needs a live Naver GL bridge first.
 */
export function setupPolygonLayers(
  mb: MapboxGLLike,
  onParcelClick: (pnu: string) => void,
  runtimeManifest: VectorTileRuntimeManifest | null,
  onComplexClick: (lakehouseComplexId: string) => void,
): void {
  if (!hasMapboxLayerApi(mb)) {
    console.warn("[ListingMap] mapbox addSource/addLayer unavailable; polygon layer setup skipped");
    return;
  }
  const runtime = acceptRuntimeManifest(runtimeManifest);
  if (!runtime) return;
  // `complex` first, so it is the bottom of this repo's stack even before any `beforeId` applies:
  // an industrial complex is the context a listing sits in, and it must never be what a user's
  // click or eye lands on instead of the parcel or the marker.
  setupComplexLayers(mb, runtime, onComplexClick);
  setupParcelLayers(mb, onParcelClick, runtime);
  setupAdminLayers(mb, runtime);
}

/**
 * Runs the registry gate once for the whole runtime manifest, and drops the manifest entirely if an
 * owned unit drifted: the gate is a statement about the document, not about one layer.
 */
function acceptRuntimeManifest(
  runtimeManifest: VectorTileRuntimeManifest | null,
): VectorTileRuntimeManifest | null {
  if (!runtimeManifest) return null;
  try {
    validateFoundationVectorManifest(runtimeManifest);
    return runtimeManifest;
  } catch (err) {
    logMapLayerFailure("vector-tile-runtime-manifest", err, {
      kind: "core",
      source: "foundation-platform",
    });
    return null;
  }
}

function hasMapboxLayerApi(mb: MapboxGLLike): mb is MapboxLayerApi {
  return typeof mb.addSource === "function" && typeof mb.addLayer === "function";
}

function setupParcelLayers(
  mb: MapboxLayerApi,
  onParcelClick: (pnu: string) => void,
  runtimeManifest: VectorTileRuntimeManifest,
): void {
  try {
    if (!runtimeManifest.publication_units.parcels) {
      logMapLayerUnavailable("parcels");
      return;
    }
    const registered = registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "parcels");
    if (registered.length > 0) registerRuntimeParcelClick(mb, onParcelClick);
  } catch (err) {
    logMapLayerFailure("parcels-fill", err, { kind: "core", source: "parcels" });
  }
}

function registerRuntimeParcelClick(mb: MapboxGLLike, onParcelClick: (pnu: string) => void): void {
  if (typeof mb.on !== "function") return;
  mb.on("click", "parcels-fill", (e: unknown) => {
    const evt = e as { features?: Array<{ properties?: { pnu?: string } }> };
    const pnu = evt.features?.[0]?.properties?.pnu;
    if (typeof pnu === "string" && pnu.length > 0) onParcelClick(pnu);
  });
}

function setupAdminLayers(mb: MapboxLayerApi, runtimeManifest: VectorTileRuntimeManifest): void {
  try {
    if (!runtimeManifest.publication_units.admin) {
      logMapLayerUnavailable("admin");
      return;
    }
    const registered = registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "admin");
    if (registered.length > 0) registerMapEditOverlayLayers(mb, "admin");
  } catch (err) {
    logMapLayerFailure("admin-fill", err, { kind: "optional", source: "admin" });
  }
}

/**
 * Draws the industrial-complex designation boundaries.
 *
 * A Catalog that has not promoted this unit is a *normal* environment, not a broken one, and the
 * map must come up with everything else intact. That case is a silent no-op by construction, which
 * is precisely how "the promotion never ran" and "the layer registration threw" become
 * indistinguishable from the outside, so it says so.
 */
function setupComplexLayers(
  mb: MapboxLayerApi,
  runtimeManifest: VectorTileRuntimeManifest,
  onComplexClick: (lakehouseComplexId: string) => void,
): void {
  try {
    if (!runtimeManifest.publication_units.complex) {
      logMapLayerUnavailable("complex");
      return;
    }
    const registered = registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "complex");
    if (registered.length > 0) {
      registerMapEditOverlayLayers(mb, "complex");
      registerComplexClick(mb, onComplexClick);
    }
  } catch (err) {
    logMapLayerFailure("complex-fill", err, { kind: "optional", source: "complex" });
  }
}

/**
 * Opens the complex panel for a click on the boundary fill, unless something more specific was hit.
 *
 * Exported because the precedence rule — not the registration — is the part worth testing, and a
 * live Naver GL bridge is not available to a unit test.
 */
export function registerComplexClick(
  mb: MapboxGLLike,
  onComplexClick: (lakehouseComplexId: string) => void,
): void {
  if (typeof mb.on !== "function") return;
  const handler = (e: unknown) => {
    const evt = e as {
      point?: unknown;
      features?: Array<{ id?: string | number; properties?: { complex_id?: string } }>;
    };
    if (aMoreSpecificLayerOwnsTheClick(mb, evt.point)) return;
    const feature = evt.features?.[0];
    // `properties.complex_id` first and `feature.id` as the fallback: `promoteId` is what puts the
    // value on `feature.id`, and a source registered without one carries no
    // `promoteId`, so only the property is guaranteed to be there.
    const id =
      feature?.properties?.complex_id ?? (typeof feature?.id === "string" ? feature.id : undefined);
    if (typeof id === "string" && id.length > 0) onComplexClick(id);
  };
  mb.on("click", foundationVectorFillLayerId("complex"), handler);
  // An edited complex is drawn from the edit overlay while its base polygon is hidden, so the same
  // click must work there too (root ADR-0112).
  if (mb.getLayer?.(mapEditOverlayFillLayerId("complex"))) {
    mb.on("click", mapEditOverlayFillLayerId("complex"), handler);
  }
}

/** How often customers' maps look for new admin edits (root ADR-0112 section 5, initial value). */
export const MAP_EDIT_OVERLAY_POLL_INTERVAL_MS = 30_000;

/**
 * Keeps the unit's edit overlay current. Returns the stop function, or nothing when the unit's
 * edit layers were never added (the base unit is not on the map).
 *
 * A failed read keeps the overlay last drawn: those edits are still pending and still true. Before
 * the first successful read the map shows the base tiles alone (root ADR-0112 section 6).
 */
export function startMapEditOverlay(
  mb: MapboxGLLike,
  unit: string,
  baseUrl: string = env.NEXT_PUBLIC_MAP_EDIT_OVERLAY_BASE_URL,
  fetcher: typeof fetch = fetch,
): (() => void) | undefined {
  if (!mb.getLayer?.(mapEditOverlayFillLayerId(unit))) return undefined;
  return startFoundationVectorManifestPolling<MapEditOverlay>({
    intervalMs: MAP_EDIT_OVERLAY_POLL_INTERVAL_MS,
    fetchManifest: (signal, previous, etag) =>
      fetchMapEditOverlay(fetcher, baseUrl, unit, {
        signal,
        ...(previous ? { previous } : {}),
        ...(etag ? { etag } : {}),
      }),
    onManifest: (overlay) => {
      try {
        applyMapEditOverlay(mb, overlay);
      } catch (error) {
        logMapLayerFailure("map-edit-overlay-apply", error, { kind: "optional", source: unit });
      }
    },
    onError: (error) =>
      logMapLayerFailure("map-edit-overlay-poll", error, { kind: "optional", source: unit }),
    visible: () => typeof document === "undefined" || document.visibilityState === "visible",
  });
}

function aMoreSpecificLayerOwnsTheClick(mb: MapboxGLLike, point: unknown): boolean {
  // No query API means no way to tell. Opening the panel is the failure that leaves the feature
  // working; staying silent is the one that looks like the layer is not clickable at all.
  if (typeof mb.queryRenderedFeatures !== "function" || point === undefined) return false;
  const present = COMPLEX_CLICK_YIELDS_TO_LAYER_IDS.filter((layerId) => mb.getLayer?.(layerId));
  if (present.length === 0) return false;
  try {
    return mb.queryRenderedFeatures(point, { layers: present }).length > 0;
  } catch (err) {
    logMapLayerFailure("complex-click-precedence", err, { kind: "optional", source: "complex" });
    return false;
  }
}

async function setupListingMarkerTileLayers(
  mb: MapboxGLLike,
  onListingClick: (listingId: string) => void,
): Promise<void> {
  if (typeof mb.addSource !== "function" || typeof mb.addLayer !== "function") {
    console.warn(
      "[ListingMap] mapbox addSource/addLayer unavailable; listing marker setup skipped",
    );
    return;
  }

  try {
    // The server state arrives later and is applied by retargetListingMarkerSources.
    const registrations = buildListingMarkerSourceRegistrations({
      filterHash: ALL_ACTIVE_MARKER_FILTER_HASH,
      projectionVersion: undefined,
    });

    for (const markerRegistration of registrations) {
      if (!mb.getSource?.(markerRegistration.sourceId)) {
        mb.addSource(markerRegistration.sourceId, markerRegistration.source);
      }
      for (const layer of markerRegistration.layers) {
        if (!mb.getLayer?.(layer.id)) {
          mb.addLayer(layer);
        }
      }
    }
    if (typeof mb.on === "function") {
      const onListingMarkerClick = (e: unknown) => {
        const evt = e as {
          features?: Array<{ properties?: Record<string, unknown> }>;
        };
        const props = evt.features?.[0]?.properties;
        const listingId = props?.detail_ref ?? props?.[LISTING_MARKER_ID_PROPERTY];
        if (typeof listingId === "string" && listingId.length > 0) {
          onListingClick(listingId);
        }
      };
      mb.on("click", LISTING_MARKER_TILE_CIRCLE_LAYER_ID, onListingMarkerClick);
      mb.on("click", LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID, onListingMarkerClick);
    }
  } catch (err) {
    logMapLayerFailure("listing-markers", err, {
      kind: "core",
      source: "listing",
    });
  }
}

function setupWebGlRecovery(mb: MapboxGLLike): (() => void) | undefined {
  if (typeof mb.getCanvas !== "function") return undefined;
  const glCanvas = mb.getCanvas();
  if (!glCanvas) return undefined;
  const onLost = (e: Event) => {
    e.preventDefault();
    console.warn("[ListingMap] WebGL context lost");
  };
  const onRestored = () => {
    try {
      const mbExt = mb as MapboxGLLike & { resize?: () => void; triggerRepaint?: () => void };
      mbExt.resize?.();
      mbExt.triggerRepaint?.();
    } catch {
      /* ignore */
    }
  };
  glCanvas.addEventListener("webglcontextlost", onLost);
  glCanvas.addEventListener("webglcontextrestored", onRestored);
  return () => {
    glCanvas.removeEventListener("webglcontextlost", onLost);
    glCanvas.removeEventListener("webglcontextrestored", onRestored);
  };
}

async function loadFoundationPlatformVectorTileRuntimeManifest(): Promise<VectorTileRuntimeManifest | null> {
  return fetchVectorTileRuntimeManifest()
    .then((result) => result.manifest)
    .catch((err: unknown) => {
      logMapLayerFailure("vector-tile-runtime-manifest", err, {
        kind: "core",
        source: "foundation-platform",
      });
      return null;
    });
}

/**
 * Says that a publication unit had no source to register at all.
 *
 * Not a failure: an environment that has not promoted a unit is the state every environment starts
 * in. It is logged so that "nothing is published yet" reads differently from "registration threw"
 * and from "the tiles are empty", which otherwise all look the same — a map with no polygons on it.
 */
function logMapLayerUnavailable(unitName: string): void {
  console.info(
    "[ListingMap]",
    JSON.stringify({
      event: "map_layer_unavailable",
      unit: unitName,
      reason: "no_publication_unit_or_artifact",
    }),
  );
}

function logMapLayerFailure(
  layerId: string,
  err: unknown,
  ctx: { kind: "core" | "optional"; source: string },
): void {
  const message = err instanceof Error ? err.message : String(err);
  const payload = { event: "map_layer_register_failed", layerId, ...ctx, message };
  if (ctx.kind === "core") {
    console.error("[ListingMap]", JSON.stringify(payload));
  } else {
    console.info("[ListingMap]", JSON.stringify(payload));
  }
}
