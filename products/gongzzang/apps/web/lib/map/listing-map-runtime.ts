import {
  COMPLEX_CLICK_YIELDS_TO_LAYER_IDS,
  foundationVectorFillLayerId,
  registerFoundationVectorFillLayers,
  registerFoundationVectorRuntimeUnit,
} from "@/lib/map/foundation-vector-fill-layers";
import { validateFoundationVectorManifest } from "@/lib/map/foundation-vector-layer-registry";
import {
  refreshFoundationVectorSources,
  startFoundationVectorManifestPolling,
} from "@/lib/map/foundation-vector-source-refresh";
import {
  LISTING_MARKER_RENDER_MAX_ZOOM,
  LISTING_MARKER_RENDER_MIN_ZOOM,
} from "@/lib/map/map-zoom-policy";
import { ALL_ACTIVE_MARKER_FILTER_HASH } from "@/lib/map/marker-tile-contract";
import {
  buildListingMarkerDeltaLayerRegistration,
  buildListingMarkerLayerRegistration,
  buildParcelAnchorMarkerLayerRegistrations,
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
} from "@/lib/map/marker-tile-style";
import {
  buildVectorTileSource,
  fetchVectorTileManifest,
  fetchVectorTileRuntimeManifest,
  getVectorTileArtifact,
  type VectorTileManifest,
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
  setFilter?: (layerId: string, filter: unknown[]) => void;
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

  const [manifest, runtimeManifest] = await Promise.all([
    loadFoundationPlatformVectorTileManifest(),
    loadFoundationPlatformVectorTileRuntimeManifest(),
  ]);
  await setupPolygonLayers(mb, onParcelClick, manifest, runtimeManifest, onComplexClick);
  await setupMarkerTileLayers(mb, onParcelClick, manifest);
  await setupListingMarkerTileLayers(mb, onListingClick);
  if (isCancelled()) return;
  if (runtimeManifest) {
    let activeRuntimeManifest = runtimeManifest;
    const stopRuntimeManifestPolling = startFoundationVectorManifestPolling({
      initialManifest: runtimeManifest,
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
 * Registers every Foundation polygon unit the two manifests describe.
 *
 * Exported because this is where "which polygons does a given pair of manifests put on the map"
 * is decided, and the answers that matter — an unpromoted unit draws nothing and breaks nothing, a
 * manifest carrying an unowned unit draws none of them — are not observable from
 * {@link setupMapboxRuntime}, which needs a live Naver GL bridge before it reaches this line.
 */
export function setupPolygonLayers(
  mb: MapboxGLLike,
  onParcelClick: (pnu: string) => void,
  manifest: VectorTileManifest | null,
  runtimeManifest: VectorTileRuntimeManifest | null,
  onComplexClick: (lakehouseComplexId: string) => void,
): void {
  if (!hasMapboxLayerApi(mb)) {
    console.warn("[ListingMap] mapbox addSource/addLayer unavailable; polygon layer setup skipped");
    return;
  }
  if (!manifest && !runtimeManifest) return;

  const runtime = acceptRuntimeManifest(runtimeManifest);
  // `complex` first, so it is the bottom of this repo's stack even before any `beforeId` applies:
  // an industrial complex is the context a listing sits in, and it must never be what a user's
  // click or eye lands on instead of the parcel or the marker.
  setupComplexLayers(mb, manifest, runtime, onComplexClick);
  setupParcelLayers(mb, onParcelClick, manifest, runtime);
  setupAdminLayers(mb, manifest, runtime);
}

/**
 * Runs the registry gate once for the whole runtime manifest, and drops the manifest entirely if it
 * fails.
 *
 * The gate used to run inside the `parcels` registration, which made it a statement about one layer
 * when it is a statement about the document: a manifest carrying a unit this client does not own
 * disabled `parcels` and then registered `admin` from that same unchecked manifest. Failing here
 * leaves the static v1 paths — a different, separately validated document — to serve what they can.
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
  manifest: VectorTileManifest | null,
  runtimeManifest: VectorTileRuntimeManifest | null,
): void {
  try {
    if (runtimeManifest?.publication_units.parcels) {
      const registered = registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "parcels");
      if (registered.length > 0) registerRuntimeParcelClick(mb, onParcelClick);
      return;
    }
    const artifact = manifest ? getVectorTileArtifact(manifest, "parcels") : undefined;
    if (!manifest || !artifact) {
      logMapLayerUnavailable("parcels");
      return;
    }
    const registered = registerFoundationVectorFillLayers(mb, "parcels", {
      sourceId: "parcels",
      source: buildVectorTileSource(manifest, "parcels", { promoteId: "pnu" }),
      sourceLayer: artifact.source_layer,
      minzoom: artifact.render_min_zoom,
      maxzoom: artifact.render_max_zoom,
    });
    if (registered.length > 0) registerLegacyParcelClick(mb, onParcelClick);
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

function registerLegacyParcelClick(mb: MapboxGLLike, onParcelClick: (pnu: string) => void): void {
  if (typeof mb.on !== "function") return;
  mb.on("click", "parcels-fill", (e: unknown) => {
    const evt = e as {
      features?: Array<{ properties?: { PNU?: string; pnu?: string } }>;
    };
    const props = evt.features?.[0]?.properties;
    const pnu = props?.PNU ?? props?.pnu;
    if (typeof pnu === "string" && pnu.length > 0) onParcelClick(pnu);
  });
}

function setupAdminLayers(
  mb: MapboxLayerApi,
  manifest: VectorTileManifest | null,
  runtimeManifest: VectorTileRuntimeManifest | null,
): void {
  try {
    if (runtimeManifest?.publication_units.admin) {
      registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "admin");
      return;
    }
    const artifact = manifest ? getVectorTileArtifact(manifest, "admin") : undefined;
    if (!manifest || !artifact) {
      logMapLayerUnavailable("admin");
      return;
    }
    registerFoundationVectorFillLayers(mb, "admin", {
      sourceId: "admin",
      source: buildVectorTileSource(manifest, "admin"),
      sourceLayer: artifact.source_layer,
      minzoom: artifact.render_min_zoom,
      maxzoom: artifact.render_max_zoom,
    });
  } catch (err) {
    logMapLayerFailure("admin-fill", err, { kind: "optional", source: "admin" });
  }
}

/**
 * Draws the industrial-complex designation boundaries.
 *
 * The runtime manifest is the only path that carries them today — ADR-0006 leaves the static
 * PMTiles release for later — so the v1 branch below exists for the shape of the contract, not for
 * anything a deployment currently publishes. What matters more is the third case: a Catalog that
 * has not promoted this unit is a *normal* environment, not a broken one, and the map must come up
 * with everything else intact. That case is a silent no-op by construction, which is precisely how
 * "the promotion never ran" and "the layer registration threw" become indistinguishable from the
 * outside, so it says so.
 */
function setupComplexLayers(
  mb: MapboxLayerApi,
  manifest: VectorTileManifest | null,
  runtimeManifest: VectorTileRuntimeManifest | null,
  onComplexClick: (lakehouseComplexId: string) => void,
): void {
  try {
    if (runtimeManifest?.publication_units.complex) {
      const registered = registerFoundationVectorRuntimeUnit(mb, runtimeManifest, "complex");
      if (registered.length > 0) registerComplexClick(mb, onComplexClick);
      return;
    }
    const artifact = manifest ? getVectorTileArtifact(manifest, "complex") : undefined;
    if (!manifest || !artifact) {
      logMapLayerUnavailable("complex");
      return;
    }
    const registered = registerFoundationVectorFillLayers(mb, "complex", {
      sourceId: "complex",
      source: buildVectorTileSource(manifest, "complex"),
      sourceLayer: artifact.source_layer,
      minzoom: artifact.render_min_zoom,
      maxzoom: artifact.render_max_zoom,
    });
    if (registered.length > 0) registerComplexClick(mb, onComplexClick);
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
  mb.on("click", foundationVectorFillLayerId("complex"), (e: unknown) => {
    const evt = e as {
      point?: unknown;
      features?: Array<{ id?: string | number; properties?: { complex_id?: string } }>;
    };
    if (aMoreSpecificLayerOwnsTheClick(mb, evt.point)) return;
    const feature = evt.features?.[0];
    // `properties.complex_id` first and `feature.id` as the fallback: `promoteId` is what puts the
    // value on `feature.id`, and a source registered from the v1 static manifest carries no
    // `promoteId`, so only the property is guaranteed to be there.
    const id =
      feature?.properties?.complex_id ?? (typeof feature?.id === "string" ? feature.id : undefined);
    if (typeof id === "string" && id.length > 0) onComplexClick(id);
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

async function setupMarkerTileLayers(
  mb: MapboxGLLike,
  onParcelClick: (pnu: string) => void,
  manifest: VectorTileManifest | null,
): Promise<void> {
  if (typeof mb.addSource !== "function" || typeof mb.addLayer !== "function") {
    console.warn("[ListingMap] mapbox addSource/addLayer unavailable; marker tile setup skipped");
    return;
  }
  if (!manifest) return;

  try {
    const registrations = buildParcelAnchorMarkerLayerRegistrations({ manifest });

    for (const registration of registrations) {
      if (!mb.getSource?.(registration.sourceId)) {
        mb.addSource(registration.sourceId, registration.source);
      }
      for (const layer of registration.layers) {
        if (!mb.getLayer?.(layer.id)) {
          mb.addLayer(layer);
        }
      }
    }
    if (typeof mb.on === "function") {
      mb.on("click", PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID, (e: unknown) => {
        const evt = e as {
          features?: Array<{ properties?: { pnu?: string; detail_ref?: string } }>;
        };
        const props = evt.features?.[0]?.properties;
        const pnu = props?.pnu ?? props?.detail_ref;
        if (typeof pnu === "string" && pnu.length > 0) {
          onParcelClick(pnu);
        }
      });
    }
  } catch (err) {
    logMapLayerFailure("parcel-anchor-markers", err, {
      kind: "core",
      source: "foundation-platform-vector-tile-manifest",
    });
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
    const registration = buildListingMarkerLayerRegistration({
      filterHash: ALL_ACTIVE_MARKER_FILTER_HASH,
      minzoom: LISTING_MARKER_RENDER_MIN_ZOOM,
      maxzoom: LISTING_MARKER_RENDER_MAX_ZOOM,
    });
    const deltaRegistration = buildListingMarkerDeltaLayerRegistration({
      baseVersion: null,
      minzoom: 0,
      maxzoom: LISTING_MARKER_RENDER_MAX_ZOOM,
    });

    for (const markerRegistration of [registration, deltaRegistration]) {
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
          features?: Array<{ properties?: { id?: string; detail_ref?: string } }>;
        };
        const props = evt.features?.[0]?.properties;
        const listingId = props?.detail_ref ?? props?.id;
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

async function loadFoundationPlatformVectorTileManifest(): Promise<VectorTileManifest | null> {
  return fetchVectorTileManifest().catch((err: unknown) => {
    logMapLayerFailure("vector-tile-manifest", err, {
      kind: "core",
      source: "foundation-platform",
    });
    return null;
  });
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
