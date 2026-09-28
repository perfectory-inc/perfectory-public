import {
  buildFoundationVectorSource,
  FOUNDATION_VECTOR_LAYER_REGISTRY,
  validateFoundationVectorManifest,
} from "@/lib/map/foundation-vector-layer-registry";
import type { VectorTileRuntimeManifest } from "@/lib/map/vector-tile-manifest";

type MutableVectorSource = { setTiles?: (tiles: string[]) => void };
type RefreshMapboxBridge = {
  getSource?: (id: string) => MutableVectorSource | undefined;
  addSource?: (id: string, source: Record<string, unknown>) => void;
};

/**
 * Retargets only units whose serving generation changed. The source id remains stable for dynamic
 * PostGIS; static sources are immutable URLs. Unfolded admin edits are not a tile source: they
 * arrive through the separate map edit overlay (root ADR-0112, `map-edit-overlay.ts`).
 */
export function refreshFoundationVectorSources(
  mapbox: RefreshMapboxBridge,
  previous: VectorTileRuntimeManifest,
  next: VectorTileRuntimeManifest,
): string[] {
  validateFoundationVectorManifest(next);
  const changed: string[] = [];
  for (const [unitName, definition] of Object.entries(FOUNDATION_VECTOR_LAYER_REGISTRY)) {
    const before = previous.publication_units[unitName];
    const after = next.publication_units[unitName];
    if (!after || !before || after.serving_generation === before.serving_generation) continue;
    const source = mapbox.getSource?.(definition.sourceId);
    const nextSource = buildFoundationVectorSource(next, unitName, definition.layerName);
    if (source?.setTiles) {
      source.setTiles(nextSource.tiles);
    } else if (mapbox.addSource && !source) {
      mapbox.addSource(definition.sourceId, nextSource);
    } else {
      throw new Error(`Mapbox source cannot be retargeted: ${definition.sourceId}`);
    }
    changed.push(unitName);
  }
  return changed;
}

/**
 * Creates an abortable four-second poll loop with one in-flight request and bounded backoff.
 *
 * Generic over what is polled so the map edit overlay reuses the same loop instead of a second copy.
 */
export function startFoundationVectorManifestPolling<T = VectorTileRuntimeManifest>(options: {
  fetchManifest: (
    signal: AbortSignal,
    previous?: T,
    etag?: string,
  ) => Promise<{ manifest: T; etag?: string }>;
  onManifest: (manifest: T, etag?: string) => void;
  onError?: (error: unknown) => void;
  visible?: () => boolean;
  intervalMs?: number;
  random?: () => number;
  initialManifest?: T;
  initialEtag?: string;
  startImmediately?: boolean;
}): () => void {
  const controller = new AbortController();
  let previous: T | undefined = options.initialManifest;
  let etag: string | undefined = options.initialEtag;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight = false;
  let stopped = false;
  let failures = 0;
  const base = options.intervalMs ?? 4_000;
  const visible = options.visible ?? (() => true);
  const random = options.random ?? Math.random;

  const schedule = (delay: number) => {
    if (!stopped) timer = setTimeout(run, delay);
  };
  const run = async () => {
    if (stopped || inFlight) return;
    if (!visible()) {
      schedule(base);
      return;
    }
    inFlight = true;
    try {
      const result = await options.fetchManifest(controller.signal, previous, etag);
      previous = result.manifest;
      etag = result.etag ?? etag;
      failures = 0;
      options.onManifest(result.manifest, result.etag);
      schedule(base);
    } catch (error) {
      if (!stopped && !controller.signal.aborted) {
        failures = Math.min(failures + 1, 5);
        options.onError?.(error);
        schedule(Math.min(base * 2 ** failures, 60_000) + Math.floor(random() * 250));
      }
    } finally {
      inFlight = false;
    }
  };
  if (options.startImmediately === false) schedule(base);
  else void run();
  return () => {
    stopped = true;
    controller.abort();
    if (timer) clearTimeout(timer);
  };
}
