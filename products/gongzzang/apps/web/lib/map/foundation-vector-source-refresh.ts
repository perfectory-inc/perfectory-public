import {
  buildFoundationVectorSource,
  FOUNDATION_VECTOR_LAYER_REGISTRY,
} from "@/lib/map/foundation-vector-layer-registry";
import type { VectorTileRuntimeManifest } from "@/lib/map/vector-tile-manifest";

type MutableVectorSource = { setTiles?: (tiles: string[]) => void };
type RefreshMapboxBridge = {
  getSource?: (id: string) => MutableVectorSource | undefined;
  addSource?: (id: string, source: Record<string, unknown>) => void;
};

/**
 * Retargets only units whose serving generation changed. The source id remains stable for dynamic
 * PostGIS; static sources are immutable URLs. No overlay or tombstone source is ever registered.
 */
export function refreshFoundationVectorSources(
  mapbox: RefreshMapboxBridge,
  previous: VectorTileRuntimeManifest,
  next: VectorTileRuntimeManifest,
): string[] {
  const changed: string[] = [];
  for (const [unitName, definition] of Object.entries(FOUNDATION_VECTOR_LAYER_REGISTRY)) {
    const before = previous.publication_units[unitName];
    const after = next.publication_units[unitName];
    if (!after || !before || after.serving_generation === before.serving_generation) continue;
    const source = mapbox.getSource?.(definition.sourceId);
    const nextSource = buildFoundationVectorSource(next, unitName, definition.sourceId);
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

/** Creates an abortable four-second poll loop with one in-flight request and bounded backoff. */
export function startFoundationVectorManifestPolling(options: {
  fetchManifest: (
    signal: AbortSignal,
    previous?: VectorTileRuntimeManifest,
    etag?: string,
  ) => Promise<{ manifest: VectorTileRuntimeManifest; etag?: string }>;
  onManifest: (manifest: VectorTileRuntimeManifest, etag?: string) => void;
  onError?: (error: unknown) => void;
  visible?: () => boolean;
  intervalMs?: number;
  random?: () => number;
}): () => void {
  const controller = new AbortController();
  let previous: VectorTileRuntimeManifest | undefined;
  let etag: string | undefined;
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
    if (stopped || inFlight || !visible()) return;
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
  void run();
  return () => {
    stopped = true;
    controller.abort();
    if (timer) clearTimeout(timer);
  };
}
