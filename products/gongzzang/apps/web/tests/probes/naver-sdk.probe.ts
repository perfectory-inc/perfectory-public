import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { expect, type Page, type TestInfo, test } from "@playwright/test";
import { plantAuthenticatedSession } from "../e2e/auth";

const OUT_DIR = "var/sample";
const MAP_BOOT_TIMEOUT_MS = 15_000;
const SOFTWARE_RENDERER_PATTERN =
  /SwiftShader|llvmpipe|softpipe|lavapipe|\bWARP\b|Microsoft Basic Render Driver|\bsoftware\b/i;

interface ProbeMapbox {
  addLayer?: (layer: Record<string, unknown>) => void;
  addSource?: (id: string, source: Record<string, unknown>) => void;
  getLayer?: (id: string) => unknown;
  getSource?: (id: string) => unknown;
  isSourceLoaded?: (id: string) => boolean;
  isStyleLoaded?: () => boolean;
  removeLayer?: (id: string) => void;
  removeSource?: (id: string) => void;
  triggerRepaint?: () => void;
}

type ProbeVectorSource = { setTiles?: (tiles: string[]) => void };
type ProbeWindow = Window & { __listingMap?: { getMapbox?: () => ProbeMapbox } };

const hardwareGlTest = test.extend({});
hardwareGlTest.use({
  launchOptions: { args: ["--enable-gpu", "--disable-software-rasterizer"] },
});

interface ViewportSpec {
  name: string;
  lat: number;
  lng: number;
  zoom: number;
}

const VIEWPORTS: ViewportSpec[] = [
  { name: "synthetic-viewport-a", lat: 36.123449, lng: 127.12347023462, zoom: 17 },
  { name: "synthetic-viewport-b", lat: 36.12344, lng: 127.1234702343, zoom: 17 },
  { name: "synthetic-viewport-c", lat: 36.123454, lng: 127.12347023445, zoom: 16 },
];

async function writeProbeJson(
  testInfo: TestInfo,
  filename: string,
  data: unknown,
): Promise<string> {
  const path = join(OUT_DIR, filename);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(data, null, 2));
  await testInfo.attach(filename, { path, contentType: "application/json" });
  return path;
}

async function openAuthenticatedListings(page: Page) {
  await page.goto("/listings", { waitUntil: "load", timeout: 60_000 });
  await page.waitForTimeout(MAP_BOOT_TIMEOUT_MS);
}

async function setNaverGlViewport(page: Page, viewport: ViewportSpec): Promise<void> {
  await page.evaluate((nextViewport) => {
    const root = window as unknown as {
      __listingMap?: {
        setCenterGL?: (latLng: unknown) => void;
        setZoomGL?: (zoom: number) => void;
      };
      naver?: { maps?: { LatLng?: new (lat: number, lng: number) => unknown } };
    };
    const LatLng = root.naver?.maps?.LatLng;
    if (!root.__listingMap || !LatLng) return;
    root.__listingMap.setCenterGL?.(new LatLng(nextViewport.lat, nextViewport.lng));
    root.__listingMap.setZoomGL?.(nextViewport.zoom);
  }, viewport);
  await page.waitForTimeout(5_000);
}

async function captureStyleCatalog(page: Page): Promise<unknown> {
  return page.evaluate(() => {
    type Dict = Record<string, unknown>;

    function asRecord(value: unknown): Dict {
      return typeof value === "object" && value !== null ? (value as Dict) : {};
    }

    function asArray(value: unknown): unknown[] {
      return Array.isArray(value) ? value : [];
    }

    function stringValue(value: unknown): string | undefined {
      return typeof value === "string" ? value : undefined;
    }

    function keysOf(value: unknown): string[] {
      return Object.keys(asRecord(value));
    }

    function getMapbox() {
      const root = window as unknown as { __listingMap?: { getMapbox?: () => unknown } };
      return asRecord(root.__listingMap?.getMapbox?.());
    }

    const style = asRecord((getMapbox().getStyle as (() => unknown) | undefined)?.());
    const layers = asArray(style.layers).map((layer) => {
      const item = asRecord(layer);
      return {
        id: stringValue(item.id),
        type: stringValue(item.type),
        source: stringValue(item.source),
        sourceLayer: stringValue(item["source-layer"]),
        minzoom: item.minzoom,
        maxzoom: item.maxzoom,
        filter: item.filter,
        paintKeys: keysOf(item.paint),
        layoutKeys: keysOf(item.layout),
      };
    });

    const layerCountByType = layers.reduce<Record<string, number>>((acc, layer) => {
      const type = layer.type ?? "unknown";
      acc[type] = (acc[type] ?? 0) + 1;
      return acc;
    }, {});

    const sources = Object.entries(asRecord(style.sources)).map(([id, source]) => ({
      id,
      ...asRecord(source),
    }));

    return {
      layerCount: layers.length,
      sourceCount: sources.length,
      layerCountByType,
      layers,
      sources,
    };
  });
}

async function captureViewportDump(page: Page, viewport: ViewportSpec): Promise<unknown> {
  return page.evaluate((currentViewport) => {
    type Dict = Record<string, unknown>;

    interface FeatureSummary {
      id: unknown;
      source: unknown;
      sourceLayer: unknown;
      layer: unknown;
      layerType: string;
      geometryType: unknown;
      propertyKeys: string[];
      properties: unknown;
    }

    function asRecord(value: unknown): Dict {
      return typeof value === "object" && value !== null ? (value as Dict) : {};
    }

    function asArray(value: unknown): unknown[] {
      return Array.isArray(value) ? value : [];
    }

    function layerTypeOf(feature: unknown): string {
      const layer = asRecord(asRecord(feature).layer);
      return typeof layer.type === "string" ? layer.type : "unknown";
    }

    function summarizeFeature(feature: unknown): FeatureSummary {
      const item = asRecord(feature);
      const layer = asRecord(item.layer);
      const geometry = asRecord(item.geometry);
      const properties = asRecord(item.properties);
      return {
        id: item.id,
        source: item.source,
        sourceLayer: item.sourceLayer,
        layer: layer.id,
        layerType: layerTypeOf(feature),
        geometryType: geometry.type,
        propertyKeys: Object.keys(properties).slice(0, 30),
        properties,
      };
    }

    function groupFeatureSamples(features: unknown[]): Record<string, unknown> {
      return features.reduce<Record<string, unknown>>((groups, feature) => {
        const summary = summarizeFeature(feature);
        const group = groups[summary.layerType] as { count: number; samples: FeatureSummary[] };
        groups[summary.layerType] = group
          ? { count: group.count + 1, samples: group.samples }
          : { count: 1, samples: [summary] };
        return groups;
      }, {});
    }

    function probeFeatureState(features: unknown[], mapbox: Dict): unknown[] {
      const setFeatureState = mapbox.setFeatureState as
        | ((target: Dict, state: Dict) => void)
        | undefined;
      const getFeatureState = mapbox.getFeatureState as ((target: Dict) => Dict) | undefined;
      const removeFeatureState = mapbox.removeFeatureState as ((target: Dict) => void) | undefined;
      if (!setFeatureState || !getFeatureState)
        return [{ ok: false, error: "feature-state API unavailable" }];

      return features
        .map(summarizeFeature)
        .filter((feature) => feature.id !== undefined && feature.id !== null)
        .slice(0, 8)
        .map((feature) => {
          const target = {
            source: feature.source,
            sourceLayer: feature.sourceLayer,
            id: feature.id,
          };
          setFeatureState(target, { __probe: true });
          const state = getFeatureState(target);
          removeFeatureState?.(target);
          return {
            type: feature.layerType,
            source: feature.source,
            sourceLayer: feature.sourceLayer,
            layer: feature.layer,
            id: feature.id,
            ok: state.__probe === true,
          };
        });
    }

    const root = window as unknown as { __listingMap?: { getMapbox?: () => unknown } };
    const mapbox = asRecord(root.__listingMap?.getMapbox?.());
    const queryRenderedFeatures = mapbox.queryRenderedFeatures as (() => unknown) | undefined;
    const features = asArray(queryRenderedFeatures?.());

    return {
      viewport: currentViewport,
      totalFeaturesInViewport: features.length,
      featuresByType: features.reduce<Record<string, number>>((acc, feature) => {
        const type = layerTypeOf(feature);
        acc[type] = (acc[type] ?? 0) + 1;
        return acc;
      }, {}),
      sampleByType: groupFeatureSamples(features),
      stateTests: probeFeatureState(features, mapbox),
    };
  }, viewport);
}

async function captureCadastralLayer(page: Page): Promise<unknown> {
  return page.evaluate(() => {
    type Dict = Record<string, unknown>;
    type CadastralLayer = { setMap?: (map: unknown) => void; getMap?: () => unknown };
    type CadastralLayerCtor = new () => CadastralLayer;

    const root = window as unknown as {
      __listingMap?: unknown;
      naver?: { maps?: { CadastralLayer?: CadastralLayerCtor } };
    };
    const CadastralLayer = root.naver?.maps?.CadastralLayer;

    if (!root.__listingMap || !CadastralLayer) {
      return {
        cadastralAvailable: false,
        note: "naver.maps.CadastralLayer unavailable",
      };
    }

    const cadastral = new CadastralLayer();
    cadastral.setMap?.(root.__listingMap);
    const prototype = Object.getPrototypeOf(cadastral) as Dict;

    return {
      cadastralAvailable: true,
      cadastralCtor: typeof CadastralLayer,
      cadastralKeys: Object.keys(cadastral),
      cadastralPrototype: Object.getOwnPropertyNames(prototype),
      hasGetMap: typeof cadastral.getMap === "function",
      mapAfterSet: Boolean(cadastral.getMap?.()),
    };
  });
}

test.describe("Naver SDK probes", () => {
  test.beforeEach(async ({ baseURL, context }) => {
    await plantAuthenticatedSession(context, { baseURL });
  });

  test("catalogs style layers and rendered feature-state support", async ({ page }, testInfo) => {
    test.setTimeout(300_000);
    await openAuthenticatedListings(page);

    await writeProbeJson(
      testInfo,
      "naver-all-features-catalog.json",
      await captureStyleCatalog(page),
    );

    for (const viewport of VIEWPORTS) {
      await setNaverGlViewport(page, viewport);
      await writeProbeJson(
        testInfo,
        `naver-all-features-${viewport.name}.json`,
        await captureViewportDump(page, viewport),
      );
    }
  });

  test("catalogs cadastral layer availability", async ({ page }, testInfo) => {
    test.setTimeout(120_000);
    await openAuthenticatedListings(page);
    const result = await captureCadastralLayer(page);
    await writeProbeJson(testInfo, "naver-cadastral-layer.json", result);
    expect(result).toBeDefined();
  });
});

hardwareGlTest.describe("hardware GL vector source reload", () => {
  hardwareGlTest.beforeEach(async ({ baseURL, context }) => {
    await plantAuthenticatedSession(context, { baseURL });
  });

  hardwareGlTest("vector source reload", async ({ page }, testInfo) => {
    hardwareGlTest.setTimeout(120_000);
    await openAuthenticatedListings(page);

    const renderer = await page.evaluate(() => {
      const gl = document.createElement("canvas").getContext("webgl");
      const debugInfo = gl?.getExtension("WEBGL_debug_renderer_info");
      return gl && debugInfo
        ? String(gl.getParameter(debugInfo.UNMASKED_RENDERER_WEBGL))
            .replace(/\s+/g, " ")
            .trim()
            .slice(0, 256)
        : null;
    });
    if (!renderer || SOFTWARE_RENDERER_PATTERN.test(renderer)) {
      throw new Error(`hardware WebGL renderer required; received ${renderer ?? "unavailable"}`);
    }
    await page.waitForFunction(
      () => (window as ProbeWindow).__listingMap?.getMapbox?.()?.isStyleLoaded?.() === true,
      undefined,
      { timeout: 5_000 },
    );

    const origin = new URL(page.url()).origin;
    const suffix = `${testInfo.workerIndex}-${Date.now()}`;
    const sourceId = `__probe-vector-reload-${suffix}`;
    const layerId = `${sourceId}-layer`;
    const probePath = `/__probe/vector-reload/${suffix}`;
    const routePattern = `${origin}${probePath}/**`;
    const tileTemplate = (generation: string) =>
      `${origin}${probePath}/${generation}/{z}/{x}/{y}.pbf`;
    const isTileRequest = (requestUrl: string, generation: string) => {
      const url = new URL(requestUrl);
      return (
        url.origin === origin &&
        url.pathname.startsWith(`${probePath}/${generation}/`) &&
        /\/\d+\/\d+\/\d+\.pbf$/.test(url.pathname)
      );
    };

    await page.route(routePattern, (route) =>
      route.fulfill({
        contentType: "application/x-protobuf",
        body: Buffer.alloc(0),
      }),
    );

    try {
      const [firstRequest, firstResponse, sourceInspection] = await Promise.all([
        page.waitForRequest((request) => isTileRequest(request.url(), "first"), {
          timeout: 5_000,
        }),
        page.waitForResponse(
          (response) => isTileRequest(response.url(), "first") && response.status() === 200,
          { timeout: 5_000 },
        ),
        page.evaluate(
          ({ firstTemplate, layerId, sourceId }) => {
            const mapbox = (window as ProbeWindow).__listingMap?.getMapbox?.();
            if (
              !mapbox?.addSource ||
              !mapbox.addLayer ||
              !mapbox.getSource ||
              !mapbox.triggerRepaint
            ) {
              throw new Error("live mapbox source/layer APIs are unavailable");
            }
            mapbox.addSource(sourceId, {
              type: "vector",
              tiles: [firstTemplate],
            });
            const source = mapbox.getSource(sourceId) as ProbeVectorSource | undefined;
            if (!source || typeof source.setTiles !== "function") {
              throw new Error("live vector source must expose setTiles");
            }
            mapbox.addLayer({
              id: layerId,
              type: "circle",
              source: sourceId,
              "source-layer": "probe",
              layout: { visibility: "visible" },
              paint: { "circle-color": "#2563eb", "circle-radius": 3 },
            });
            mapbox.triggerRepaint();
            return { sourceExists: true, setTilesCallable: true };
          },
          { firstTemplate: tileTemplate("first"), layerId, sourceId },
        ),
      ]);
      expect(firstRequest).toBeDefined();
      expect(await firstResponse.finished()).toBeNull();
      expect(sourceInspection).toEqual({ sourceExists: true, setTilesCallable: true });
      await page.evaluate(
        ({ deadlineMs, requiredFrames, sourceId }) =>
          new Promise<void>((resolve, reject) => {
            const deadlineAt = performance.now() + deadlineMs;
            let consecutiveLoadedFrames = 0;
            const isSourceLoaded = () =>
              (window as ProbeWindow).__listingMap?.getMapbox?.()?.isSourceLoaded?.(sourceId) ===
              true;
            const rejectAtDeadline = () =>
              reject(
                new Error(
                  `vector source ${sourceId} was not loaded for ${requiredFrames} consecutive animation frames within ${deadlineMs}ms`,
                ),
              );
            const deadlineTimer = window.setTimeout(rejectAtDeadline, deadlineMs);
            const checkSourceLoaded = () => {
              if (performance.now() >= deadlineAt) {
                window.clearTimeout(deadlineTimer);
                rejectAtDeadline();
                return;
              }
              consecutiveLoadedFrames = isSourceLoaded() ? consecutiveLoadedFrames + 1 : 0;
              if (consecutiveLoadedFrames >= requiredFrames) {
                window.clearTimeout(deadlineTimer);
                resolve();
                return;
              }
              requestAnimationFrame(checkSourceLoaded);
            };
            requestAnimationFrame(checkSourceLoaded);
          }),
        { deadlineMs: 5_000, requiredFrames: 3, sourceId },
      );

      const startedAt = Date.now();
      const [secondRequest, strategy] = await Promise.all([
        page.waitForRequest((request) => isTileRequest(request.url(), "second"), {
          timeout: 5_000,
        }),
        page.evaluate(
          ({ secondTemplate, sourceId }) => {
            const mapbox = (window as ProbeWindow).__listingMap?.getMapbox?.();
            const source = mapbox?.getSource?.(sourceId) as ProbeVectorSource | undefined;
            if (!source || typeof source.setTiles !== "function" || !mapbox?.triggerRepaint) {
              throw new Error("live vector source lost setTiles");
            }
            source.setTiles([secondTemplate]);
            mapbox.triggerRepaint();
            return "setTiles" as const;
          },
          { secondTemplate: tileTemplate("second"), sourceId },
        ),
      ]);
      const elapsedMs = Date.now() - startedAt;
      const evidence = { strategy, elapsedMs, renderer };
      expect(secondRequest).toBeDefined();
      expect(evidence.strategy).toBe("setTiles");
      expect(evidence.elapsedMs).toBeLessThanOrEqual(5_000);
      await testInfo.attach("vector-source-reload-evidence.json", {
        body: JSON.stringify(evidence),
        contentType: "application/json",
      });
      console.info(`[naver probe] vector source reload ${JSON.stringify(evidence)}`);
    } finally {
      try {
        await page
          .evaluate(
            ({ layerId, sourceId }) => {
              const mapbox = (window as ProbeWindow).__listingMap?.getMapbox?.();
              const bestEffort = (remove: () => void) => {
                try {
                  remove();
                } catch {
                  // Cleanup must not replace the probe failure.
                }
              };
              bestEffort(() => {
                if (mapbox?.getLayer?.(layerId) && mapbox.removeLayer) {
                  mapbox.removeLayer(layerId);
                }
              });
              bestEffort(() => {
                if (mapbox?.getSource?.(sourceId) && mapbox.removeSource) {
                  mapbox.removeSource(sourceId);
                }
              });
            },
            { layerId, sourceId },
          )
          .catch(() => undefined);
      } finally {
        await page.unroute(routePattern).catch(() => undefined);
      }
    }
  });
});
