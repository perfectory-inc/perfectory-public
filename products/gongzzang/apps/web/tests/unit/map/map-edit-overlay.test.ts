// @vitest-environment node
import { describe, expect, it, vi } from "vitest";

import { registerComplexClick, startMapEditOverlay } from "@/lib/map/listing-map-runtime";
import {
  applyMapEditOverlay,
  fetchMapEditOverlay,
  mapEditOverlayFillLayerId,
  mapEditOverlayOutlineLayerId,
  mapEditOverlaySourceId,
  mapEditTombstoneFilter,
  parseMapEditOverlay,
  registerMapEditOverlayLayers,
} from "@/lib/map/map-edit-overlay";

const EDITED = "00000000-0000-5000-8000-000000000001";
const DELETED = "00000000-0000-5000-8000-000000000002";
// The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py (half-open).
const SQUARE = {
  type: "Polygon",
  coordinates: [
    [
      [127.1231, 36.1231],
      [127.1234, 36.1231],
      [127.1234, 36.1234],
      [127.1231, 36.1234],
      [127.1231, 36.1231],
    ],
  ],
};

function overlayBody(overrides: Record<string, unknown> = {}) {
  return {
    unit: "complex",
    feature_id_property: "complex_id",
    folded_through_change_seq: 0,
    latest_change_seq: 2,
    hidden_feature_ids: [EDITED, DELETED],
    features: {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: { official_complex_code: "SYN", complex_id: EDITED },
          geometry: SQUARE,
        },
      ],
    },
    ...overrides,
  };
}

function fakeMapbox(existingLayers: string[]) {
  const layers = new Set(existingLayers);
  const sources = new Map<string, { setData: ReturnType<typeof vi.fn> }>();
  const added: Array<{ layer: Record<string, unknown>; beforeId: string | undefined }> = [];
  const filters = new Map<string, unknown>();
  const clicks: string[] = [];
  return {
    added,
    filters,
    sources,
    clicks,
    addSource: (id: string) => {
      sources.set(id, { setData: vi.fn() });
    },
    addLayer: (layer: Record<string, unknown>, beforeId?: string) => {
      layers.add(String(layer.id));
      added.push({ layer, beforeId });
    },
    getSource: (id: string) => sources.get(id),
    getLayer: (id: string) => (layers.has(id) ? { id } : undefined),
    setFilter: (id: string, filter: unknown[] | null) => {
      filters.set(id, filter);
    },
    on: (_event: string, layerId: string) => {
      clicks.push(layerId);
    },
  };
}

describe("parsing the overlay", () => {
  it("accepts the edit store's contract", () => {
    expect(parseMapEditOverlay(overlayBody(), "complex").hidden_feature_ids).toEqual([
      EDITED,
      DELETED,
    ]);
  });

  it("refuses another unit's overlay, unknown fields, and a drawn feature that is not hidden", () => {
    expect(() => parseMapEditOverlay(overlayBody(), "admin")).toThrow(/expected admin/);
    expect(() => parseMapEditOverlay(overlayBody({ extra: 1 }), "complex")).toThrow();
    expect(() =>
      parseMapEditOverlay(overlayBody({ hidden_feature_ids: [DELETED] }), "complex"),
    ).toThrow(/without hiding/);
  });
});

describe("fetching the overlay", () => {
  it("revalidates with the ETag and keeps the previous overlay on 304", async () => {
    const previous = parseMapEditOverlay(overlayBody(), "complex");
    const fetcher = vi.fn(async () => new Response(null, { status: 304 }));
    const result = await fetchMapEditOverlay(
      fetcher as unknown as typeof fetch,
      "https://edits.example.test/",
      "complex",
      {
        previous,
        etag: '"complex-0-2"',
      },
    );
    expect(result.manifest).toBe(previous);
    expect(fetcher).toHaveBeenCalledWith(
      "https://edits.example.test/overlay/complex",
      expect.objectContaining({
        headers: expect.objectContaining({ "If-None-Match": '"complex-0-2"' }),
      }),
    );
  });

  it("throws on an error answer so the last drawn overlay stays", async () => {
    const fetcher = vi.fn(async () => new Response("{}", { status: 503 }));
    await expect(
      fetchMapEditOverlay(
        fetcher as unknown as typeof fetch,
        "https://edits.example.test",
        "complex",
      ),
    ).rejects.toThrow(/503/);
  });
});

describe("drawing the overlay", () => {
  it("adds the edit layers in the base style, under the same layers the base unit stays under", () => {
    const mb = fakeMapbox(["complex-fill", "complex-outline", "parcels-fill"]);
    expect(registerMapEditOverlayLayers(mb, "complex")).toEqual([
      mapEditOverlayFillLayerId("complex"),
      mapEditOverlayOutlineLayerId("complex"),
    ]);
    expect(mb.sources.has(mapEditOverlaySourceId("complex"))).toBe(true);
    expect(mb.added.map(({ beforeId }) => beforeId)).toEqual(["parcels-fill", "parcels-fill"]);
    expect(mb.added[0]?.layer.paint).toMatchObject({ "fill-opacity": 0.08 });
    expect(registerMapEditOverlayLayers(mb, "complex")).toEqual([]);
  });

  it("adds nothing when the base unit is not on the map", () => {
    expect(registerMapEditOverlayLayers(fakeMapbox([]), "complex")).toEqual([]);
  });

  it("hides every edited id in both base layers and draws the fresh polygons", () => {
    const mb = fakeMapbox(["complex-fill", "complex-outline"]);
    registerMapEditOverlayLayers(mb, "complex");
    const overlay = parseMapEditOverlay(overlayBody(), "complex");
    applyMapEditOverlay(mb, overlay);
    const expected = ["!", ["in", ["get", "complex_id"], ["literal", [EDITED, DELETED]]]];
    expect(mb.filters.get("complex-fill")).toEqual(expected);
    expect(mb.filters.get("complex-outline")).toEqual(expected);
    expect(mb.sources.get(mapEditOverlaySourceId("complex"))?.setData).toHaveBeenCalledWith(
      overlay.features,
    );
  });

  it("clears the filter once a bake has folded every edit", () => {
    expect(mapEditTombstoneFilter("complex_id", [])).toBeNull();
    const mb = fakeMapbox(["complex-fill", "complex-outline"]);
    registerMapEditOverlayLayers(mb, "complex");
    applyMapEditOverlay(
      mb,
      parseMapEditOverlay(
        overlayBody({
          hidden_feature_ids: [],
          features: { type: "FeatureCollection", features: [] },
        }),
        "complex",
      ),
    );
    expect(mb.filters.get("complex-fill")).toBeNull();
  });

  it("opens the complex panel from the edited polygon too", () => {
    const mb = fakeMapbox(["complex-fill", "complex-outline"]);
    registerMapEditOverlayLayers(mb, "complex");
    registerComplexClick(mb, () => undefined);
    expect(mb.clicks).toEqual(["complex-fill", mapEditOverlayFillLayerId("complex")]);
  });
});

describe("keeping the overlay current", () => {
  it("starts only when the edit layers exist, and applies the first overlay", async () => {
    expect(
      startMapEditOverlay(fakeMapbox(["complex-fill"]), "complex", "https://edits.example.test"),
    ).toBeUndefined();
    const mb = fakeMapbox(["complex-fill", "complex-outline"]);
    registerMapEditOverlayLayers(mb, "complex");
    const fetcher = vi.fn(async () =>
      Response.json(overlayBody(), { headers: { ETag: '"complex-0-2"' } }),
    );
    const stop = startMapEditOverlay(
      mb,
      "complex",
      "https://edits.example.test",
      fetcher as unknown as typeof fetch,
    );
    await vi.waitFor(() => expect(mb.filters.get("complex-fill")).not.toBeUndefined());
    stop?.();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
