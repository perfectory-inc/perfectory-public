import { shuffle } from "es-toolkit";
import { describe, expect, it } from "vitest";

import type { FlowNode, FlowView, Stage } from "./dataFlow";
import { elkGraph, layoutFlow, NODE_WIDTH, READABLE_ZOOM, startViewport } from "./flowLayout";

function node(id: string, stage: Stage): FlowNode {
  return { id, title: id, stage, node: { id }, writers: [] };
}

// A cycle (the serving group feeds the Silver table it is made from, as staff decisions do), a
// dataset with no edge at all, and two datasets of one stage that feed each other.
const view: FlowView = {
  nodes: [
    node("source", "collect"),
    node("silver", "silver"),
    node("reference", "silver"),
    node("reference-change", "silver"),
    node("gold", "gold"),
    node("tiles", "bake"),
    node("decisions", "serve"),
    node("alone", "serve"),
  ],
  edges: [
    { id: "a", from: "source", to: "silver", jobs: [] },
    { id: "b", from: "silver", to: "gold", jobs: [] },
    { id: "c", from: "gold", to: "tiles", jobs: [] },
    { id: "d", from: "gold", to: "decisions", jobs: [] },
    { id: "e", from: "decisions", to: "silver", jobs: [] },
    { id: "f", from: "reference", to: "reference-change", jobs: [] },
  ],
};

describe("the flow layout", () => {
  it("keeps every stage's datasets right of the stage before's, cycles and loose datasets included", async () => {
    const layout = await layoutFlow(view);
    const x = (id: string) => layout.positions.get(id)?.x ?? Number.NaN;
    expect(x("source")).toBeLessThan(x("silver"));
    expect(x("source")).toBeLessThan(x("reference"));
    expect(Math.max(x("silver"), x("reference"), x("reference-change"))).toBeLessThan(x("gold"));
    expect(x("gold")).toBeLessThan(x("tiles"));
    expect(x("tiles")).toBeLessThan(x("decisions"));
    expect(x("tiles")).toBeLessThan(x("alone"));
    expect(layout.bands.map((band) => band.stage)).toEqual(["collect", "silver", "gold", "bake", "serve"]);
    for (const [left, right] of layout.bands.slice(1).map((band, index) => [layout.bands[index], band] as const)) {
      expect((left?.x ?? 0) + (left?.width ?? 0)).toBeLessThan(right.x);
    }
    expect(layout.bands[0]?.width).toBe(NODE_WIDTH);
  });

  it("is the same picture whatever order the datasets and edges arrive in", async () => {
    const first = await layoutFlow(view);
    const again = await layoutFlow({ nodes: shuffle(view.nodes), edges: shuffle(view.edges) });
    expect([...again.positions.entries()].sort()).toEqual([...first.positions.entries()].sort());
    expect(again.bands).toEqual(first.bands);
    expect(elkGraph({ nodes: [...view.nodes].reverse(), edges: [...view.edges].reverse() })).toEqual(elkGraph(view));
  });

  it("opens at the first stage's heading, fitting the width but never below a readable zoom", () => {
    expect(startViewport({ x: 100, y: -50, width: 1000 }, 2024)).toEqual({ x: -88, y: 62, zoom: 1 });
    expect(startViewport({ x: 0, y: 0, width: 1000 }, 824)).toEqual({ x: 12, y: 12, zoom: 0.8 });
    expect(startViewport({ x: 0, y: 0, width: 10_000 }, 824).zoom).toBe(READABLE_ZOOM);
  });
});
