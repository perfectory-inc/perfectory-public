// Where each dataset of the 데이터 흐름 graph sits (root ADR-0188). The Eclipse Layout Kernel's
// layered algorithm places them left to right, with each stage a partition so the stages stay in
// order as columns; this file only states the graph to it and reads its answer back.
import type { ElkNode } from "elkjs/lib/elk-api";
import { clamp, groupBy, maxBy, minBy, orderBy } from "es-toolkit";

import { STAGES, type FlowView, type Stage } from "./dataFlow";

export const NODE_WIDTH = 248;
export const NODE_HEIGHT = 112;

/** ELK's options for the flow. Fixed, so the same view always gets the same picture. */
export const ELK_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "RIGHT",
  // Each stage is a partition: a stage's datasets all sit right of the stage before's.
  "elk.partitioning.activate": "true",
  // Without this, a dataset unconnected to the rest would be laid out on its own and could land
  // in any column.
  "elk.separateConnectedComponents": "false",
  "elk.layered.spacing.nodeNodeBetweenLayers": "96",
  "elk.spacing.nodeNode": "28",
  "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
  "elk.randomSeed": "1",
};

/** The graph as ELK reads it, in id order so the order the data came in does not matter. */
export function elkGraph(view: FlowView): ElkNode {
  return {
    id: "flow",
    layoutOptions: ELK_OPTIONS,
    children: orderBy(view.nodes, [(node) => node.id], ["asc"]).map((node) => ({
      id: node.id,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      layoutOptions: { "elk.partitioning.partition": String(STAGES.indexOf(node.stage)) },
    })),
    edges: orderBy(view.edges, [(edge) => edge.id], ["asc"]).map((edge) => ({ id: edge.id, sources: [edge.from], targets: [edge.to] })),
  };
}

export interface StageBand {
  stage: Stage;
  /** The left edge of the band's first dataset and the right edge of its last. */
  x: number;
  width: number;
}

export interface FlowLayout {
  positions: Map<string, { x: number; y: number }>;
  /** The stages that hold a dataset, left to right. */
  bands: StageBand[];
  /** The top of the highest dataset. */
  top: number;
}

/** Positions and stage bands from ELK's answer for `view`. */
export function readLayout(view: FlowView, laidOut: ElkNode): FlowLayout {
  const positions = new Map((laidOut.children ?? []).map((child) => [child.id, { x: child.x ?? 0, y: child.y ?? 0 }]));
  const byStage = groupBy(view.nodes, (node) => node.stage);
  const bands = STAGES.flatMap((stage): StageBand[] => {
    const xs = (byStage[stage] ?? []).flatMap((node) => {
      const at = positions.get(node.id);
      return at ? [at.x] : [];
    });
    const left = minBy(xs, (x) => x);
    const right = maxBy(xs, (x) => x);
    return left === undefined || right === undefined ? [] : [{ stage, x: left, width: right - left + NODE_WIDTH }];
  });
  const top = minBy([...positions.values()], (at) => at.y)?.y ?? 0;
  return { positions, bands, top };
}

/** Lays `view` out; ELK is loaded the first time, so the console's first screen does not wait for it. */
export async function layoutFlow(view: FlowView): Promise<FlowLayout> {
  const { default: ELK } = await import("elkjs/lib/elk.bundled.js");
  const laidOut = await new ELK().layout(elkGraph(view));
  return readLayout(view, laidOut);
}

/** The smallest zoom at which a card's words stay readable. */
export const READABLE_ZOOM = 0.6;

/**
 * Where the graph opens: its top-left corner (the first stage's heading) at the frame's top-left,
 * as wide as the frame allows but never smaller than readable — the rest is a pan away.
 */
export function startViewport(bounds: { x: number; y: number; width: number }, frameWidth: number, margin = 12): { x: number; y: number; zoom: number } {
  const zoom = clamp((frameWidth - 2 * margin) / Math.max(1, bounds.width), READABLE_ZOOM, 1);
  return { x: margin - bounds.x * zoom, y: margin - bounds.y * zoom, zoom };
}
