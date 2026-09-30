// Where each node of the data map sits: one column per layer, left to right in the order data
// flows, and inside a column the healthy nodes first so problems read as a band at the bottom.
import type { components } from "../api/pipeline-graph";

type S = components["schemas"];
export type PipelineNode = S["PipelineGraphNode"];
export type PipelineEdge = S["PipelineGraphEdge"];

export const LAYERS = [
  "source_group",
  "reference_table",
  "silver_table",
  "gold_table",
  "serving_group",
  "serving_surface",
] as const;

export const LAYER_LABEL: Record<string, string> = {
  source_group: "원천",
  reference_table: "기준표",
  silver_table: "실버",
  gold_table: "골드",
  serving_group: "서빙 표",
  serving_surface: "서비스",
};

export type Health = "working" | "declared" | "pending" | "broken";

/** What a node's status means for someone watching the pipeline. */
export function health(status: string): Health {
  switch (status) {
    case "implemented":
      return "working";
    case "partial":
    case "collection_available":
    case "schema_defined":
    case "contract_only":
      return "declared";
    case "waiting":
    case "planned":
    case "manual_approval":
      return "pending";
    default:
      return "broken";
  }
}

export const HEALTH_LABEL: Record<Health, string> = {
  working: "동작 중",
  declared: "정의만 됨",
  pending: "대기·계획",
  broken: "막힘·없음",
};

const HEALTH_ORDER: Record<Health, number> = { working: 0, declared: 1, pending: 2, broken: 3 };

export const COLUMN_WIDTH = 300;
export const ROW_HEIGHT = 64;

/** Position of every node, by its layer column and its place inside it. */
export function layout(nodes: readonly PipelineNode[]): Map<string, { x: number; y: number }> {
  const columns = new Map<number, PipelineNode[]>();
  for (const node of nodes) {
    const index = LAYERS.indexOf(node.type as (typeof LAYERS)[number]);
    const column = index === -1 ? LAYERS.length : index;
    columns.set(column, [...(columns.get(column) ?? []), node]);
  }
  const positions = new Map<string, { x: number; y: number }>();
  for (const [column, members] of columns) {
    const ordered = [...members].sort(
      (a, b) => HEALTH_ORDER[health(a.status)] - HEALTH_ORDER[health(b.status)] || a.title.localeCompare(b.title, "ko"),
    );
    ordered.forEach((node, row) => positions.set(node.id, { x: column * COLUMN_WIDTH, y: row * ROW_HEIGHT }));
  }
  return positions;
}
