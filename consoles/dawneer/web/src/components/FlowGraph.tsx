// The 데이터 흐름 graph (root ADR-0188): the datasets of the pipeline graph as cards in stage
// columns, left to right, and the scheduled jobs as the arrows between them, carrying each job's
// live state — the way Dagster's asset graph puts the catalog and the run status in one picture.
// What is drawn where is derived in `lib/dataFlow.ts` and `lib/flowLayout.ts`; this only renders it.
import "@xyflow/react/dist/style.css";

import { useQuery } from "@tanstack/react-query";
import { Background, Controls, Handle, MarkerType, Position, ReactFlow, type Edge, type Node, type NodeProps } from "@xyflow/react";
import { useMemo, useRef } from "react";

import {
  datasetKind,
  datasetState,
  edgeState,
  flowStatus,
  nodeStatus,
  registryNote,
  STAGE_TEXT,
  STATUS_TEXT,
  statusNote,
  type FlowStatus,
  type FlowView,
  type JobState,
  type Stage,
} from "../lib/dataFlow";
import { layoutFlow, NODE_HEIGHT, NODE_WIDTH, startViewport } from "../lib/flowLayout";

/** Colour per state; every state also has its own icon and word (`STATUS_TEXT`). */
export const STATUS_CLASS: Record<FlowStatus, { card: string; pill: string; stroke: string }> = {
  running: { card: "border-sky-500 ring-2 ring-sky-200", pill: "bg-sky-100 text-sky-800", stroke: "#0284c7" },
  retry: { card: "border-red-400", pill: "bg-red-100 text-red-800", stroke: "#f87171" },
  failed: { card: "border-red-600 ring-2 ring-red-100", pill: "bg-red-600 text-white", stroke: "#dc2626" },
  blocked: { card: "border-slate-400 border-dashed bg-slate-50", pill: "bg-slate-500 text-white", stroke: "#64748b" },
  late: { card: "border-orange-500 ring-2 ring-orange-100", pill: "bg-orange-100 text-orange-900", stroke: "#ea580c" },
  slow: { card: "border-yellow-500 ring-2 ring-yellow-100", pill: "bg-yellow-100 text-yellow-900", stroke: "#ca8a04" },
  volume: { card: "border-violet-500 ring-2 ring-violet-100", pill: "bg-violet-100 text-violet-900", stroke: "#7c3aed" },
  waiting: { card: "border-amber-400 border-dashed", pill: "bg-amber-100 text-amber-800", stroke: "#d97706" },
  done: { card: "border-emerald-400", pill: "bg-emerald-100 text-emerald-800", stroke: "#10b981" },
  skipped: { card: "border-slate-300", pill: "bg-slate-100 text-slate-600", stroke: "#94a3b8" },
  never: { card: "border-slate-300", pill: "bg-slate-100 text-slate-600", stroke: "#94a3b8" },
  off: { card: "border-slate-200 bg-slate-50 opacity-70", pill: "bg-slate-200 text-slate-600", stroke: "#cbd5e1" },
  manual: { card: "border-slate-200", pill: "bg-slate-50 text-slate-500", stroke: "#cbd5e1" },
  on_event: { card: "border-slate-300", pill: "bg-slate-100 text-slate-700", stroke: "#94a3b8" },
  not_started: { card: "border-slate-200 border-dashed", pill: "bg-slate-50 text-slate-500", stroke: "#cbd5e1" },
  store: { card: "border-slate-200 bg-slate-50", pill: "bg-slate-100 text-slate-600", stroke: "#cbd5e1" },
  outside: { card: "border-slate-200 bg-slate-50", pill: "bg-slate-100 text-slate-600", stroke: "#cbd5e1" },
  unread: { card: "border-amber-300", pill: "bg-amber-50 text-amber-800", stroke: "#94a3b8" },
};

export function StatusPill({ status }: { status: FlowStatus }) {
  const text = STATUS_TEXT[status];
  return (
    <span className={`inline-flex items-center gap-1 whitespace-nowrap rounded px-1.5 py-0.5 text-xs font-semibold ${STATUS_CLASS[status].pill}`}>
      <span aria-hidden className={status === "running" ? "inline-block animate-spin [animation-duration:2s]" : ""}>
        {text.icon}
      </span>
      {text.word}
    </span>
  );
}

type DatasetData = {
  title: string;
  icon: string;
  kindWord: string;
  status: FlowStatus;
  note: string | null;
  progress: number | null;
  selected: boolean;
};
type StageData = { stage: Stage; height: number };

function DatasetCard({ data }: NodeProps<Node<DatasetData, "dataset">>) {
  const tone = STATUS_CLASS[data.status];
  return (
    <>
      <Handle type="target" position={Position.Left} className="!border-0 !bg-slate-300" />
      <button
        type="button"
        aria-label={`${data.title}, ${data.kindWord}, ${STATUS_TEXT[data.status].word}${data.note ? `, ${data.note}` : ""}`}
        aria-pressed={data.selected}
        className={`flex h-full w-full flex-col gap-1 rounded-xl border-2 bg-white px-3 py-2 text-left shadow-sm outline-none hover:shadow-md focus-visible:ring-4 focus-visible:ring-slate-400 ${tone.card} ${data.selected ? "ring-4 ring-slate-800" : ""}`}
        style={{ width: NODE_WIDTH, height: NODE_HEIGHT }}
      >
        <span className="flex items-start gap-2">
          <span aria-hidden className="text-xl leading-6">
            {data.icon}
          </span>
          <span className="min-w-0">
            <span className="line-clamp-2 text-sm font-semibold leading-5 text-slate-900">{data.title}</span>
            <span className="block text-[11px] text-slate-500">{data.kindWord}</span>
          </span>
        </span>
        <span className="mt-auto flex items-center gap-2">
          <StatusPill status={data.status} />
          {data.note ? <span className="truncate text-xs text-slate-600">{data.note}</span> : null}
        </span>
        {data.progress === null ? null : (
          <span className="block h-1.5 w-full overflow-hidden rounded bg-slate-100" role="progressbar" aria-valuenow={Math.round(data.progress * 100)} aria-valuemin={0} aria-valuemax={100}>
            <span className="block h-1.5 bg-sky-500 transition-all" style={{ width: `${Math.round(data.progress * 100)}%` }} />
          </span>
        )}
      </button>
      <Handle type="source" position={Position.Right} className="!border-0 !bg-slate-300" />
    </>
  );
}

function StageColumn({ data }: NodeProps<Node<StageData, "stage">>) {
  const text = STAGE_TEXT[data.stage];
  return (
    <div className="h-full rounded-2xl bg-slate-100/70 px-4 pt-3" style={{ height: data.height }}>
      <div className="text-lg font-bold text-slate-800">{text.title}</div>
      <div className="text-sm text-slate-600">{text.sentence}</div>
    </div>
  );
}

const NODE_TYPES = { dataset: DatasetCard, stage: StageColumn };
/** The room above the topmost card for a stage's heading and sentence. */
const HEADING = 72;
const PAD = 16;

export function FlowGraph({
  view,
  states,
  titleOf,
  now,
  selected,
  onSelect,
}: {
  view: FlowView;
  states: Map<string, JobState>;
  titleOf: (jobId: string) => string;
  now: Date;
  selected: string | null;
  onSelect: (nodeId: string) => void;
}) {
  // The layout depends only on which datasets and edges there are, not on their states.
  const shape = useMemo(
    () => JSON.stringify([view.nodes.map((node) => [node.id, node.stage]), view.edges.map((edge) => [edge.id, edge.from, edge.to])]),
    [view],
  );
  const layout = useQuery({ queryKey: ["flow-layout", shape], queryFn: () => layoutFlow(view), staleTime: Infinity });

  const drawn = useMemo(() => {
    if (!layout.data) return null;
    const { positions, bands, top } = layout.data;
    const bottom = Math.max(top, ...[...positions.values()].map((at) => at.y)) + NODE_HEIGHT;
    const height = bottom - top + HEADING + 2 * PAD;
    const stages: Node[] = bands.map((band) => ({
      id: `stage-${band.stage}`,
      type: "stage",
      position: { x: band.x - PAD, y: top - HEADING - PAD },
      data: { stage: band.stage, height },
      // Under the cards and transparent to the pointer, so a drag on a column still pans.
      style: { width: band.width + 2 * PAD, height, pointerEvents: "none" },
      draggable: false,
      selectable: false,
      focusable: false,
      zIndex: -1,
    }));
    const datasets: Node[] = view.nodes.map((node) => {
      const state = datasetState(node, states);
      const status = nodeStatus(node.node, state);
      const kind = datasetKind(node.node);
      return {
        id: node.id,
        type: "dataset",
        position: positions.get(node.id) ?? { x: 0, y: 0 },
        data: {
          title: node.title,
          icon: kind.icon,
          kindWord: kind.word,
          status,
          note: state ? statusNote(state, titleOf, now) : registryNote(node.node, status),
          progress: status === "running" ? (state?.running?.progress ?? null) : null,
          selected: selected === node.id,
        } satisfies DatasetData,
        draggable: false,
        focusable: false,
        style: { width: NODE_WIDTH, height: NODE_HEIGHT },
      };
    });
    const edges: Edge[] = view.edges.map((edge) => {
      const status = flowStatus(edgeState(edge, states));
      const stroke = STATUS_CLASS[status].stroke;
      // A waiting step is shown dashed on its card's side, not shouted along every arrow into it.
      const loud = status === "running" || status === "failed" || status === "retry" || status === "blocked";
      return {
        id: edge.id,
        source: edge.from,
        target: edge.to,
        type: "smoothstep",
        animated: status === "running",
        markerEnd: { type: MarkerType.ArrowClosed, color: stroke },
        style: { stroke, strokeWidth: loud ? 3 : 1.5, ...(status === "off" || status === "manual" || status === "waiting" || status === "blocked" ? { strokeDasharray: "4 4" } : {}) },
        ...(loud
          ? {
              label: `${STATUS_TEXT[status].icon} ${STATUS_TEXT[status].word}`,
              labelStyle: { fontSize: 11, fontWeight: 600 },
              labelBgStyle: { fill: "#ffffff" },
            }
          : {}),
        zIndex: loud ? 1 : 0,
      };
    });
    const left = Math.min(...bands.map((band) => band.x)) - PAD;
    const right = Math.max(...bands.map((band) => band.x + band.width)) + PAD;
    return { nodes: [...stages, ...datasets], edges, bounds: { x: left, y: top - HEADING - PAD, width: right - left } };
  }, [layout.data, view, states, titleOf, now, selected, onSelect]);

  const frame = useRef<HTMLDivElement>(null);
  if (layout.isError) return <p className="text-sm text-red-700">그림을 그리지 못했습니다: {layout.error.message}</p>;
  if (!drawn) return <p className="p-6 text-sm text-slate-500">그림을 그리는 중…</p>;
  return (
    <div ref={frame} className="h-[min(78vh,860px)] min-h-[420px] rounded-xl border border-slate-200 bg-white" aria-label="데이터 흐름 지도">
      <ReactFlow
        nodes={drawn.nodes}
        edges={drawn.edges}
        nodeTypes={NODE_TYPES}
        onInit={(instance) => void instance.setViewport(startViewport(drawn.bounds, frame.current?.clientWidth ?? 1200))}
        // A click on a card, or Enter/Space on its button (whose click bubbles here), opens it.
        onNodeClick={(_, clicked) => {
          if (clicked.type === "dataset") onSelect(clicked.id);
        }}
        minZoom={0.15}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background gap={24} color="#e2e8f0" />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
