import "@xyflow/react/dist/style.css";

import { useQuery } from "@tanstack/react-query";
import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node } from "@xyflow/react";
import { useMemo, useState } from "react";

import type { DawneerClient, PipelineGraph } from "../api/client";
import { health, HEALTH_LABEL, layout, LAYER_LABEL, LAYERS, type Health, type PipelineNode } from "../lib/pipelineLayout";

const HEALTH_STYLE: Record<Health, { border: string; background: string }> = {
  working: { border: "#16a34a", background: "#f0fdf4" },
  declared: { border: "#64748b", background: "#f8fafc" },
  pending: { border: "#d97706", background: "#fffbeb" },
  broken: { border: "#dc2626", background: "#fef2f2" },
};

type Runtime = NonNullable<PipelineGraph["runtime"]>;

/** Where every dataset comes from and goes, drawn left to right, with its state and live checks. */
export function PipelinePage({ client }: { client: DawneerClient }) {
  const graph = useQuery({ queryKey: ["pipeline-graph"], queryFn: () => client.pipelineGraph(), refetchInterval: 60_000 });
  const [shown, setShown] = useState<Record<Health, boolean>>({ working: true, declared: true, pending: true, broken: true });
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<string | null>(null);

  const { nodes, edges, counts } = useMemo(() => {
    const data = graph.data;
    if (!data) return { nodes: [] as Node[], edges: [] as Edge[], counts: { working: 0, declared: 0, pending: 0, broken: 0 } };
    const needle = search.trim().toLowerCase();
    const visible = data.nodes.filter(
      (n) =>
        shown[health(n.status)] &&
        (!needle || n.title.toLowerCase().includes(needle) || n.id.includes(needle) || (n.table_name ?? "").includes(needle)),
    );
    const ids = new Set(visible.map((n) => n.id));
    const positions = layout(visible);
    const tally = { working: 0, declared: 0, pending: 0, broken: 0 };
    for (const n of data.nodes) tally[health(n.status)] += 1;
    return {
      counts: tally,
      nodes: visible.map((n) => {
        const style = HEALTH_STYLE[health(n.status)];
        return {
          id: n.id,
          position: positions.get(n.id) ?? { x: 0, y: 0 },
          data: { label: n.title },
          style: {
            width: 240,
            fontSize: 12,
            borderRadius: 8,
            border: `2px solid ${style.border}`,
            background: style.background,
            outline: selected === n.id ? "3px solid #1d4ed8" : undefined,
          },
        } satisfies Node;
      }),
      edges: data.edges
        .filter((e) => ids.has(e.from) && ids.has(e.to))
        .map(
          (e) =>
            ({
              id: e.id,
              source: e.from,
              target: e.to,
              animated: e.status !== "implemented",
              style: { stroke: e.status === "implemented" ? "#94a3b8" : "#d97706", strokeDasharray: e.status === "implemented" ? undefined : "4 4" },
            }) satisfies Edge,
        ),
    };
  }, [graph.data, shown, search, selected]);

  if (graph.isPending) return <p className="text-slate-500">불러오는 중…</p>;
  if (graph.isError) return <p className="text-red-700">{graph.error.message}</p>;
  const node = graph.data.nodes.find((n) => n.id === selected);

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center gap-4">
        <h1 className="mr-auto text-xl font-semibold">데이터 흐름</h1>
        <span className={`rounded px-2 py-0.5 text-xs ${graph.data.runtime?.database_ready ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
          DB {graph.data.runtime?.database_ready ? "정상" : "응답 없음"}
        </span>
        {(Object.keys(HEALTH_LABEL) as Health[]).map((h) => (
          <label key={h} className="flex items-center gap-1 text-sm">
            <input type="checkbox" checked={shown[h]} onChange={(e) => setShown({ ...shown, [h]: e.target.checked })} />
            <span className="inline-block h-3 w-3 rounded-sm" style={{ background: HEALTH_STYLE[h].border }} />
            {HEALTH_LABEL[h]} {counts[h]}
          </label>
        ))}
        <input
          className="w-48 rounded border border-slate-300 px-2 py-1 text-sm"
          placeholder="이름·표 이름 찾기"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>
      <div className="flex gap-2 text-xs text-slate-500">
        {LAYERS.map((layer) => (
          <span key={layer} className="rounded bg-slate-100 px-2 py-0.5">
            {LAYER_LABEL[layer]}
          </span>
        ))}
        <span>← 왼쪽에서 오른쪽으로 데이터가 흐릅니다</span>
      </div>
      <div className="flex gap-4">
        <div className="h-[70vh] flex-1 rounded-lg border border-slate-200 bg-white">
          <ReactFlow nodes={nodes} edges={edges} onNodeClick={(_, n) => setSelected(n.id)} fitView minZoom={0.1} nodesDraggable={false}>
            <Background />
            <Controls />
            <MiniMap pannable zoomable />
          </ReactFlow>
        </div>
        {node && <NodeDetail node={node} runtime={graph.data.runtime} onClose={() => setSelected(null)} />}
      </div>
    </section>
  );
}

function NodeDetail({ node, runtime, onClose }: { node: PipelineNode; runtime: Runtime | undefined; onClose: () => void }) {
  const live = runtime?.nodes?.[node.id];
  return (
    <aside className="w-80 shrink-0 space-y-2 rounded-lg bg-white p-4 text-sm shadow-sm">
      <div className="flex items-start">
        <h2 className="mr-auto font-semibold">{node.title}</h2>
        <button type="button" onClick={onClose} className="text-slate-400 hover:text-slate-700" aria-label="닫기">
          ✕
        </button>
      </div>
      <p className="text-xs text-slate-500">
        {LAYER_LABEL[node.type] ?? node.type} · {HEALTH_LABEL[health(node.status)]} ({node.status})
      </p>
      {node.description && <p className="text-slate-700">{node.description}</p>}
      {node.table_name && <p className="font-mono text-xs">{node.table_name}</p>}
      {node.blocking_reason && <p className="text-red-700">막힌 이유: {node.blocking_reason}</p>}
      {live && (
        <div className="rounded bg-slate-50 p-2 text-xs">
          <p>실시간: {live.status}</p>
          {live.reason && <p className="text-slate-600">{live.reason}</p>}
          {live.observed !== undefined && live.observed !== null && <pre className="mt-1 whitespace-pre-wrap">{JSON.stringify(live.observed, null, 1)}</pre>}
        </div>
      )}
      {node.evidence_refs.length > 0 && (
        <div className="text-xs text-slate-500">
          근거:
          <ul className="list-disc pl-4">
            {node.evidence_refs.map((ref) => (
              <li key={ref} className="break-all font-mono">
                {ref}
              </li>
            ))}
          </ul>
        </div>
      )}
    </aside>
  );
}
