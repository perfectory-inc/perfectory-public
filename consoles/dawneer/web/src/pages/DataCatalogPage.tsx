import "@xyflow/react/dist/style.css";

import { useQuery } from "@tanstack/react-query";
import { Background, Controls, MarkerType, ReactFlow, type Edge, type Node } from "@xyflow/react";
import { useMemo, useState } from "react";

import type { CatalogEntity, CatalogNeighbourhood, DawneerClient } from "../api/client";

const PAGE = 20;
const COLUMN_GAP = 320;
const ROW_GAP = 70;

const KIND_STYLE = {
  dataset: { border: "#0f766e", background: "#f0fdfa" },
  job: { border: "#64748b", background: "#f8fafc" },
} as const;

/** The selected entity in the middle, what it comes from on the left, what comes from it on the right. */
function neighbourhoodGraph(data: CatalogNeighbourhood): { nodes: Node[]; edges: Edge[] } {
  const card = (entity: CatalogEntity, x: number, y: number, selected = false): Node => ({
    id: entity.urn,
    position: { x, y },
    data: { label: `${entity.kind === "job" ? "⚙ " : ""}${entity.name}` },
    style: {
      width: 260,
      fontSize: 12,
      borderRadius: 8,
      border: `${selected ? 3 : 1.5}px solid ${KIND_STYLE[entity.kind].border}`,
      background: KIND_STYLE[entity.kind].background,
      cursor: selected ? "default" : "pointer",
    },
  });
  const column = (entities: CatalogEntity[], x: number) =>
    entities.map((entity, index) => card(entity, x, (index - (entities.length - 1) / 2) * ROW_GAP));
  const centre = data.entity.urn;
  const edge = (source: string, target: string): Edge => ({
    id: `${source}->${target}`,
    source,
    target,
    markerEnd: { type: MarkerType.ArrowClosed },
  });
  return {
    nodes: [
      card(data.entity, 0, 0, true),
      ...column(data.upstream, -COLUMN_GAP),
      ...column(data.downstream, COLUMN_GAP),
    ],
    edges: [
      ...data.upstream.map((entity) => edge(entity.urn, centre)),
      ...data.downstream.map((entity) => edge(centre, entity.urn)),
    ],
  };
}

function EntityView({
  client,
  urn,
  onSelect,
}: {
  client: DawneerClient;
  urn: string;
  onSelect: (entity: CatalogEntity) => void;
}) {
  const detail = useQuery({ queryKey: ["catalog-entity", urn], queryFn: () => client.catalogEntity(urn) });
  const graph = useMemo(() => (detail.data ? neighbourhoodGraph(detail.data) : null), [detail.data]);

  if (detail.isPending) return <p className="p-5 text-slate-500">불러오는 중…</p>;
  if (detail.isError) return <p className="p-5 text-red-700">읽지 못했습니다: {detail.error.message}</p>;
  const { entity, fields, upstream, downstream } = detail.data;
  const byUrn = new Map([...upstream, ...downstream].map((neighbour) => [neighbour.urn, neighbour]));

  return (
    <div className="flex flex-col gap-4">
      <div className="rounded-lg border border-slate-200 bg-white p-5">
        <p className="text-xs text-slate-500">
          {entity.kind === "job" ? "작업" : "데이터"}
          {entity.platform ? ` · ${entity.platform}` : ""}
        </p>
        <h2 className="mt-1 break-all font-mono text-lg font-semibold">{entity.name}</h2>
        <p className="mt-2 whitespace-pre-line text-sm text-slate-700">{entity.description ?? "설명이 없습니다."}</p>
        <p className="mt-3 text-sm text-slate-500">
          앞 단계 {upstream.length}개 · 뒤 단계 {downstream.length}개 — 그림에서 칸을 누르면 그 칸으로 옮겨 갑니다.
        </p>
      </div>
      <div className="h-[520px] rounded-lg border border-slate-200 bg-white">
        {graph ? (
          <ReactFlow
            nodes={graph.nodes}
            edges={graph.edges}
            fitView
            nodesDraggable={false}
            onNodeClick={(_, node) => {
              const next = byUrn.get(node.id);
              if (next) onSelect(next);
            }}
          >
            <Background />
            <Controls showInteractive={false} />
          </ReactFlow>
        ) : null}
      </div>
      {fields.length > 0 ? (
        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-500">
              <tr>
                <th className="px-3 py-2 font-normal">칸</th>
                <th className="px-3 py-2 font-normal">타입</th>
                <th className="px-3 py-2 font-normal">설명</th>
              </tr>
            </thead>
            <tbody>
              {fields.map((field) => (
                <tr key={field.path} className="border-t border-slate-100">
                  <td className="px-3 py-2 font-mono text-xs">{field.path}</td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-500">{field.native_type ?? "—"}</td>
                  <td className="px-3 py-2">{field.description ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}

/** Find data, read what it is, and walk its lineage one step at a time (root ADR-0117, ADR-0119). */
export function DataCatalogPage({ client }: { client: DawneerClient }) {
  const [text, setText] = useState("");
  const [query, setQuery] = useState("");
  const [start, setStart] = useState(0);
  const [trail, setTrail] = useState<CatalogEntity[]>([]);
  const results = useQuery({
    queryKey: ["catalog-search", query, start],
    queryFn: () => client.searchCatalog(query, start, PAGE),
  });
  const selected = trail.at(-1);
  const select = (entity: CatalogEntity) => setTrail((current) => [...current, entity]);

  return (
    <section>
      <div className="flex flex-wrap items-end gap-4">
        <h1 className="mr-auto text-xl font-semibold">데이터 카탈로그</h1>
        <form
          className="flex gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            setQuery(text.trim());
            setStart(0);
          }}
        >
          <input
            className="w-72 rounded border border-slate-300 px-2 py-1 text-sm"
            placeholder="표 이름이나 설명 (예: 공시가격, silver.)"
            value={text}
            onChange={(event) => setText(event.target.value)}
          />
          <button type="submit" className="rounded bg-slate-900 px-3 py-1 text-sm text-white hover:bg-slate-700">
            찾기
          </button>
        </form>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-[24rem_1fr]">
        <div className="self-start rounded-lg border border-slate-200 bg-white">
          {results.isPending ? (
            <p className="p-5 text-slate-500">불러오는 중…</p>
          ) : results.isError ? (
            <p className="p-5 text-red-700">찾지 못했습니다: {results.error.message}</p>
          ) : results.data.results.length === 0 ? (
            <p className="p-5 text-slate-500">맞는 데이터가 없습니다.</p>
          ) : (
            <ul>
              {results.data.results.map((entity) => (
                <li key={entity.urn} className="border-b border-slate-100 last:border-0">
                  <button
                    type="button"
                    onClick={() => setTrail([entity])}
                    className={`block w-full px-4 py-3 text-left hover:bg-slate-50 ${selected?.urn === entity.urn ? "bg-teal-50" : ""}`}
                  >
                    <span className="block break-all font-mono text-sm">{entity.name}</span>
                    {entity.description ? (
                      <span className="mt-1 line-clamp-2 block text-xs text-slate-500">{entity.description}</span>
                    ) : null}
                  </button>
                </li>
              ))}
            </ul>
          )}
          {results.data ? (
            <div className="flex items-center justify-between border-t border-slate-100 px-4 py-2 text-sm text-slate-600">
              <span>
                전체 {results.data.total.toLocaleString("ko-KR")}개 중 {results.data.results.length ? start + 1 : 0}–
                {start + results.data.results.length}
              </span>
              <span className="flex gap-2">
                <button
                  type="button"
                  disabled={start === 0}
                  onClick={() => setStart(Math.max(0, start - PAGE))}
                  className="rounded border border-slate-300 px-2 py-0.5 disabled:opacity-40"
                >
                  이전
                </button>
                <button
                  type="button"
                  disabled={start + PAGE >= results.data.total}
                  onClick={() => setStart(start + PAGE)}
                  className="rounded border border-slate-300 px-2 py-0.5 disabled:opacity-40"
                >
                  다음
                </button>
              </span>
            </div>
          ) : null}
        </div>

        <div>
          {trail.length > 1 ? (
            <nav className="mb-2 flex flex-wrap items-center gap-1 text-xs text-slate-500">
              {trail.map((entity, index) => (
                <span key={`${entity.urn}-${index}`} className="flex items-center gap-1">
                  {index > 0 ? <span>›</span> : null}
                  <button
                    type="button"
                    className="max-w-[16rem] truncate hover:text-slate-900 hover:underline"
                    onClick={() => setTrail(trail.slice(0, index + 1))}
                  >
                    {entity.name}
                  </button>
                </span>
              ))}
            </nav>
          ) : null}
          {selected ? (
            <EntityView client={client} urn={selected.urn} onSelect={select} />
          ) : (
            <p className="rounded-lg border border-dashed border-slate-300 p-8 text-sm text-slate-500">
              왼쪽에서 데이터를 고르면 설명과 앞뒤 흐름이 여기 나옵니다.
            </p>
          )}
        </div>
      </div>
    </section>
  );
}
