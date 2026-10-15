// 데이터 흐름 (root ADR-0188): one screen for where the data is and what works on it now — the
// "single pane of glass" Dagster's global asset lineage is, where the catalog and the live run
// status are the same graph. It replaces the 운영 현황 and 데이터 카탈로그 menus; everything those
// showed is in the side panel or a tab below the graph.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { DawneerClient } from "../api/client";
import { CatalogSearch } from "../components/CatalogSearch";
import { FlowDetails, type Selection } from "../components/FlowDetails";
import { FlowGraph, StatusPill } from "../components/FlowGraph";
import {
  Freshness,
  Loading,
  QualityChecks,
  RunningNow,
  ScheduledJobs,
  useNow,
  usePipelineGraph,
  useQualityChecks,
  useScheduledJobs,
} from "../components/OperationsTables";
import { RunTimeline } from "../components/RunTimeline";
import {
  catalogUrnOf,
  contractOf,
  flowSummary,
  flowView,
  jobStates,
  jobTitle,
  problemRows,
  problemSentence,
  summarySentence,
  type ProblemRow,
} from "../lib/dataFlow";
import { FLOW_TABS, type FlowFocus, type FlowTab } from "../lib/routes";

/** What a link's focus opens: a job's panel, or a dataset's — turning on every connection when the plain view hides it.
 * Nothing for a dataset the map does not have. */
export function focusTarget(
  focus: FlowFocus,
  shown: ReadonlySet<string>,
  known: ReadonlySet<string>,
): { selection: Selection; everything: boolean } | null {
  if (focus.kind === "job") return { selection: { kind: "job", id: focus.id }, everything: false };
  if (!known.has(focus.id)) return null;
  return { selection: { kind: "dataset", id: focus.id }, everything: !shown.has(focus.id) };
}

export function FlowPage({ client, tab, focus }: { client: DawneerClient; tab: FlowTab; focus: FlowFocus | null }) {
  const jobs = useScheduledJobs(client);
  const graph = usePipelineGraph(client);
  const quality = useQualityChecks(client);
  const now = useNow(30_000);
  const [everything, setEverything] = useState(false);
  const [selection, setSelection] = useState<Selection | null>(null);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setSelection(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const jobList = useMemo(() => jobs.data?.jobs ?? [], [jobs.data]);
  const runsReadable = jobs.data?.run_history_error == null;
  const states = useMemo(() => jobStates(jobList, now, runsReadable), [jobList, now, runsReadable]);
  const view = useMemo(() => (graph.data ? flowView(graph.data, jobList, everything) : null), [graph.data, jobList, everything]);
  const titles = useMemo(() => new Map(jobList.map((job) => [job.job_id, jobTitle(job)])), [jobList]);
  const titleOf = useCallback((jobId: string) => titles.get(jobId) ?? jobId, [titles]);
  const selectDataset = useCallback((id: string) => setSelection({ kind: "dataset", id }), []);

  const problems = useMemo(() => problemRows(jobList), [jobList]);
  const shownNodes = useMemo(() => new Set(view?.nodes.map((candidate) => candidate.id) ?? []), [view]);
  const openProblem = useCallback(
    (row: ProblemRow) =>
      setSelection(row.nodeId && shownNodes.has(row.nodeId) ? { kind: "dataset", id: row.nodeId } : { kind: "job", id: row.job.job_id }),
    [shownNodes],
  );

  // A link (a problem's Slack notice) opens its panel once, when the map is known.
  const applied = useRef<string | null>(null);
  const focusKey = focus ? `${focus.kind}:${focus.id}` : null;
  useEffect(() => {
    if (!focus || !graph.data || !jobs.data || !view || applied.current === focusKey) return;
    applied.current = focusKey;
    const target = focusTarget(focus, shownNodes, new Set(graph.data.nodes.map((candidate) => candidate.id)));
    if (!target) return;
    if (target.everything) setEverything(true);
    setSelection(target.selection);
  }, [focus, focusKey, graph.data, jobs.data, view, shownNodes]);

  const loading = Loading({ what: "데이터 흐름", query: jobs }) ?? Loading({ what: "파이프라인 지도", query: graph });
  const node = selection?.kind === "dataset" ? (view?.nodes.find((candidate) => candidate.id === selection.id) ?? null) : null;

  return (
    <section>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold">데이터 흐름</h1>
          <p className="mt-1 text-sm text-slate-600">정부 자료를 받아서 정리하고, 합치고, 구워서 서비스에 내보내기까지. 칸을 누르면 자세히 볼 수 있어요.</p>
        </div>
        <label className="flex items-center gap-2 text-sm text-slate-700">
          <input type="checkbox" checked={everything} onChange={(event) => setEverything(event.target.checked)} />
          예약 작업이 없는 연결까지 모두 보기
        </label>
      </div>

      {jobs.data && runsReadable ? <HealthLine rows={problems} titleOf={titleOf} now={now} onOpen={openProblem} /> : null}
      {jobs.data ? (
        <p className="mt-2 rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white" role="status">
          {summarySentence(flowSummary(jobList, states), now)}
        </p>
      ) : null}
      {jobs.data?.scheduler_error ? (
        <p className="mt-2 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          실패 원인을 Airflow 로그에서 읽지 못했어요({jobs.data.scheduler_error}).
        </p>
      ) : null}
      {jobs.data?.lakehouse_error ? (
        <p className="mt-2 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          표의 스냅숏을 읽지 못해 쓴 행 수와 양 이상은 알 수 없어요({jobs.data.lakehouse_error}).
        </p>
      ) : null}
      {jobs.data && !runsReadable ? (
        <p className="mt-2 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          데이터 카탈로그에서 실행 기록을 읽지 못해 지금 상태를 알 수 없어요({jobs.data.run_history_error}).
        </p>
      ) : null}

      <div className={`mt-4 grid gap-4 ${selection ? "lg:grid-cols-[minmax(0,1fr)_26rem]" : ""}`}>
        <div className="min-w-0">
          {loading ?? (view ? <FlowGraph view={view} states={states} titleOf={titleOf} now={now} selected={node?.id ?? null} onSelect={selectDataset} /> : null)}
        </div>
        {selection ? (
          <FlowDetails
            key={`${selection.kind}-${selection.id}`}
            client={client}
            selection={selection}
            node={node}
            jobs={jobList}
            states={states}
            contract={node ? contractOf(node.node, quality.data?.contracts ?? []) : null}
            catalogUrn={node ? catalogUrnOf(node.id, jobList) : null}
            links={jobs.data?.links}
            onSelectDataset={selectDataset}
            titleOf={titleOf}
            now={now}
            onClose={() => setSelection(null)}
          />
        ) : null}
      </div>

      <nav className="mt-8 flex flex-wrap gap-1 border-b border-slate-200" aria-label="자세히">
        {FLOW_TABS.map((item) => (
          <a
            key={item.id}
            href={`#/flow/${item.id}`}
            aria-current={item.id === tab ? "page" : undefined}
            className={`rounded-t-lg px-4 py-2 text-sm ${item.id === tab ? "border border-b-white border-slate-200 bg-white font-semibold text-slate-900" : "text-slate-600 hover:text-slate-900"}`}
          >
            {item.label}
          </a>
        ))}
      </nav>
      <div className="mt-4">
        {tab === "timeline" ? (
          jobs.data ? (
            <RunTimeline jobs={jobList} now={now} onSelectJob={(id) => setSelection({ kind: "job", id })} />
          ) : null
        ) : tab === "jobs" ? (
          <div className="space-y-6">
            <div>
              <h2 className="mb-2 text-lg font-semibold">지금 도는 작업</h2>
              <RunningNow client={client} />
            </div>
            <div>
              <h2 className="mb-2 text-lg font-semibold">예약 작업</h2>
              <ScheduledJobs client={client} />
            </div>
          </div>
        ) : tab === "quality" ? (
          <QualityChecks client={client} />
        ) : tab === "freshness" ? (
          <Freshness client={client} />
        ) : (
          <CatalogSearch client={client} />
        )}
      </div>
    </section>
  );
}

/** The top line (root ADR-0195): all fine, or how many problems and each one, a click away. */
export function HealthLine({
  rows,
  titleOf,
  now,
  onOpen,
}: {
  rows: ProblemRow[];
  titleOf: (jobId: string) => string;
  now: Date;
  onOpen: (row: ProblemRow) => void;
}) {
  if (rows.length === 0) {
    return (
      <p className="mt-4 rounded-lg bg-emerald-600 px-4 py-3 text-lg font-bold text-white" role="status">
        <span aria-hidden>✓</span> 전체 정상
      </p>
    );
  }
  return (
    <section className="mt-4 rounded-lg border-2 border-red-600 bg-red-50 px-4 py-3" aria-labelledby="health-line">
      {/* The live region wraps the heading: role="status" on the h2 would take its heading role away. */}
      <div role="status">
        <h2 id="health-line" className="text-lg font-bold text-red-800">
          <span aria-hidden>⚠</span> 문제 {rows.length}건
        </h2>
      </div>
      <ul className="mt-2 space-y-1">
        {rows.map((row) => (
          <li key={`${row.job.job_id}-${row.problem.kind}-${row.problem.kind === "volume" ? row.problem.node_id : ""}`}>
            <button
              type="button"
              onClick={() => onOpen(row)}
              className="flex w-full flex-wrap items-center gap-2 rounded px-2 py-1 text-left text-sm hover:bg-red-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-700"
            >
              <StatusPill status={row.problem.kind} />
              <span className="font-semibold text-slate-900">{titleOf(row.job.job_id)}</span>
              <span className="min-w-0 break-words text-slate-700">{problemSentence(row.problem, titleOf, now)}</span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
