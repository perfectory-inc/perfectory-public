// The side panel of the 데이터 흐름 screen (root ADR-0188): what a dataset is, the jobs that make
// it with their schedule and recent runs, its data contract's checks, and its catalog entry with
// columns and lineage — what the old 데이터 카탈로그 entity view and 운영 현황 job rows showed.
// Root ADR-0195 adds what Marquez shows for a dataset: each run with its rows, the table's Iceberg
// snapshots with the run that made each and the snapshots built from it, and links to the
// scheduler's run log and the data catalog's lineage page.
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import type { CatalogEntity, DataContractQuality, DatasetSnapshot, DawneerClient, OperationsLinks, ScheduledJobStatus } from "../api/client";
import {
  catalogLineageUrl,
  datasetKind,
  jobTitle,
  problemSentence,
  schedulerUrl,
  statusNote,
  type FlowNode,
  type JobState,
} from "../lib/dataFlow";
import { durationLabel, QUALITY_RESULT_LABEL, RUN_OUTCOME_LABEL, runDuration } from "../lib/operations";
import { EntityView } from "./CatalogSearch";
import { StatusPill } from "./FlowGraph";
import { Badge, When } from "./OperationsTables";

export type Selection = { kind: "dataset"; id: string } | { kind: "job"; id: string };

/** Snapshots shown before the rest are asked for. */
const SNAPSHOTS_SHOWN = 10;

function ExternalLink({ href, children }: { href: string | null; children: string }) {
  if (!href) return null;
  return (
    <a href={href} target="_blank" rel="noreferrer" className="text-xs font-semibold text-sky-700 underline hover:text-sky-900">
      {children} ↗
    </a>
  );
}

function JobCard({
  job,
  state,
  titleOf,
  now,
  links,
}: {
  job: ScheduledJobStatus;
  state: JobState | undefined;
  titleOf: (jobId: string) => string;
  now: Date;
  links: OperationsLinks | undefined;
}) {
  const problems = job.problems ?? [];
  const failed = problems.find((problem) => problem.kind === "failed");
  return (
    <li className="rounded-lg border border-slate-200 p-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold text-slate-900">{jobTitle(job)}</span>
        {state ? <StatusPill status={state.status} /> : null}
      </div>
      <div className="font-mono text-xs text-slate-500">{job.job_id}</div>
      {state && problems.length === 0 ? <p className="mt-1 text-sm text-slate-700">{statusNote(state, titleOf, now)}</p> : null}
      {problems.length > 0 ? (
        <ul className="mt-2 space-y-1">
          {problems.map((problem, index) => (
            <li key={index} className="flex flex-wrap items-start gap-2 text-xs">
              <StatusPill status={problem.kind} />
              <span className="min-w-0 break-words text-slate-800">{problemSentence(problem, titleOf, now)}</span>
            </li>
          ))}
        </ul>
      ) : null}
      <div className="mt-2 flex flex-wrap gap-3">
        <ExternalLink href={schedulerUrl(links, job.dag_id, failed?.kind === "failed" ? failed.scheduler_run_id : null)}>
          {failed ? "Airflow 실패 로그" : "Airflow 실행 기록"}
        </ExternalLink>
        <ExternalLink href={catalogLineageUrl(links, job.catalog_job_urn ?? null)}>DataHub 계보</ExternalLink>
      </div>
      <p className="mt-2 text-xs text-slate-600">{job.description}</p>
      <dl className="mt-2 grid grid-cols-[5rem_1fr] gap-x-2 gap-y-1 text-xs">
        <dt className="text-slate-500">일정</dt>
        <dd>
          <span className="font-mono">{job.schedule}</span> (UTC)
          {job.started_by === "inputs" ? " · 앞 단계가 바꾸면 바로 시작" : ""}
        </dd>
        <dt className="text-slate-500">다음 실행</dt>
        <dd>{job.enabled ? <When iso={job.next_run_at} /> : `꺼짐${job.disabled_reason ? ` · ${job.disabled_reason}` : ""}`}</dd>
        <dt className="text-slate-500">앞 단계</dt>
        <dd>{job.upstream_job_ids.length > 0 ? job.upstream_job_ids.map(titleOf).join(", ") : "없음"}</dd>
        {job.typical_duration_seconds == null ? null : (
          <>
            <dt className="text-slate-500">보통 걸림</dt>
            <dd>
              {durationLabel(job.typical_duration_seconds)} (최근 성공 {job.typical_duration_runs}회)
            </dd>
          </>
        )}
      </dl>
      <h4 className="mt-3 text-xs font-semibold text-slate-700">최근 실행</h4>
      {job.recent_runs.length === 0 ? (
        <p className="text-xs text-slate-500">기록 없음</p>
      ) : (
        <table className="mt-1 w-full text-left text-xs">
          <thead className="text-slate-500">
            <tr>
              <th scope="col" className="py-0.5 font-normal">
                시작
              </th>
              <th scope="col" className="py-0.5 font-normal">
                걸린 시간
              </th>
              <th scope="col" className="py-0.5 text-right font-normal">
                쓴 행
              </th>
              <th scope="col" className="py-0.5 text-right font-normal">
                결과
              </th>
            </tr>
          </thead>
          <tbody>
            {job.recent_runs.map((run) => (
              <tr key={run.run_urn} className="border-t border-slate-100">
                <td className="py-0.5">
                  <When iso={run.started_at ?? run.finished_at} />
                </td>
                <td className="py-0.5 text-slate-600">{runDuration(run.started_at, run.finished_at, now)}</td>
                <td className="py-0.5 text-right tabular-nums text-slate-600">{run.rows_written == null ? "–" : run.rows_written.toLocaleString("ko-KR")}</td>
                <td className="py-0.5 text-right">
                  <Badge tone={run.outcome === "succeeded" ? "ok" : run.outcome === "failed" || run.outcome === "up_for_retry" ? "bad" : "idle"}>
                    {RUN_OUTCOME_LABEL[run.outcome]}
                  </Badge>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </li>
  );
}

type SnapshotLink = DatasetSnapshot["read_by"][number];

function SnapshotLinks({ label, links, onSelectDataset }: { label: string; links: SnapshotLink[]; onSelectDataset: (nodeId: string) => void }) {
  if (links.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-1 text-slate-600">
      {label}:
      {links.map((link) => {
        const target = link.node_id;
        return target ? (
          <button
            key={`${link.table}-${link.snapshot_id}`}
            type="button"
            onClick={() => onSelectDataset(target)}
            className="rounded bg-slate-100 px-1 font-mono text-[11px] text-sky-800 underline hover:bg-slate-200"
          >
            {link.table}
          </button>
        ) : (
          <span key={`${link.table}-${link.snapshot_id}`} className="rounded bg-slate-100 px-1 font-mono text-[11px]">
            {link.table}
          </span>
        );
      })}
    </div>
  );
}

/** The table's Iceberg snapshots: when, which run, how many rows, and what read it. */
function Snapshots({
  client,
  nodeId,
  titleOf,
  onSelectDataset,
}: {
  client: DawneerClient;
  nodeId: string;
  titleOf: (jobId: string) => string;
  onSelectDataset: (nodeId: string) => void;
}) {
  const [all, setAll] = useState(false);
  const answer = useQuery({ queryKey: ["dataset-snapshots", nodeId], queryFn: () => client.datasetSnapshots(nodeId), refetchInterval: 60_000 });
  if (answer.isPending) return <p className="text-xs text-slate-500">읽는 중…</p>;
  if (answer.isError) return <p className="text-xs text-red-700">읽지 못했어요: {answer.error.message}</p>;
  const { table, snapshots, error } = answer.data;
  if (!table) return <p className="text-xs text-slate-500">Iceberg 표가 아니에요.</p>;
  if (error) return <p className="text-xs text-amber-800">스냅숏을 읽지 못했어요({error}).</p>;
  if (snapshots.length === 0) return <p className="text-xs text-slate-500">스냅숏이 없어요.</p>;
  const shown = all ? snapshots : snapshots.slice(0, SNAPSHOTS_SHOWN);
  return (
    <>
      <ol className="mt-1 space-y-2 text-xs">
        {shown.map((snapshot) => (
          <li key={snapshot.snapshot_id} className="rounded border border-slate-100 p-2">
            <div className="flex flex-wrap items-center gap-2">
              <When iso={snapshot.committed_at ?? null} />
              <span className="text-slate-500">{snapshot.operation ?? ""}</span>
              <span className="ml-auto tabular-nums text-slate-700">
                +{(snapshot.added_records ?? 0).toLocaleString("ko-KR")}행
                {snapshot.total_records == null ? "" : ` / 모두 ${snapshot.total_records.toLocaleString("ko-KR")}행`}
              </span>
            </div>
            <div className="text-slate-600">{snapshot.job_id ? `만든 실행: ${titleOf(snapshot.job_id)}` : "예약 작업 밖에서 만들어졌어요"}</div>
            {snapshot.ingested_objects.length > 0 ? <div className="text-slate-600">받은 파일 {snapshot.ingested_objects.length}개를 넣었어요</div> : null}
            <SnapshotLinks label="읽은 것" links={snapshot.read_from} onSelectDataset={onSelectDataset} />
            <SnapshotLinks label="이걸 읽은 것" links={snapshot.read_by} onSelectDataset={onSelectDataset} />
          </li>
        ))}
      </ol>
      {!all && snapshots.length > SNAPSHOTS_SHOWN ? (
        <button type="button" onClick={() => setAll(true)} className="mt-1 text-xs text-sky-700 underline">
          {snapshots.length - SNAPSHOTS_SHOWN}개 더 보기
        </button>
      ) : null}
    </>
  );
}

function ContractChecks({ contract }: { contract: DataContractQuality }) {
  return (
    <ul className="space-y-1 text-xs">
      {contract.checks.map((check) => (
        <li key={check.assertion_urn} className="flex flex-wrap items-center gap-2">
          <Badge tone={check.result === "success" ? "ok" : check.result === "not_run" ? "idle" : "bad"}>{QUALITY_RESULT_LABEL[check.result]}</Badge>
          <span className="font-mono">
            {check.kind}
            {check.field ? ` · ${check.field}` : ""}
          </span>
        </li>
      ))}
    </ul>
  );
}

export function FlowDetails({
  client,
  selection,
  node,
  jobs,
  states,
  contract,
  catalogUrn,
  links,
  titleOf,
  now,
  onSelectDataset,
  onClose,
}: {
  client: DawneerClient;
  selection: Selection;
  node: FlowNode | null;
  jobs: ScheduledJobStatus[];
  states: Map<string, JobState>;
  contract: DataContractQuality | null;
  catalogUrn: string | null;
  links: OperationsLinks | undefined;
  titleOf: (jobId: string) => string;
  now: Date;
  onSelectDataset: (nodeId: string) => void;
  onClose: () => void;
}) {
  // Opened from the graph or the problem list, the panel takes the keyboard focus.
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => heading.current?.focus(), []);
  const [catalogUrnShown, setCatalogUrnShown] = useState<string | null>(null);
  const byId = new Map(jobs.map((job) => [job.job_id, job]));
  const makers = selection.kind === "job" ? [selection.id] : (node?.writers ?? []);
  const readers = node ? jobs.filter((job) => job.reads.some((dataset) => dataset.node_id === node.id)).map((job) => job.job_id) : [];
  const kind = node ? datasetKind(node.node) : null;
  const shown = catalogUrnShown ?? catalogUrn;

  return (
    <aside className="flex max-h-[calc(100vh-8rem)] flex-col overflow-y-auto rounded-xl border border-slate-200 bg-white p-4" aria-label="자세히">
      <div className="flex items-start justify-between gap-2">
        <div>
          {node && kind ? (
            <>
              <p className="text-xs text-slate-500">
                <span aria-hidden>{kind.icon}</span> {kind.word}
              </p>
              <h2 ref={heading} tabIndex={-1} className="text-lg font-semibold text-slate-900 outline-none">
                {node.title}
              </h2>
              <p className="font-mono text-xs text-slate-400">{node.node.table_name ?? node.id}</p>
            </>
          ) : (
            <h2 ref={heading} tabIndex={-1} className="text-lg font-semibold text-slate-900 outline-none">
              {titleOf(selection.id)}
            </h2>
          )}
        </div>
        <button type="button" onClick={onClose} className="rounded border border-slate-300 px-2 py-0.5 text-sm hover:bg-slate-100" aria-label="닫기">
          ✕
        </button>
      </div>
      {node?.node.description ? <p className="mt-2 text-sm text-slate-700">{node.node.description}</p> : null}
      {node ? (
        <div className="mt-2">
          <ExternalLink href={catalogLineageUrl(links, catalogUrn)}>DataHub 계보</ExternalLink>
        </div>
      ) : null}

      <h3 className="mt-4 text-sm font-semibold">{selection.kind === "job" ? "작업" : "이 데이터를 만드는 작업"}</h3>
      {makers.length === 0 ? (
        <p className="text-sm text-slate-500">예약된 작업이 만들지 않습니다(사람이나 다른 시스템이 넣어요).</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {makers.flatMap((jobId) => {
            const job = byId.get(jobId);
            return job ? [<JobCard key={jobId} job={job} state={states.get(jobId)} titleOf={titleOf} now={now} links={links} />] : [];
          })}
        </ul>
      )}
      {readers.length > 0 ? (
        <p className="mt-2 text-xs text-slate-600">이 데이터를 읽는 작업: {readers.map(titleOf).join(", ")}</p>
      ) : null}

      {node ? (
        <>
          <h3 className="mt-4 text-sm font-semibold">스냅숏 (Iceberg)</h3>
          <Snapshots client={client} nodeId={node.id} titleOf={titleOf} onSelectDataset={onSelectDataset} />

          <h3 className="mt-4 text-sm font-semibold">품질 검사</h3>
          {contract ? <ContractChecks contract={contract} /> : <p className="text-sm text-slate-500">데이터 계약이 없습니다.</p>}

          <h3 className="mt-4 text-sm font-semibold">데이터 카탈로그</h3>
          {shown ? (
            <div className="mt-2">
              <EntityView client={client} urn={shown} onSelect={(entity: CatalogEntity) => setCatalogUrnShown(entity.urn)} compact />
            </div>
          ) : (
            <p className="text-sm text-slate-500">카탈로그 이름을 알 수 없습니다. 아래 ‘데이터 찾기’에서 찾아보세요.</p>
          )}
        </>
      ) : null}
    </aside>
  );
}
