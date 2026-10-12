// The side panel of the 데이터 흐름 screen (root ADR-0188): what a dataset is, the jobs that make
// it with their schedule and recent runs, its data contract's checks, and its catalog entry with
// columns and lineage — what the old 데이터 카탈로그 entity view and 운영 현황 job rows showed.
import { useState } from "react";

import type { CatalogEntity, DataContractQuality, DawneerClient, ScheduledJobStatus } from "../api/client";
import { datasetKind, jobTitle, statusNote, type FlowNode, type JobState } from "../lib/dataFlow";
import { durationLabel, QUALITY_RESULT_LABEL, RUN_OUTCOME_LABEL, runDuration } from "../lib/operations";
import { EntityView } from "./CatalogSearch";
import { StatusPill } from "./FlowGraph";
import { Badge, When } from "./OperationsTables";

export type Selection = { kind: "dataset"; id: string } | { kind: "job"; id: string };

function JobCard({ job, state, titleOf, now }: { job: ScheduledJobStatus; state: JobState | undefined; titleOf: (jobId: string) => string; now: Date }) {
  return (
    <li className="rounded-lg border border-slate-200 p-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold text-slate-900">{jobTitle(job)}</span>
        {state ? <StatusPill status={state.status} /> : null}
      </div>
      <div className="font-mono text-xs text-slate-500">{job.job_id}</div>
      {state ? <p className="mt-1 text-sm text-slate-700">{statusNote(state, titleOf, now)}</p> : null}
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
        <ul className="mt-1 space-y-1 text-xs">
          {job.recent_runs.map((run) => (
            <li key={run.run_urn} className="flex flex-wrap items-center gap-2">
              <Badge tone={run.outcome === "succeeded" ? "ok" : run.outcome === "failed" || run.outcome === "up_for_retry" ? "bad" : "idle"}>
                {RUN_OUTCOME_LABEL[run.outcome]}
              </Badge>
              <When iso={run.started_at ?? run.finished_at} />
              <span className="text-slate-500">{runDuration(run.started_at, run.finished_at, now)}</span>
            </li>
          ))}
        </ul>
      )}
    </li>
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
  titleOf,
  now,
  onClose,
}: {
  client: DawneerClient;
  selection: Selection;
  node: FlowNode | null;
  jobs: ScheduledJobStatus[];
  states: Map<string, JobState>;
  contract: DataContractQuality | null;
  catalogUrn: string | null;
  titleOf: (jobId: string) => string;
  now: Date;
  onClose: () => void;
}) {
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
              <h2 className="text-lg font-semibold text-slate-900">{node.title}</h2>
              <p className="font-mono text-xs text-slate-400">{node.node.table_name ?? node.id}</p>
            </>
          ) : (
            <h2 className="text-lg font-semibold text-slate-900">{titleOf(selection.id)}</h2>
          )}
        </div>
        <button type="button" onClick={onClose} className="rounded border border-slate-300 px-2 py-0.5 text-sm hover:bg-slate-100" aria-label="닫기">
          ✕
        </button>
      </div>
      {node?.node.description ? <p className="mt-2 text-sm text-slate-700">{node.node.description}</p> : null}

      <h3 className="mt-4 text-sm font-semibold">{selection.kind === "job" ? "작업" : "이 데이터를 만드는 작업"}</h3>
      {makers.length === 0 ? (
        <p className="text-sm text-slate-500">예약된 작업이 만들지 않습니다(사람이나 다른 시스템이 넣어요).</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {makers.flatMap((jobId) => {
            const job = byId.get(jobId);
            return job ? [<JobCard key={jobId} job={job} state={states.get(jobId)} titleOf={titleOf} now={now} />] : [];
          })}
        </ul>
      )}
      {readers.length > 0 ? (
        <p className="mt-2 text-xs text-slate-600">이 데이터를 읽는 작업: {readers.map(titleOf).join(", ")}</p>
      ) : null}

      {node ? (
        <>
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
