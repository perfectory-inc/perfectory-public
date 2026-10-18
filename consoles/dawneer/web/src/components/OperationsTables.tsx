// The tables of the 데이터 흐름 screen's 자세히 tabs: the job list, what runs now, the quality
// checks and collection freshness (root ADR-0165, ADR-0188). They were the 운영 현황 page.
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState, type ReactNode } from "react";

import type { DawneerClient, ScheduledJobStatus } from "../api/client";
import { ago, label } from "../lib/catalogLabels";
import {
  BINDING_KIND_LABEL,
  durationLabel,
  freshnessRows,
  jobTone,
  nextStarts,
  PIPELINE_STATUS_LABEL,
  QUALITY_RESULT_LABEL,
  RUN_OUTCOME_LABEL,
  runningJobs,
  summarizeQuality,
  type RunningJob,
  type Tone,
} from "../lib/operations";
import { runTimes } from "../lib/dataFlow";

export const TONE_CLASS: Record<Tone, string> = {
  bad: "bg-red-100 text-red-800",
  warn: "bg-amber-100 text-amber-800",
  ok: "bg-emerald-100 text-emerald-800",
  idle: "bg-slate-100 text-slate-600",
};

const PIPELINE_TONE: Record<string, Tone> = { failed: "bad", stale: "warn", unknown: "warn", running: "ok", healthy: "ok" };

export function Badge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={`inline-block whitespace-nowrap rounded px-2 py-0.5 text-xs font-medium ${TONE_CLASS[tone]}`}>{children}</span>;
}

export function When({ iso }: { iso: string | null | undefined }) {
  if (!iso) return <span className="text-slate-400">—</span>;
  return (
    <span title={iso}>
      {new Date(iso).toLocaleString("ko-KR")} <span className="text-slate-500">({ago(iso)})</span>
    </span>
  );
}

export function Section({ title, note, children }: { title: string; note: ReactNode; children: ReactNode }) {
  return (
    <section className="mt-8">
      <h2 className="text-lg font-semibold">{title}</h2>
      <p className="mt-1 text-sm text-slate-500">{note}</p>
      <div className="mt-3">{children}</div>
    </section>
  );
}

const TH = "px-3 py-2 text-left font-medium";
const TD = "px-3 py-2 align-top";

export function Loading({ what, query }: { what: string; query: { isPending: boolean; isError: boolean; error: Error | null } }) {
  if (query.isPending) return <p className="text-sm text-slate-500">불러오는 중…</p>;
  if (query.isError) return <p className="text-sm text-red-700">{what}을(를) 읽지 못했습니다: {query.error?.message}</p>;
  return null;
}

function JobRow({ job, runsReadable }: { job: ScheduledJobStatus; runsReadable: boolean }) {
  const run = job.last_run;
  return (
    <tr className="border-t border-slate-100">
      <td className={TD}>
        <div className="font-mono text-xs font-semibold text-slate-900">{job.job_id}</div>
        <div className="mt-0.5 line-clamp-2 max-w-xl text-xs text-slate-500" title={job.description}>
          {job.description}
        </div>
      </td>
      <td className={TD}>
        <div className="font-mono text-xs">{job.schedule} (UTC)</div>
        {job.enabled ? (
          <div className="mt-0.5 text-xs text-slate-600">
            다음 <When iso={job.next_run_at} />
          </div>
        ) : (
          <div className="mt-0.5 max-w-xs text-xs text-slate-500" title={job.disabled_reason ?? undefined}>
            꺼짐{job.disabled_reason ? ` · ${job.disabled_reason}` : ""}
          </div>
        )}
      </td>
      <td className={`${TD} text-xs`}>{run ? <When iso={run.queued_at ?? run.started_at ?? run.finished_at} /> : runsReadable ? "기록 없음" : "—"}</td>
      <td className={`${TD} text-xs`}>{run ? runTimes(run) : "—"}</td>
      <td className={TD}>
        {run ? (
          <Badge tone={jobTone(job, runsReadable)}>
            {RUN_OUTCOME_LABEL[run.outcome]}
            {run.native_result ? <span className="ml-1 font-mono font-normal opacity-70">{run.native_result}</span> : null}
          </Badge>
        ) : (
          <Badge tone={jobTone(job, runsReadable)}>{job.enabled ? (runsReadable ? "실행 기록 없음" : "읽지 못함") : "꺼짐"}</Badge>
        )}
      </td>
    </tr>
  );
}

export function useScheduledJobs(client: DawneerClient) {
  return useQuery({ queryKey: ["scheduled-jobs"], queryFn: () => client.scheduledJobs(), refetchInterval: 60_000 });
}

/** The clock, advanced every `everyMs`, so elapsed and remaining times move between refetches. */
export function useNow(everyMs: number): Date {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const timer = setInterval(() => setNow(new Date()), everyMs);
    return () => clearInterval(timer);
  }, [everyMs]);
  return now;
}

function RunningRow({ row }: { row: RunningJob }) {
  const overrun = (row.overrunSeconds ?? 0) > 0;
  return (
    <li className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <div>
          <span className="font-mono text-sm font-semibold text-slate-900">{row.job.job_id}</span>{" "}
          <Badge tone={row.outcome === "running" ? "ok" : "bad"}>{RUN_OUTCOME_LABEL[row.outcome]}</Badge>
          <div className="mt-0.5 line-clamp-2 max-w-3xl text-xs text-slate-500" title={row.job.description}>
            {row.job.description}
          </div>
        </div>
        <div className="text-right text-sm">
          {row.remainingSeconds === null ? (
            <span className="text-slate-500">예상 시간 없음(성공 기록 없음)</span>
          ) : overrun ? (
            <Badge tone="warn">예상보다 {durationLabel(row.overrunSeconds ?? 0)} 초과</Badge>
          ) : (
            <span className="font-semibold text-slate-900">약 {durationLabel(row.remainingSeconds)} 남음</span>
          )}
        </div>
      </div>
      {row.progress === null ? null : (
        <div className="mt-2 h-2 overflow-hidden rounded bg-slate-100">
          <div className={`h-2 ${overrun ? "bg-amber-500" : "bg-emerald-500"}`} style={{ width: `${Math.round(row.progress * 100)}%` }} />
        </div>
      )}
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600">
        <span>
          시작 <When iso={row.startedAt} />
        </span>
        <span>경과 {row.elapsedSeconds === null ? "—" : durationLabel(row.elapsedSeconds)}</span>
        {row.typicalSeconds === null ? null : (
          <span>
            보통 {durationLabel(row.typicalSeconds)} (최근 성공 {row.typicalRuns}회 기준)
          </span>
        )}
        {row.expectedFinishAt === null ? null : (
          <span>
            예상 종료 <span title={row.expectedFinishAt}>{new Date(row.expectedFinishAt).toLocaleString("ko-KR")}</span>
          </span>
        )}
      </div>
    </li>
  );
}

export function RunningNow({ client }: { client: DawneerClient }) {
  const jobs = useScheduledJobs(client);
  const now = useNow(30_000);
  const loading = Loading({ what: "지금 도는 작업", query: jobs });
  if (loading || !jobs.data) return loading;
  if (jobs.data.run_history_error != null) {
    return (
      <p className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
        데이터 카탈로그에서 실행 기록을 읽지 못해 지금 도는 작업을 알 수 없습니다({jobs.data.run_history_error}).
      </p>
    );
  }
  const rows = runningJobs(jobs.data.jobs, now);
  if (rows.length > 0) {
    return (
      <ul className="space-y-2">
        {rows.map((row) => (
          <RunningRow key={row.job.job_id} row={row} />
        ))}
      </ul>
    );
  }
  const next = nextStarts(jobs.data.jobs);
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm">
      <p className="text-slate-700">지금 도는 작업이 없습니다.</p>
      {next.length > 0 ? (
        <ul className="mt-2 space-y-1 text-xs text-slate-600">
          {next.map((job) => (
            <li key={job.job_id}>
              다음 <When iso={job.next_run_at} /> · <span className="font-mono">{job.job_id}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

export function ScheduledJobs({ client }: { client: DawneerClient }) {
  const jobs = useScheduledJobs(client);
  const loading = Loading({ what: "예약 작업", query: jobs });
  if (loading || !jobs.data) return loading;
  const runsReadable = jobs.data.run_history_error == null;
  return (
    <>
      {runsReadable ? null : (
        <p className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          데이터 카탈로그에서 실행 기록을 읽지 못했습니다({jobs.data.run_history_error}). 아래 작업에 기록이 없다고 실행되지 않은 것은 아닙니다.
        </p>
      )}
      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className={TH}>작업</th>
              <th className={TH}>일정</th>
              <th className={TH}>마지막 실행</th>
              <th className={TH}>걸린 시간</th>
              <th className={TH}>결과</th>
            </tr>
          </thead>
          <tbody>
            {jobs.data.jobs.map((job) => (
              <JobRow key={job.job_id} job={job} runsReadable={runsReadable} />
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function useQualityChecks(client: DawneerClient) {
  return useQuery({ queryKey: ["quality-checks"], queryFn: () => client.qualityChecks(), refetchInterval: 300_000 });
}

export function usePipelineGraph(client: DawneerClient) {
  return useQuery({ queryKey: ["pipeline-graph"], queryFn: () => client.pipelineGraph(), refetchInterval: 60_000 });
}

export function QualityChecks({ client }: { client: DawneerClient }) {
  const quality = useQualityChecks(client);
  const [open, setOpen] = useState<string | null>(null);
  const loading = Loading({ what: "품질 검사 결과", query: quality });
  if (loading || !quality.data) return loading;
  const summary = summarizeQuality(quality.data.contracts);
  if (summary.rows.length === 0) return <p className="text-sm text-amber-800">데이터 카탈로그에 등록된 데이터 계약이 없습니다.</p>;
  return (
    <>
      <p className="mb-3 text-sm text-slate-700">
        계약 {summary.rows.length}개 · 검사 {summary.checks}개 · 통과 {summary.passed} · 실패 {summary.failed} · 검사 오류 {summary.errored} ·
        결과 없음 {summary.notRun} · 마지막 검사 <When iso={summary.lastCheckedAt} />
      </p>
      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className={TH}>데이터 계약</th>
              <th className={TH}>검사</th>
              <th className={TH}>통과</th>
              <th className={TH}>실패</th>
              <th className={TH}>결과 없음</th>
              <th className={TH}>마지막 검사</th>
            </tr>
          </thead>
          <tbody>
            {summary.rows.map((row) => {
              const id = row.contract.contract_id;
              const troubled = row.failed + row.errored > 0;
              return (
                <tr key={id} className="border-t border-slate-100">
                  <td className={TD}>
                    <button
                      type="button"
                      className="font-mono text-xs text-slate-900 hover:underline"
                      onClick={() => setOpen(open === id ? null : id)}
                    >
                      {id}
                    </button>
                    {open === id ? (
                      <ul className="mt-2 space-y-1 text-xs">
                        {row.contract.checks.map((check) => (
                          <li key={check.assertion_urn}>
                            <Badge tone={check.result === "success" ? "ok" : check.result === "not_run" ? "idle" : "bad"}>
                              {QUALITY_RESULT_LABEL[check.result]}
                            </Badge>{" "}
                            <span className="font-mono">
                              {check.kind}
                              {check.field ? ` · ${check.field}` : ""}
                            </span>
                            {check.details.length > 0 ? (
                              <span className="ml-2 font-mono text-slate-500">
                                {check.details.map((detail) => `${detail.key}=${detail.value}`).join(" ")}
                              </span>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                  </td>
                  <td className={TD}>{row.contract.checks.length}</td>
                  <td className={TD}>{row.passed}</td>
                  <td className={TD}>{troubled ? <Badge tone="bad">{row.failed + row.errored}</Badge> : 0}</td>
                  <td className={TD}>{row.notRun > 0 ? <Badge tone="warn">{row.notRun}</Badge> : 0}</td>
                  <td className={`${TD} text-xs`}>
                    <When iso={row.lastCheckedAt} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function Freshness({ client }: { client: DawneerClient }) {
  const graph = usePipelineGraph(client);
  const loading = Loading({ what: "수집·적재 기록", query: graph });
  if (loading || !graph.data) return loading;
  const rows = freshnessRows(graph.data);
  return (
    <>
      {graph.data.runtime?.database_ready === false ? (
        <p className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          Foundation 이 기록 DB 에 닿지 못해 아래 상태를 확인하지 못했습니다.
        </p>
      ) : null}
      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className={TH}>데이터</th>
              <th className={TH}>구분</th>
              <th className={TH}>상태</th>
              <th className={TH}>마지막 기록</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.nodeId} className="border-t border-slate-100">
                <td className={TD}>
                  <div className="text-sm text-slate-900">{row.title}</div>
                  <div className="font-mono text-xs text-slate-400">{row.nodeId}</div>
                </td>
                <td className={`${TD} text-xs`}>{row.kinds.map((kind) => label(BINDING_KIND_LABEL, kind)).join(", ")}</td>
                <td className={TD}>
                  <span title={row.reason}>
                    <Badge tone={PIPELINE_TONE[row.status] ?? "warn"}>{label(PIPELINE_STATUS_LABEL, row.status)}</Badge>
                  </span>
                </td>
                <td className={`${TD} text-xs`}>
                  <When iso={row.observedAt} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
