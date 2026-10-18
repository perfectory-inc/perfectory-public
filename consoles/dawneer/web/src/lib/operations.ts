// What the 운영 현황 screen derives from the platform's answers (root ADR-0165). The facts are the
// platform's: the job list, the runs and quality results the data catalog holds, and the pipeline
// graph's live overlay, and how long each job usually takes. This file only orders and counts them,
// measures them against the clock and says the codes in Korean.
import { clamp, orderBy, take } from "es-toolkit";

import type {
  DataContractQuality,
  DataQualityResult,
  PipelineGraph,
  ScheduledJobRunOutcome,
  ScheduledJobStatus,
} from "../api/client";

export const RUN_OUTCOME_LABEL: Record<ScheduledJobRunOutcome, string> = {
  running: "실행 중",
  succeeded: "성공",
  failed: "실패",
  skipped: "건너뜀",
  up_for_retry: "재시도 대기",
  unknown: "알 수 없음",
};

export const QUALITY_RESULT_LABEL: Record<DataQualityResult, string> = {
  success: "통과",
  failure: "실패",
  error: "검사 오류",
  not_run: "결과 없음",
};

/** The pipeline graph's runtime status codes (Foundation `pipeline_graph.rs`). */
export const PIPELINE_STATUS_LABEL: Record<string, string> = {
  healthy: "정상",
  stale: "주의",
  failed: "실패",
  running: "실행 중",
  unknown: "모름",
};

/** What a runtime binding of the pipeline graph observes. */
export const BINDING_KIND_LABEL: Record<string, string> = {
  ingestion_source: "원천 수집",
  lakehouse_contract: "레이크하우스 적재",
  outbox_scope: "이벤트 발행",
};

export type Tone = "bad" | "warn" | "ok" | "idle";

/** How a job's row should read at a glance. */
export function jobTone(job: ScheduledJobStatus, runsReadable: boolean): Tone {
  if (!job.enabled) return "idle";
  const outcome = job.last_run?.outcome;
  if (outcome === "failed" || outcome === "up_for_retry") return "bad";
  if (outcome === "succeeded") return "ok";
  if (outcome === undefined) return runsReadable ? "warn" : "idle";
  return outcome === "running" ? "ok" : "warn";
}

/** How long a run took or has taken, in the largest whole units. */
export function runDuration(startedAt: string | null | undefined, finishedAt: string | null | undefined, now: Date = new Date()): string {
  if (!startedAt) return "—";
  const end = finishedAt ? new Date(finishedAt) : now;
  return durationLabel(secondsBetween(startedAt, end));
}

/** A run the scheduler began whose own work has not started: it waits for a slot of its pool. */
export function isWaitingForPool(run: { outcome: string; queued_at?: string | null; started_at?: string | null }): boolean {
  return run.outcome === "running" && Boolean(run.queued_at) && !run.started_at;
}

/** How long a run waited for its pool before its work started, or null when it did not wait. */
export function waitSeconds(run: { queued_at?: string | null; started_at?: string | null }, now: Date = new Date()): number | null {
  if (!run.queued_at) return null;
  const seconds = secondsBetween(run.queued_at, run.started_at ? new Date(run.started_at) : now);
  return seconds >= 60 ? seconds : null;
}

function secondsBetween(fromIso: string, to: Date): number {
  return Math.max(0, Math.round((to.getTime() - new Date(fromIso).getTime()) / 1000));
}

/** A number of seconds in the largest whole units. */
export function durationLabel(seconds: number): string {
  if (seconds < 60) return `${seconds}초`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}분`;
  const hours = Math.floor(minutes / 60);
  return minutes % 60 === 0 ? `${hours}시간` : `${hours}시간 ${minutes % 60}분`;
}

export interface QualityRow {
  contract: DataContractQuality;
  passed: number;
  failed: number;
  errored: number;
  notRun: number;
  lastCheckedAt: string | null;
}

export interface QualitySummary {
  rows: QualityRow[];
  checks: number;
  passed: number;
  failed: number;
  errored: number;
  notRun: number;
  lastCheckedAt: string | null;
}

function later(a: string | null, b: string | null | undefined): string | null {
  if (!b) return a;
  if (!a) return b;
  return new Date(b).getTime() > new Date(a).getTime() ? b : a;
}

/** Counts each contract's results; contracts with a failure come first, then those never checked. */
export function summarizeQuality(contracts: DataContractQuality[]): QualitySummary {
  const rows = contracts.map((contract): QualityRow => {
    const count = (result: DataQualityResult) => contract.checks.filter((check) => check.result === result).length;
    return {
      contract,
      passed: count("success"),
      failed: count("failure"),
      errored: count("error"),
      notRun: count("not_run"),
      lastCheckedAt: contract.checks.reduce<string | null>((latest, check) => later(latest, check.checked_at), null),
    };
  });
  const rank = (row: QualityRow) => (row.failed + row.errored > 0 ? 0 : row.notRun > 0 ? 1 : 2);
  rows.sort((a, b) => rank(a) - rank(b) || a.contract.contract_id.localeCompare(b.contract.contract_id));
  const sum = (pick: (row: QualityRow) => number) => rows.reduce((total, row) => total + pick(row), 0);
  return {
    rows,
    checks: sum((row) => row.contract.checks.length),
    passed: sum((row) => row.passed),
    failed: sum((row) => row.failed),
    errored: sum((row) => row.errored),
    notRun: sum((row) => row.notRun),
    lastCheckedAt: rows.reduce<string | null>((latest, row) => later(latest, row.lastCheckedAt), null),
  };
}

export interface FreshnessRow {
  nodeId: string;
  title: string;
  kinds: string[];
  status: string;
  /** When the observed run finished or the batch was recorded, ISO-8601; null when not stated. */
  observedAt: string | null;
  reason: string;
}

function isoFromUnixSeconds(value: unknown): string | null {
  return typeof value === "number" && Number.isFinite(value) ? new Date(value * 1000).toISOString() : null;
}

const STATUS_RANK: Record<string, number> = { failed: 0, stale: 1, unknown: 2, running: 3, healthy: 4 };

/**
 * Every node of the pipeline graph that names a runtime binding, with what Foundation's live check
 * observed for it. A bound node the check found nothing for is listed as `unknown`, so a source
 * that never reported cannot drop out of sight. Worst first, then by name.
 */
export function freshnessRows(graph: PipelineGraph): FreshnessRow[] {
  const runtime = graph.runtime?.nodes ?? {};
  const rows = graph.nodes
    .filter((node) => (node.runtime_bindings ?? []).length > 0)
    .map((node): FreshnessRow => {
      const observed = runtime[node.id];
      const facts = observed?.observed ?? {};
      return {
        nodeId: node.id,
        title: node.title ?? node.table_name ?? node.id,
        kinds: [...new Set((node.runtime_bindings ?? []).map((binding) => binding.kind))],
        status: observed?.status ?? "unknown",
        observedAt: isoFromUnixSeconds(facts.finished_at_unix_seconds ?? facts.recorded_at_unix_seconds),
        reason: observed?.reason ?? "실시간 확인이 이 항목의 기록을 찾지 못했습니다",
      };
    });
  return rows.sort(
    (a, b) => (STATUS_RANK[a.status] ?? 2) - (STATUS_RANK[b.status] ?? 2) || a.title.localeCompare(b.title),
  );
}

/** One job the scheduler is on now, measured against the clock. */
export interface RunningJob {
  job: ScheduledJobStatus;
  /** `running`, or `up_for_retry` for a run that failed and waits for its next try. */
  outcome: "running" | "up_for_retry";
  startedAt: string | null;
  /** Seconds since the run's first start; null when its start was not recorded. */
  elapsedSeconds: number | null;
  /** Foundation's typical duration of the job; null when it has no succeeded run to go by. */
  typicalSeconds: number | null;
  typicalRuns: number;
  /** When the run should end at its typical pace, ISO-8601; null without a start or a typical duration. */
  expectedFinishAt: string | null;
  /** Seconds left until `expectedFinishAt`; 0 once it has passed. */
  remainingSeconds: number | null;
  /** Seconds past `expectedFinishAt`; 0 while it has not passed. */
  overrunSeconds: number | null;
  /** Elapsed over typical, between 0 and 1. */
  progress: number | null;
}

/**
 * Every job whose latest run is running or waiting for a retry, measured at `now`. The typical
 * duration is Foundation's (`typical_duration_seconds`); this only sets it against the clock.
 * Running before waiting, then the soonest expected finish first (an overrun is the soonest),
 * runs without an estimate last.
 */
export function runningJobs(jobs: ScheduledJobStatus[], now: Date = new Date()): RunningJob[] {
  const rows = jobs.flatMap((job): RunningJob[] => {
    const run = job.last_run;
    if (run?.outcome !== "running" && run?.outcome !== "up_for_retry") return [];
    // Queued but not started: the run waits for its pool, it is not running (`isWaitingForPool`).
    if (isWaitingForPool(run)) return [];
    const startedAt = run.started_at ?? null;
    const elapsedSeconds = startedAt ? secondsBetween(startedAt, now) : null;
    const typicalSeconds = job.typical_duration_seconds ?? null;
    const measured = elapsedSeconds !== null && typicalSeconds !== null && startedAt !== null;
    const left = measured ? typicalSeconds - elapsedSeconds : null;
    return [
      {
        job,
        outcome: run.outcome,
        startedAt,
        elapsedSeconds,
        typicalSeconds,
        typicalRuns: job.typical_duration_runs,
        expectedFinishAt: measured ? new Date(new Date(startedAt).getTime() + typicalSeconds * 1000).toISOString() : null,
        remainingSeconds: left === null ? null : Math.max(0, left),
        overrunSeconds: left === null ? null : Math.max(0, -left),
        progress: measured ? (typicalSeconds === 0 ? 1 : clamp(elapsedSeconds / typicalSeconds, 0, 1)) : null,
      },
    ];
  });
  return orderBy(
    rows,
    [
      (row) => (row.outcome === "running" ? 0 : 1),
      (row) => (row.expectedFinishAt === null ? 1 : 0),
      (row) => (row.expectedFinishAt === null ? 0 : new Date(row.expectedFinishAt).getTime()),
      (row) => row.job.job_id,
    ],
    ["asc", "asc", "asc", "asc"],
  );
}

/** The next `count` starts among the enabled jobs, soonest first. */
export function nextStarts(jobs: ScheduledJobStatus[], count = 3): ScheduledJobStatus[] {
  const scheduled = jobs.filter((job) => job.enabled && job.next_run_at);
  return take(
    orderBy(scheduled, [(job) => new Date(job.next_run_at ?? 0).getTime(), (job) => job.job_id], ["asc", "asc"]),
    count,
  );
}
