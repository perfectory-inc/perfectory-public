// What the 운영 현황 screen derives from the platform's answers (root ADR-0165). The facts are the
// platform's: the job list, the runs and quality results the data catalog holds, and the pipeline
// graph's live overlay. This file only orders and counts them and says the codes in Korean.
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
  const seconds = Math.max(0, Math.round((end.getTime() - new Date(startedAt).getTime()) / 1000));
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
