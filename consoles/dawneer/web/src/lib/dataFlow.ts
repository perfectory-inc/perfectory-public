// What the 데이터 흐름 screen derives (root ADR-0188). The facts are the platform's: the pipeline
// graph's datasets and edges, which scheduled job carries which edge (the job list) and writes and
// waits on what (`job_specs.py`'s derivation, served by Foundation), and how each job's runs stand.
// This file only places the datasets in stages, gives each one the state of the job that makes
// it, and says those states in plain Korean.
import { maxBy, minBy, orderBy, uniq } from "es-toolkit";

import type { DataContractQuality, PipelineGraph, ScheduledJobStatus } from "../api/client";
import { runningJobs, type RunningJob } from "./operations";

/** The stages of the flow, left to right. */
export const STAGES = ["collect", "silver", "gold", "bake", "serve"] as const;
export type Stage = (typeof STAGES)[number];

export const STAGE_TEXT: Record<Stage, { title: string; sentence: string }> = {
  collect: { title: "① 자료 받기", sentence: "여기서 정부·공공기관 자료를 받아와요" },
  silver: { title: "② 정리하기", sentence: "받은 자료를 깔끔한 표로 정리해요" },
  gold: { title: "③ 합치기", sentence: "정리한 표를 합쳐서 쓰기 좋게 만들어요" },
  bake: { title: "④ 굽기", sentence: "지도와 화면에 바로 쓸 파일로 구워요" },
  serve: { title: "⑤ 서비스", sentence: "사람들이 보는 서비스로 내보내요" },
};

/** Serving surfaces a bake makes (the scheduler's `BAKED_SURFACE_KINDS`, job_specs.py). */
const BAKED_SURFACES = new Set(["tiles", "r2_baked_documents"]);

type RegistryNode = PipelineGraph["nodes"][number];
/** What a dataset's stage and kind are read from: a registry node, or a dataset a job names. */
type Kinded = { type?: string | undefined; surface_kind?: string | null | undefined };

/** The stage a dataset of the pipeline graph belongs to, by its kind. */
export function datasetStage(node: Kinded): Stage {
  switch (node.type) {
    case "source_group":
      return "collect";
    case "silver_table":
    case "reference_table":
      return "silver";
    case "gold_table":
      return "gold";
    case "serving_surface":
      return BAKED_SURFACES.has(node.surface_kind ?? "") ? "bake" : "serve";
    default:
      return "serve";
  }
}

/** An icon and a plain word for what a dataset is. */
export function datasetKind(node: Kinded): { icon: string; word: string } {
  switch (node.type) {
    case "source_group":
      return { icon: "📥", word: "받은 원천 자료" };
    case "silver_table":
      return { icon: "🗂️", word: "정리된 표" };
    case "reference_table":
      return { icon: "📖", word: "기준 코드표" };
    case "gold_table":
      return { icon: "⭐", word: "합친 표" };
    case "serving_group":
      return { icon: "🌐", word: "서비스" };
    default:
      break;
  }
  switch (node.surface_kind) {
    case "tiles":
      return { icon: "🗺️", word: "지도 타일" };
    case "r2_baked_documents":
      return { icon: "📦", word: "구운 파일" };
    case "scheduler":
      return { icon: "⏰", word: "예약 실행기" };
    case "gateway":
    case "api":
      return { icon: "🚪", word: "들어오는 입구" };
    case "consumer_receiver":
      return { icon: "📬", word: "받는 곳" };
    case "missing_capability":
      return { icon: "🚧", word: "아직 만드는 중인 기능" };
    default:
      return { icon: "🌐", word: "서비스" };
  }
}

export interface FlowNode {
  id: string;
  title: string;
  stage: Stage;
  node: RegistryNode;
  /** The jobs that write it, in job-list order. */
  writers: string[];
}

export interface FlowEdge {
  id: string;
  from: string;
  to: string;
  /** The jobs that carry it out, in job-list order; empty when no scheduled job does. */
  jobs: string[];
}

export interface FlowView {
  nodes: FlowNode[];
  edges: FlowEdge[];
}

/**
 * The datasets and edges to draw. By default, the flow the scheduler runs: every pipeline-graph
 * edge a scheduled job carries out and its two datasets. With `everything`, the whole pipeline
 * graph, edges no job carries included. A dataset nothing in view leads into (where data enters)
 * sits no later than the earliest stage it feeds, so data enters from the left.
 */
export function flowView(graph: PipelineGraph, jobs: ScheduledJobStatus[], everything = false): FlowView {
  const carriers = new Map<string, string[]>();
  for (const job of jobs) {
    for (const edge of job.pipeline_graph_edges) carriers.set(edge, [...(carriers.get(edge) ?? []), job.job_id]);
  }
  const registryNodes = new Map(graph.nodes.map((node) => [node.id, node]));
  const edges = (graph.edges ?? [])
    .filter((edge) => everything || carriers.has(edge.id))
    .filter((edge) => registryNodes.has(edge.from) && registryNodes.has(edge.to))
    .map((edge): FlowEdge => ({ id: edge.id, from: edge.from, to: edge.to, jobs: carriers.get(edge.id) ?? [] }));
  const shown = everything ? graph.nodes.map((node) => node.id) : uniq(edges.flatMap((edge) => [edge.from, edge.to]));
  const writers = new Map<string, string[]>();
  for (const job of jobs) {
    for (const dataset of job.writes) writers.set(dataset.node_id, [...(writers.get(dataset.node_id) ?? []), job.job_id]);
  }
  const own = (id: string) => datasetStage(registryNodes.get(id) ?? {});
  const entered = new Set(edges.map((edge) => edge.to));
  const nodes = shown.flatMap((id): FlowNode[] => {
    const node = registryNodes.get(id);
    if (!node) return [];
    const fed = edges.filter((edge) => edge.from === id).map((edge) => own(edge.to));
    const earliest = minBy([own(id), ...fed], (stage) => STAGES.indexOf(stage)) ?? own(id);
    return [
      {
        id,
        title: node.title ?? node.table_name ?? id,
        stage: entered.has(id) ? own(id) : earliest,
        node,
        writers: writers.get(id) ?? [],
      },
    ];
  });
  return {
    nodes: orderBy(nodes, [(node) => node.id], ["asc"]),
    edges: orderBy(edges, [(edge) => edge.id], ["asc"]),
  };
}

/** A job another job waits on: running now, or failed and waiting for its next try. */
export interface Waiting {
  /** The running upstream jobs, in the order Foundation lists the job's upstream. */
  on: string[];
  /** True when the job is started by its inputs, so it starts once they finish. */
  startsAfter: boolean;
}

function busy(job: ScheduledJobStatus): boolean {
  return job.last_run?.outcome === "running" || job.last_run?.outcome === "up_for_retry";
}

/**
 * Every enabled job, not itself running, with a direct upstream job that is running or waiting for
 * a retry. Airflow starts a job that is `started_by: inputs` when such a run changes its inputs
 * (root ADR-0171); a scheduled one only reads what that run left.
 */
export function waitingJobs(jobs: ScheduledJobStatus[]): Map<string, Waiting> {
  const running = new Set(jobs.filter(busy).map((job) => job.job_id));
  const waiting = new Map<string, Waiting>();
  for (const job of jobs) {
    if (!job.enabled || running.has(job.job_id)) continue;
    const on = job.upstream_job_ids.filter((upstream) => running.has(upstream));
    if (on.length > 0) waiting.set(job.job_id, { on, startsAfter: job.started_by === "inputs" });
  }
  return waiting;
}

export type FlowStatus = "running" | "retry" | "failed" | "waiting" | "done" | "skipped" | "never" | "off" | "manual" | "unread";

/** One icon and one word per state, so colour is never the only signal. */
export const STATUS_TEXT: Record<FlowStatus, { icon: string; word: string }> = {
  running: { icon: "⟳", word: "하는 중" },
  retry: { icon: "✕", word: "실패 · 다시 할 예정" },
  failed: { icon: "✕", word: "실패" },
  waiting: { icon: "⏸", word: "기다림" },
  done: { icon: "✓", word: "끝남" },
  skipped: { icon: "↷", word: "건너뜀" },
  never: { icon: "·", word: "아직 안 돌았어요" },
  off: { icon: "–", word: "꺼짐" },
  manual: { icon: "–", word: "자동 작업 없음" },
  unread: { icon: "?", word: "기록을 못 읽음" },
};

/** The state of one job, as a node or an edge shows it. */
export interface JobState {
  job: ScheduledJobStatus;
  status: FlowStatus;
  running: RunningJob | null;
  waiting: Waiting | null;
  /** When it last finished, ISO-8601; null when it never did. */
  finishedAt: string | null;
}

/** Each job's state at `now`; `runsReadable` false when the data catalog did not answer. */
export function jobStates(jobs: ScheduledJobStatus[], now: Date, runsReadable = true): Map<string, JobState> {
  const running = new Map(runningJobs(jobs, now).map((row) => [row.job.job_id, row]));
  const waiting = waitingJobs(jobs);
  return new Map(
    jobs.map((job): [string, JobState] => {
      const outcome = job.last_run?.outcome;
      const status: FlowStatus = !job.enabled
        ? "off"
        : !runsReadable
          ? "unread"
          : outcome === "running"
            ? "running"
            : outcome === "up_for_retry"
              ? "retry"
              : waiting.has(job.job_id)
                ? "waiting"
                : outcome === "failed"
                  ? "failed"
                  : outcome === "succeeded"
                    ? "done"
                    : outcome === "skipped"
                      ? "skipped"
                      : outcome === undefined
                        ? "never"
                        : "unread";
      const finishedAt = job.recent_runs.find((run) => run.finished_at)?.finished_at ?? null;
      return [job.job_id, { job, status, running: running.get(job.job_id) ?? null, waiting: waiting.get(job.job_id) ?? null, finishedAt }];
    }),
  );
}

/** Which state wins when several jobs touch one dataset or edge: what needs eyes first. */
const STATUS_RANK: Record<FlowStatus, number> = {
  running: 0,
  retry: 1,
  failed: 2,
  waiting: 3,
  unread: 4,
  done: 5,
  skipped: 6,
  never: 7,
  off: 8,
  manual: 9,
};

/** The state that speaks for a set of jobs; `manual` when there is none. */
export function strongest(states: (JobState | undefined)[]): JobState | null {
  const present = states.filter((state): state is JobState => state !== undefined);
  return minBy(present, (state) => STATUS_RANK[state.status]) ?? null;
}

/** The status a dataset or edge shows for the jobs that make it. */
export function flowStatus(state: JobState | null): FlowStatus {
  return state?.status ?? "manual";
}

/** The one line under a status: how long is left, what it waits for, or how fresh it is. */
export function statusNote(state: JobState | null, titleOf: (jobId: string) => string, now: Date): string | null {
  if (!state) return null;
  const minutes = (seconds: number) => `${Math.max(1, Math.round(seconds / 60))}분`;
  switch (state.status) {
    case "running": {
      const measured = state.running;
      if (!measured || measured.remainingSeconds === null) return "얼마나 걸릴지 아직 몰라요";
      if ((measured.overrunSeconds ?? 0) > 0) return `보통보다 ${minutes(measured.overrunSeconds ?? 0)} 더 걸리고 있어요`;
      return `약 ${minutes(measured.remainingSeconds)} 남음`;
    }
    case "retry":
      return "곧 다시 해요";
    case "waiting": {
      const names = (state.waiting?.on ?? []).map(titleOf).join(", ");
      return state.waiting?.startsAfter ? `앞 단계가 끝나면 시작해요 (${names})` : `앞 단계가 일하는 중이에요 (${names})`;
    }
    default:
      return state.finishedAt ? `${agoWords(state.finishedAt, now)}` : null;
  }
}

/** How long ago `iso` was, as people say it. */
export function agoWords(iso: string, now: Date): string {
  const seconds = Math.max(0, Math.round((now.getTime() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return "방금";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}분 전`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}시간 전`;
  return `${Math.floor(hours / 24)}일 전`;
}

/** The short Korean name of a job: what it makes, the dataset of its own furthest stage first. */
export function jobTitle(job: ScheduledJobStatus): string {
  if (job.writes.length === 0) return job.reads.length > 0 ? `데이터 ${job.reads.length}개 검사` : job.job_id;
  const furthest = maxBy(job.writes, (dataset) => STAGES.indexOf(datasetStage({ type: dataset.kind, surface_kind: dataset.surface_kind })));
  const first = furthest?.title ?? job.job_id;
  return job.writes.length > 1 ? `${first} 외 ${job.writes.length - 1}` : first;
}

export interface FlowSummary {
  working: number;
  failed: number;
  /** The latest finish of a succeeded run of a job that bakes or serves, ISO-8601. */
  lastServedAt: string | null;
}

/** The one-line summary over every job. */
export function flowSummary(jobs: ScheduledJobStatus[], states: Map<string, JobState>): FlowSummary {
  const serving = jobs.filter((job) =>
    job.writes.some((dataset) => ["bake", "serve"].includes(datasetStage({ type: dataset.kind, surface_kind: dataset.surface_kind }))),
  );
  const finishes = serving.flatMap((job) =>
    job.recent_runs.filter((run) => run.outcome === "succeeded" && run.finished_at).map((run) => run.finished_at ?? ""),
  );
  const count = (wanted: FlowStatus[]) => [...states.values()].filter((state) => wanted.includes(state.status)).length;
  return {
    working: count(["running"]),
    failed: count(["failed", "retry"]),
    lastServedAt: maxBy(finishes, (iso) => new Date(iso).getTime()) ?? null,
  };
}

export function summarySentence(summary: FlowSummary, now: Date): string {
  const working = summary.working > 0 ? `지금 ${summary.working}개가 일하고 있어요` : "지금 일하는 작업이 없어요";
  const served = summary.lastServedAt ? `마지막 서비스 반영 ${agoWords(summary.lastServedAt, now)}` : "서비스 반영 기록 없음";
  return `${working} · 실패 ${summary.failed} · ${served}`;
}

/** A dataset's data contract, by the table it promises; null when it has none. */
export function contractOf(node: RegistryNode, contracts: DataContractQuality[]): DataContractQuality | null {
  if (!node.table_name) return null;
  return contracts.find((contract) => contract.contract_id === node.table_name) ?? null;
}

/** A dataset's entity in the data catalog, as Foundation names it for the jobs that touch it. */
export function catalogUrnOf(nodeId: string, jobs: ScheduledJobStatus[]): string | null {
  for (const job of jobs) {
    const found = [...job.writes, ...job.reads].find((dataset) => dataset.node_id === nodeId);
    if (found) return found.catalog_urn;
  }
  return null;
}

/** The state a dataset shows: the strongest of the jobs that write it. */
export function datasetState(node: FlowNode, states: Map<string, JobState>): JobState | null {
  return strongest(node.writers.map((jobId) => states.get(jobId)));
}

/** The state an edge shows: the strongest of the jobs that carry it out. */
export function edgeState(edge: FlowEdge, states: Map<string, JobState>): JobState | null {
  return strongest(edge.jobs.map((jobId) => states.get(jobId)));
}
