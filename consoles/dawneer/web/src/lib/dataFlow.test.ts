import { describe, expect, it } from "vitest";

import type { PipelineGraph, ScheduledJobDataset, ScheduledJobRun, ScheduledJobStatus } from "../api/client";
import {
  catalogUrnOf,
  contractOf,
  datasetState,
  datasetStage,
  edgeState,
  flowStatus,
  flowSummary,
  flowView,
  jobStates,
  jobTitle,
  statusNote,
  strongest,
  summarySentence,
  waitingJobs,
} from "./dataFlow";

const NOW = new Date("2026-01-02T12:00:00Z");

// A small synthetic pipeline shaped like the real one: a scheduler feeds a source, a sweep's source
// feeds two Silver tables, both feed a Gold panel, which a bake turns into served files. One edge,
// from Gold to a dashboard, is carried by no job.
const graph: PipelineGraph = {
  nodes: [
    { id: "scheduler", type: "serving_surface", surface_kind: "scheduler", title: "예약 실행기" },
    { id: "source", type: "source_group", title: "원천 파일" },
    { id: "silver-a", type: "silver_table", title: "실버 가", table_name: "silver.a" },
    { id: "silver-b", type: "silver_table", title: "실버 나", table_name: "silver.b" },
    { id: "gold", type: "gold_table", title: "패널", table_name: "gold.panel" },
    { id: "files", type: "serving_surface", surface_kind: "r2_baked_documents", title: "구운 문서" },
    { id: "dashboard", type: "serving_surface", surface_kind: "dashboard_panel", title: "대시보드" },
    { id: "lonely", type: "silver_table", title: "외톨이" },
  ],
  edges: [
    { id: "e-scheduler-source", from: "scheduler", to: "source" },
    { id: "e-source-a", from: "source", to: "silver-a" },
    { id: "e-source-b", from: "source", to: "silver-b" },
    { id: "e-a-gold", from: "silver-a", to: "gold" },
    { id: "e-b-gold", from: "silver-b", to: "gold" },
    { id: "e-gold-files", from: "gold", to: "files" },
    { id: "e-gold-dashboard", from: "gold", to: "dashboard" },
  ],
};

function dataset(id: string): ScheduledJobDataset {
  const node = graph.nodes.find((candidate) => candidate.id === id);
  return {
    dataset: `synthetic://${id}`,
    catalog_urn: `urn:li:dataset:(urn:li:dataPlatform:synthetic,${id},PROD)`,
    node_id: id, title: node?.title ?? id, kind: node?.type ?? "", surface_kind: node?.surface_kind ?? null };
}

function run(outcome: ScheduledJobRun["outcome"], startedAt = "2026-01-02T11:00:00Z", finishedAt: string | null = null): ScheduledJobRun {
  return { run_urn: `urn:li:dataProcessInstance:${outcome}-${startedAt}`, started_at: startedAt, finished_at: finishedAt, outcome };
}

function job(id: string, overrides: Partial<ScheduledJobStatus>): ScheduledJobStatus {
  const base: ScheduledJobStatus = {
    job_id: id,
    dag_id: `synthetic_${id}`,
    description: "a synthetic job",
    schedule: "20 7 * * *",
    enabled: true,
    disabled_reason: null,
    next_run_at: null,
    catalog_job_urn: null,
    last_run: null,
    typical_duration_seconds: null,
    typical_duration_runs: 0,
    recent_runs: [],
    started_by: "schedule",
    upstream_job_ids: [],
    pipeline_graph_edges: [],
    reads: [],
    writes: [],
  };
  return { ...base, ...overrides };
}

/** `base` with `latest` as its latest and only recent run. */
function ran(base: ScheduledJobStatus, latest: ScheduledJobRun, overrides: Partial<ScheduledJobStatus> = {}): ScheduledJobStatus {
  return { ...base, last_run: latest, recent_runs: [latest], ...overrides };
}

const sweep = job("sweep", { pipeline_graph_edges: ["e-scheduler-source"], writes: [dataset("source")] });
const loadA = job("load_a", {
  started_by: "inputs",
  upstream_job_ids: ["sweep"],
  pipeline_graph_edges: ["e-source-a"],
  reads: [dataset("source")],
  writes: [dataset("silver-a")],
});
const loadB = job("load_b", {
  started_by: "inputs",
  upstream_job_ids: ["sweep"],
  pipeline_graph_edges: ["e-source-b"],
  reads: [dataset("source")],
  writes: [dataset("silver-b")],
});
const rebuild = job("rebuild", {
  started_by: "inputs",
  upstream_job_ids: ["load_a", "load_b"],
  pipeline_graph_edges: ["e-a-gold", "e-b-gold"],
  writes: [dataset("gold")],
});
const bake = job("bake", { started_by: "inputs", upstream_job_ids: ["rebuild"], pipeline_graph_edges: ["e-gold-files"], writes: [dataset("files")] });
const jobs = [sweep, loadA, loadB, rebuild, bake];

describe("the flow view", () => {
  it("shows the edges the scheduled jobs carry and their datasets, each in its stage", () => {
    const view = flowView(graph, jobs);
    expect(view.nodes.map((node) => [node.id, node.stage])).toEqual([
      ["files", "bake"],
      ["gold", "gold"],
      // Where data enters sits no later than the earliest stage it feeds.
      ["scheduler", "collect"],
      ["silver-a", "silver"],
      ["silver-b", "silver"],
      ["source", "collect"],
    ]);
    expect(view.edges.find((edge) => edge.id === "e-a-gold")?.jobs).toEqual(["rebuild"]);
    expect(view.edges.some((edge) => edge.id === "e-gold-dashboard")).toBe(false);
    expect(view.nodes.find((node) => node.id === "gold")?.writers).toEqual(["rebuild"]);
  });

  it("shows the whole pipeline graph on request, with edges no job carries", () => {
    const view = flowView(graph, jobs, true);
    expect(view.nodes.map((node) => node.id)).toContain("lonely");
    expect(view.edges.find((edge) => edge.id === "e-gold-dashboard")).toMatchObject({ jobs: [] });
    expect(view.nodes.find((node) => node.id === "dashboard")?.stage).toBe("serve");
  });

  it("is the same whatever order the jobs and the graph arrive in", () => {
    const reversed = { nodes: [...graph.nodes].reverse(), edges: [...(graph.edges ?? [])].reverse() };
    expect(flowView(reversed, [...jobs].reverse(), true).nodes.map((node) => node.id)).toEqual(
      flowView(graph, jobs, true).nodes.map((node) => node.id),
    );
  });

  it("places datasets by kind, a baked serving surface before an unbaked one", () => {
    expect(datasetStage({ type: "reference_table" })).toBe("silver");
    expect(datasetStage({ type: "serving_surface", surface_kind: "tiles" })).toBe("bake");
    expect(datasetStage({ type: "serving_surface", surface_kind: "consumer_receiver" })).toBe("serve");
    expect(datasetStage({ type: "serving_group" })).toBe("serve");
  });
});

describe("waiting on a running job", () => {
  it("marks the enabled jobs directly downstream of a running or retrying job, and how they start", () => {
    const waiting = waitingJobs([
      { ...loadA, last_run: run("running") },
      { ...loadB, last_run: run("up_for_retry") },
      rebuild,
      { ...bake, enabled: false },
      sweep,
    ]);
    expect(waiting.get("rebuild")).toEqual({ on: ["load_a", "load_b"], startsAfter: true });
    expect(waiting.has("bake")).toBe(false);
    expect(waiting.has("load_a")).toBe(false);
    expect(waiting.has("sweep")).toBe(false);
  });

  it("does not mark a running job as waiting on its own upstream", () => {
    expect(waitingJobs([{ ...sweep, last_run: run("running") }, { ...loadA, last_run: run("running") }]).size).toBe(0);
  });
});

describe("job states", () => {
  const states = jobStates(
    [
      ran(sweep, run("succeeded", "2026-01-02T08:00:00Z", "2026-01-02T09:00:00Z")),
      ran(loadA, run("running", "2026-01-02T11:30:00Z"), { typical_duration_seconds: 3600 }),
      ran(loadB, run("failed", "2026-01-02T10:00:00Z", "2026-01-02T10:05:00Z")),
      rebuild,
      { ...bake, enabled: false },
    ],
    NOW,
  );

  it("reads off first, then a run going on, then waiting, then the last result", () => {
    expect(["sweep", "load_a", "load_b", "rebuild", "bake"].map((id) => states.get(id)?.status)).toEqual([
      "done",
      "running",
      "failed",
      "waiting",
      "off",
    ]);
    expect(jobStates([loadA], NOW, false).get("load_a")?.status).toBe("unread");
  });

  it("says how long is left, what a job waits for and how fresh a result is", () => {
    const titleOf = (id: string) => (id === "load_a" ? "실버 가" : id);
    expect(statusNote(states.get("load_a") ?? null, titleOf, NOW)).toBe("약 30분 남음");
    expect(statusNote(states.get("rebuild") ?? null, titleOf, NOW)).toBe("앞 단계가 끝나면 시작해요 (실버 가)");
    expect(statusNote(states.get("sweep") ?? null, titleOf, NOW)).toBe("3시간 전");
    expect(statusNote(null, titleOf, NOW)).toBeNull();
  });

  it("lets the state that needs eyes first speak for a dataset several jobs write", () => {
    expect(strongest([states.get("sweep"), states.get("load_b"), states.get("load_a")])?.job.job_id).toBe("load_a");
    expect(flowStatus(strongest([]))).toBe("manual");
  });

  it("sums the jobs up in one line", () => {
    const summary = flowSummary(
      [...jobs.slice(0, 4), ran(bake, run("succeeded", "2026-01-02T09:00:00Z", "2026-01-02T10:00:00Z"))],
      states,
    );
    expect(summary).toEqual({ working: 1, failed: 1, lastServedAt: "2026-01-02T10:00:00Z" });
    expect(summarySentence(summary, NOW)).toBe("지금 1개가 일하고 있어요 · 실패 1 · 마지막 서비스 반영 2시간 전");
    expect(summarySentence({ working: 0, failed: 0, lastServedAt: null }, NOW)).toBe("지금 일하는 작업이 없어요 · 실패 0 · 서비스 반영 기록 없음");
  });
});

describe("names", () => {
  it("names a job by what it makes, a checker by what it checks", () => {
    expect(jobTitle(rebuild)).toBe("패널");
    expect(jobTitle(job("two", { writes: [dataset("gold"), dataset("files")] }))).toBe("구운 문서 외 1");
    expect(jobTitle(job("check", { reads: [dataset("silver-a"), dataset("gold")] }))).toBe("데이터 2개 검사");
  });

  it("finds a dataset's data contract by its table", () => {
    const contract = { contract_id: "gold.panel", dataset_urn: "urn:li:dataset:x", checks: [] };
    expect(contractOf(graph.nodes[4] ?? { id: "x" }, [contract])).toBe(contract);
    expect(contractOf(graph.nodes[1] ?? { id: "x" }, [contract])).toBeNull();
  });

  it("opens a dataset's catalog entity by the name Foundation gives it", () => {
    expect(catalogUrnOf("gold", jobs)).toBe("urn:li:dataset:(urn:li:dataPlatform:synthetic,gold,PROD)");
    expect(catalogUrnOf("source", jobs)).toBe("urn:li:dataset:(urn:li:dataPlatform:synthetic,source,PROD)");
    expect(catalogUrnOf("lonely", jobs)).toBeNull();
  });
});

describe("what a dataset and an edge show", () => {
  const states = jobStates([ran(loadA, run("running", "2026-01-02T11:30:00Z")), ran(sweep, run("succeeded", "2026-01-02T08:00:00Z", "2026-01-02T09:00:00Z"))], NOW);
  const view = flowView(graph, jobs);

  it("shows the state of the job that writes a dataset, and none for a dataset no job writes", () => {
    const node = (id: string) => {
      const found = view.nodes.find((candidate) => candidate.id === id);
      if (!found) throw new Error(`no ${id} in view`);
      return found;
    };
    expect(datasetState(node("silver-a"), states)?.status).toBe("running");
    expect(datasetState(node("source"), states)?.status).toBe("done");
    expect(flowStatus(datasetState(node("scheduler"), states))).toBe("manual");
  });

  it("shows the state of the job that carries an edge", () => {
    const edge = view.edges.find((candidate) => candidate.id === "e-source-a");
    expect(edge && edgeState(edge, states)?.status).toBe("running");
  });
});
