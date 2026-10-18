import { describe, expect, it } from "vitest";

import type { DataContractQuality, PipelineGraph, ScheduledJobStatus } from "../api/client";
import { durationLabel, freshnessRows, jobTone, nextStarts, runDuration, runningJobs, summarizeQuality, waitSeconds } from "./operations";

function job(overrides: Partial<ScheduledJobStatus>): ScheduledJobStatus {
  return {
    job_id: "synthetic",
    dag_id: "synthetic_synthetic",
    description: "a synthetic job",
    schedule: "20 7 * * *",
    enabled: true,
    disabled_reason: null,
    next_run_at: "2026-01-03T07:20:00Z",
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
    ...overrides,
  };
}

function check(urn: string, result: DataContractQuality["checks"][number]["result"], checkedAt: string | null) {
  return { assertion_urn: urn, kind: "FIELD", field: "kind", result, checked_at: checkedAt, details: [] };
}

describe("scheduled jobs", () => {
  it("reads a failure as bad and a job with no run as a warning only when the runs were readable", () => {
    const run = (outcome: "failed" | "succeeded" | "running" | "up_for_retry") => ({
      run_urn: "urn:li:dataProcessInstance:x",
      started_at: "2026-01-02T07:20:00Z",
      finished_at: null,
      outcome,
      native_result: null,
    });
    expect(jobTone(job({ last_run: run("failed") }), true)).toBe("bad");
    expect(jobTone(job({ last_run: run("up_for_retry") }), true)).toBe("bad");
    expect(jobTone(job({ last_run: run("succeeded") }), true)).toBe("ok");
    expect(jobTone(job({}), true)).toBe("warn");
    expect(jobTone(job({}), false)).toBe("idle");
    expect(jobTone(job({ enabled: false, last_run: run("failed") }), true)).toBe("idle");
  });

  it("says how long a run took, or has taken so far", () => {
    expect(runDuration("2026-01-02T07:20:00Z", "2026-01-02T07:20:42Z")).toBe("42초");
    expect(runDuration("2026-01-02T07:20:00Z", "2026-01-02T09:20:00Z")).toBe("2시간");
    expect(runDuration("2026-01-02T07:20:00Z", null, new Date("2026-01-02T08:35:00Z"))).toBe("1시간 15분");
    expect(runDuration(null, null)).toBe("—");
  });
});

describe("running now", () => {
  const now = new Date("2026-01-02T08:00:00Z");
  const running = (id: string, startedAt: string, typical: number | null, outcome: "running" | "up_for_retry" = "running") =>
    job({
      job_id: id,
      typical_duration_seconds: typical,
      typical_duration_runs: typical === null ? 0 : 5,
      last_run: { run_urn: `urn:li:dataProcessInstance:${id}`, started_at: startedAt, finished_at: null, outcome, native_result: null },
    });

  it("measures a run against its typical duration: elapsed, expected finish, remaining", () => {
    const [row] = runningJobs([running("synthetic_a", "2026-01-02T07:30:00Z", 60 * 60)], now);
    expect(row?.elapsedSeconds).toBe(30 * 60);
    expect(row?.expectedFinishAt).toBe("2026-01-02T08:30:00.000Z");
    expect(row?.remainingSeconds).toBe(30 * 60);
    expect(row?.overrunSeconds).toBe(0);
    expect(row?.progress).toBe(0.5);
    expect(row?.typicalRuns).toBe(5);
  });

  it("says how far a run is past its typical duration instead of a negative remaining", () => {
    const [row] = runningJobs([running("synthetic_a", "2026-01-02T06:00:00Z", 60 * 60)], now);
    expect(row?.remainingSeconds).toBe(0);
    expect(row?.overrunSeconds).toBe(60 * 60);
    expect(row?.progress).toBe(1);
  });

  it("shows only the elapsed time of a job that has no succeeded run to go by", () => {
    const [row] = runningJobs([running("synthetic_a", "2026-01-02T07:00:00Z", null)], now);
    expect(row?.elapsedSeconds).toBe(60 * 60);
    expect([row?.typicalSeconds, row?.expectedFinishAt, row?.remainingSeconds, row?.overrunSeconds, row?.progress]).toEqual([
      null,
      null,
      null,
      null,
      null,
    ]);
  });

  it("lists running before waiting, soonest expected finish first, runs without an estimate last", () => {
    const done = job({
      job_id: "synthetic_done",
      last_run: { run_urn: "urn:x", started_at: "2026-01-02T07:00:00Z", finished_at: "2026-01-02T07:10:00Z", outcome: "succeeded", native_result: null },
    });
    const rows = runningJobs(
      [
        running("synthetic_late", "2026-01-02T07:50:00Z", 60 * 60),
        running("synthetic_unknown", "2026-01-02T07:00:00Z", null),
        running("synthetic_retry", "2026-01-02T07:00:00Z", 10 * 60, "up_for_retry"),
        running("synthetic_overrun", "2026-01-02T06:00:00Z", 60 * 60),
        running("synthetic_soon", "2026-01-02T07:30:00Z", 40 * 60),
        done,
        job({ job_id: "synthetic_never" }),
      ],
      now,
    );
    expect(rows.map((row) => [row.job.job_id, row.outcome])).toEqual([
      ["synthetic_overrun", "running"],
      ["synthetic_soon", "running"],
      ["synthetic_late", "running"],
      ["synthetic_unknown", "running"],
      ["synthetic_retry", "up_for_retry"],
    ]);
  });

  it("names the next starts of the enabled jobs, soonest first, when nothing runs", () => {
    const jobs = [
      job({ job_id: "synthetic_c", next_run_at: "2026-01-02T09:00:00Z" }),
      job({ job_id: "synthetic_off", enabled: false, next_run_at: null }),
      job({ job_id: "synthetic_a", next_run_at: "2026-01-02T08:10:00Z" }),
      job({ job_id: "synthetic_d", next_run_at: "2026-01-03T07:00:00Z" }),
      job({ job_id: "synthetic_b", next_run_at: "2026-01-02T08:30:00Z" }),
    ];
    expect(runningJobs(jobs, now)).toEqual([]);
    expect(nextStarts(jobs).map((next) => next.job_id)).toEqual(["synthetic_a", "synthetic_b", "synthetic_c"]);
  });

  it("says a number of seconds in the largest whole units", () => {
    expect(durationLabel(42)).toBe("42초");
    expect(durationLabel(90 * 60)).toBe("1시간 30분");
  });
});

describe("quality checks", () => {
  it("counts each result and puts failing, then unchecked, contracts first", () => {
    const contracts: DataContractQuality[] = [
      { contract_id: "a.clean", dataset_urn: "urn:a", checks: [check("1", "success", "2026-01-02T07:20:00Z")] },
      { contract_id: "b.unchecked", dataset_urn: "urn:b", checks: [check("2", "not_run", null)] },
      {
        contract_id: "c.failing",
        dataset_urn: "urn:c",
        checks: [check("3", "failure", "2026-01-02T07:21:00Z"), check("4", "success", "2026-01-01T07:20:00Z")],
      },
    ];
    const summary = summarizeQuality(contracts);
    expect(summary.rows.map((row) => row.contract.contract_id)).toEqual(["c.failing", "b.unchecked", "a.clean"]);
    expect([summary.checks, summary.passed, summary.failed, summary.notRun]).toEqual([4, 2, 1, 1]);
    expect(summary.lastCheckedAt).toBe("2026-01-02T07:21:00Z");
    expect(summary.rows[0]?.lastCheckedAt).toBe("2026-01-02T07:21:00Z");
    expect(summary.rows[1]?.lastCheckedAt).toBeNull();
  });
});

describe("collection freshness", () => {
  it("lists every bound node, keeps one the live check did not see, and shows the worst first", () => {
    const graph: PipelineGraph = {
      nodes: [
        { id: "declared-only", title: "not bound" },
        { id: "loaded", title: "Synthetic silver", runtime_bindings: [{ kind: "lakehouse_contract", value: "silver.synthetic" }] },
        { id: "collected", title: "Synthetic source", runtime_bindings: [{ kind: "ingestion_source", value: "synthetic" }] },
        { id: "silent", table_name: "silver.silent", runtime_bindings: [{ kind: "lakehouse_contract", value: "silver.silent" }] },
      ],
      runtime: {
        database_ready: true,
        nodes: {
          loaded: { status: "healthy", reason: "rows", observed: { recorded_at_unix_seconds: 1767337200 } },
          collected: { status: "failed", reason: "failed run", observed: { finished_at_unix_seconds: 1767340800 } },
        },
      },
    };
    const rows = freshnessRows(graph);
    expect(rows.map((row) => [row.nodeId, row.status])).toEqual([
      ["collected", "failed"],
      ["silent", "unknown"],
      ["loaded", "healthy"],
    ]);
    expect(rows[0]?.observedAt).toBe("2026-01-02T08:00:00.000Z");
    expect(rows[1]?.title).toBe("silver.silent");
    expect(rows[1]?.observedAt).toBeNull();
  });
});

describe("a run waiting for its pool", () => {
  it("is not running and its wait is counted to its start", () => {
    const waiting = job({ last_run: { run_urn: "urn:li:dataProcessInstance:w", queued_at: "2026-01-02T07:00:00Z", started_at: null, finished_at: null, outcome: "running" } });
    expect(runningJobs([waiting], new Date("2026-01-02T08:00:00Z"))).toEqual([]);
    expect(waitSeconds({ queued_at: "2026-01-02T07:00:00Z", started_at: "2026-01-02T10:25:00Z" })).toBe(12300);
    expect(waitSeconds({ queued_at: "2026-01-02T07:00:00Z", started_at: "2026-01-02T07:00:30Z" })).toBeNull();
    expect(waitSeconds({ started_at: "2026-01-02T07:00:00Z" })).toBeNull();
  });
});
