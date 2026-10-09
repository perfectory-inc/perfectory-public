import { describe, expect, it } from "vitest";

import type { DataContractQuality, PipelineGraph, ScheduledJobStatus } from "../api/client";
import { freshnessRows, jobTone, runDuration, summarizeQuality } from "./operations";

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
