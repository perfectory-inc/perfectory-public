import { describe, expect, it } from "vitest";

import type { ScheduledJobRun, ScheduledJobStatus } from "../api/client";
import { runTimeline } from "./runTimeline";

const NOW = new Date("2026-01-02T12:00:00Z");

function job(id: string, overrides: Partial<ScheduledJobStatus> = {}): ScheduledJobStatus {
  return {
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
    ...overrides,
  };
}

function run(urn: string, startedAt: string | null, finishedAt: string | null, outcome: ScheduledJobRun["outcome"]): ScheduledJobRun {
  return { run_urn: urn, started_at: startedAt, finished_at: finishedAt, outcome };
}

describe("run timeline", () => {
  it("places finished runs on a 24-hour axis that reaches an hour past now when nothing runs", () => {
    const timeline = runTimeline(
      [
        job("nightly", {
          recent_runs: [
            run("b", "2026-01-02T06:00:00Z", "2026-01-02T08:30:00Z", "failed"),
            run("a", "2026-01-01T18:00:00Z", "2026-01-01T19:00:00Z", "succeeded"),
          ],
        }),
      ],
      NOW,
    );
    expect(timeline.start.toISOString()).toBe("2026-01-01T12:00:00.000Z");
    expect(timeline.end.toISOString()).toBe("2026-01-02T13:00:00.000Z");
    expect(timeline.now).toBeCloseTo(24 / 25);
    const [first, second] = timeline.rows[0]?.bars ?? [];
    expect(first).toMatchObject({ outcome: "succeeded", clippedStart: false, ghost: null });
    expect(first?.x).toBeCloseTo(6 / 25);
    expect(first?.width).toBeCloseTo(1 / 25);
    expect(second?.x).toBeCloseTo(18 / 25);
    expect(second?.width).toBeCloseTo(2.5 / 25);
    // Ticks fall on whole three-hour marks from the axis's start.
    expect(timeline.ticks.map((tick) => tick.at.toISOString().slice(11, 16))).toEqual([
      "12:00",
      "15:00",
      "18:00",
      "21:00",
      "00:00",
      "03:00",
      "06:00",
      "09:00",
      "12:00",
    ]);
  });

  it("runs a running bar to now and a ghost to its expected finish, widening the axis to show it", () => {
    const timeline = runTimeline(
      [
        job("bake", {
          typical_duration_seconds: 6 * 3600,
          recent_runs: [run("now", "2026-01-02T10:00:00Z", null, "running")],
        }),
      ],
      NOW,
    );
    // Started two hours ago and usually takes six: four hours left, so the axis ends four hours on.
    expect(timeline.end.toISOString()).toBe("2026-01-02T16:00:00.000Z");
    const bar = timeline.rows[0]?.bars[0];
    expect(bar?.x).toBeCloseTo(22 / 28);
    expect(bar?.width).toBeCloseTo(2 / 28);
    expect(bar?.ghost?.x).toBeCloseTo(24 / 28);
    expect(bar?.ghost?.width).toBeCloseTo(4 / 28);
  });

  it("drops the ghost of an overrun, bounds a far ghost, and clips a run that began before the axis", () => {
    const overrun = runTimeline(
      [job("late", { typical_duration_seconds: 3600, recent_runs: [run("late", "2026-01-02T09:00:00Z", null, "running")] })],
      NOW,
    );
    expect(overrun.rows[0]?.bars[0]?.ghost).toBeNull();
    expect(overrun.end.toISOString()).toBe("2026-01-02T13:00:00.000Z");

    const long = runTimeline(
      [job("long", { typical_duration_seconds: 48 * 3600, recent_runs: [run("long", "2026-01-01T06:00:00Z", null, "up_for_retry")] })],
      NOW,
    );
    expect(long.end.toISOString()).toBe("2026-01-03T00:00:00.000Z");
    const bar = long.rows[0]?.bars[0];
    expect(bar).toMatchObject({ x: 0, clippedStart: true });
    expect((bar?.ghost?.x ?? 0) + (bar?.ghost?.width ?? 0)).toBeCloseTo(1);
  });

  it("leaves out runs outside the window or without a start, and counts the enabled jobs left with none", () => {
    const timeline = runTimeline(
      [
        job("old", { recent_runs: [run("old", "2025-12-30T00:00:00Z", "2025-12-30T01:00:00Z", "succeeded")] }),
        job("unstarted", { recent_runs: [run("x", null, "2026-01-02T11:00:00Z", "succeeded")] }),
        job("off", { enabled: false }),
        job("fresh", { recent_runs: [run("y", "2026-01-02T11:00:00Z", "2026-01-02T11:30:00Z", "succeeded")] }),
      ],
      NOW,
    );
    expect(timeline.rows.map((row) => row.job.job_id)).toEqual(["fresh"]);
    expect(timeline.idleJobs).toBe(2);
  });
});
