// The geometry of the 데이터 흐름 run timeline (root ADR-0188): each job's recent runs as bars on a
// time axis, the way Airflow's Gantt view and Prefect's run timeline draw them. The runs and the
// typical durations are Foundation's; this only places them against the clock.
import { clamp, orderBy } from "es-toolkit";

import type { ScheduledJobRun, ScheduledJobRunOutcome, ScheduledJobStatus } from "../api/client";

const HOUR_MS = 3_600_000;

/** How far back the timeline reaches. */
export const TIMELINE_HOURS = 24;
/** The axis reaches past now at least this far, and at most `MAX_AHEAD_HOURS` for a long ghost. */
const MIN_AHEAD_HOURS = 1;
const MAX_AHEAD_HOURS = 12;

export interface TimelineBar {
  run: ScheduledJobRun;
  outcome: ScheduledJobRunOutcome;
  /** Left edge and width as fractions of the axis, 0 to 1. */
  x: number;
  width: number;
  /** The run began before the axis does; its left edge is the axis's. */
  clippedStart: boolean;
  /**
   * For a run still going: where it should end at the job's typical pace, from now to then, as
   * fractions of the axis (clipped to its end); null when it has no estimate or is already past it.
   */
  ghost: { x: number; width: number } | null;
}

export interface TimelineRow {
  job: ScheduledJobStatus;
  bars: TimelineBar[];
}

export interface Timeline {
  start: Date;
  end: Date;
  /** Where now is on the axis, 0 to 1. */
  now: number;
  /** Whole hours on the axis every `stepHours`, with their place, 0 to 1. */
  ticks: { at: Date; x: number }[];
  rows: TimelineRow[];
  /** Enabled jobs with no run on the axis. */
  idleJobs: number;
}

function expectedFinish(job: ScheduledJobStatus, run: ScheduledJobRun): number | null {
  if (run.outcome !== "running" && run.outcome !== "up_for_retry") return null;
  if (!run.started_at || job.typical_duration_seconds == null) return null;
  return new Date(run.started_at).getTime() + job.typical_duration_seconds * 1000;
}

/**
 * The runs of the last `TIMELINE_HOURS` hours, one row per job that has one, in the order `jobs`
 * gives. A run without a recorded start has no place on a time axis and is left out. A run still
 * going reaches to now, and its ghost to its expected finish; the axis reaches past now far enough
 * to show the furthest ghost, within bounds.
 */
export function runTimeline(jobs: ScheduledJobStatus[], now: Date, stepHours = 3): Timeline {
  const nowMs = now.getTime();
  const startMs = nowMs - TIMELINE_HOURS * HOUR_MS;
  const finishes = jobs.flatMap((job) => job.recent_runs.map((run) => expectedFinish(job, run) ?? nowMs));
  const ahead = clamp(Math.max(nowMs, ...finishes) - nowMs, MIN_AHEAD_HOURS * HOUR_MS, MAX_AHEAD_HOURS * HOUR_MS);
  const endMs = nowMs + ahead;
  const span = endMs - startMs;
  const at = (ms: number) => clamp((ms - startMs) / span, 0, 1);

  const rows = jobs.flatMap((job): TimelineRow[] => {
    const bars = job.recent_runs.flatMap((run): TimelineBar[] => {
      if (!run.started_at) return [];
      const began = new Date(run.started_at).getTime();
      const ended = run.finished_at ? new Date(run.finished_at).getTime() : nowMs;
      if (ended < startMs || began > nowMs) return [];
      const x = at(began);
      const finish = expectedFinish(job, run);
      const ghost = finish !== null && finish > nowMs ? { x: at(nowMs), width: at(finish) - at(nowMs) } : null;
      return [{ run, outcome: run.outcome, x, width: at(Math.min(ended, nowMs)) - x, clippedStart: began < startMs, ghost }];
    });
    return bars.length > 0 ? [{ job, bars: orderBy(bars, [(bar) => bar.x], ["asc"]) }] : [];
  });

  const ticks: Timeline["ticks"] = [];
  const stepMs = stepHours * HOUR_MS;
  for (let tick = Math.ceil(startMs / stepMs) * stepMs; tick <= endMs; tick += stepMs) {
    ticks.push({ at: new Date(tick), x: at(tick) });
  }
  const shown = new Set(rows.map((row) => row.job.job_id));
  return {
    start: new Date(startMs),
    end: new Date(endMs),
    now: at(nowMs),
    ticks,
    rows,
    idleJobs: jobs.filter((job) => job.enabled && !shown.has(job.job_id)).length,
  };
}
