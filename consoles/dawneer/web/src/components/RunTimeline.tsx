// The last day's runs of every job as bars on a time axis (root ADR-0188), the way Airflow's Gantt
// view and Prefect's run timeline draw them: a bar per run coloured by its result, a running bar to
// now and a dashed ghost to its expected finish, and a line at now.
import type { ScheduledJobRunOutcome, ScheduledJobStatus } from "../api/client";
import { jobTitle, STATUS_TEXT } from "../lib/dataFlow";
import { RUN_OUTCOME_LABEL, runDuration } from "../lib/operations";
import { runTimeline, TIMELINE_HOURS } from "../lib/runTimeline";

const BAR_CLASS: Record<ScheduledJobRunOutcome, string> = {
  running: "bg-sky-500",
  succeeded: "bg-emerald-500",
  failed: "bg-red-500",
  up_for_retry: "bg-red-300",
  skipped: "bg-slate-300",
  unknown: "bg-slate-400",
};

const OUTCOME_ICON: Record<ScheduledJobRunOutcome, string> = {
  running: STATUS_TEXT.running.icon,
  succeeded: STATUS_TEXT.done.icon,
  failed: STATUS_TEXT.failed.icon,
  up_for_retry: STATUS_TEXT.retry.icon,
  skipped: STATUS_TEXT.skipped.icon,
  unknown: "?",
};

const percent = (fraction: number) => `${(fraction * 100).toFixed(3)}%`;

export function RunTimeline({ jobs, now, onSelectJob }: { jobs: ScheduledJobStatus[]; now: Date; onSelectJob: (jobId: string) => void }) {
  const timeline = runTimeline(jobs, now);
  const hour = (date: Date) => date.toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit", hour12: false });
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-600">
        <span>최근 {TIMELINE_HOURS}시간 동안 돈 작업입니다. 막대 하나가 한 번 돈 것이고, 점선은 보통 걸리는 시간으로 어림한 남은 부분입니다.</span>
        {(["succeeded", "running", "failed", "up_for_retry", "skipped"] as const).map((outcome) => (
          <span key={outcome} className="flex items-center gap-1">
            <span className={`inline-block h-2.5 w-4 rounded-sm ${BAR_CLASS[outcome]}`} />
            {OUTCOME_ICON[outcome]} {RUN_OUTCOME_LABEL[outcome]}
          </span>
        ))}
      </div>
      {timeline.rows.length === 0 ? (
        <p className="text-sm text-slate-500">최근 {TIMELINE_HOURS}시간 동안 돈 작업이 없습니다.</p>
      ) : (
        <div className="grid grid-cols-[minmax(10rem,16rem)_1fr] gap-x-3 text-xs">
          <div />
          <div className="relative h-5 border-b border-slate-200">
            {timeline.ticks.map((tick) => (
              <span key={tick.at.toISOString()} className="absolute -translate-x-1/2 text-slate-500" style={{ left: percent(tick.x) }}>
                {hour(tick.at)}
              </span>
            ))}
          </div>
          {timeline.rows.map((row) => (
            <div key={row.job.job_id} className="contents">
              <button
                type="button"
                onClick={() => onSelectJob(row.job.job_id)}
                className="truncate py-1.5 text-left text-slate-800 hover:underline"
                title={row.job.job_id}
              >
                {jobTitle(row.job)}
              </button>
              <div className="relative h-7 border-b border-slate-100">
                {timeline.ticks.map((tick) => (
                  <span key={tick.at.toISOString()} className="absolute top-0 h-full border-l border-slate-100" style={{ left: percent(tick.x) }} />
                ))}
                {row.bars.map((bar) => (
                  <div key={bar.run.run_urn}>
                    <div
                      className={`absolute top-1.5 h-4 rounded-sm ${BAR_CLASS[bar.outcome]} ${bar.outcome === "running" ? "animate-pulse" : ""}`}
                      style={{ left: percent(bar.x), width: `max(3px, ${percent(bar.width)})` }}
                      title={`${RUN_OUTCOME_LABEL[bar.outcome]} · ${bar.run.started_at ? new Date(bar.run.started_at).toLocaleString("ko-KR") : "—"} · ${runDuration(bar.run.started_at, bar.run.finished_at, now)}`}
                      role="img"
                      aria-label={`${jobTitle(row.job)} ${RUN_OUTCOME_LABEL[bar.outcome]} ${runDuration(bar.run.started_at, bar.run.finished_at, now)}`}
                    />
                    {bar.ghost ? (
                      <div
                        className="absolute top-1.5 h-4 rounded-sm border border-dashed border-sky-500 bg-sky-50"
                        style={{ left: percent(bar.ghost.x), width: percent(bar.ghost.width) }}
                        title="보통 걸리는 시간으로 어림한 남은 부분"
                      />
                    ) : null}
                  </div>
                ))}
                <span className="absolute top-0 h-full border-l-2 border-sky-700" style={{ left: percent(timeline.now) }} title="지금" />
              </div>
            </div>
          ))}
        </div>
      )}
      {timeline.idleJobs > 0 ? (
        <p className="mt-2 text-xs text-slate-500">켜져 있지만 최근 {TIMELINE_HOURS}시간 동안 돈 기록이 없는 작업 {timeline.idleJobs}개는 줄에서 뺐습니다.</p>
      ) : null}
    </div>
  );
}
