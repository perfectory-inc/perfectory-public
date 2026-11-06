// One scheduled job's operator switches (root ADR-0282): pause, resume, run once now, stop. The
// platform records each with the signed-in staff member and a reason; only staff it lets operate
// jobs may, and it answers 403 to anyone else.
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { ApiError, type DawneerClient, type OperatorActionRecorded } from "../api/client";
import {
  MAX_REASON_CHARS,
  MAX_WINDOW_HOURS,
  OPERATOR_ACTION_LABEL,
  OPERATOR_ACTION_NOTE,
  OPERATOR_ACTIONS,
  operatorActionRequest,
  type OperatorAction,
} from "../lib/operatorActions";
import { fullBakeRequest, isByPnuBake } from "../lib/byPnuCutovers";

/** A by-PNU bake job also takes a full bake of one lane (root ADR-0433 §5), never a hand-written drop-in. */
const FULL_BAKE = "force_full";
const FULL_BAKE_LABEL = "레인 전량 굽기 승인";
const FULL_BAKE_NOTE = "다음 굽기 한 번이 이 레인을 전량으로 굽습니다(재기준). 7일 안에 쓰이지 않으면 만료됩니다.";

export function operatorActionMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 403) return "이 작업을 조작할 권한이 없습니다.";
    if (error.status === 503) return `지금은 기록하지 못했습니다. 잠시 뒤 다시 해 주세요: ${error.message}`;
    return error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

export function recordedMessage(recorded: OperatorActionRecorded): string {
  const label =
    recorded.action === FULL_BAKE
      ? `${FULL_BAKE_LABEL} (${recorded.lanes})`
      : recorded.action === "approve_cutover"
        ? `cut-over 승인 (${recorded.lanes} · g${recorded.generation})`
        : (OPERATOR_ACTION_LABEL[recorded.action as OperatorAction] ?? recorded.action);
  if (recorded.window_start && recorded.window_end) {
    const until = recorded.expires_at ? ` · ${new Date(recorded.expires_at).toLocaleString("ko-KR")}까지` : "";
    return `${label} 기록됨 (${recorded.id.slice(0, 8)}) · ${recorded.window_start}-${recorded.window_end} 한국 시각${until}`;
  }
  const expiry = recorded.expires_at ? ` · ${new Date(recorded.expires_at).toLocaleTimeString("ko-KR")}까지 시작` : "";
  return `${label} 기록됨 (${recorded.id.slice(0, 8)})${expiry}`;
}

export function OperatorActions({ client, jobId }: { client: DawneerClient; jobId: string }) {
  const queryClient = useQueryClient();
  const [action, setAction] = useState<OperatorAction | typeof FULL_BAKE>("run_now");
  const [lane, setLane] = useState("");
  const bake = isByPnuBake(jobId);
  const [reason, setReason] = useState("");
  const [dropPending, setDropPending] = useState(false);
  const [windowStart, setWindowStart] = useState("09:00");
  const [windowEnd, setWindowEnd] = useState("18:00");
  const [windowHours, setWindowHours] = useState(3);
  const [notice, setNotice] = useState<string | null>(null);
  const checked =
    action === FULL_BAKE
      ? fullBakeRequest(lane, reason)
      : operatorActionRequest(action, reason, dropPending, {
          start: windowStart,
          end: windowEnd,
          hours: windowHours,
        });
  const send = useMutation({
    mutationFn: () => {
      if ("problem" in checked) throw new Error(checked.problem);
      return client.recordOperatorAction(jobId, checked.request);
    },
    onSuccess: (recorded) => {
      setNotice(recordedMessage(recorded));
      setReason("");
      void queryClient.invalidateQueries({ queryKey: ["scheduled-jobs"] });
    },
    onError: (error) => setNotice(operatorActionMessage(error)),
  });
  return (
    <form
      className="flex min-w-56 flex-col gap-1 text-xs"
      onSubmit={(event) => {
        event.preventDefault();
        send.mutate();
      }}
    >
      <select
        aria-label={`${jobId} 조작`}
        className="rounded border border-slate-300 px-1 py-0.5"
        value={action}
        onChange={(event) => {
          setAction(event.target.value as OperatorAction | typeof FULL_BAKE);
          setDropPending(false);
        }}
      >
        {OPERATOR_ACTIONS.map((name) => (
          <option key={name} value={name}>
            {OPERATOR_ACTION_LABEL[name]}
          </option>
        ))}
        {bake ? <option value={FULL_BAKE}>{FULL_BAKE_LABEL}</option> : null}
      </select>
      <span className="text-slate-500">{action === FULL_BAKE ? FULL_BAKE_NOTE : OPERATOR_ACTION_NOTE[action]}</span>
      {action === FULL_BAKE ? (
        <input
          aria-label={`${jobId} 전량 레인`}
          className="rounded border border-slate-300 px-1 py-0.5"
          placeholder="레인 (예: parcel-public-auction)"
          value={lane}
          onChange={(event) => setLane(event.target.value)}
        />
      ) : null}
      <input
        aria-label={`${jobId} 이유`}
        className="rounded border border-slate-300 px-1 py-0.5"
        maxLength={MAX_REASON_CHARS}
        placeholder="이유 (기록에 남습니다)"
        value={reason}
        onChange={(event) => setReason(event.target.value)}
      />
      {action === "set_window" ? (
        <div className="flex flex-wrap items-center gap-1 text-slate-600">
          <input
            aria-label={`${jobId} 시간대 시작`}
            type="time"
            className="rounded border border-slate-300 px-1 py-0.5"
            value={windowStart}
            onChange={(event) => setWindowStart(event.target.value)}
          />
          ~
          <input
            aria-label={`${jobId} 시간대 끝`}
            type="time"
            className="rounded border border-slate-300 px-1 py-0.5"
            value={windowEnd}
            onChange={(event) => setWindowEnd(event.target.value)}
          />
          <input
            aria-label={`${jobId} 유지 시간`}
            type="number"
            min={1}
            max={MAX_WINDOW_HOURS}
            className="w-16 rounded border border-slate-300 px-1 py-0.5"
            value={windowHours}
            onChange={(event) => setWindowHours(Number(event.target.value))}
          />
          시간 동안
        </div>
      ) : null}
      {action === "pause" ? (
        <label className="flex items-center gap-1 text-slate-600">
          <input type="checkbox" checked={dropPending} onChange={(event) => setDropPending(event.target.checked)} />
          쌓인 회차도 버림
        </label>
      ) : null}
      <button type="submit" disabled={send.isPending || "problem" in checked} className="rounded bg-slate-900 px-2 py-1 text-white disabled:opacity-40">
        {action === FULL_BAKE ? FULL_BAKE_LABEL : OPERATOR_ACTION_LABEL[action]}
      </button>
      {notice ? <span role="status" className="text-slate-700">{notice}</span> : null}
    </form>
  );
}
