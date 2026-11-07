// by-PNU cut-over tab (root ADR-0433, ADR-0435): a lane's first publish starts by itself (the host's
// auto-promotion); an approval here is the exceptional act, a lane cut over again at a new generation.
// The platform records it with the signed-in staff member and a reason; the host starts it once, at
// once, it waits for the spark pool (not for a clock), and runs the gates (preview, gold, bake,
// equality, latency, publish, verify) as its steps. Each approval's progress is below.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import type { ByPnuCutoverApproval, DawneerClient } from "../api/client";
import { CUTOVER_STATE_LABEL, CUTOVER_SUBJECT, STEP_LABEL, cutoverRequest } from "../lib/byPnuCutovers";
import { MAX_REASON_CHARS } from "../lib/operatorActions";
import { operatorActionMessage, recordedMessage } from "./OperatorActions";
import { Loading } from "./OperationsTables";

const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";
const QUERY_KEY = ["by-pnu-cutovers"];

function ApproveForm({ client }: { client: DawneerClient }) {
  const queryClient = useQueryClient();
  const [lanes, setLanes] = useState("");
  const [generation, setGeneration] = useState(1);
  const [reason, setReason] = useState("");
  const [notice, setNotice] = useState<string | null>(null);
  const checked = cutoverRequest(lanes, generation, reason);
  const send = useMutation({
    mutationFn: () => {
      if ("problem" in checked) throw new Error(checked.problem);
      return client.recordOperatorAction(CUTOVER_SUBJECT, checked.request);
    },
    onSuccess: (recorded) => {
      setNotice(`${recordedMessage(recorded)} · 호스트가 바로 시작하고 Spark 풀을 기다립니다`);
      setReason("");
      void queryClient.invalidateQueries({ queryKey: QUERY_KEY });
    },
    onError: (error) => setNotice(operatorActionMessage(error)),
  });
  return (
    <form
      className="flex flex-wrap items-end gap-2 rounded border border-slate-200 bg-white p-3 text-sm"
      onSubmit={(event) => {
        event.preventDefault();
        send.mutate();
      }}
    >
      <label className="flex flex-col text-xs text-slate-600">
        레인
        <input
          aria-label="cut-over 레인"
          className="w-72 rounded border border-slate-300 px-1 py-0.5 text-sm"
          placeholder="lanes · parcel-ownership · a,b (순서대로)"
          value={lanes}
          onChange={(event) => setLanes(event.target.value)}
        />
      </label>
      <label className="flex flex-col text-xs text-slate-600">
        세대
        <input
          aria-label="cut-over 세대"
          type="number"
          min={1}
          className="w-20 rounded border border-slate-300 px-1 py-0.5 text-sm"
          value={generation}
          onChange={(event) => setGeneration(Number(event.target.value))}
        />
      </label>
      <label className="flex grow flex-col text-xs text-slate-600">
        이유
        <input
          aria-label="cut-over 이유"
          className="rounded border border-slate-300 px-1 py-0.5 text-sm"
          maxLength={MAX_REASON_CHARS}
          placeholder="왜 지금 전환하나요(한 줄, 기록에 남습니다)"
          value={reason}
          onChange={(event) => setReason(event.target.value)}
        />
      </label>
      <button
        type="submit"
        disabled={"problem" in checked || send.isPending}
        className="rounded bg-slate-900 px-3 py-1 text-white disabled:opacity-40"
      >
        cut-over 승인
      </button>
      <span className="basis-full text-xs text-slate-500">
        {"problem" in checked && (lanes || reason)
          ? checked.problem
          : "이미 cut-over 된 레인을 새 세대로 다시 전환할 때만 씁니다(첫 발행은 자동). 바로 한 번 시작하고, 48시간 뒤 만료."}
      </span>
      {notice ? (
        <span role="status" className="basis-full text-xs text-slate-700">
          {notice}
        </span>
      ) : null}
    </form>
  );
}

/** Cancels every approval not started yet and ends a run between steps. */
function StopButton({ client }: { client: DawneerClient }) {
  const queryClient = useQueryClient();
  const [reason, setReason] = useState("");
  const [notice, setNotice] = useState<string | null>(null);
  const send = useMutation({
    mutationFn: () => client.recordOperatorAction(CUTOVER_SUBJECT, { action: "stop", reason, drop_pending: false }),
    onSuccess: (recorded) => {
      setNotice(recordedMessage(recorded));
      setReason("");
      void queryClient.invalidateQueries({ queryKey: QUERY_KEY });
    },
    onError: (error) => setNotice(operatorActionMessage(error)),
  });
  return (
    <form
      className="flex flex-wrap items-center gap-2 text-xs"
      onSubmit={(event) => {
        event.preventDefault();
        send.mutate();
      }}
    >
      <input
        aria-label="cut-over 중지 이유"
        className="w-72 rounded border border-slate-300 px-1 py-0.5"
        placeholder="중지 이유"
        value={reason}
        onChange={(event) => setReason(event.target.value)}
      />
      <button
        type="submit"
        disabled={!reason.trim() || send.isPending}
        className="rounded border border-red-300 px-2 py-0.5 text-red-700 disabled:opacity-40"
      >
        대기 승인 취소 · 도는 cut-over 중지
      </button>
      {notice ? <span role="status">{notice}</span> : null}
    </form>
  );
}

function Events({ approval }: { approval: ByPnuCutoverApproval }) {
  if (!approval.events.length) return <span className="text-slate-500">아직 호스트 기록 없음</span>;
  return (
    <ol className="space-y-0.5 text-xs">
      {approval.events.map((event, index) => (
        <li key={index} className={event.outcome === "failed" || event.state === "refused" ? "text-red-700" : ""}>
          <span className="text-slate-500">{event.at.slice(5, 16).replace("T", " ")}Z</span>{" "}
          {event.state === "step"
            ? `${event.lane} · ${STEP_LABEL[event.step ?? ""] ?? event.step} · ${event.outcome}${event.seconds ? ` (${event.seconds}s)` : ""}`
            : event.state === "scheduled"
              ? `예약: ${event.start_at} 에 시작`
              : (CUTOVER_STATE_LABEL[event.state] ?? event.state)}
          {event.reason && event.state !== "step" ? ` — ${event.reason}` : ""}
        </li>
      ))}
    </ol>
  );
}

export function ByPnuCutovers({ client }: { client: DawneerClient }) {
  const query = useQuery({ queryKey: QUERY_KEY, queryFn: () => client.byPnuCutovers(), refetchInterval: 30_000 });
  return (
    <div className="space-y-4">
      <ApproveForm client={client} />
      <StopButton client={client} />
      {!query.data ? (
        <Loading what="cut-over 승인 목록" query={query} />
      ) : (
        <>
          {query.data.error ? <p className="text-sm text-amber-700">{query.data.error}</p> : null}
          {/* A phone is narrower than the four columns (root ADR-0434 §6): the table scrolls, the page does not. */}
          <div className="overflow-x-auto">
            <table className="w-full border-collapse rounded border border-slate-200 bg-white">
              <thead className="bg-slate-50">
                <tr>
                  <th className={TH}>승인</th>
                  <th className={TH}>레인 · 세대</th>
                  <th className={TH}>상태</th>
                  <th className={TH}>진행</th>
                </tr>
              </thead>
              <tbody>
                {query.data.approvals.map((approval) => (
                  <tr key={approval.id} className="border-t border-slate-100">
                    <td className={TD}>
                      <div>{new Date(approval.requested_at).toLocaleString("ko-KR")}</div>
                      <div className="text-xs text-slate-500">
                        {approval.actor.slice(0, 8)} · {approval.reason}
                      </div>
                    </td>
                    <td className={TD}>
                      {approval.lanes} · g{approval.generation}
                    </td>
                    <td className={TD}>{CUTOVER_STATE_LABEL[approval.state] ?? approval.state}</td>
                    <td className={TD}>
                      <Events approval={approval} />
                    </td>
                  </tr>
                ))}
                {query.data.approvals.length === 0 ? (
                  <tr>
                    <td className={TD} colSpan={4}>
                      아직 승인한 cut-over 가 없습니다.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
