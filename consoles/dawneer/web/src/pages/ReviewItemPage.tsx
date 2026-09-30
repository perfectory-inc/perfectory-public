import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { ApiError, type DawneerClient, type DecisionRequest } from "../api/client";
import type { components } from "../api/foundation";
import { formatPnu, gradeLabel, OUTCOME_LABEL, REASON_LABEL, STATUS_LABEL } from "../lib/labels";

type Outcome = components["schemas"]["LineageDecisionOutcome"];
type Reason = components["schemas"]["LineageReasonCode"];

const newKey = () => `dawneer-${crypto.randomUUID()}`;

function message(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 409) return `다시 확인해 주세요: ${error.message}`;
    if (error.status === 403) return `권한이 없습니다: ${error.message}`;
    return error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

/** One parcel: what the derivation found, who holds it, and the decision form. */
export function ReviewItemPage({ client, itemId, me }: { client: DawneerClient; itemId: string; me: string }) {
  const queries = useQueryClient();
  const item = useQuery({ queryKey: ["item", itemId], queryFn: () => client.getItem(itemId) });
  const [outcome, setOutcome] = useState<Outcome>("link");
  const [predecessor, setPredecessor] = useState("");
  const [reason, setReason] = useState<Reason>("building_register");
  const [note, setNote] = useState("");
  // One key per decision attempt: a retry after a network error replays instead of deciding twice.
  const [idempotencyKey, setIdempotencyKey] = useState(newKey);
  const [notice, setNotice] = useState<string | null>(null);

  const refresh = () => {
    void queries.invalidateQueries({ queryKey: ["item", itemId] });
    void queries.invalidateQueries({ queryKey: ["items"] });
  };
  const draft = (): DecisionRequest | null => {
    if (!item.data) return null;
    const writesLineage = outcome === "link" || outcome === "not_a_link";
    return {
      outcome,
      note,
      evidence_etag: item.data.evidence_etag,
      ...(outcome === "link" ? { predecessor_pnu: predecessor } : {}),
      ...(writesLineage ? { reason_code: reason } : {}),
    };
  };

  const claim = useMutation({ mutationFn: () => client.claim(itemId), onSuccess: refresh, onError: (e) => setNotice(message(e)) });
  const release = useMutation({ mutationFn: () => client.release(itemId), onSuccess: refresh, onError: (e) => setNotice(message(e)) });
  const dryRun = useMutation({
    mutationFn: () => {
      const body = draft();
      if (!body) throw new Error("항목을 아직 불러오지 못했습니다");
      return client.dryRun(itemId, body);
    },
    onSuccess: (result) =>
      setNotice(result.requires_approval ? "문제없음 — 이 결정은 다른 사람의 승인이 필요합니다." : "문제없음 — 바로 결정할 수 있습니다."),
    onError: (e) => setNotice(message(e)),
  });
  const decide = useMutation({
    mutationFn: () => {
      const body = draft();
      if (!body) throw new Error("항목을 아직 불러오지 못했습니다");
      return client.decide(itemId, body, idempotencyKey);
    },
    onSuccess: (result) => {
      setNotice(
        result.disposition === "replayed"
          ? "이미 기록된 결정입니다."
          : result.requires_approval
            ? "기록했습니다. 조정자의 승인을 기다립니다."
            : "기록했습니다.",
      );
      setIdempotencyKey(newKey());
      refresh();
    },
    onError: (e) => setNotice(message(e)),
  });
  const rule = useMutation({
    mutationFn: ({ decisionId, approve }: { decisionId: string; approve: boolean }) =>
      client.rule(decisionId, { verdict: approve ? "approved" : "rejected", note: "" }),
    onSuccess: refresh,
    onError: (e) => setNotice(message(e)),
  });

  if (item.isPending) return <p className="text-slate-500">불러오는 중…</p>;
  if (item.isError) return <p className="text-red-700">{message(item.error)}</p>;
  const data = item.data;
  const heldByOther = data.claim && data.claim.claimed_by !== me;
  const busy = decide.isPending || dryRun.isPending;

  return (
    <section className="space-y-6">
      <div>
        <a href="#/" className="text-sm text-blue-700 hover:underline">
          ← 목록
        </a>
        <h1 className="mt-2 font-mono text-xl font-semibold">{formatPnu(data.subject_pnu)}</h1>
        <p className="text-sm text-slate-600">
          {STATUS_LABEL[data.status]} · 필지 번호 {data.subject_pnu}
        </p>
      </div>

      <div className="flex items-center gap-3 rounded-lg bg-white p-4 text-sm shadow-sm">
        {data.claim ? (
          <span>
            {heldByOther ? "다른 직원이 맡고 있습니다" : "내가 맡고 있습니다"} ·{" "}
            {new Date(data.claim.expires_at).toLocaleTimeString("ko-KR")}까지
          </span>
        ) : (
          <span className="text-slate-500">아무도 맡지 않았습니다</span>
        )}
        <span className="ml-auto flex gap-2">
          <button type="button" onClick={() => claim.mutate()} disabled={Boolean(heldByOther)} className="rounded border border-slate-300 px-3 py-1 disabled:opacity-40">
            맡기 (30분)
          </button>
          {data.claim && !heldByOther && (
            <button type="button" onClick={() => release.mutate()} className="rounded border border-slate-300 px-3 py-1">
              반납
            </button>
          )}
        </span>
      </div>

      <div className="rounded-lg bg-white p-4 shadow-sm">
        <h2 className="font-semibold">옛 번호 후보</h2>
        {data.candidates.length === 0 ? (
          <p className="mt-2 text-sm text-slate-500">자동 도출이 찾은 후보가 없습니다. 원래 번호가 없으면 "원래 번호 없음"으로 결정합니다.</p>
        ) : (
          <table className="mt-2 w-full text-sm">
            <thead className="text-left text-slate-500">
              <tr>
                <th className="py-1">선택</th>
                <th>옛 필지</th>
                <th>관계</th>
                <th>등급</th>
                <th>근거</th>
              </tr>
            </thead>
            <tbody>
              {data.candidates.map((c) => (
                <tr key={c.predecessor_pnu} className="border-t border-slate-100">
                  <td className="py-1">
                    <input
                      type="radio"
                      name="predecessor"
                      checked={predecessor === c.predecessor_pnu}
                      onChange={() => {
                        setPredecessor(c.predecessor_pnu);
                        setOutcome("link");
                      }}
                    />
                  </td>
                  <td className="font-mono">{formatPnu(c.predecessor_pnu)}</td>
                  <td>{c.relation}</td>
                  <td>
                    {gradeLabel(c.grade)}
                    {c.in_effect && <span className="ml-1 rounded bg-amber-100 px-1 text-amber-800">현재 효력</span>}
                  </td>
                  <td className="text-slate-600">{c.evidence_kind}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <form
        className="space-y-3 rounded-lg bg-white p-4 shadow-sm"
        onSubmit={(event) => {
          event.preventDefault();
          setNotice(null);
          decide.mutate();
        }}
      >
        <h2 className="font-semibold">결정</h2>
        <div className="flex flex-wrap gap-4 text-sm">
          {(Object.keys(OUTCOME_LABEL) as Outcome[]).map((code) => (
            <label key={code} className="flex items-center gap-1">
              <input type="radio" name="outcome" checked={outcome === code} onChange={() => setOutcome(code)} />
              {OUTCOME_LABEL[code]}
            </label>
          ))}
        </div>
        {(outcome === "link" || outcome === "not_a_link") && (
          <label className="block text-sm">
            <span className="text-slate-500">이유</span>
            <select className="ml-2 rounded border border-slate-300 px-2 py-1" value={reason} onChange={(e) => setReason(e.target.value as Reason)}>
              {(Object.keys(REASON_LABEL) as Reason[]).map((code) => (
                <option key={code} value={code}>
                  {REASON_LABEL[code]}
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="block text-sm">
          <span className="text-slate-500">메모</span>
          <textarea className="mt-1 w-full rounded border border-slate-300 p-2" rows={2} maxLength={2000} value={note} onChange={(e) => setNote(e.target.value)} />
        </label>
        {outcome === "link" && !predecessor && <p className="text-sm text-amber-700">위 후보 중 같은 땅을 고르세요.</p>}
        <div className="flex gap-2">
          <button
            type="button"
            disabled={busy || (outcome === "link" && !predecessor)}
            onClick={() => {
              setNotice(null);
              dryRun.mutate();
            }}
            className="rounded border border-slate-300 px-4 py-1.5 text-sm disabled:opacity-40"
          >
            미리 검사
          </button>
          <button type="submit" disabled={busy || Boolean(heldByOther) || (outcome === "link" && !predecessor)} className="rounded bg-slate-900 px-4 py-1.5 text-sm text-white disabled:opacity-40">
            결정 기록
          </button>
        </div>
        {notice && <p className="text-sm text-slate-700">{notice}</p>}
      </form>

      <div className="rounded-lg bg-white p-4 shadow-sm">
        <h2 className="font-semibold">결정 이력</h2>
        {data.decisions.length === 0 ? (
          <p className="mt-2 text-sm text-slate-500">아직 결정이 없습니다.</p>
        ) : (
          <ul className="mt-2 space-y-2 text-sm">
            {data.decisions.map((d) => (
              <li key={d.decision_id} className="flex flex-wrap items-center gap-2 border-t border-slate-100 pt-2">
                <span className="font-medium">{OUTCOME_LABEL[d.outcome]}</span>
                {d.predecessor_pnu && <span className="font-mono">{formatPnu(d.predecessor_pnu)}</span>}
                <span className="text-slate-500">{new Date(d.decided_at).toLocaleString("ko-KR")}</span>
                {d.requires_approval && (
                  <span className="rounded bg-slate-100 px-1">{d.approval === "approved" ? "승인됨" : d.approval === "rejected" ? "거절됨" : "승인 대기"}</span>
                )}
                {d.requires_approval && !d.approval && (
                  <span className="ml-auto flex gap-2">
                    <button type="button" onClick={() => rule.mutate({ decisionId: d.decision_id, approve: true })} className="rounded border border-slate-300 px-2 py-0.5">
                      승인
                    </button>
                    <button type="button" onClick={() => rule.mutate({ decisionId: d.decision_id, approve: false })} className="rounded border border-slate-300 px-2 py-0.5">
                      거절
                    </button>
                  </span>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}
