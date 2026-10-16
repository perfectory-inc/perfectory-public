import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { ApiError, type DawneerClient, type DecisionRequest, type ReviewItem, type StewardDecision } from "../api/client";
import type { components } from "../api/foundation";
import {
  formatCode,
  formatDay,
  gradeLabel,
  KIND_LABEL,
  LEGAL_DONG_OUTCOME_LABEL,
  legalDongRuleLabel,
  levelLabel,
  officialHistoryLabel,
  OUTCOME_LABEL,
  REASON_LABEL,
  STATUS_LABEL,
} from "../lib/labels";

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

/** A 법정동 item only a steward may pair; a split or one awaiting data is the data's (root ADR-0144 §4). */
function stewardMayPair(item: ReviewItem): boolean {
  return item.kind === "parcel" || item.status === "steward";
}

/** One item: what the derivation found, who holds it, the decision form and the decision trail. */
export function ReviewItemPage({ client, itemId, me }: { client: DawneerClient; itemId: string; me: string }) {
  const queries = useQueryClient();
  const item = useQuery({ queryKey: ["item", itemId], queryFn: () => client.getItem(itemId) });
  const [outcome, setOutcome] = useState<Outcome>("link");
  const [chosen, setChosen] = useState("");
  const [reason, setReason] = useState<Reason | null>(null);
  const [note, setNote] = useState("");
  // One key per decision attempt: a retry after a network error replays instead of deciding twice.
  const [idempotencyKey, setIdempotencyKey] = useState(newKey);
  const [notice, setNotice] = useState<string | null>(null);

  const refresh = () => {
    void queries.invalidateQueries({ queryKey: ["item", itemId] });
    void queries.invalidateQueries({ queryKey: ["items"] });
  };
  const legalDong = item.data?.kind === "legal_dong";
  // A 법정동 pair is most often settled by an official document; a parcel by the building register.
  const reasonCode: Reason = reason ?? (legalDong ? "official_document" : "building_register");
  const writesLineage = outcome === "link" || outcome === "not_a_link";
  const draft = (): DecisionRequest | null => {
    if (!item.data) return null;
    return {
      outcome,
      note,
      evidence_etag: item.data.evidence_etag,
      ...(outcome === "link" ? { candidate_code: chosen } : {}),
      ...(writesLineage ? { reason_code: reasonCode } : {}),
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
  const outcomeLabel = legalDong ? LEGAL_DONG_OUTCOME_LABEL : OUTCOME_LABEL;
  const mayPair = stewardMayPair(data);
  const noteMissing = legalDong && writesLineage && !note.trim();
  const blocked = (outcome === "link" && !chosen) || (writesLineage && !mayPair) || noteMissing;
  const choose = (code: string) => {
    setChosen(code);
    setOutcome("link");
  };

  return (
    <section className="space-y-6">
      <div>
        <a href="#/" className="text-sm text-blue-700 hover:underline">
          ← 목록
        </a>
        <h1 className="mt-2 text-xl font-semibold">
          <span className="font-mono">{formatCode(data.kind, data.subject_code)}</span>
          {data.legal_dong && <span className="ml-2">{data.legal_dong.name}</span>}
        </h1>
        <p className="text-sm text-slate-600">
          {KIND_LABEL[data.kind]} · {STATUS_LABEL[data.status]}
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

      {data.legal_dong ? (
        <LegalDongPanel item={data} chosen={chosen} onChoose={choose} />
      ) : (
        <ParcelCandidates item={data} chosen={chosen} onChoose={choose} />
      )}

      <form
        className="space-y-3 rounded-lg bg-white p-4 shadow-sm"
        onSubmit={(event) => {
          event.preventDefault();
          setNotice(null);
          decide.mutate();
        }}
      >
        <h2 className="font-semibold">결정</h2>
        {!mayPair && (
          <p className="text-sm text-amber-700">
            {STATUS_LABEL[data.status]} 항목은 데이터가 정합니다. 승인·거절할 수 없고, 모르겠음·조정자에게 넘김만 남길 수 있습니다.
          </p>
        )}
        <div className="flex flex-wrap gap-4 text-sm">
          {(Object.keys(outcomeLabel) as Outcome[]).map((code) => (
            <label key={code} className="flex items-center gap-1">
              <input type="radio" name="outcome" checked={outcome === code} onChange={() => setOutcome(code)} />
              {outcomeLabel[code]}
            </label>
          ))}
        </div>
        {writesLineage && (
          <label className="block text-sm">
            <span className="text-slate-500">이유</span>
            <select className="ml-2 rounded border border-slate-300 px-2 py-1" value={reasonCode} onChange={(e) => setReason(e.target.value as Reason)}>
              {(Object.keys(REASON_LABEL) as Reason[]).map((code) => (
                <option key={code} value={code}>
                  {REASON_LABEL[code]}
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="block text-sm">
          <span className="text-slate-500">메모{legalDong && writesLineage ? " (필수: 왜 이렇게 정했는지)" : ""}</span>
          <textarea className="mt-1 w-full rounded border border-slate-300 p-2" rows={2} maxLength={2000} value={note} onChange={(e) => setNote(e.target.value)} />
        </label>
        {outcome === "link" && !chosen && (
          <p className="text-sm text-amber-700">{legalDong ? "위에서 짝지을 새 코드를 고르세요." : "위 후보 중 같은 땅을 고르세요."}</p>
        )}
        <div className="flex gap-2">
          <button
            type="button"
            disabled={busy || blocked}
            onClick={() => {
              setNotice(null);
              dryRun.mutate();
            }}
            className="rounded border border-slate-300 px-4 py-1.5 text-sm disabled:opacity-40"
          >
            미리 검사
          </button>
          <button type="submit" disabled={busy || Boolean(heldByOther) || blocked} className="rounded bg-slate-900 px-4 py-1.5 text-sm text-white disabled:opacity-40">
            결정 기록
          </button>
        </div>
        {notice && <p className="text-sm text-slate-700">{notice}</p>}
      </form>

      <div className="rounded-lg bg-white p-4 shadow-sm">
        <h2 className="font-semibold">결정 이력</h2>
        <DecisionTrail item={data} onRule={(decisionId, approve) => rule.mutate({ decisionId, approve })} />
      </div>
    </section>
  );
}

function ParcelCandidates({ item, chosen, onChoose }: { item: ReviewItem; chosen: string; onChoose: (code: string) => void }) {
  return (
    <div className="rounded-lg bg-white p-4 shadow-sm">
      <h2 className="font-semibold">옛 번호 후보</h2>
      {item.candidates.length === 0 ? (
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
            {item.candidates.map((c) => (
              <tr key={c.predecessor_pnu} className="border-t border-slate-100">
                <td className="py-1">
                  <input type="radio" name="candidate" checked={chosen === c.predecessor_pnu} onChange={() => onChoose(c.predecessor_pnu)} />
                </td>
                <td className="font-mono">{formatCode("parcel", c.predecessor_pnu)}</td>
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
  );
}

/** What the pairing run recorded for an abolished 법정동 code, and the new codes it offers. */
export function LegalDongPanel({ item, chosen, onChoose }: { item: ReviewItem; chosen: string; onChoose: (code: string) => void }) {
  const evidence = item.legal_dong;
  if (!evidence) return null;
  const split = Object.entries(evidence.split_into);
  const rows: [string, string][] = [
    ["옛 코드", `${item.subject_code} ${evidence.name}`],
    ["단위", levelLabel(evidence.level)],
    ["폐지일", formatDay(evidence.abolished_date)],
    ["이름 규칙", legalDongRuleLabel(evidence.rule)],
    ["지번 비교", evidence.jibun || "—"],
    ["필지 번호 공식 이력 (30527)", officialHistoryLabel(evidence.official_history)],
    ["공식 이력이 옮긴 필지 (짝은 아님)", evidence.official_partial_moves ? `${evidence.official_partial_moves}필지` : "—"],
    ...(evidence.waiting_for ? ([["기다리는 데이터", evidence.waiting_for]] as [string, string][]) : []),
  ];
  return (
    <div className="space-y-4 rounded-lg bg-white p-4 shadow-sm">
      <h2 className="font-semibold">짝 맞추기가 기록한 근거</h2>
      <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
        {rows.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-slate-500">{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
      {split.length > 0 && (
        <div className="text-sm">
          <h3 className="font-medium">지번이 간 곳</h3>
          <ul className="mt-1 font-mono">
            {split.map(([code, count]) => (
              <li key={code}>
                {code} · {count}필지
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="text-sm">
        <h3 className="font-medium">새 코드 후보</h3>
        {evidence.candidates.length === 0 ? (
          <label className="mt-1 block">
            <span className="text-slate-500">짝 맞추기가 후보를 찾지 못했습니다. 짝지을 새 코드(10자리)를 적으면 다음 짝 맞추기가 전체 코드 표로 다시 확인합니다.</span>
            <input
              className="mt-1 block w-40 rounded border border-slate-300 px-2 py-1 font-mono"
              inputMode="numeric"
              value={chosen}
              onChange={(e) => onChoose(e.target.value.replace(/\D/g, "").slice(0, 10))}
            />
          </label>
        ) : (
          <table className="mt-1 w-full">
            <thead className="text-left text-slate-500">
              <tr>
                <th className="py-1">선택</th>
                <th>새 코드</th>
                <th>이름</th>
                <th>생성일</th>
              </tr>
            </thead>
            <tbody>
              {evidence.candidates.map((c) => (
                <tr key={c.code} className="border-t border-slate-100">
                  <td className="py-1">
                    <input type="radio" name="candidate" checked={chosen === c.code} onChange={() => onChoose(c.code)} />
                  </td>
                  <td className="font-mono">{c.code}</td>
                  <td>{c.name}</td>
                  <td>{formatDay(c.created_date)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/** Every decision on the item, newest first: who, when, what, why. */
export function DecisionTrail({ item, onRule }: { item: ReviewItem; onRule: (decisionId: string, approve: boolean) => void }) {
  if (item.decisions.length === 0) return <p className="mt-2 text-sm text-slate-500">아직 결정이 없습니다.</p>;
  const labels = item.kind === "legal_dong" ? LEGAL_DONG_OUTCOME_LABEL : OUTCOME_LABEL;
  return (
    <ul className="mt-2 space-y-2 text-sm">
      {item.decisions.map((d: StewardDecision) => (
        <li key={d.decision_id} className="flex flex-wrap items-center gap-2 border-t border-slate-100 pt-2">
          <span className="font-medium">{labels[d.outcome]}</span>
          {d.candidate_code && <span className="font-mono">{formatCode(item.kind, d.candidate_code)}</span>}
          {d.reason_code && <span className="text-slate-600">{REASON_LABEL[d.reason_code]}</span>}
          {d.note && <span className="text-slate-700">“{d.note}”</span>}
          <span className="text-slate-500">
            {new Date(d.decided_at).toLocaleString("ko-KR")} · <span className="font-mono">{d.decided_by}</span>
          </span>
          {d.requires_approval && (
            <span className="rounded bg-slate-100 px-1">{d.approval === "approved" ? "승인됨" : d.approval === "rejected" ? "거절됨" : "승인 대기"}</span>
          )}
          {d.requires_approval && !d.approval && (
            <span className="ml-auto flex gap-2">
              <button type="button" onClick={() => onRule(d.decision_id, true)} className="rounded border border-slate-300 px-2 py-0.5">
                승인
              </button>
              <button type="button" onClick={() => onRule(d.decision_id, false)} className="rounded border border-slate-300 px-2 py-0.5">
                거절
              </button>
            </span>
          )}
        </li>
      ))}
    </ul>
  );
}
