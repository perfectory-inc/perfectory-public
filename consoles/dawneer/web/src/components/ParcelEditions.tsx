// 지적 판 tab (root ADR-0291, ADR-0379): every cadastral edition, whether its data is all there, and who
// served which edition when. A ready edition may be asked for (root ADR-0381): the request is recorded
// with the signed-in staff member and a reason, the readiness check grants it only for a ready edition,
// and serving moves through the reviewed change that grant prepares, every lane together.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import type { DawneerClient } from "../api/client";
import {
  PROMOTION_JOB,
  STAGES,
  STAGE_LABEL,
  STAGE_NOTE,
  editionLabel,
  promotionRequest,
  readinessSentence,
  requestOutcomeLabel,
} from "../lib/parcelEditions";
import { operatorActionMessage, recordedMessage } from "./OperatorActions";
import { Loading } from "./OperationsTables";

const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";

/** Ask to serve one ready edition, with why; the readiness check answers on its next run. */
function PromoteForm({ client, edition }: { client: DawneerClient; edition: string }) {
  const queryClient = useQueryClient();
  const [reason, setReason] = useState("");
  const [notice, setNotice] = useState<string | null>(null);
  const checked = promotionRequest(edition, reason);
  const send = useMutation({
    mutationFn: () => {
      if ("problem" in checked) throw new Error(checked.problem);
      return client.recordOperatorAction(PROMOTION_JOB, checked.request);
    },
    onSuccess: (recorded) => {
      setNotice(`${recordedMessage(recorded)} · 다음 판정에서 답합니다`);
      setReason("");
      void queryClient.invalidateQueries({ queryKey: ["parcel-editions"] });
    },
    onError: (error) => setNotice(operatorActionMessage(error)),
  });
  return (
    <form
      className="mt-2 flex flex-col gap-1 text-xs"
      onSubmit={(event) => {
        event.preventDefault();
        send.mutate();
      }}
    >
      <input
        aria-label={`${editionLabel(edition)} 승격 이유`}
        className="rounded border border-slate-300 px-1 py-0.5"
        placeholder="왜 이 판으로 옮기나요(한 줄)"
        value={reason}
        onChange={(event) => setReason(event.target.value)}
      />
      <button
        type="submit"
        disabled={"problem" in checked || send.isPending}
        className="rounded bg-slate-900 px-2 py-1 text-white disabled:bg-slate-300"
      >
        {editionLabel(edition)}으로 승격 요청
      </button>
      {notice ? <span className="text-slate-600">{notice}</span> : null}
    </form>
  );
}

export function ParcelEditions({ client }: { client: DawneerClient }) {
  const report = useQuery({ queryKey: ["parcel-editions"], queryFn: () => client.parcelEditions(), refetchInterval: 300_000 });
  const loading = Loading({ what: "지적 판", query: report });
  if (loading) return loading;
  const data = report.data;
  if (!data) return null;
  if (data.error) {
    return (
      <p className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
        판 준비 판정이 아직 없어요({data.error}).
      </p>
    );
  }

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-lg font-semibold">판마다 데이터가 다 있나</h2>
        <p className="mt-1 text-sm text-slate-600">
          세 단계를 모두 통과한 판만 서빙판으로 올릴 수 있어요(ADR-0379). 지금 서빙판:{" "}
          {data.served_edition ? editionLabel(data.served_edition) : "-"}
          {data.checked_at_utc ? ` · ${new Date(data.checked_at_utc).toLocaleString("ko-KR")} 측정` : null}
        </p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>판</th>
                {STAGES.map((stage) => (
                  <th key={stage} className={TH} title={STAGE_NOTE[stage]}>
                    {STAGE_LABEL[stage]}
                  </th>
                ))}
                <th className={TH}>한 줄</th>
              </tr>
            </thead>
            <tbody>
              {data.editions.map((row) => (
                <tr key={row.edition} className={`border-t border-slate-100 ${row.served ? "bg-emerald-50" : ""}`}>
                  <td className={`${TD} whitespace-nowrap font-semibold`}>{editionLabel(row.edition)}</td>
                  {STAGES.map((stage) => (
                    <td key={stage} className={TD}>
                      <span className={`font-semibold ${row.stages[stage].passed ? "text-emerald-700" : "text-red-700"}`}>
                        {row.stages[stage].passed ? "통과" : "미통과"}
                      </span>
                      <div className="text-xs text-slate-600">{row.stages[stage].detail}</div>
                    </td>
                  ))}
                  <td className={`${TD} whitespace-nowrap ${row.ready ? "text-emerald-700" : "text-slate-700"}`}>
                    {readinessSentence(row)}
                    {row.ready && !row.served ? <PromoteForm client={client} edition={row.edition} /> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {data.promotion_requests?.length ? (
        <section>
          <h2 className="text-lg font-semibold">승격 요청</h2>
          <p className="mt-1 text-sm text-slate-600">
            콘솔에서 한 요청과 판정이 답한 결과예요. 승인된 요청은 서버에 승격된 계약이 놓이고, 그 변경이 검토를 거쳐 들어가면
            모든 레인이 함께 새 판으로 옮겨 가요.
          </p>
          <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
            <table className="min-w-full">
              <thead className="bg-slate-50">
                <tr>
                  <th className={TH}>언제</th>
                  <th className={TH}>판</th>
                  <th className={TH}>누가</th>
                  <th className={TH}>왜</th>
                  <th className={TH}>답</th>
                </tr>
              </thead>
              <tbody>
                {data.promotion_requests.map((entry) => (
                  <tr key={entry.request} className="border-t border-slate-100">
                    <td className={`${TD} whitespace-nowrap`}>
                      {entry.requested_at ? new Date(entry.requested_at).toLocaleString("ko-KR") : "-"}
                    </td>
                    <td className={`${TD} whitespace-nowrap`}>{entry.edition ? editionLabel(entry.edition) : "-"}</td>
                    <td className={TD}>{entry.by ?? "-"}</td>
                    <td className={TD}>{entry.reason ?? "-"}</td>
                    <td className={`${TD} ${entry.outcome === "granted" ? "text-emerald-700" : "text-slate-700"}`}>
                      {requestOutcomeLabel(entry.outcome)}
                      {entry.contract ? <div className="font-mono text-xs text-slate-500">{entry.contract}</div> : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      <section>
        <h2 className="text-lg font-semibold">승격 기록</h2>
        <p className="mt-1 text-sm text-slate-600">계약에 덧붙이기만 하는 원장이에요. 마지막 줄이 지금 서빙판이에요.</p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>언제</th>
                <th className={TH}>판</th>
                <th className={TH}>누가</th>
                <th className={TH}>왜</th>
                <th className={TH}>준비 판정</th>
              </tr>
            </thead>
            <tbody>
              {data.promotions.map((entry) => (
                <tr key={entry.promoted_at_utc} className="border-t border-slate-100">
                  <td className={`${TD} whitespace-nowrap`}>{new Date(entry.promoted_at_utc).toLocaleString("ko-KR")}</td>
                  <td className={`${TD} whitespace-nowrap`}>
                    {entry.from_edition ? `${editionLabel(entry.from_edition)} → ` : ""}
                    {editionLabel(entry.edition)}
                  </td>
                  <td className={TD}>{entry.by}</td>
                  <td className={TD}>{entry.reason}</td>
                  <td className={`${TD} whitespace-nowrap`}>
                    {entry.readiness ? `${new Date(entry.readiness.checked_at_utc).toLocaleString("ko-KR")} 판정` : "관문 이전"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
