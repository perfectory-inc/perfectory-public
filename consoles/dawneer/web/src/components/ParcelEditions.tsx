// 지적 판 tab (root ADR-0291, ADR-0379): every cadastral edition, whether its data is all there, and who
// served which edition when. The served edition moves only through a gated promotion (runbook
// vworld-parcel-editions.md §3a), so this screen shows the evidence and the ledger, and no button.
import { useQuery } from "@tanstack/react-query";

import type { DawneerClient } from "../api/client";
import { STAGES, STAGE_LABEL, STAGE_NOTE, editionLabel, readinessSentence } from "../lib/parcelEditions";
import { Loading } from "./OperationsTables";

const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";

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
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

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
