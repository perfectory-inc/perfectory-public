// 서비스 점검 tab (root ADR-0207): the host's hourly check of how Korea reaches the live hostnames,
// and the weekly pull request CI time against its one budget.
import { useQuery } from "@tanstack/react-query";

import type { DawneerClient } from "../api/client";
import { ciWeekSentence, edgeSentence, enteredAbroad } from "../lib/serviceChecks";
import { Loading } from "./OperationsTables";

const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";

export function ServiceChecks({ client }: { client: DawneerClient }) {
  const checks = useQuery({ queryKey: ["service-checks"], queryFn: () => client.serviceChecks(), refetchInterval: 300_000 });
  const loading = Loading({ what: "서비스 점검", query: checks });
  if (loading) return loading;
  const data = checks.data;
  if (!data) return null;
  if (data.error) return <p className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">점검 결과가 아직 없어요({data.error}).</p>;
  const edge = data.edge;
  const ci = data.ci;
  const longest = Math.max(ci?.budget_minutes ?? 0, ...(ci?.weeks ?? []).map((week) => week.p90 ?? 0), 1);

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-lg font-semibold">한국에서 서비스 주소까지</h2>
        <p className="mt-1 text-sm text-slate-600">
          서버(한국)가 한 시간마다 각 주소에 10번씩 물어본 결과예요. 들어간 곳이 서울(ICN)이 아니면 멀리 돌아가서 느려져요.
          {edge ? ` 기준: cloudflare.com 은 ${edge.reference_colo ?? "알 수 없음"} 로 들어가요 · ${new Date(edge.checked_at).toLocaleString("ko-KR")} 측정` : null}
        </p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>주소</th>
                <th className={TH}>들어간 곳</th>
                <th className={`${TH} text-right`}>보통(p50)</th>
                <th className={`${TH} text-right`}>느리면(p95)</th>
                <th className={TH}>한 줄</th>
              </tr>
            </thead>
            <tbody>
              {(edge?.hosts ?? []).map((host) => (
                <tr key={host.hostname} className="border-t border-slate-100">
                  <td className={`${TD} font-mono text-xs`}>{host.hostname}</td>
                  <td className={`${TD} font-mono font-semibold ${enteredAbroad(host) ? "text-red-700" : "text-emerald-700"}`}>{host.colo ?? "-"}</td>
                  <td className={`${TD} text-right tabular-nums`}>{host.p50_ms != null ? `${host.p50_ms.toLocaleString("ko-KR")}ms` : "-"}</td>
                  <td className={`${TD} text-right tabular-nums`}>{host.p95_ms != null ? `${host.p95_ms.toLocaleString("ko-KR")}ms` : "-"}</td>
                  <td className={`${TD} text-slate-700`}>{edgeSentence(host, edge?.reference_colo)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <h2 className="text-lg font-semibold">PR CI 기다리는 시간 (주별)</h2>
        <p className="mt-1 text-sm text-slate-600">
          푸시부터 마지막 판정까지 걸린 시간의 주별 중앙값이에요(ADR-0203). 예산은 {ci?.budget_minutes ?? "-"}분.
          {ci?.pending_days ? ` 아직 못 가져온 날 ${ci.pending_days}일 — 그 주는 다 모이면 나와요.` : null}
          {ci?.error ? ` 마지막 가져오기 실패: ${ci.error}` : null}
        </p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>주 시작(UTC 월)</th>
                <th className={TH}>보통 · 느리면 (막대: 예산선까지)</th>
                <th className={TH}>한 줄</th>
                <th className={TH}>가장 느린 워크플로</th>
              </tr>
            </thead>
            <tbody>
              {(ci?.weeks ?? []).map((week) => {
                const over = ci?.budget_minutes != null && week.p50 != null && week.p50 > ci.budget_minutes;
                return (
                  <tr key={week.week_start} className="border-t border-slate-100">
                    <td className={`${TD} font-mono text-xs`}>{week.week_start}</td>
                    <td className={`${TD} w-64`}>
                      <div className="relative h-4 rounded bg-slate-100" aria-hidden>
                        <div className="absolute inset-y-0 left-0 rounded bg-slate-300" style={{ width: `${((week.p90 ?? 0) / longest) * 100}%` }} />
                        <div
                          className={`absolute inset-y-0 left-0 rounded ${over ? "bg-red-600" : "bg-slate-700"}`}
                          style={{ width: `${((week.p50 ?? 0) / longest) * 100}%` }}
                        />
                        {ci?.budget_minutes != null ? (
                          <div className="absolute inset-y-[-2px] w-0.5 bg-amber-500" style={{ left: `${(ci.budget_minutes / longest) * 100}%` }} />
                        ) : null}
                      </div>
                    </td>
                    <td className={`${TD} ${over ? "font-semibold text-red-700" : "text-slate-700"}`}>{ciWeekSentence(week, ci?.budget_minutes)}</td>
                    <td className={`${TD} text-xs text-slate-600`}>{week.worst_workflow ? `${week.worst_workflow} (${week.worst_workflow_p50}분)` : "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
