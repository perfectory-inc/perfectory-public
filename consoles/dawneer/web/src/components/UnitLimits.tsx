// 자원 한도 tab (root ADR-0574): every host unit's and container's declared limits against what it
// measured. Read-only; a change is a pull request on the unit's declared entry, checked by the host
// budget guard.
import { useQuery } from "@tanstack/react-query";

import type { DawneerClient } from "../api/client";
import {
  byUrgency,
  containerSentence,
  duration,
  lastFailure,
  percent,
  proposalLink,
  size,
  STATE_LABEL,
  unitSentence,
} from "../lib/unitLimits";
import { Loading } from "./OperationsTables";

const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";
const NUM = `${TD} text-right tabular-nums`;
const STATE_CLASS: Record<string, string> = {
  failing: "text-red-700",
  near: "text-amber-700",
  fits: "text-emerald-700",
  unmeasured: "text-slate-500",
};

function State({ state }: { state: string }) {
  return <span className={`font-semibold ${STATE_CLASS[state] ?? ""}`}>{STATE_LABEL[state] ?? state}</span>;
}

export function UnitLimits({ client }: { client: DawneerClient }) {
  const limits = useQuery({ queryKey: ["unit-limits"], queryFn: () => client.unitLimits(), refetchInterval: 300_000 });
  const loading = Loading({ what: "자원 한도", query: limits });
  if (loading) return loading;
  const data = limits.data;
  if (!data) return null;
  if (data.error) return <p className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">한도 측정이 아직 없어요({data.error}).</p>;
  const units = byUrgency(data.units ?? []);
  const containers = byUrgency(data.containers ?? []);
  const count = (state: string) => units.filter((unit) => unit.state === state).length;

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-lg font-semibold">유닛의 한도와 잰 값</h2>
        <p className="mt-1 text-sm text-slate-600">
          서버가 30분마다 지난 {data.window_days ?? "-"}일의 run 을 systemd 기록에서 잰 결과예요
          {data.measured_at ? ` (${new Date(data.measured_at).toLocaleString("ko-KR")})` : ""}. 한도에서 죽음 {count("failing")} · 가까움{" "}
          {count("near")} · 넉넉함 {count("fits")}. 한도는 코드에만 있어요: 바꾸려면 「변경 제안」으로 그 유닛의 선언을 고치는 PR 을 열고,
          호스트 예산 가드가 합을 검사해요. 입력이 자라 닿은 것이면 한도가 아니라 성장(흘려 처리)을 고쳐요.
        </p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>유닛</th>
                <th className={TH}>상태</th>
                <th className={`${TH} text-right`}>메모리 한도</th>
                <th className={`${TH} text-right`}>최고점 p95 / 최대</th>
                <th className={`${TH} text-right`}>한도 대비</th>
                <th className={`${TH} text-right`}>시간 한도</th>
                <th className={`${TH} text-right`}>run p95 / 최장</th>
                <th className={`${TH} text-right`}>한도 대비</th>
                <th className={TH}>마지막 OOM·timeout</th>
                <th className={TH}>한 줄</th>
                <th className={TH}>변경</th>
              </tr>
            </thead>
            <tbody>
              {units.map((unit) => (
                <tr key={unit.unit} className="border-t border-slate-100">
                  <td className={`${TD} font-mono text-xs`}>{unit.unit}</td>
                  <td className={TD}>
                    <State state={unit.state} />
                  </td>
                  <td className={NUM}>{size(unit.memory_max)}</td>
                  <td className={NUM}>
                    {size(unit.peak_p95)} / {size(unit.peak_max)}
                  </td>
                  <td className={NUM}>{percent(unit.memory_share)}</td>
                  <td className={NUM}>{duration(unit.time_limit_seconds)}</td>
                  <td className={NUM}>
                    {duration(unit.seconds_p95)} / {duration(unit.seconds_max)}
                  </td>
                  <td className={NUM}>{percent(unit.time_share)}</td>
                  <td className={`${TD} text-xs`}>{lastFailure(unit)}</td>
                  <td className={`${TD} text-slate-700`}>{unitSentence(unit)}</td>
                  <td className={TD}>
                    <a className="text-xs text-sky-700 underline" href={proposalLink(unit.unit)} target="_blank" rel="noreferrer">
                      변경 제안
                    </a>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <h2 className="text-lg font-semibold">컨테이너의 상한과 잰 값</h2>
        <p className="mt-1 text-sm text-slate-600">
          지금 도는 컨테이너 상한의 합은 {size(data.container_caps_bytes)}, 서버 메모리는 {size(data.host_memory_bytes)} 예요. 계약에 적힌 예산 합(상주 + 가장
          큰 일회성·풀 + 여유 ≤ 62G)은 CI 의 호스트 예산 가드가 PR 마다 검사해요.
          {data.containers_error ? ` 컨테이너를 재지 못했어요(${data.containers_error}).` : ""}
        </p>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>컨테이너</th>
                <th className={TH}>상태</th>
                <th className={`${TH} text-right`}>상한</th>
                <th className={`${TH} text-right`}>최대 사용</th>
                <th className={`${TH} text-right`}>상한 대비</th>
                <th className={TH}>한 줄</th>
              </tr>
            </thead>
            <tbody>
              {containers.map((container) => (
                <tr key={container.container} className="border-t border-slate-100">
                  <td className={`${TD} font-mono text-xs`}>{container.container}</td>
                  <td className={TD}>
                    <State state={container.state} />
                  </td>
                  <td className={NUM}>{size(container.memory_max)}</td>
                  <td className={NUM}>{size(container.peak_max)}</td>
                  <td className={NUM}>{percent(container.memory_share)}</td>
                  <td className={`${TD} text-slate-700`}>{containerSentence(container)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
