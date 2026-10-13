// 서비스 점검 (root ADR-0207): how Korea reaches the live hostnames and how long pull request CI
// takes, as the host's hourly check wrote them. Plain sentences for the tab under the graph.
import type { CiWeek, EdgeHostCheck } from "../api/client";

/** Cloudflare's Seoul data centre: where a Korean visitor should enter. */
export const SEOUL = "ICN";

/** Whether this hostname is entered somewhere other than Seoul. */
export function enteredAbroad(host: EdgeHostCheck): boolean {
  return host.colo != null && host.colo !== SEOUL;
}

/** One hostname as one sentence: where Korea enters it and how long an answer takes. */
export function edgeSentence(host: EdgeHostCheck, referenceColo: string | null | undefined): string {
  if (host.samples === 0) return `측정 실패 · ${host.error ?? "응답 없음"}`;
  const speed = `보통 ${host.p50_ms?.toLocaleString("ko-KR")}ms, 느리면 ${host.p95_ms?.toLocaleString("ko-KR")}ms`;
  if (!enteredAbroad(host)) return `서울(${SEOUL})로 들어와요 · ${speed}`;
  const cause =
    referenceColo === SEOUL ? " — cloudflare.com 은 서울로 들어오니, 요금제의 경로 배정 때문이에요(코드 문제 아님)" : "";
  return `서울이 아니라 ${host.colo} 로 들어가요 · ${speed}${cause}`;
}

/** A week's verdict against the one budget (tools/ci/ci-time-budget.contract.json). */
export function ciWeekSentence(week: CiWeek, budgetMinutes: number | null | undefined): string {
  if (week.count === 0 || week.p50 == null) return "측정된 PR 없음";
  const median = `PR ${week.count}건 · 보통 ${week.p50}분, 느리면 ${week.p90}분`;
  if (budgetMinutes == null) return median;
  return week.p50 > budgetMinutes ? `${median} · 예산 ${budgetMinutes}분 초과` : `${median} · 예산 ${budgetMinutes}분 안`;
}
