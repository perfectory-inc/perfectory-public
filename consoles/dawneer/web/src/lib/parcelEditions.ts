// 지적 판 tab (root ADR-0291, ADR-0379): what each readiness stage means and one line per edition.
import type { ParcelEditionReadiness } from "../api/client";

export const STAGES = ["source", "silver", "rehearsal"] as const;
export type Stage = (typeof STAGES)[number];

export const STAGE_LABEL: Record<Stage, string> = {
  source: "원천",
  silver: "Silver 적재",
  rehearsal: "리허설",
};

export const STAGE_NOTE: Record<Stage, string> = {
  source: "모든 시군구 파일을 재 뒀고, 제공자가 그 뒤에 다시 올린 파일이 없다",
  silver: "silver.parcel_boundaries 에 들어 있고, 서빙판보다 1% 넘게 적지 않다",
  rehearsal: "리허설 Gold 가 대조 관문 거부 없이 만들어지고, 그 Gold 의 굽기가 통과했다",
};

/** "YYYYMM" as the operator says it: 2026년 10월판. */
export function editionLabel(edition: string): string {
  return `${edition.slice(0, 4)}년 ${Number(edition.slice(4, 6))}월판`;
}

/** One line: served, ready to promote, or the first stage it stops at. */
export function readinessSentence(row: ParcelEditionReadiness): string {
  if (row.served) return "지금 서빙 중";
  if (row.ready) return "준비됨 — 승격할 수 있어요";
  const stage = row.first_failing_stage;
  if (!stage) return "준비 안 됨";
  return `${(STAGE_LABEL as Record<string, string>)[stage] ?? stage} 단계에서 멈춤`;
}
