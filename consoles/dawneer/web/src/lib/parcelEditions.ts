// 지적 판 tab (root ADR-0291, ADR-0379): what each readiness stage means and one line per edition.
import type { OperatorActionRequest, ParcelEditionReadiness } from "../api/client";
import { operatorActionRequest } from "./operatorActions";

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

/** The job whose run answers a promotion request (root ADR-0381). */
export const PROMOTION_JOB = "parcel_edition_readiness";

/** The request that asks to serve `edition`, or why it would be refused. The reason follows every operator
 * request's rules (one line, at most MAX_REASON_CHARS); the platform checks the same and the edition's
 * readiness, and only a ready edition's request is granted. */
export function promotionRequest(edition: string, reason: string): { request: OperatorActionRequest } | { problem: string } {
  const checked = operatorActionRequest("resume", reason, false);
  if ("problem" in checked) return checked;
  return { request: { action: "promote_edition", reason, drop_pending: false, edition } };
}

/** How the readiness check answered a request, in a few words. */
export function requestOutcomeLabel(outcome: string): string {
  if (outcome === "granted") return "승인됨 — 승격 PR 을 낼 차례";
  if (outcome === "expired") return "만료됨 (48시간 안에 판정되지 않음)";
  return outcome.startsWith("refused: ") ? `거부됨: ${outcome.slice("refused: ".length)}` : outcome;
}
