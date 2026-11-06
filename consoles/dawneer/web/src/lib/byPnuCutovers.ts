// A by-PNU cut-over approval and a lane's full bake as the console asks them (root ADR-0433). The
// platform checks the same rules and decides who may; these only spare a round trip.
import type { OperatorActionRequest } from "../api/client";
import { MAX_REASON_CHARS } from "./operatorActions";

/** The cut-over runner, the subject of every approval (catalog-domain CUTOVER_SUBJECT). */
export const CUTOVER_SUBJECT = "by_pnu_lane_cutover";
export const MAX_GENERATION = 9999;

const LANE = /^[a-z0-9]+(-[a-z0-9]+)*$/;

function reasonProblem(reason: string): string | null {
  if (!reason.trim()) return "이유를 적어 주세요. 기록에 남습니다.";
  if ([...reason].length > MAX_REASON_CHARS) return `이유는 ${MAX_REASON_CHARS}자까지입니다.`;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(reason)) return "이유는 한 줄로 적어 주세요.";
  return null;
}

/** The lanes as typed: spaces dropped, `lanes`, one lane or `a,b,...` each once. */
export function lanesProblem(lanes: string, single: boolean): string | null {
  const listed = lanes.split(",");
  if (!listed.every((lane) => LANE.test(lane))) return "레인은 소문자·숫자·- 로, 여러 개면 쉼표로 적어 주세요.";
  if (new Set(listed).size !== listed.length) return "같은 레인을 두 번 적었습니다.";
  if (listed.length > 1 && listed.includes("lanes")) return "`lanes` 는 혼자 씁니다(아직 전환 안 된 레인 전부).";
  if (single && (listed.length > 1 || lanes === "lanes")) return "전량 굽기는 레인 하나에만 씁니다.";
  return null;
}

/** The approval to send, or why it would be refused. */
export function cutoverRequest(
  lanes: string,
  generation: number,
  reason: string,
): { request: OperatorActionRequest } | { problem: string } {
  const compact = lanes.replace(/\s+/g, "");
  const problem =
    lanesProblem(compact, false) ??
    (!Number.isInteger(generation) || generation < 1 || generation > MAX_GENERATION
      ? `세대는 1~${MAX_GENERATION} 입니다.`
      : null) ??
    reasonProblem(reason);
  if (problem) return { problem };
  return { request: { action: "approve_cutover", lanes: compact, generation, reason, drop_pending: false } };
}

/** A full bake of one lane, asked of the job that bakes it. */
export function fullBakeRequest(lane: string, reason: string): { request: OperatorActionRequest } | { problem: string } {
  const compact = lane.trim();
  const problem = lanesProblem(compact, true) ?? reasonProblem(reason);
  if (problem) return { problem };
  return { request: { action: "force_full", lanes: compact, reason, drop_pending: false } };
}

/** Whether a job is a by-PNU bake, the only jobs a full bake is asked of. */
export function isByPnuBake(jobId: string): boolean {
  return /^by_pnu_[a-z0-9_]*[a-z0-9]_bake$/.test(jobId);
}

export const CUTOVER_STATE_LABEL: Record<string, string> = {
  pending: "호스트 응답 기다림",
  scheduled: "창 열리면 시작",
  started: "도는 중",
  passed: "끝남 · 통과",
  failed: "끝남 · 실패",
  refused: "거부됨",
  cancelled: "취소됨",
};

export const STEP_LABEL: Record<string, string> = {
  history: "실거래 이력",
  preview: "미리보기",
  gold: "Gold",
  parity: "대등성",
  bake: "굽기",
  equality: "관문 (가) 동일성",
  latency: "관문 (나) 지연",
  publish: "발행",
  "monitor-sample": "감시 표본",
};
