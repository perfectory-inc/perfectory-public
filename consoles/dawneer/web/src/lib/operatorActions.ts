// What an operator may ask of one scheduled job, and the request the platform takes (root ADR-0282).
// The platform checks the same rules and decides who may; these only spare a round trip.
import type { OperatorActionRequest } from "../api/client";

export const OPERATOR_ACTIONS = ["pause", "resume", "run_now", "stop", "set_window", "clear_window"] as const;
export type OperatorAction = (typeof OPERATOR_ACTIONS)[number];

export const OPERATOR_ACTION_LABEL: Record<OperatorAction, string> = {
  pause: "멈춤",
  resume: "다시 켜기",
  run_now: "지금 한 번 돌리기",
  stop: "도는 회차 중지",
  set_window: "실행 시간대 바꾸기",
  clear_window: "실행 시간대 되돌리기",
};

export const OPERATOR_ACTION_NOTE: Record<OperatorAction, string> = {
  pause: "새 회차를 시작하지 않습니다. 다시 켤 때까지 회차는 건너뜀으로 남습니다.",
  resume: "멈춤을 풉니다.",
  run_now: "10분 안에 한 번 시작합니다. 대기열·빌드는 예약 실행처럼 기다립니다.",
  stop: "지금 도는 회차를 멈추고 실패(중지)로 남깁니다. 다음 회차는 막지 않습니다.",
  set_window: "계약의 실행 시간대 대신 이 시간대(한국 시각)에 돌게 합니다. 정한 시간이 지나면 계약 값으로 돌아갑니다. 시작=끝이면 하루 종일입니다.",
  clear_window: "바꾼 실행 시간대를 지금 끝내고 계약 값으로 돌아갑니다.",
};

export const MAX_REASON_CHARS = 300;
export const MAX_WINDOW_HOURS = 168;

/** A run window override as entered: two KST clock times and how many hours it lasts. */
export interface WindowInput {
  start: string;
  end: string;
  hours: number;
}

const CLOCK = /^([01][0-9]|2[0-3]):[0-5][0-9]$/;

/** The request to send, or why it would be refused. */
export function operatorActionRequest(
  action: OperatorAction,
  reason: string,
  dropPending: boolean,
  window?: WindowInput,
): { request: OperatorActionRequest } | { problem: string } {
  if (!reason.trim()) return { problem: "이유를 적어 주세요." };
  if ([...reason].length > MAX_REASON_CHARS) return { problem: `이유는 ${MAX_REASON_CHARS}자까지입니다.` };
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(reason)) return { problem: "이유는 한 줄로 적어 주세요." };
  if (dropPending && action !== "pause") return { problem: "쌓인 회차 버리기는 멈춤에만 씁니다." };
  if (action !== "set_window") return { request: { action, reason, drop_pending: dropPending } };
  if (!window || !CLOCK.test(window.start) || !CLOCK.test(window.end)) {
    return { problem: "시간대는 시작과 끝을 HH:MM(한국 시각)으로 적어 주세요." };
  }
  if (!Number.isInteger(window.hours) || window.hours < 1 || window.hours > MAX_WINDOW_HOURS) {
    return { problem: `유지 시간은 1~${MAX_WINDOW_HOURS}시간입니다.` };
  }
  return {
    request: {
      action,
      reason,
      drop_pending: false,
      window_start: window.start,
      window_end: window.end,
      window_hours: window.hours,
    },
  };
}
