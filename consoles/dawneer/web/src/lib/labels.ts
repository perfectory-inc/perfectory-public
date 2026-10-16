// Korean words for the steward API's codes. The codes are the contract; these are only how the
// screen says them.
import type { components } from "../api/foundation";

type S = components["schemas"];

export const STATUS_LABEL: Record<S["LineageReviewStatus"], string> = {
  needs_review: "소유 불일치",
  pending: "짝 없음",
  sample: "표본 검사",
  steward: "사람 확인",
  split: "분할 (데이터가 정함)",
  awaiting_data: "데이터 대기",
};

export const KIND_LABEL: Record<S["LineageReviewKind"], string> = {
  parcel: "필지 계보",
  legal_dong: "법정동 짝",
};

/** A 법정동 decision is a pair approved or refused, so its outcomes read differently. */
export const LEGAL_DONG_OUTCOME_LABEL: Record<S["LineageDecisionOutcome"], string> = {
  link: "승인 — 고른 새 코드와 짝",
  not_a_link: "거절 — 후보 중 짝 없음",
  unsure: "모르겠음",
  escalate: "조정자에게 넘김",
};

// The pairing run's codes (`legal_dong_code_change_pairs.review_handoff`).
const LEGAL_DONG_RULE_LABEL: Record<string, string> = {
  ambiguous_name_match: "이름 규칙: 맞는 이름이 여럿",
  no_candidate: "이름 규칙: 맞는 이름 없음",
};

const LEVEL_LABEL: Record<string, string> = {
  sido: "시도",
  sigungu: "시군구",
  eupmyeondong: "읍면동",
  ri: "리",
};

const OFFICIAL_HISTORY_LABEL: Record<string, string> = {
  read: "이번 짝 맞추기가 읽음",
  not_collected: "아직 수집 안 됨",
};

export function legalDongRuleLabel(rule: string): string {
  return LEGAL_DONG_RULE_LABEL[rule] ?? rule;
}

export function levelLabel(level: string): string {
  return LEVEL_LABEL[level] ?? level;
}

export function officialHistoryLabel(status: string): string {
  return OFFICIAL_HISTORY_LABEL[status] ?? status;
}

export const OUTCOME_LABEL: Record<S["LineageDecisionOutcome"], string> = {
  link: "같은 땅이다",
  not_a_link: "원래 번호 없음",
  unsure: "모르겠음",
  escalate: "조정자에게 넘김",
};

export const REASON_LABEL: Record<S["LineageReasonCode"], string> = {
  building_register: "건축물대장",
  ownership_record: "소유 기록",
  site_survey: "현장 확인",
  official_document: "공문서",
  cadastral_map: "지적도",
  other: "기타 (메모 필수)",
};

const GRADE_LABEL: Record<string, string> = {
  official: "공식",
  code_derived: "코드 도출",
  evidence_strong: "강한 근거",
  evidence_weak: "약한 근거",
  needs_review: "검토 필요",
  pending: "보류",
};

export function gradeLabel(grade: string): string {
  return GRADE_LABEL[grade] ?? grade;
}

/** A code-table day, `YYYYMMDD`, as `YYYY-MM-DD`; empty as a dash. */
export function formatDay(day: string): string {
  return /^\d{8}$/.test(day) ? `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6)}` : day || "—";
}

/** A review item's subject or candidate code as staff read it: a PNU split, a 법정동 code as is. */
export function formatCode(kind: S["LineageReviewKind"], code: string): string {
  return kind === "parcel" ? formatPnu(code) : code;
}

/** `1234567890 1 0001-0000` → "1234567890 산 1-0" style: dong code, mountain mark, main-sub lot. */
export function formatPnu(pnu: string): string {
  if (!/^\d{19}$/.test(pnu)) return pnu;
  const dong = pnu.slice(0, 10);
  const mountain = pnu[10] === "2" ? "산 " : "";
  const main = Number(pnu.slice(11, 15));
  const sub = Number(pnu.slice(15, 19));
  return `${dong} ${mountain}${main}${sub ? `-${sub}` : ""}`;
}
