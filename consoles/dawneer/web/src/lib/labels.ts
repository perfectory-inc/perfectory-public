// Korean words for the steward API's codes. The codes are the contract; these are only how the
// screen says them.
import type { components } from "../api/foundation";

type S = components["schemas"];

export const STATUS_LABEL: Record<S["LineageReviewStatus"], string> = {
  needs_review: "소유 불일치",
  pending: "짝 없음",
  sample: "표본 검사",
};

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

/** `1234567890 1 0001-0000` → "1234567890 산 1-0" style: dong code, mountain mark, main-sub lot. */
export function formatPnu(pnu: string): string {
  if (!/^\d{19}$/.test(pnu)) return pnu;
  const dong = pnu.slice(0, 10);
  const mountain = pnu[10] === "2" ? "산 " : "";
  const main = Number(pnu.slice(11, 15));
  const sub = Number(pnu.slice(15, 19));
  return `${dong} ${mountain}${main}${sub ? `-${sub}` : ""}`;
}
