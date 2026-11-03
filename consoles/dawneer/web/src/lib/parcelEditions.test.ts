import { describe, expect, it } from "vitest";

import type { ParcelEditionReadiness } from "../api/client";
import { editionLabel, promotionRequest, readinessSentence, requestOutcomeLabel } from "./parcelEditions";

const stage = (passed: boolean) => ({ passed, detail: "합성" });
const row = (overrides: Partial<ParcelEditionReadiness>): ParcelEditionReadiness => ({
  edition: "209910",
  provider_base_month: "2099-10",
  served: false,
  ready: false,
  first_failing_stage: "rehearsal",
  stages: { source: stage(true), silver: stage(true), rehearsal: stage(false) },
  ...overrides,
});

describe("parcel editions", () => {
  it("names an edition by its month", () => {
    expect(editionLabel("209910")).toBe("2099년 10월판");
    expect(editionLabel("209906")).toBe("2099년 6월판");
  });

  it("says served, ready, or the first stage it stops at", () => {
    expect(readinessSentence(row({ served: true, ready: true, first_failing_stage: null }))).toBe("지금 서빙 중");
    expect(readinessSentence(row({ ready: true, first_failing_stage: null }))).toBe("준비됨 — 승격할 수 있어요");
    expect(readinessSentence(row({}))).toBe("리허설 단계에서 멈춤");
    expect(readinessSentence(row({ first_failing_stage: "source" }))).toBe("원천 단계에서 멈춤");
  });

  it("asks to serve an edition with one line of why", () => {
    expect(promotionRequest("209910", "새 판이 다 있다")).toEqual({
      request: { action: "promote_edition", reason: "새 판이 다 있다", drop_pending: false, edition: "209910" },
    });
    expect(promotionRequest("209910", "  ")).toEqual({ problem: "이유를 적어 주세요." });
    expect(promotionRequest("209910", "두\n줄")).toEqual({ problem: "이유는 한 줄로 적어 주세요." });
  });

  it("says how a request was answered", () => {
    expect(requestOutcomeLabel("granted")).toBe("승인됨 — 승격 PR 을 낼 차례");
    expect(requestOutcomeLabel("refused: 209910 is not ready")).toBe("거부됨: 209910 is not ready");
    expect(requestOutcomeLabel("expired")).toBe("만료됨 (48시간 안에 판정되지 않음)");
  });
});
