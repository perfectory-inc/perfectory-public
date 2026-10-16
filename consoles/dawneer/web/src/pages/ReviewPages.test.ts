import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ReviewItem } from "../api/client";
import { DecisionTrail, LegalDongPanel } from "./ReviewItemPage";
import { ReviewTable } from "./ReviewListPage";

// Synthetic codes in the repository-reserved 99999 namespace (public-fixture-safety).
const parcel: ReviewItem = {
  item_id: "00000000-0000-5000-8000-000000000001",
  kind: "parcel",
  subject_code: "9999930100200120003",
  status: "pending",
  evidence_etag: "0".repeat(64),
  candidates: [],
  legal_dong: null,
  claim: null,
  decisions: [],
};

const dong: ReviewItem = {
  item_id: "00000000-0000-5000-8000-000000000002",
  kind: "legal_dong",
  subject_code: "9999910100",
  status: "steward",
  evidence_etag: "1".repeat(64),
  candidates: [],
  legal_dong: {
    name: "합성시 옛동",
    level: "eupmyeondong",
    abolished_date: "20990630",
    rule: "ambiguous_name_match",
    jibun: "no 지번 in any new code",
    split_into: {},
    waiting_for: "",
    official_history: "read",
    official_partial_moves: 0,
    candidates: [
      { code: "9999930100", name: "합성시 새동", created_date: "20990701" },
      { code: "9999930200", name: "합성시 다른동", created_date: "" },
    ],
  },
  claim: null,
  decisions: [
    {
      decision_id: "00000000-0000-7000-8000-000000000009",
      outcome: "link",
      candidate_code: "9999930100",
      reason_code: "official_document",
      note: "합성 공문",
      decided_by: "00000000-0000-0000-0000-000000000001",
      decided_at: "2099-07-02T00:00:00Z",
      requires_approval: false,
      approval: null,
      supersedes_decision_id: null,
    },
  ],
};

const render = (element: Parameters<typeof renderToStaticMarkup>[0]) => renderToStaticMarkup(element);

describe("the review list", () => {
  it("shows a parcel and a 법정동 pair in one table, each read its own way", () => {
    const markup = render(createElement(ReviewTable, { items: [dong, parcel] }));
    expect(markup).toContain("필지 계보");
    expect(markup).toContain("법정동 짝");
    expect(markup).toContain("9999930100 산 12-3"); // a PNU split into dong, mountain mark and lot
    expect(markup).toContain("9999910100");
    expect(markup).toContain("합성시 옛동");
    expect(markup).toContain("사람 확인");
    expect(markup).toContain(`href="#/items/${dong.item_id}"`);
  });
});

describe("a 법정동 item", () => {
  it("shows what the pairing run recorded and the codes it offers", () => {
    const markup = render(createElement(LegalDongPanel, { item: dong, chosen: "", onChoose: () => {} }));
    for (const shown of ["합성시 옛동", "읍면동", "2099-06-30", "2099-07-01", "이름 규칙: 맞는 이름이 여럿", "필지 번호 공식 이력 (30527)", "이번 짝 맞추기가 읽음", "합성시 새동", "9999930200"]) {
      expect(markup).toContain(shown);
    }
  });

  it("asks for a code when the run offered none", () => {
    if (!dong.legal_dong) throw new Error("the fixture is a 법정동 item");
    const bare: ReviewItem = { ...dong, legal_dong: { ...dong.legal_dong, candidates: [] } };
    const markup = render(createElement(LegalDongPanel, { item: bare, chosen: "", onChoose: () => {} }));
    expect(markup).toContain("후보를 찾지 못했습니다");
    expect(markup).toContain("<input");
  });

  it("shows the decision trail: what, which code, why and who", () => {
    const markup = render(createElement(DecisionTrail, { item: dong, onRule: () => {} }));
    expect(markup).toContain("승인 — 고른 새 코드와 짝");
    expect(markup).toContain("공문서");
    expect(markup).toContain("합성 공문");
    expect(markup).toContain("00000000-0000-0000-0000-000000000001");
  });
});
