import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { describe, expect, it, vi } from "vitest";
import { ComplexSummaryCard } from "@/components/panels/complex/summary";
import { type ComplexInfo, ComplexInfoSchema } from "@/lib/api/complexes";
import koMessages from "@/lib/i18n/ko.json";
import type { PanelStackEntry } from "@/lib/panel/types";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

const COMPLEX_ID = "7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5b";
const entry = {
  kind: "complex",
  id: COMPLEX_ID,
  view: "summary",
} satisfies Extract<PanelStackEntry, { kind: "complex" }>;

/**
 * Builds card input the same way production does — through the schema — so a test cannot assert a
 * shape the API layer would never produce.
 */
function complexInfo(overrides: Record<string, unknown> = {}): ComplexInfo {
  return ComplexInfoSchema.parse({
    lakehouse_complex_id: COMPLEX_ID,
    official_complex_code: "111010",
    name: "반월특수지역",
    kind: "national",
    area_m2: 12_345_678,
    ...overrides,
  });
}

function renderCard(data: ComplexInfo): void {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <NextIntlClientProvider locale="ko" messages={koMessages}>
      {children}
    </NextIntlClientProvider>
  );
  render(<ComplexSummaryCard entry={entry} data={data} />, { wrapper });
}

describe("ComplexSummaryCard", () => {
  it("leads with what the complex is and where it is", () => {
    renderCard(complexInfo({ address_text: "경기도 안산시 단원구", status: "operating" }));

    expect(screen.getByRole("heading", { level: 2 }).textContent).toBe("반월특수지역");
    expect(screen.getByText("국가산업단지")).toBeTruthy();
    expect(screen.getByText("경기도 안산시 단원구")).toBeTruthy();
    expect(screen.getByText("운영중")).toBeTruthy();
  });

  it("omits the row entirely for a column the source did not state", () => {
    // 26% of complexes have no completion date. A `—` would claim the field was answered and found
    // empty; the source made no such statement, so there is no row.
    renderCard(complexInfo());

    expect(screen.queryByText("준공일")).toBeNull();
    expect(screen.queryByText("착공일")).toBeNull();
    expect(screen.queryByText("주소")).toBeNull();
    expect(screen.queryByText("—")).toBeNull();
  });

  it("treats blank-but-present text as no statement", () => {
    renderCard(complexInfo({ management_agency_name: "   " }));

    expect(screen.queryByText("관리기관")).toBeNull();
  });

  it("renders zero progress, because not-started is an answer", () => {
    // The one field where a truthiness check would erase a fact: `0.00` means site works have not
    // started, which the contract states is different from the source saying nothing.
    renderCard(complexInfo({ development_progress_percent: "0.00" }));

    expect(screen.getByText("조성진행률")).toBeTruthy();
    expect(screen.getByText("0.00%")).toBeTruthy();
  });

  it("distinguishes a lifecycle this contract does not recognize from no lifecycle at all", () => {
    renderCard(complexInfo({ status: "unknown" }));
    expect(screen.getByText("원천 미분류")).toBeTruthy();
  });

  it("shows a wire value it has no Korean label for rather than a message key", () => {
    renderCard(complexInfo({ lot_sales_status: "some_future_value" }));

    expect(screen.getByText("some_future_value")).toBeTruthy();
    expect(screen.queryByText(/lotSalesValue\./)).toBeNull();
  });

  it("puts the long free text below the short facts", () => {
    renderCard(
      complexInfo({
        designated_date: "1977-03-07",
        development_purpose_raw: "수도권 공업 재배치",
        invited_industries_raw: "기계, 전기전자",
      }),
    );

    const body = document.body.textContent ?? "";
    expect(body.indexOf("1977-03-07")).toBeGreaterThan(-1);
    expect(body.indexOf("1977-03-07")).toBeLessThan(body.indexOf("수도권 공업 재배치"));
    expect(body.indexOf("수도권 공업 재배치")).toBeLessThan(body.indexOf("기계, 전기전자"));
  });
});

describe("ComplexSummaryCard document facts", () => {
  const SHA = "a".repeat(64);
  const source = (media_format: string) => ({
    notice_sequence: 99001,
    notice_number: "합성시 고시 제2099-1호",
    notice_date: "2099-01-01",
    sha256: SHA,
    media_format,
  });
  const fact = (value: Record<string, unknown>, page_number: number) => ({
    value,
    page_number,
    quote: "합성 인용",
    method: "rule",
    page_method: "text_layer",
    check: "table_total",
  });
  const facts = (media_format = "pdf") => ({
    table: "gold.complex_document_facts",
    iceberg_snapshot_id: "999990000000000001",
    run_id: "20991231T000000Z",
    kinds: {
      land_use_area: {
        source: source(media_format),
        facts: [
          fact({ label: "산업시설용지", area_m2: 123456, ratio_pct: 61.5 }, 4),
          fact({ label: "도로", parent: "공공시설용지", area_m2: 2345.5, ratio_pct: null }, 5),
        ],
      },
      invited_industry: {
        source: source(media_format),
        facts: [fact({ ksic_division: "24", ksic_name: "1차 금속 제조업" }, 7)],
      },
    },
  });

  it("labels what the notices state as AI 추출 and opens the PDF at the page it was read on", () => {
    renderCard(complexInfo({ document_facts: facts() }));

    expect(screen.getAllByText("AI 추출")).toHaveLength(2);
    expect(screen.getByText("산업시설용지")).toBeTruthy();
    expect(screen.getByText("공공시설용지 · 도로")).toBeTruthy();
    expect(screen.getByText("123,456 ㎡")).toBeTruthy();
    expect(screen.getByText("61.5%")).toBeTruthy();
    expect(screen.getByText("1차 금속 제조업 (24)")).toBeTruthy();
    const link = screen.getByRole("link", {
      name: "원문 합성시 고시 제2099-1호 · 2099-01-01 · 4–5쪽",
    });
    expect(link.getAttribute("href")).toBe(
      `https://catalog.example.test/onbid/media/sha256/${SHA}.pdf#page=4`,
    );
  });

  it("names an HWP notice's page without a link the edge does not serve", () => {
    renderCard(complexInfo({ document_facts: facts("hwp") }));

    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByText("원문 합성시 고시 제2099-1호 · 2099-01-01 · 7쪽")).toBeTruthy();
  });

  it("shows nothing for a complex with no admitted facts or an older profile", () => {
    renderCard(complexInfo());
    expect(screen.queryByText("AI 추출")).toBeNull();
    renderCard(complexInfo({ document_facts: { ...facts(), kinds: {} } }));
    expect(screen.queryByText("AI 추출")).toBeNull();
  });
});

describe("ComplexInfoSchema", () => {
  it("rejects a response missing a column the card always draws", () => {
    expect(() =>
      ComplexInfoSchema.parse({
        lakehouse_complex_id: COMPLEX_ID,
        official_complex_code: "111010",
        kind: "national",
        area_m2: 1,
      }),
    ).toThrow();
  });

  it("does not pass through a column the schema does not name", () => {
    // The API growing a column must not reach a card until this file admits it. `ComplexInfo` is
    // derived from this schema, so a card reading an undeclared column also fails to type-check —
    // this is the runtime half of the same guard.
    const parsed = ComplexInfoSchema.parse({
      ...complexInfo(),
      secret_internal_column: "should not reach the card",
    }) as Record<string, unknown>;

    expect("secret_internal_column" in parsed).toBe(false);
  });

  it("accepts both spellings of an absent column", () => {
    expect(ComplexInfoSchema.parse({ ...complexInfo(), status: null }).status).toBeNull();
    expect(complexInfo().status).toBeUndefined();
  });
});
