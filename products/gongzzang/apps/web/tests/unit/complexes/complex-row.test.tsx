import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mockPush = vi.fn();
const mockReplace = vi.fn();
const mockSearchParams = new URLSearchParams();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: mockPush, replace: mockReplace, back: vi.fn() }),
  useSearchParams: () => mockSearchParams,
  usePathname: () => "/complexes",
}));

import { ComplexRow } from "@/components/complexes/complex-row";
import { ComplexListItemSchema } from "@/lib/complexes/api";
import koMessages from "@/lib/i18n/ko.json";

const LAKEHOUSE_ID = "024905cb-8a9f-54ef-ac7d-ca30d796a6b9";
const CATALOG_ID = "01a0136d-1b7c-7d61-9acc-fb2d2f64a146";

function row(overrides: Record<string, unknown> = {}) {
  return ComplexListItemSchema.parse({
    lakehouse_complex_id: LAKEHOUSE_ID,
    official_complex_code: "141010",
    name: "반월특수지역",
    kind: "national",
    status: "developing",
    address_text: "경기도 안산시, 시흥시, 화성시 일원",
    ...overrides,
  });
}

function renderRow(item: ReturnType<typeof row>): void {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <NextIntlClientProvider locale="ko" messages={koMessages}>
      {children}
    </NextIntlClientProvider>
  );
  render(<ComplexRow item={item} />, { wrapper });
}

beforeEach(() => {
  mockPush.mockClear();
  mockReplace.mockClear();
  mockSearchParams.delete("p");
});

describe("ComplexRow", () => {
  it("says what the complex is in the reader's words, not the wire's", () => {
    renderRow(row());

    expect(screen.getByText("반월특수지역")).toBeTruthy();
    expect(
      screen.getByText("경기도 안산시, 시흥시, 화성시 일원 · 국가산업단지 · 조성중"),
    ).toBeTruthy();
  });

  it("drops the piece the source did not state instead of leaving a gap", () => {
    // Six canonical complexes have no address. A row that kept the separator would read as
    // " · 일반산업단지 · 운영중", which looks like a rendering fault rather than an unstated fact.
    renderRow(row({ address_text: null, kind: "general", status: "operating" }));

    expect(screen.getByText("일반산업단지 · 운영중")).toBeTruthy();
  });

  it("opens the panel on the lakehouse id the row carries", () => {
    renderRow(row());

    screen.getByTestId("complex-row").click();

    expect(mockPush).toHaveBeenCalledWith(`/complexes?p=complex%3A${LAKEHOUSE_ID}.summary`, {
      scroll: false,
    });
  });

  it("does not offer to open a complex whose id the panel cannot accept", () => {
    // Feeding the row the catalog `id` for the same complex — the id-space mix-up root ADR-0048
    // exists to prevent. The row must not become a click that 404s.
    renderRow(row({ lakehouse_complex_id: CATALOG_ID }));

    expect(screen.queryByTestId("complex-row")).toBeNull();
    expect(screen.getByTestId("complex-row-unopenable")).toBeTruthy();
    expect(mockPush).not.toHaveBeenCalled();
  });

  it("renders a complex with no lakehouse identity as a row that says so", () => {
    renderRow(row({ lakehouse_complex_id: null }));

    expect(screen.queryByTestId("complex-row")).toBeNull();
    expect(screen.getByText("반월특수지역")).toBeTruthy();
    expect(screen.getByText(koMessages.complexes.row.noDetail)).toBeTruthy();
  });

  it("shows a wire value it has no Korean label for rather than a message key", () => {
    renderRow(row({ kind: "some_future_kind", status: null }));

    expect(screen.getByText(/some_future_kind/)).toBeTruthy();
    expect(screen.queryByText(/complexValues\./)).toBeNull();
  });
});
