import { fireEvent, render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, expect, it } from "vitest";
import { ZoningLayerToggle } from "@/components/listings/zoning-layer-toggle";
import koMessages from "@/lib/i18n/ko.json";

function fakeMapbox() {
  const filters = new Map<string, unknown>();
  return {
    filters,
    getLayer: (id: string) =>
      ["zoning-fill", "zoning-outline", "zoning@z10-fill", "zoning@z10-outline"].includes(id)
        ? { id }
        : undefined,
    setFilter: (id: string, filter: unknown[] | null) => {
      filters.set(id, filter);
    },
  };
}

function renderToggle(mb: ReturnType<typeof fakeMapbox>) {
  render(
    <NextIntlClientProvider locale="ko" messages={koMessages} onError={() => {}}>
      <ZoningLayerToggle mapbox={mb} />
    </NextIntlClientProvider>,
  );
}

const codes = (mb: ReturnType<typeof fakeMapbox>, id: string) =>
  (mb.filters.get(id) as [string, unknown, [string, string[]]])[2][1];

describe("zoning layer toggle", () => {
  it("starts with every family off and the layers filtered to nothing", () => {
    const mb = fakeMapbox();
    renderToggle(mb);
    const buttons = screen.getAllByRole("button");
    expect(buttons.map((b) => b.getAttribute("aria-pressed"))).toEqual(
      Array(buttons.length).fill("false"),
    );
    expect(codes(mb, "zoning-fill")).toEqual([]);
    expect(codes(mb, "zoning-outline")).toEqual([]);
  });

  it("shows a family's layers when pressed and hides them when pressed again", () => {
    const mb = fakeMapbox();
    renderToggle(mb);
    const greenbelt = screen.getByRole("button", { name: "개발제한구역" });

    fireEvent.click(greenbelt);
    expect(greenbelt.getAttribute("aria-pressed")).toBe("true");
    expect(codes(mb, "zoning-fill")).toEqual(["D032"]);
    // Root ADR-0489: the low-zoom band draws the same zones and follows the same toggle.
    expect(codes(mb, "zoning@z10-outline")).toEqual(["D032"]);

    fireEvent.click(greenbelt);
    expect(greenbelt.getAttribute("aria-pressed")).toBe("false");
    expect(codes(mb, "zoning-fill")).toEqual([]);
  });

  it("says which families wait for collection instead of leaving them out", () => {
    renderToggle(fakeMapbox());
    const useDistrict = screen.getByRole("button", { name: /용도지역/ });
    const forest = screen.getByRole("button", { name: /산지/ });
    expect(useDistrict.textContent).toContain("일부 구역 수집 대기");
    expect((useDistrict as HTMLButtonElement).disabled).toBe(false);
    expect(forest.textContent).toContain("일부 구역 수집 대기");
    expect((forest as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByRole("button", { name: "개발제한구역" }).textContent).not.toContain("대기");
  });
});
