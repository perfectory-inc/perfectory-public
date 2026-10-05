import { describe, expect, it } from "vitest";
import "@/components/panels/panel-renderer";
import { getView } from "@/lib/panel/registry";

describe("PanelRenderer registry bootstrap", () => {
  it("registers default panel views in the client module graph", () => {
    expect(getView("parcel", "summary")).toBeDefined();
    expect(getView("parcel", "buildings")).toBeDefined();
    expect(getView("parcel", "listings")).toBeDefined();
    expect(getView("listing", "summary")).toBeDefined();
    expect(getView("complex", "summary")).toBeDefined();
  });
});
