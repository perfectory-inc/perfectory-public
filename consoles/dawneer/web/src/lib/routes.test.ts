import { describe, expect, it } from "vitest";

import { FLOW_TABS, MENU, movedTo, resolveRoute } from "./routes";

describe("routes", () => {
  it("sends the old 운영 현황 and 데이터 카탈로그 addresses to the 데이터 흐름 screen", () => {
    expect(movedTo("/operations")).toBe("/flow");
    expect(movedTo("/catalog")).toBe("/flow/catalog");
    expect(resolveRoute("/operations")).toEqual({ page: "flow", tab: "timeline" });
    expect(resolveRoute("/catalog")).toEqual({ page: "flow", tab: "catalog" });
    expect(movedTo("/flow")).toBeNull();
  });

  it("keeps every capability of the two old pages reachable from the flow screen", () => {
    // 운영 현황: running now and the job list, quality checks, collection freshness;
    // 데이터 카탈로그: search and an entity's detail and lineage (opened from a search result).
    expect(FLOW_TABS.map((tab) => tab.id)).toEqual(["timeline", "jobs", "quality", "freshness", "catalog"]);
    for (const tab of FLOW_TABS) expect(resolveRoute(`/flow/${tab.id}`)).toEqual({ page: "flow", tab: tab.id });
    expect(resolveRoute("/flow/unknown")).toEqual({ page: "flow", tab: "timeline" });
  });

  it("has one menu for the flow and none for the moved pages", () => {
    expect(MENU.map((item) => item.href)).toEqual(["#/", "#/flow", "#/tiles", "#/complexes"]);
  });

  it("still finds the other screens", () => {
    expect(resolveRoute("/")).toEqual({ page: "review-list" });
    expect(resolveRoute("/items/00000000-0000-0000-0000-000000000001")).toEqual({
      page: "review-item",
      itemId: "00000000-0000-0000-0000-000000000001",
    });
    expect(resolveRoute("/tiles")).toEqual({ page: "tiles" });
    expect(resolveRoute("/complexes")).toEqual({ page: "complexes" });
  });
});
