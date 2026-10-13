import { createTranslator } from "next-intl";
import { describe, expect, it } from "vitest";
import koMessages from "@/lib/i18n/ko.json";
import { compareHoName, unitFloorRank, unitFloorText } from "./unit-floor";

const t = createTranslator({
  locale: "ko",
  messages: koMessages,
  namespace: "panels.parcel.floors",
});
const floor = (floor_kind: string | null, floor_number: number | null) => ({
  floor_kind,
  floor_number,
});

describe("unit floor — composed from the data's values", () => {
  it("writes each floor kind from its number", () => {
    expect(unitFloorText(floor("basement", 2), t)).toBe("지하 2층");
    expect(unitFloorText(floor("above_ground", 12), t)).toBe("12층");
    expect(unitFloorText(floor("rooftop", null), t)).toBe("옥탑");
    expect(unitFloorText(floor("all_floors", null), t)).toBe("전층");
  });

  it("says nothing where the data does not place the floor", () => {
    expect(unitFloorText(floor("unknown", null), t)).toBeNull();
    expect(unitFloorText(floor("above_ground", null), t)).toBeNull();
    expect(unitFloorText(floor("a_kind_not_yet_known", 3), t)).toBeNull();
  });

  it("draws a bundle document's server label as given until the bundle retires", () => {
    expect(unitFloorText({ floor_label: "지하1층" }, t)).toBe("지하1층");
    expect(unitFloorText({ floor_label: " " }, t)).toBeNull();
  });

  it("orders floors bottom to top from the values", () => {
    const order = [
      floor("rooftop", null),
      floor("above_ground", 10),
      floor("basement", 1),
      floor("unknown", null),
      floor("above_ground", 2),
      floor("basement", 3),
    ]
      .sort((a, b) => unitFloorRank(a) - unitFloorRank(b))
      .map((f) => unitFloorText(f, t));
    expect(order).toEqual(["지하 3층", "지하 1층", "2층", "10층", "옥탑", null]);
  });

  it("keeps a bundle document's own unit order — no floor is guessed from its label", () => {
    expect(unitFloorRank({ floor_label: "지하1층" })).toBe(unitFloorRank({ floor_label: "3층" }));
  });

  it("orders unit names as read, without parsing them", () => {
    const names = ["1001호", "B01호", "101호", "102호"].map((ho_name) => ({ ho_name }));
    expect(names.sort(compareHoName).map((u) => u.ho_name)).toEqual([
      "101호",
      "102호",
      "1001호",
      "B01호",
    ]);
  });
});
