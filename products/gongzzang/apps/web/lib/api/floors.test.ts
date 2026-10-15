import { describe, expect, it } from "vitest";
import type { EdgeBuildingProfile } from "./building-edge";
import { toFloorsResponse } from "./floors";

const profile = (rooftop_usage: string | string[]) =>
  ({
    buildings: [
      {
        id: "00000000-0000-8000-8000-000000000001",
        stories: 3,
        below_ground_floors: 1,
        has_rooftop: true,
        rooftop_area_m2: 10,
        rooftop_usage,
        floors: [],
      },
    ],
  }) as unknown as EdgeBuildingProfile;

describe("toFloorsResponse — rooftop usages", () => {
  it("takes a kind lane's list as the source's usages", () => {
    expect(toFloorsResponse(profile(["계단실", "물탱크"])).buildings[0]?.rooftop_usages).toEqual([
      "계단실",
      "물탱크",
    ]);
  });

  it("keeps a bundle document's joined string whole and an empty one as none", () => {
    expect(toFloorsResponse(profile("계단실 · 물탱크")).buildings[0]?.rooftop_usages).toEqual([
      "계단실 · 물탱크",
    ]);
    expect(toFloorsResponse(profile("")).buildings[0]?.rooftop_usages).toEqual([]);
  });
});
