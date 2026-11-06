import { describe, expect, it } from "vitest";

import { cutoverRequest, fullBakeRequest, isByPnuBake, lanesProblem } from "./byPnuCutovers";

// Root ADR-0433, the 2026-11-06 cut-over: what the owner typed over ssh and sudo is now one approval.
describe("a cut-over approval", () => {
  it("is the lanes, the generation and a reason, as the platform takes them", () => {
    expect(cutoverRequest(" parcel-energy, parcel-ownership ", 1, "g1 first publish")).toEqual({
      request: {
        action: "approve_cutover",
        lanes: "parcel-energy,parcel-ownership",
        generation: 1,
        reason: "g1 first publish",
        drop_pending: false,
      },
    });
    expect("request" in cutoverRequest("lanes", 9999, "every lane left")).toBe(true);
  });

  it("is refused before a round trip when the platform would refuse it", () => {
    for (const [lanes, generation, reason] of [
      ["", 1, "r"],
      ["Parcel", 1, "r"],
      ["parcel-energy,parcel-energy", 1, "r"],
      ["lanes,parcel-energy", 1, "r"],
      ["parcel-ownership", 0, "r"],
      ["parcel-ownership", 1.5, "r"],
      ["parcel-ownership", 1, " "],
      ["parcel-ownership", 1, "two\nlines"],
    ] as const) {
      expect("problem" in cutoverRequest(lanes, generation, reason), `${lanes} ${generation} ${reason}`).toBe(true);
    }
  });
});

describe("a full bake", () => {
  it("is one lane of a by-PNU bake job, with a reason", () => {
    expect(fullBakeRequest("parcel-public-auction", "re-base before the cut-over")).toEqual({
      request: { action: "force_full", lanes: "parcel-public-auction", reason: "re-base before the cut-over", drop_pending: false },
    });
    expect(lanesProblem("lanes", true)).not.toBeNull();
    expect(lanesProblem("parcel,building", true)).not.toBeNull();
    expect("problem" in fullBakeRequest("parcel", "")).toBe(true);
    expect(isByPnuBake("by_pnu_public_auction_bake")).toBe(true);
    expect(isByPnuBake("by_pnu_serving_bake")).toBe(true);
    expect(isByPnuBake("parcel_tile_bake")).toBe(false);
    expect(isByPnuBake("by_pnu_bake")).toBe(false);
  });
});
