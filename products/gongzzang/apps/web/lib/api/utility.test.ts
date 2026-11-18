import { describe, expect, it } from "vitest";
import type { Energy } from "./energy";
import { gasConnection } from "./utility";

const month = (use_ym: string, gas_rows: number) => ({
  use_ym,
  electricity_usage: null,
  electricity_rows: 0,
  gas_usage: gas_rows > 0 ? 0 : null,
  gas_rows,
});
const energy = (months: Energy["months"]): Energy => ({ months, electricity_unit: "kWh" });

describe("gasConnection", () => {
  it("is the newest month with a 가스 row, a zero 사용량 included (a meter is there)", () => {
    expect(
      gasConnection(energy([month("209906", 1), month("209908", 0), month("209907", 2)])),
    ).toEqual({ latestUseYm: "209907" });
  });

  it("says nothing without a 가스 row: the provider leaves houses and 산업용 out", () => {
    expect(gasConnection(energy([month("209908", 0)]))).toBeNull();
    expect(gasConnection(null)).toBeNull();
    expect(gasConnection(undefined)).toBeNull();
  });
});
