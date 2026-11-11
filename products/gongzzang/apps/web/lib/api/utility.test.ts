import { describe, expect, it } from "vitest";
import type { Energy } from "./energy";
import { gasConnection, sewageMethods } from "./utility";

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

describe("sewageMethods", () => {
  const records = (names: (string | null)[]) => ({
    sewage: {
      treatment_plant_connection: false,
      facility_total: names.length,
      methods: names.map((name, i) => ({ code: `${200 + i}`, name, facilities: 1 })),
    },
  });

  it("is the register's method names in its order, unnamed ones left out, at most the limit", () => {
    expect(
      sewageMethods(records(["부패탱크방법", null, " ", "접촉폭기방법", "임호프방식", "폭기방법"])),
    ).toEqual(["부패탱크방법", "접촉폭기방법", "임호프방식"]);
  });

  it("is empty for a parcel the lane has no document for", () => {
    expect(sewageMethods(null)).toEqual([]);
  });
});
