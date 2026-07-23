import { describe, expect, it } from "vitest";
import { parsePreviewParcels } from "@/app/(public)/preview-floors/preview-data";

describe("preview floor parcel configuration", () => {
  it("has no public operational parcel defaults", () => {
    expect(parsePreviewParcels(undefined)).toEqual([]);
    expect(parsePreviewParcels("  ")).toEqual([]);
  });

  it("accepts explicitly configured parcels", () => {
    const raw = JSON.stringify([{ pnu: "9999900101100010001", address: "SYNTHETIC-PARCEL-1" }]);

    expect(parsePreviewParcels(raw)).toEqual([
      { pnu: "9999900101100010001", address: "SYNTHETIC-PARCEL-1" },
    ]);
  });

  it("rejects malformed operational bindings", () => {
    expect(() =>
      parsePreviewParcels(JSON.stringify([{ pnu: "not-a-pnu", address: "fixture" }])),
    ).toThrow();
  });
});
