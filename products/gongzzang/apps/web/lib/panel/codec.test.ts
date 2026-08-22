// apps/web/lib/panel/codec.test.ts
import { describe, expect, it } from "vitest";
import { g1Codec, ParseError } from "./codec";
import type { PanelStack } from "./types";

describe("g1Codec", () => {
  it("serializes single parcel.summary entry", () => {
    const stack: PanelStack = {
      v: 1,
      entries: [{ kind: "parcel", id: "9999900501107370000", view: "summary" }],
    };
    expect(g1Codec.serialize(stack)).toBe("parcel:9999900501107370000.summary");
  });

  it("serializes 2-entry chain with > separator", () => {
    const stack: PanelStack = {
      v: 1,
      entries: [
        { kind: "parcel", id: "9999900501107370000", view: "summary" },
        { kind: "listing", id: "lst_01HXY3NK0Z9F6S1B2C3D4E5F6G", view: "summary" },
      ],
    };
    expect(g1Codec.serialize(stack)).toBe(
      "parcel:9999900501107370000.summary>listing:lst_01HXY3NK0Z9F6S1B2C3D4E5F6G.summary",
    );
  });

  it("serializes empty stack to empty string", () => {
    expect(g1Codec.serialize({ v: 1, entries: [] })).toBe("");
  });

  it("round-trips a 2-entry stack", () => {
    const s = "parcel:9999900501107370000.summary>listing:lst_01HXY3NK0Z9F6S1B2C3D4E5F6G.summary";
    const parsed = g1Codec.deserialize(s);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) expect(g1Codec.serialize(parsed.value)).toBe(s);
  });

  it("rejects unknown kind", () => {
    const r = g1Codec.deserialize("alien:abc.summary");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.UnknownKind);
  });

  it("rejects unknown view for parcel", () => {
    const r = g1Codec.deserialize("parcel:9999900501107370000.alienView");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.UnknownView);
  });

  it("deserializes the parcel floors view", () => {
    const r = g1Codec.deserialize("parcel:9999900501107370000.floors");
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.value.entries[0]).toMatchObject({ kind: "parcel", view: "floors" });
  });

  it("rejects PNU pattern violation", () => {
    const r = g1Codec.deserialize("parcel:notapnu.summary");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.IdPatternViolation);
  });

  it("rejects UUID listing ids because Listing ids are lst-prefixed ULIDs", () => {
    const r = g1Codec.deserialize("listing:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.summary");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.IdPatternViolation);
  });

  it("rejects malformed entry (missing dot)", () => {
    const r = g1Codec.deserialize("parcel:9999900501107370000");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.Malformed);
  });

  it("rejects depth > PANEL_DEPTH_MAX", () => {
    const long = Array.from({ length: 9 }, () => "parcel:9999900501107370000.summary").join(">");
    const r = g1Codec.deserialize(long);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.DepthExceeded);
  });

  it("treats empty string as empty stack (length-0)", () => {
    // empty string is a valid empty stack — caller decides which
    const r = g1Codec.deserialize("");
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.value.entries).toHaveLength(0);
  });

  it("round-trips a complex summary entry", () => {
    const s = "complex:7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5b.summary";
    const parsed = g1Codec.deserialize(s);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.value.entries[0]).toMatchObject({ kind: "complex", view: "summary" });
      expect(g1Codec.serialize(parsed.value)).toBe(s);
    }
  });

  it("rejects a Catalog complex id in the complex slot", () => {
    // `01a0136d-…-7e61-…` is what `Uuid::now_v7()` mints for `catalog.industrial_complex.id`. It
    // names a real complex — a different identity of it — and is not what the tile publishes, so a
    // URL carrying it must fail here rather than resolve to nothing at fetch time.
    const r = g1Codec.deserialize("complex:01a0136d-2b3c-7e61-8f90-a1b2c3d4e5f6.summary");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.IdPatternViolation);
  });

  it("rejects an uppercase complex id", () => {
    const r = g1Codec.deserialize("complex:7DF3859C-1E0A-51FA-8B7D-9A1C2E3F4A5B.summary");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toBe(ParseError.IdPatternViolation);
  });
});
