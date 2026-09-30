import { describe, expect, it } from "vitest";

import { formatPnu } from "./labels";

describe("formatPnu", () => {
  it("shows the dong code, a mountain mark and the lot number", () => {
    expect(formatPnu("9999930100100010000")).toBe("9999930100 1");
    expect(formatPnu("9999930100200120003")).toBe("9999930100 산 12-3");
  });

  it("leaves anything that is not a PNU as it is", () => {
    expect(formatPnu("not-a-pnu")).toBe("not-a-pnu");
  });
});
