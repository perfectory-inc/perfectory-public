import { describe, expect, it } from "vitest";

import { ago, COMPLEX_KIND_LABEL, DEVELOPMENT_STAGE_LABEL, formatArea, formatBytes, label, sourceWord } from "./catalogLabels";

describe("catalog labels", () => {
  it("says a known code in Korean and shows an unknown one as it is", () => {
    expect(label(COMPLEX_KIND_LABEL, "urban_high_tech")).toBe("도시첨단");
    expect(label(COMPLEX_KIND_LABEL, "a_new_kind")).toBe("a_new_kind");
    expect(label(COMPLEX_KIND_LABEL, null)).toBe("—");
  });

  it("formats sizes and areas", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(103_726_913)).toBe("98.9 MB");
    expect(formatArea(3306)).toBe("3,306㎡ (1,000평)");
  });

  it("says how long ago in the largest whole unit", () => {
    const now = new Date("2026-01-03T00:00:00Z");
    expect(ago("2026-01-02T23:59:30Z", now)).toBe("방금");
    expect(ago("2026-01-02T23:15:00Z", now)).toBe("45분 전");
    expect(ago("2026-01-02T00:00:00Z", now)).toBe("24시간 전");
    expect(ago("2025-12-30T00:00:00Z", now)).toBe("4일 전");
  });

  it("prefers the source's own word and falls back to the table only when it is absent", () => {
    expect(sourceWord("보상중", DEVELOPMENT_STAGE_LABEL, "compensating")).toBe("보상중");
    expect(sourceWord(null, DEVELOPMENT_STAGE_LABEL, "compensating")).toBe("보상중");
    expect(sourceWord("  ", DEVELOPMENT_STAGE_LABEL, "preparing")).toBe("준비중");
  });
});
