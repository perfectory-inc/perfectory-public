import { describe, expect, it } from "vitest";

import type { UnitLimit } from "../api/client";
import { byUrgency, duration, lastFailure, proposalLink, size, unitSentence } from "./unitLimits";

const unit = (overrides: Partial<UnitLimit>): UnitLimit => ({
  unit: "foundation-example.service",
  state: "fits",
  runs: 10,
  memory_max: 512 * 2 ** 20,
  memory_high: 460 * 2 ** 20,
  time_limit_seconds: 2400,
  peak_p95: 200 * 2 ** 20,
  peak_max: 210 * 2 ** 20,
  container_peak_max: null,
  memory_share: 0.41,
  seconds_p50: 300,
  seconds_p95: 600,
  seconds_max: 700,
  time_share: 0.29,
  oom: 0,
  timeout: 0,
  failed_other: 0,
  near_memory_runs: 0,
  near_time_runs: 0,
  last_run_at: "2026-11-18T00:00:00+00:00",
  last_oom_at: null,
  last_timeout_at: null,
  ...overrides,
});

describe("order", () => {
  it("puts what failed first, then what is near by how near", () => {
    const rows = [
      unit({ unit: "a", state: "fits" }),
      unit({ unit: "b", state: "near", memory_share: 0.85 }),
      unit({ unit: "c", state: "failing", oom: 2 }),
      unit({ unit: "d", state: "near", time_share: 0.97 }),
    ];
    expect(byUrgency(rows).map((row) => row.unit)).toEqual(["c", "d", "b", "a"]);
  });
});

describe("numbers", () => {
  it("reads like the units declare them", () => {
    expect(size(512 * 2 ** 20)).toBe("512M");
    expect(size(14 * 2 ** 30)).toBe("14.0G");
    expect(size(null)).toBe("-");
    expect(duration(90)).toBe("90초");
    expect(duration(2400)).toBe("40분");
    expect(duration(6900 * 2)).toBe("3.8시간");
  });
});

describe("sentences", () => {
  it("sends a failing unit to its growth, not a raise", () => {
    expect(unitSentence(unit({ state: "failing", oom: 6, runs: 40 }))).toContain("40번 중 OOM 6번 — 입력이 자랐으면 흘려 처리로");
  });

  it("says when the near alert went", () => {
    expect(unitSentence(unit({ state: "near", near_memory_runs: 3 }))).toBe("최근 3번 연달아 80% 넘음 — 알림이 갔어요");
    expect(unitSentence(unit({ state: "near", near_memory_runs: 1 }))).toContain("연달아 3번이면 알림");
  });

  it("shows the newest failure at a limit", () => {
    expect(lastFailure(unit({}))).toBe("-");
    const shown = lastFailure(unit({ last_oom_at: "2026-11-10T00:00:00+00:00", last_timeout_at: "2026-11-12T00:00:00+00:00" }));
    expect(shown.startsWith("timeout ")).toBe(true);
  });
});

describe("a change is a pull request", () => {
  it("links to the unit's declared entry, never a write from the console", () => {
    const link = new URL(proposalLink("foundation-example.service"));
    expect(link.hostname).toBe("github.com");
    expect(link.searchParams.get("q")).toBe('"foundation-example.service" path:platforms/foundation-platform/definitions');
  });
});
