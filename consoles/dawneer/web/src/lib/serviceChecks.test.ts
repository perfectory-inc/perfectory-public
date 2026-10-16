import { describe, expect, it } from "vitest";

import type { CiWeek, EdgeHostCheck } from "../api/client";
import { ciWeekSentence, edgeSentence, enteredAbroad } from "./serviceChecks";

const host = (overrides: Partial<EdgeHostCheck>): EdgeHostCheck => ({
  hostname: "tiles.example.test",
  colo: "ICN",
  p50_ms: 120,
  p95_ms: 480,
  samples: 10,
  failed_samples: 0,
  error: null,
  ...overrides,
});

describe("edge", () => {
  it("says Seoul when entered at ICN", () => {
    expect(enteredAbroad(host({}))).toBe(false);
    expect(edgeSentence(host({}), "ICN")).toBe("서울(ICN)로 들어와요 · 보통 120ms, 느리면 480ms");
  });

  it("flags an entry abroad and blames routing when cloudflare.com itself enters at Seoul", () => {
    const abroad = host({ colo: "SEA", p50_ms: 2500, p95_ms: 14000 });
    expect(enteredAbroad(abroad)).toBe(true);
    expect(edgeSentence(abroad, "ICN")).toBe(
      "서울이 아니라 SEA 로 들어가요 · 보통 2,500ms, 느리면 14,000ms — cloudflare.com 은 서울로 들어오니, 요금제의 경로 배정 때문이에요(코드 문제 아님)",
    );
    expect(edgeSentence(abroad, "SEA")).toBe("서울이 아니라 SEA 로 들어가요 · 보통 2,500ms, 느리면 14,000ms");
  });

  it("says why nothing was measured", () => {
    expect(edgeSentence(host({ samples: 0, colo: null, error: "URLError: timed out" }), "ICN")).toBe("측정 실패 · URLError: timed out");
  });
});

describe("ci", () => {
  const week: CiWeek = { week_start: "2026-10-05", count: 120, p50: 23.4, p90: 31.3, max: 58, worst_workflow: "CI", worst_workflow_p50: 24.5 };

  it("holds the median to the budget", () => {
    expect(ciWeekSentence(week, 10)).toBe("PR 120건 · 보통 23.4분, 느리면 31.3분 · 예산 10분 초과");
    expect(ciWeekSentence({ ...week, p50: 9.5 }, 10)).toBe("PR 120건 · 보통 9.5분, 느리면 31.3분 · 예산 10분 안");
  });

  it("says when a week measured nothing", () => {
    expect(ciWeekSentence({ ...week, count: 0, p50: null }, 10)).toBe("측정된 PR 없음");
  });
});
