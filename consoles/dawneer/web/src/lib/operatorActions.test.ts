import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";

import { ApiError, type DawneerClient } from "../api/client";
import { OperatorActions, operatorActionMessage, recordedMessage } from "../components/OperatorActions";
import { OPERATOR_ACTIONS, OPERATOR_ACTION_LABEL, operatorActionRequest } from "./operatorActions";

describe("operatorActionRequest (root ADR-0282)", () => {
  it("sends the action, the reason and drop_pending for a pause", () => {
    expect(operatorActionRequest("pause", "a stop for the night", true)).toEqual({
      request: { action: "pause", reason: "a stop for the night", drop_pending: true },
    });
    expect(operatorActionRequest("run_now", "원천 회차 확인 뒤 한 번", false)).toEqual({
      request: { action: "run_now", reason: "원천 회차 확인 뒤 한 번", drop_pending: false },
    });
  });

  it("refuses what the platform would refuse, before asking it", () => {
    for (const [action, reason, drop] of [
      ["pause", "", false],
      ["pause", "   ", false],
      ["pause", "two\nlines", false],
      ["pause", "가".repeat(301), false],
      ["run_now", "a reason", true],
    ] as const) {
      expect(operatorActionRequest(action, reason, drop)).toHaveProperty("problem");
    }
    expect(operatorActionRequest("stop", "가".repeat(300), false)).toHaveProperty("request");
  });

  it("sends a window override only with two clock times and 1 to 168 hours", () => {
    expect(operatorActionRequest("set_window", "collect in the day once", false, { start: "09:00", end: "18:00", hours: 3 })).toEqual({
      request: {
        action: "set_window",
        reason: "collect in the day once",
        drop_pending: false,
        window_start: "09:00",
        window_end: "18:00",
        window_hours: 3,
      },
    });
    for (const window of [
      { start: "9:00", end: "18:00", hours: 3 },
      { start: "09:00", end: "24:00", hours: 3 },
      { start: "09:00", end: "18:00", hours: 0 },
      { start: "09:00", end: "18:00", hours: 169 },
      { start: "09:00", end: "18:00", hours: 2.5 },
    ]) {
      expect(operatorActionRequest("set_window", "r", false, window)).toHaveProperty("problem");
    }
    expect(operatorActionRequest("set_window", "r", false)).toHaveProperty("problem");
    // Another action never carries the window it was not asked for.
    expect(operatorActionRequest("clear_window", "back to night", false, { start: "09:00", end: "18:00", hours: 3 })).toEqual({
      request: { action: "clear_window", reason: "back to night", drop_pending: false },
    });
  });

  it("names every action the platform takes", () => {
    expect([...OPERATOR_ACTIONS]).toEqual(["pause", "resume", "run_now", "stop", "set_window", "clear_window"]);
    for (const action of OPERATOR_ACTIONS) expect(OPERATOR_ACTION_LABEL[action]).toBeTruthy();
  });

  it("says a refusal plainly and a recorded run-now with its start-by time", () => {
    expect(operatorActionMessage(new ApiError(403, "forbidden"))).toBe("이 작업을 조작할 권한이 없습니다.");
    expect(operatorActionMessage(new ApiError(503, "ledger down"))).toContain("ledger down");
    const message = recordedMessage({
      id: "11111111-1111-4111-8111-111111111111",
      job_id: "shape_ledger_check",
      action: "run_now",
      drop_pending: false,
      actor: "staff",
      requested_at: "2026-10-19T09:00:00Z",
      expires_at: "2026-10-19T09:10:00Z",
    });
    expect(message).toContain("지금 한 번 돌리기 기록됨 (11111111)");
    expect(message).toContain("까지 시작");
    const window = recordedMessage({
      id: "22222222-2222-4222-8222-222222222222",
      job_id: "court_auction_collect",
      action: "set_window",
      drop_pending: false,
      actor: "staff",
      requested_at: "2026-10-20T00:00:00Z",
      expires_at: "2026-10-20T03:00:00Z",
      window_start: "09:00",
      window_end: "18:00",
    });
    expect(window).toContain("실행 시간대 바꾸기 기록됨 (22222222) · 09:00-18:00 한국 시각");
  });

  it("renders a form per job whose button waits for a reason", () => {
    const client = { recordOperatorAction: vi.fn() } as unknown as DawneerClient;
    const html = renderToStaticMarkup(
      createElement(QueryClientProvider, { client: new QueryClient() },
        createElement(OperatorActions, { client, jobId: "shape_ledger_check" })),
    );
    expect(html).toContain('aria-label="shape_ledger_check 조작"');
    expect(html).toContain('aria-label="shape_ledger_check 이유"');
    expect(html).toMatch(/<button type="submit" disabled=""/);
    expect(client.recordOperatorAction).not.toHaveBeenCalled();
  });
});
