import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KINDS } from "@/lib/panel/codec";

// The gateway's contract block is the source of the path, the names and the bounds (root ADR-0255).
const rum = JSON.parse(
  readFileSync(
    resolve(
      __dirname,
      "../../../../../platforms/foundation-platform/config/r2-connections.contract.json",
    ),
    "utf8",
  ),
).map_edit_gateway.real_user_metrics;

const sendBeacon = vi.fn((_url: string, _body: string) => true);

async function freshRum(random: number) {
  vi.resetModules();
  vi.spyOn(Math, "random").mockReturnValue(random);
  return import("./rum");
}

function beacons() {
  return sendBeacon.mock.calls.map(([url, body]) => ({
    url,
    body: JSON.parse(body),
  }));
}

beforeEach(() => {
  sendBeacon.mockClear();
  vi.stubGlobal("navigator", { ...navigator, sendBeacon });
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("real-user beacon", () => {
  it("uses the gateway contract's path, metric names and per-beacon bound", async () => {
    const { RUM_MAX_METRICS, RUM_PATH, RUM_WEB_VITALS } = await freshRum(0);
    expect(RUM_PATH).toBe(rum.path);
    expect(RUM_MAX_METRICS).toBe(rum.max_metrics);
    expect([...RUM_WEB_VITALS, "panel_data_ready"].sort()).toEqual([...rum.metrics].sort());
  });

  it("names every panel lane in a form the gateway accepts", () => {
    const lane = new RegExp(`^(?:${rum.lane_pattern})$`);
    const lanes = Object.entries(KINDS).flatMap(([kind, meta]) =>
      [...meta.views].map((view) => `${kind}.${view}`),
    );
    for (const name of [...lanes, "parcel.unit-prices"]) expect(name).toMatch(lane);
  });

  it("carries only a metric, a lane and a value, at most the bound per beacon", async () => {
    const { flushRum, recordPanelDataReady, recordWebVital } = await freshRum(0);
    recordWebVital("LCP", 1234.5);
    recordWebVital("FCP", 800); // not a metric the gateway takes
    for (let i = 0; i < rum.max_metrics; i++) recordPanelDataReady("parcel.buildings", 900.4);
    flushRum();

    const sent = beacons();
    expect(sent).toHaveLength(2);
    expect(sent.every(({ url }) => url === `https://map-edits.perfectory.io${rum.path}`)).toBe(
      true,
    );
    const entries = sent.flatMap(({ body }) => {
      expect(Object.keys(body).sort()).toEqual(["m", "v"]);
      expect(body.m.length).toBeLessThanOrEqual(rum.max_metrics);
      return body.m;
    });
    expect(entries).toHaveLength(1 + rum.max_metrics);
    for (const entry of entries) {
      for (const key of Object.keys(entry)) expect(["n", "l", "v"]).toContain(key);
    }
    expect(entries[0]).toEqual({ n: "LCP", v: 1234.5 });
    expect(entries[1]).toEqual({ n: "panel_data_ready", l: "parcel.buildings", v: 900 });
  });

  it("sends nothing from a page outside the sample", async () => {
    const { flushRum, recordPanelDataReady, recordWebVital } = await freshRum(0.99);
    recordWebVital("LCP", 1000);
    recordPanelDataReady("parcel.summary", 500);
    flushRum();
    expect(sendBeacon).not.toHaveBeenCalled();
  });
});
