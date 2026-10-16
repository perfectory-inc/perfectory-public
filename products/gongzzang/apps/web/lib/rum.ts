// apps/web/lib/rum.ts
import { env } from "@/lib/env";

// Root ADR-0255: real-user timings, sent as one beacon per page view to the map-edit gateway,
// which writes them to Workers Analytics Engine. The gateway's contract block
// `map_edit_gateway.real_user_metrics` is the source of these values; rum.test.ts holds them to it.
export const RUM_PATH = "/rum";
export const RUM_MAX_METRICS = 32;
export const RUM_WEB_VITALS = ["LCP", "INP", "CLS", "TTFB"] as const;
type WebVital = (typeof RUM_WEB_VITALS)[number];

// A metric, a lane and a number: nothing names a parcel, a page or a person.
type Entry = { n: WebVital; v: number } | { n: "panel_data_ready"; l: string; v: number };

const queue: Entry[] = [];
let sampled: boolean | undefined;

/** Decided once per page load, so a sampled page reports all of its metrics. */
function inSample(): boolean {
  sampled ??= Math.random() < env.NEXT_PUBLIC_RUM_SAMPLE_RATE;
  return sampled;
}

function isWebVital(name: string): name is WebVital {
  return (RUM_WEB_VITALS as readonly string[]).includes(name);
}

export function recordWebVital(name: string, value: number): void {
  if (isWebVital(name) && inSample()) queue.push({ n: name, v: value });
}

/** `lane` is `<kind>.<view>` (or `parcel.unit-prices`); `ms` runs from the click to the data. */
export function recordPanelDataReady(lane: string, ms: number): void {
  if (inSample()) queue.push({ n: "panel_data_ready", l: lane, v: Math.round(ms) });
}

/** Sends what the page has collected; called when the page is hidden. */
export function flushRum(): void {
  const url = `${env.NEXT_PUBLIC_MAP_EDIT_OVERLAY_BASE_URL.replace(/\/$/, "")}${RUM_PATH}`;
  while (queue.length > 0) {
    // A string body is text/plain: a CORS-safelisted beacon, no preflight, answer never read.
    navigator.sendBeacon(url, JSON.stringify({ v: 1, m: queue.splice(0, RUM_MAX_METRICS) }));
  }
}
