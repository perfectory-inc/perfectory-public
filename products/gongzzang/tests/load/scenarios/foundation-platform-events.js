import crypto from "k6/crypto";
import exec from "k6/execution";
import { profile, runTags, targetBaseUrl } from "../lib/env.js";
import { safePostJson } from "../lib/http.js";

const scenarioName = "foundation-platform-events";
const goldPointerEventType = "catalog.industrial_complex.gold_pointer.published.v1";
const duplicateEventId = "00000000-0000-4000-8000-000000000001";
const webhookSecret = __ENV.FOUNDATION_PLATFORM_WEBHOOK_SECRET;

if (!webhookSecret) {
  throw new Error(
    "FOUNDATION_PLATFORM_WEBHOOK_SECRET is required for the signed webhook load scenario",
  );
}

export const options = {
  scenarios: {
    foundation_platform_events: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.LOAD_RPS || 5),
      timeUnit: "1s",
      duration: __ENV.LOAD_DURATION || "2m",
      preAllocatedVUs: Number(__ENV.LOAD_PRE_ALLOCATED_VUS || 10),
      maxVUs: Number(__ENV.LOAD_MAX_VUS || 50),
    },
  },
  thresholds: {
    "http_req_failed{event_case:valid}": ["rate<0.01"],
    "http_req_failed{event_case:duplicate}": ["rate<0.01"],
    "http_req_duration{event_case:valid}": ["p(95)<500", "p(99)<1500"],
    "http_req_duration{event_case:duplicate}": ["p(95)<500", "p(99)<1500"],
  },
};

function baseTags(eventCase, priority = "normal") {
  return {
    ...runTags(scenarioName),
    profile: profile(),
    route_group: "foundation_platform_events",
    request_kind: "webhook_post",
    event_case: eventCase,
    priority,
  };
}

function uuidForIteration(eventCase) {
  if (eventCase === "duplicate") {
    return duplicateEventId;
  }

  const sequence = exec.scenario.iterationInTest + 1;
  const hex = sequence.toString(16).padStart(12, "0").slice(-12);
  return `20000000-0000-4000-8000-${hex}`;
}

function eventBody(eventCase) {
  const timestamp = new Date().toISOString();

  return {
    event_id: uuidForIteration(eventCase),
    event_type: goldPointerEventType,
    occurred_at: timestamp,
    scope: "catalog",
    payload: {
      type: goldPointerEventType,
      schema_version: 1,
      complex_id: "load-industrial-complex-001",
      current_version: `gold-pointer-load-${exec.scenario.iterationInTest}`,
      source_snapshot_id: "industrial-complex-source-load",
      iceberg_snapshot_id: "industrial-complex-iceberg-load",
    },
  };
}

function eventHeaders(body) {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const bodyText = JSON.stringify(body);
  const signature = crypto.hmac("sha256", webhookSecret, `${timestamp}.${bodyText}`, "hex");

  return {
    "x-foundation-platform-event-id": body.event_id,
    "x-foundation-platform-event-type": body.event_type,
    "x-foundation-platform-outbox-scope": body.scope,
    "x-foundation-platform-timestamp": timestamp,
    "x-foundation-platform-signature": `v1=${signature}`,
  };
}

function eventCaseForIteration() {
  const iteration = exec.scenario.iterationInTest;
  if (iteration % 4 === 0) {
    return "duplicate";
  }
  return "valid";
}

export default function () {
  const baseUrl = targetBaseUrl();
  const currentCase = eventCaseForIteration();
  const body = eventBody(currentCase);
  const url = `${baseUrl}/foundation-platform/events`;
  const tags = baseTags(currentCase, currentCase === "valid" ? "high" : "normal");
  const headers = eventHeaders(body);

  safePostJson(url, body, tags, headers);
}
