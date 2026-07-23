// @vitest-environment node

import { describe, expect, it } from "vitest";
import { parseGoldPointerEvent } from "@/lib/foundation-platform/webhook-handler";

const event = {
  event_id: "0196f0b0-3e01-7000-8000-000000000001",
  event_type: "catalog.industrial_complex.gold_pointer.published.v1",
  occurred_at: "2026-07-14T00:00:00Z",
  scope: "catalog",
  payload: {
    type: "catalog.industrial_complex.gold_pointer.published.v1",
    schema_version: 1,
    complex_id: "018f0000-0000-7000-8000-000000000001",
    current_version: "0196e7e0-3c20-7000-8000-100000000001",
    source_snapshot_id: "bronze:datagokr:2026-07-14",
    iceberg_snapshot_id: "987654321",
  },
};

describe("Foundation Platform Gold pointer contract", () => {
  it("accepts an artifact identifier as version metadata", () => {
    expect(parseGoldPointerEvent(event)?.payload.current_version).toBe(
      "0196e7e0-3c20-7000-8000-100000000001",
    );
  });

  it("accepts a path-free opaque producer version", () => {
    expect(
      parseGoldPointerEvent({
        ...event,
        payload: {
          ...event.payload,
          current_version: "gold-2026-07-14T00-00-00Z",
        },
      })?.payload.current_version,
    ).toBe("gold-2026-07-14T00-00-00Z");
  });

  it("rejects an R2 object path in version metadata", () => {
    expect(
      parseGoldPointerEvent({
        ...event,
        payload: {
          ...event.payload,
          current_version: "gold/industrial-complex/profiles/current.json",
        },
      }),
    ).toBeUndefined();
  });
});
