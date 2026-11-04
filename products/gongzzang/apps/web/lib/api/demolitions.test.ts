import { afterEach, describe, expect, it, vi } from "vitest";
import { dayLabel, fetchDemolitions, reportDates } from "./demolitions";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, no person or place.
const pnu = "9999900000100000000";
const record = (extra: Record<string, string | null> = {}) => ({
  demolition_kind_name: "철거",
  demolition_start_date: "20990101",
  demolition_end_date: "20990110",
  demolition_loss_date: null,
  total_floor_area_m2: "305.08",
  building_count: "1",
  main_purpose_name: "공장",
  structure_name: "철근콘크리트구조",
  ...extra,
});
const document = () => ({
  schema_version: "foundation-platform.parcel_demolitions_by_pnu.v1",
  schema_revision: 1,
  pnu,
  source: { table: "gold.parcel_demolitions", iceberg_snapshot_id: "999990000000000001" },
  demolitions: {
    records: [record({ demolition_kind_name: "멸실", demolition_loss_date: "20990505" }), record()],
  },
  demolition_total: 3,
  attached_via: null,
});

afterEach(() => vi.unstubAllGlobals());

describe("fetchDemolitions", () => {
  it("reads the parcel's reports from the demolitions edge path and keeps the full count", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(document()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const demolitions = await fetchDemolitions(pnu);
    expect(fetchMock).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/demolitions/by-pnu/${pnu}`,
      expect.anything(),
    );
    expect(demolitions?.total).toBe(3);
    expect(demolitions?.records[0]?.demolition_kind_name).toBe("멸실");
  });

  it("is null for a parcel the lane has no document for", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("", { status: 404 })),
    );
    expect(await fetchDemolitions(pnu)).toBeNull();
  });
});

describe("report dates", () => {
  it("shows 철거멸실일, else the 철거 span, else nothing", () => {
    expect(reportDates(record({ demolition_loss_date: "20990505" }))).toBe("2099.05.05");
    expect(reportDates(record())).toBe("2099.01.01~2099.01.10");
    expect(
      reportDates(record({ demolition_start_date: null, demolition_end_date: null })),
    ).toBeNull();
    expect(dayLabel("2099")).toBe("2099");
  });
});
