import { afterEach, describe, expect, it, vi } from "vitest";
import { COMPLEX_INDEX_PATH, fetchComplex, wholeSquareMetres } from "./complexes";

// Root ADR-0456: the panel reads the index on the parcel edge, then the profile it names. Synthetic ids.
const ID = "0196e7e0-3c20-5000-8000-000000000001";
const PROFILE = "gold/industrial-complex/profiles/0196e7e0-3c20-5000-9000-000000000001.json";

const index = {
  schema_version: "foundation-platform.industrial_complex_index.v1",
  generation: 3,
  complexes: [
    {
      complex_id: ID,
      official_complex_code: "999901",
      name: "합성일반산업단지",
      kind: "general",
      status: "operating",
      profile_object_key: PROFILE,
    },
  ],
};

const profile = {
  schema_version: "foundation-platform.industrial_complex_gold_profile.v1",
  schema_revision: 1,
  artifact_id: "0196e7e0-3c20-5000-9000-000000000001",
  complex_id: ID,
  source: { table: "gold.complex_catalog", iceberg_snapshot_id: "999990000000000001" },
  attributes: {
    complex_id: ID,
    official_complex_code: "999901",
    name: "합성일반산업단지",
    kind: "general",
    status: "operating",
    official_area_sqm: "297986.40",
    development_progress_percent: "0.00",
    management_agency_name: null,
  },
};

afterEach(() => vi.unstubAllGlobals());

describe("fetchComplex", () => {
  it("reads the profile the index names and maps it to the summary", async () => {
    const fetched: string[] = [];
    vi.stubGlobal("fetch", async (url: string) => {
      fetched.push(url);
      return Response.json(url.endsWith(COMPLEX_INDEX_PATH) ? index : profile);
    });
    const info = await fetchComplex(ID);
    expect(fetched.map((url) => new URL(url).pathname)).toEqual([
      `/${COMPLEX_INDEX_PATH}`,
      `/${PROFILE}`,
    ]);
    expect(info.lakehouse_complex_id).toBe(ID);
    expect(info.area_m2).toBe(297_986);
    expect(info.development_progress_percent).toBe("0.00");
    expect(info.management_agency_name).toBeNull();
  });

  it("fails for a complex the index does not list", async () => {
    vi.stubGlobal("fetch", async () => Response.json(index));
    await expect(fetchComplex("0196e7e0-3c20-5000-8000-000000000009")).rejects.toThrow(
      "not in the index",
    );
  });
});

describe("wholeSquareMetres", () => {
  it("rounds decimal text and keeps an unstated area null", () => {
    expect(wholeSquareMetres("1234.50")).toBe(1235);
    expect(wholeSquareMetres(null)).toBeNull();
    expect(wholeSquareMetres(" ")).toBeNull();
  });
});
