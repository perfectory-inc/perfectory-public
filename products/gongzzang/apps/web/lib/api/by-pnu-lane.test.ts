// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { env } from "@/lib/env";
import {
  OUTPUT_PATH,
  readContract,
  renderByPnuLanes,
  resolveWebLane,
} from "../../../../scripts/generate-by-pnu-lanes";
import { fetchLaneDocument, kindSection, lanesForSections, laneUrl } from "./by-pnu-lane";
import { BY_PNU_LANES, type ByPnuLaneName } from "./by-pnu-lanes.generated";
import { type EdgeParcelProfile, parcelSummary, SUMMARY_LANES } from "./parcels";

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";
const contract = readContract();
const kindLines = contract.by_pnu_lanes as Record<
  string,
  { host?: string; section?: string; columns?: string[]; public_hostname?: string }
>;

describe("the lane registry is the contract's", () => {
  it("is the generator's output for the committed contract", () => {
    expect(readFileSync(OUTPUT_PATH, "utf8")).toBe(renderByPnuLanes(contract));
  });

  it("resolves a kind line by the rule the publisher and the Worker share", () => {
    const rule = contract.by_pnu_lane_rule as {
      test_vectors: {
        lane: string;
        host: string;
        kind: string;
        request_prefix: string;
        document_schema_version: string;
      }[];
    };
    expect(rule.test_vectors.length).toBeGreaterThan(0);
    for (const vector of rule.test_vectors) {
      const entry = { host: vector.host, kind: vector.kind, section: "s" };
      const lane = resolveWebLane(
        { ...contract, by_pnu_lanes: { [vector.lane]: entry } },
        vector.lane,
      );
      expect(lane.path).toBe(vector.request_prefix);
      expect(lane.documentSchemaVersion).toBe(vector.document_schema_version);
      expect(lane.kind).toBe(vector.kind);
      expect(lane.goldTable).toBe(`gold.${vector.lane.replaceAll("-", "_")}`);
    }
  });

  it("refuses a kind line without a gateway host or a section", () => {
    const refused = (entry: object) => () =>
      resolveWebLane({ ...contract, by_pnu_lanes: { x: entry } }, "x");
    expect(refused({ host: "nowhere_by_pnu_gateway", kind: "x", section: "s" })).toThrow(
      /not a kind line/,
    );
    expect(refused({ host: "parcel_by_pnu_gateway", kind: "x" })).toThrow(/not a kind line/);
  });

  it("sends every lane to the public host the contract serves it on", () => {
    for (const lane of Object.keys(BY_PNU_LANES) as ByPnuLaneName[]) {
      const entry = kindLines[lane];
      const block = entry?.host === undefined ? entry : (contract[entry.host] as typeof entry);
      expect(new URL(laneUrl(lane, PNU)).host, lane).toBe(block?.public_hostname);
    }
  });

  it("reads each kind's document field under the bundle section it is named for", () => {
    // The document's first field is the first column without `_json` (the publisher's rule), and
    // the bundle answers for a kind during migration only under that same name.
    const panel = JSON.parse(
      readFileSync(
        resolve(
          __dirname,
          "../../../../../../platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json",
        ),
        "utf8",
      ),
    ).contracts["gold.parcel_panel"].columns.map(({ name }: { name: string }) => name);
    for (const [lane, entry] of Object.entries(BY_PNU_LANES)) {
      if (entry.section === null) continue;
      expect(kindLines[lane]?.columns?.[0], lane).toBe(`${entry.section}_json`);
      if (entry.host === "parcel") expect(panel, lane).toContain(`${entry.section}_json`);
    }
  });

  it("finds the lane serving each summary section, and none on another host", () => {
    expect(SUMMARY_LANES).toEqual({
      zonings: "parcel-zoning",
      price: "parcel-price",
      characteristics: "parcel-characteristics",
    });
    expect(lanesForSections("unit-price", ["zonings"])).toEqual({});
  });
});

describe("fetchLaneDocument", () => {
  afterEach(() => vi.unstubAllGlobals());

  const zoning = BY_PNU_LANES["parcel-zoning"];
  const document = {
    schema_version: zoning.documentSchemaVersion,
    pnu: PNU,
    source: { table: zoning.goldTable, iceberg_snapshot_id: "1" },
    zonings: [{ zone_code: "UQA121", zone_name: "residential" }],
    attached_via: null,
  };
  const answer = (status: number, body?: unknown) => {
    const fetch = vi.fn(
      async (_url: string) =>
        new Response(body === undefined ? null : JSON.stringify(body), { status }),
    );
    vi.stubGlobal("fetch", fetch);
    return fetch;
  };

  it("reads the kind's document from its host under its path", async () => {
    const fetch = answer(200, document);
    expect(await fetchLaneDocument("parcel-zoning", PNU)).toEqual({ status: "found", document });
    expect(fetch.mock.calls[0]?.[0]).toBe(
      `${env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "")}/parcels/zoning/by-pnu/${PNU}`,
    );
  });

  it("reads 404 as the lane's word that the PNU has none", async () => {
    answer(404);
    expect(await fetchLaneDocument("parcel-zoning", PNU)).toEqual({ status: "absent" });
  });

  it("fails on an unpublished lane, another kind's document or another PNU", async () => {
    answer(503);
    await expect(fetchLaneDocument("parcel-zoning", PNU)).rejects.toThrow(/503/);
    answer(200, { ...document, schema_version: "foundation-platform.parcel_by_pnu_profile.v1" });
    await expect(fetchLaneDocument("parcel-zoning", PNU)).rejects.toThrow();
    answer(200, { ...document, source: { table: "gold.parcel_panel", iceberg_snapshot_id: "1" } });
    await expect(fetchLaneDocument("parcel-zoning", PNU)).rejects.toThrow();
    answer(200, { ...document, pnu: "9999900000100000001" });
    await expect(fetchLaneDocument("parcel-zoning", PNU)).rejects.toThrow(/disagrees/);
  });
});

describe("a kind's section", () => {
  const found = (value: unknown) => ({
    data: { status: "found" as const, document: { zonings: value } },
    isError: false,
  });
  const absent = { data: { status: "absent" as const }, isError: false };
  const failed = { isError: true };
  const pending = { isError: false };
  const bundle = { zonings: ["bundle"] };

  it("is the lane's document whenever the lane serves one", () => {
    expect(kindSection(found(["lane"]), "zonings", bundle)).toEqual({
      state: "ok",
      value: ["lane"],
    });
    expect(kindSection(found([]), "zonings", bundle)).toEqual({ state: "empty" });
  });

  it("is the bundle's while the bundle carries it and the lane has no document", () => {
    for (const lane of [absent, failed, pending, undefined]) {
      expect(kindSection(lane, "zonings", bundle)).toEqual({ state: "ok", value: ["bundle"] });
    }
    expect(kindSection(failed, "zonings", { zonings: null })).toEqual({ state: "empty" });
  });

  it("is the lane's own answer once no bundle carries it", () => {
    expect(kindSection(absent, "zonings", undefined)).toEqual({ state: "empty" });
    expect(kindSection(failed, "zonings", undefined)).toEqual({ state: "error" });
    expect(kindSection(pending, "zonings", undefined)).toEqual({ state: "loading" });
    expect(kindSection(undefined, "zonings", undefined)).toEqual({ state: "error" });
  });
});

describe("parcelSummary", () => {
  const bundle: EdgeParcelProfile = {
    schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
    pnu: PNU,
    zonings: [{ zone_code: "UQA121", zone_name: "residential" }],
    price: { price_per_m2: 1000, base_year: 2026, base_month: 1 },
    characteristics: { land_category: "x" },
  };
  const lane = (zonings: unknown) => ({
    zonings: { data: { status: "found" as const, document: { zonings } }, isError: false },
  });

  it("draws the zoning from its lane and the other kinds from the bundle", () => {
    const { info, states } = parcelSummary(
      bundle,
      PNU,
      lane([{ zone_code: "UQA410", zone_name: "industrial" }]),
    );
    expect(info.zoning).toBe("industrial");
    expect(info.official_land_price_per_m2).toBe(1000);
    expect(info.land_use_type).toBe("x");
    expect(states).toEqual({ zonings: "ok", price: "ok", characteristics: "ok" });
  });

  it("falls back to the bundle when a lane's section does not parse", () => {
    expect(parcelSummary(bundle, PNU, lane([{ zone_code: 7 }])).info.zoning).toBe("residential");
  });

  it("draws a kind its lane does not have as none, and one nothing serves as failed", () => {
    const { info, states } = parcelSummary(undefined, PNU, {
      zonings: { data: { status: "absent" }, isError: false },
      price: { isError: true },
    });
    expect(info.zoning).toBeNull();
    expect(states).toEqual({ zonings: "empty", price: "error", characteristics: "error" });
  });
});
