import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { LANES, resolveLane } from "../src/lane";

// A lane written as a host and a kind (root ADR-0193 §4) is served by its host's Worker under its
// own paths: its own manifest, pack root, capabilities and edge copies. The pack bytes are the
// publisher's golden packs (section-packs.test.ts); PNUs sit in the repository-reserved synthetic
// namespace.
const PNU = "9999900000100000000";
const UNIT = "9999900000";
const PARCEL = connectionContract.parcel_by_pnu_gateway;
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const RULE = connectionContract.by_pnu_lane_rule;
const ZONING = LANES.find((lane) => lane.name === "parcel-zoning");
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

function manifest(unit: string, withPacks: boolean): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit,
    base_generation: 1,
    base_object_count: 0,
    document_schema_version: "foundation-platform.synthetic_by_pnu.v1",
    gold_table: "gold.synthetic",
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: 5,
    patches: [],
    object_count: 0,
    published_at_utc: "2026-01-01T00:00:00Z",
    ...(withPacks
      ? {
          section_packs: {
            schema_version: PACK_POLICY.manifest_section_packs_schema_version,
            format_version: PACK_POLICY.format_version,
            unit_prefix_length: PACK_POLICY.unit_prefix_length,
            document_schema_version: "foundation-platform.synthetic_by_pnu.v1",
            gold_table: "gold.synthetic",
            reflected_gold_iceberg_snapshot_id: "999990000000000001",
            document_count: 2,
            sections: [{ name: "documents", generation: 1, gold_iceberg_snapshot_id: "999990000000000001", document_count: 2, pack_count: 1, patch_floor: 0 }],
            patches: [],
          },
        }
      : {}),
  })}\n`;
}

describe("a kind lane on the parcel host (root ADR-0193 §4)", () => {
  let runtime: Miniflare | undefined;

  afterEach(async () => {
    await runtime?.dispose();
    runtime = undefined;
  });

  async function start(): Promise<Miniflare> {
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      define: { __FOUNDATION_BY_PNU_LANE__: JSON.stringify("parcel") },
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      absWorkingDir: packageRoot,
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    return new Miniflare({
      compatibilityDate: PARCEL.compatibility_date,
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [PARCEL.r2_binding],
      bindings: { [PARCEL.allowed_origins_binding]: "https://app.example.test" },
      cache: true,
    });
  }

  async function put(key: string, body: string | Uint8Array): Promise<void> {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await (await runtime.getR2Bucket(PARCEL.r2_binding)).put(key, body);
  }

  const pack = async () => new Uint8Array(await readFile(new URL("g1-documents.pack", fixtures)));
  const packKey = (root: string) => `${root}/documents/g1/${UNIT}${PACK_POLICY.suffix}`;
  const read = (prefix: string) =>
    runtime!.dispatchFetch(`https://catalog.example.test${prefix}${PNU}`, { headers: { "Accept-Encoding": "gzip" } });

  it("derives the paths the publisher derives, for every test vector of the contract", () => {
    expect(RULE.test_vectors.length).toBeGreaterThan(0);
    for (const vector of RULE.test_vectors) {
      const host = connectionContract[vector.host as "parcel_by_pnu_gateway"];
      const lane = resolveLane(vector.lane, host, vector.kind);
      expect(lane.prefix).toBe(vector.request_prefix);
      expect(lane.capabilities).toBe(vector.capabilities);
      expect(lane.manifestObject).toBe(vector.manifest_object);
      expect(lane.packRoot).toBe(vector.pack_root);
    }
  });

  it("serves the zoning lane from its own manifest and pack root, beside the parcel lane", async () => {
    expect(ZONING).toBeDefined();
    runtime = await start();
    await put(ZONING!.manifestObject, manifest("parcel-zoning-by-pnu", true));
    await put(packKey(ZONING!.packRoot), await pack());
    const documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as { base: Record<string, string> };
    const answer = await read(ZONING!.prefix);
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU]);
    // The parcel lane has published nothing here: its own path is an outage, not the zoning answer.
    expect((await read(PARCEL.request_path.prefix)).status).toBe(503);
    const capabilities = await runtime.dispatchFetch(`https://catalog.example.test${ZONING!.capabilities}`);
    expect(((await capabilities.json()) as { unit: string }).unit).toBe("parcel-zoning-by-pnu");
  });

  it("never answers one lane from another lane's packs or edge copies", async () => {
    runtime = await start();
    // Both lanes at the same generation, so their served states would share a bare fingerprint.
    await put(PARCEL.object_key.manifest_object, manifest("parcel-by-pnu", true));
    await put(ZONING!.manifestObject, manifest("parcel-zoning-by-pnu", true));
    await put(packKey(PARCEL.section_packs.root), await pack());
    expect((await read(PARCEL.request_path.prefix)).status).toBe(200);
    expect((await read(PARCEL.request_path.prefix)).status).toBe(200);
    expect((await read(ZONING!.prefix)).status).toBe(404);
  });

  it("is pack-only: a zoning manifest without packs, or of another unit, is an outage", async () => {
    runtime = await start();
    await put(ZONING!.manifestObject, manifest("parcel-zoning-by-pnu", false));
    expect((await read(ZONING!.prefix)).status).toBe(503);
    await runtime.dispose();
    runtime = await start();
    await put(ZONING!.manifestObject, manifest("parcel-by-pnu", true));
    await put(packKey(ZONING!.packRoot), await pack());
    expect((await read(ZONING!.prefix)).status).toBe(503);
  });
});
