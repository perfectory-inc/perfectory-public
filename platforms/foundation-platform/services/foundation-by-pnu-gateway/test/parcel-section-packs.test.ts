import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";

// The parcel lane reads section packs with the same code as the building lane (root ADR-0160);
// these hold it to the parcel contract block: its pack root, its bindings, its manifest unit. The
// pack bytes are the publisher's golden packs (section-packs.test.ts): the read path never looks
// at a member's content. PNUs sit in the repository-reserved synthetic namespace.
const PNU_A = "9999900000100000000";
const UNIT = "9999900000";
const GATEWAY = connectionContract.parcel_by_pnu_gateway;
const BUILDING = connectionContract.building_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const R2_BINDING = GATEWAY.r2_binding;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

const packKey = (root: string) => `${root}/documents/g1/${UNIT}${PACK_POLICY.suffix}`;
const url = (query = "") => `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU_A}${query}`;

function manifest(withPacks: boolean): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "parcel-by-pnu",
    base_generation: 7,
    base_object_count: 1,
    document_schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
    gold_table: "gold.parcel_panel",
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: 5,
    patches: [],
    object_count: 1,
    published_at_utc: "2026-01-01T00:00:00Z",
    ...(withPacks
      ? {
          section_packs: {
            schema_version: PACK_POLICY.manifest_section_packs_schema_version,
            format_version: PACK_POLICY.format_version,
            unit_prefix_length: PACK_POLICY.unit_prefix_length,
            document_schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
            gold_table: "gold.parcel_panel",
            reflected_gold_iceberg_snapshot_id: "999990000000000001",
            document_count: 2,
            sections: PACKS.sections.map((name) => ({
              name,
              generation: 1,
              gold_iceberg_snapshot_id: "999990000000000001",
              document_count: 2,
              pack_count: 1,
              patch_floor: 0,
            })),
            patches: [],
          },
        }
      : {}),
  })}\n`;
}

describe("foundation parcel gateway section packs (root ADR-0147, ADR-0160)", () => {
  let runtime: Miniflare | undefined;
  let documents: { base: Record<string, string> };

  async function start(bindings: Record<string, string> = {}, packRoot = PACKS.root): Promise<Miniflare> {
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
    const started = new Miniflare({
      compatibilityDate: GATEWAY.compatibility_date,
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [R2_BINDING],
      bindings: { [GATEWAY.allowed_origins_binding]: "https://app.example.test", ...bindings },
      cache: true,
    });
    const bucket = await started.getR2Bucket(R2_BINDING);
    await bucket.put(packKey(packRoot), new Uint8Array(await readFile(new URL("g1-documents.pack", fixtures))));
    return started;
  }

  async function serve(withPacks: boolean): Promise<void> {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await (await runtime.getR2Bucket(R2_BINDING)).put(GATEWAY.object_key.manifest_object, manifest(withPacks));
  }

  beforeEach(async () => {
    documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as typeof documents;
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  it("reads the parcel pack root and hands out the stored member", async () => {
    runtime = await start();
    await serve(true);
    const answer = await runtime.dispatchFetch(url(), { headers: { "Accept-Encoding": "gzip" } });
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_A]);
  });

  it("never reads the building lane's packs", async () => {
    expect(PACKS.root).not.toBe(BUILDING.section_packs.root);
    runtime = await start({}, BUILDING.section_packs.root);
    await serve(true);
    expect((await runtime.dispatchFetch(url())).status).toBe(404);
  });

  it("serves an unpublished generation only on a preview version, by the parcel bindings", async () => {
    runtime = await start();
    await serve(false);
    expect((await runtime.dispatchFetch(url("?packs=g1"))).status).toBe(404);
    await runtime.dispose();
    runtime = await start({ [PACKS.preview_binding]: "true" });
    await serve(false);
    const preview = await runtime.dispatchFetch(url("?packs=g1"));
    expect(preview.status).toBe(200);
    expect(await preview.text()).toBe(documents.base[PNU_A]);
  });

  it("a version whose serving binding is off answers from objects under a pack manifest", async () => {
    runtime = await start({ [PACKS.serving_binding]: "off" });
    const object = `{"pnu":"${PNU_A}","served_by":"objects"}\n`;
    await (await runtime.getR2Bucket(R2_BINDING)).put(
      `${GATEWAY.object_key.root}/v7/${PNU_A}${GATEWAY.object_key.suffix}`,
      object,
    );
    await serve(true);
    const answer = await runtime.dispatchFetch(url());
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(object);
  });

  it("refuses the building lane's schema query", async () => {
    runtime = await start();
    await serve(true);
    expect(GATEWAY.request_path.accepted_queries).toEqual([]);
    expect((await runtime.dispatchFetch(url("?schema=2"))).status).toBe(404);
  });
});
