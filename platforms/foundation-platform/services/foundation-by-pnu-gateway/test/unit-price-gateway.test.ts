import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";

// A pack-only lane of the contract's `by_pnu_lanes` (root ADR-0189): it never had objects, so a
// manifest without section packs is an outage and an object at the base key is never read, whatever
// the serving binding says. PNUs and snapshot ids sit in the repository-reserved synthetic
// namespaces (`scripts/guard/public-fixture-safety.py`).
const PNU = "9999900000100000000";
const GATEWAY = connectionContract.by_pnu_lanes["unit-price"];
const URL_OF_PNU = `https://unit-prices.example.test${GATEWAY.request_path.prefix}${PNU}`;
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

function envelope(): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "unit-price-by-pnu",
    base_generation: 1,
    base_object_count: 0,
    document_schema_version: GATEWAY.document_schema_version,
    gold_table: GATEWAY.gold_table,
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: connectionContract.by_pnu_serving_patches.pnu_prefix_length,
    patches: [],
    object_count: 0,
    published_at_utc: "2026-01-01T00:00:00Z",
  })}\n`;
}

describe("foundation unit price gateway (pack-only)", () => {
  let runtime: Miniflare | undefined;

  async function start(serving: string): Promise<Miniflare> {
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      define: { __FOUNDATION_BY_PNU_LANE__: JSON.stringify("unit-price") },
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      absWorkingDir: packageRoot,
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    const started = new Miniflare({
      compatibilityDate: "2026-04-26",
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [GATEWAY.r2_binding],
      bindings: {
        FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: "https://app.example.test",
        [GATEWAY.section_packs.serving_binding]: serving,
      },
      cache: true,
    });
    const bucket = await started.getR2Bucket(GATEWAY.r2_binding);
    await bucket.put(GATEWAY.object_key.manifest_object, envelope());
    // An object where an object lane's base would hold one: a pack-only lane must never read it.
    await bucket.put(`${GATEWAY.object_key.root}/v1/${PNU}${GATEWAY.object_key.suffix}`, "{}\n");
    return started;
  }

  afterEach(async () => {
    await runtime?.dispose();
    runtime = undefined;
  });

  beforeEach(() => {
    runtime = undefined;
  });

  it.each([["on"], ["off"]])("a manifest without packs is an outage, never an object read (binding %s)", async (serving) => {
    runtime = await start(serving);
    const response = await runtime.dispatchFetch(URL_OF_PNU);
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("answers its own capabilities under its own path", async () => {
    runtime = await start("on");
    const response = await runtime.dispatchFetch(
      `https://unit-prices.example.test${GATEWAY.request_path.capabilities}`,
    );
    expect(response.status).toBe(200);
    expect(((await response.json()) as { unit: string }).unit).toBe("unit-price-by-pnu");
  });
});
