import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { syntheticArchive, TILE_BYTES } from "./helpers/pmtiles";

const policy = connectionContract.vector_tile_gateway;
const SOURCE = "synthetic-00000000-0000-4000-8000-000000000002";
const root = connectionContract.connections.tile_derivatives.expected_values.FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX;
const KEY = `${root}/${SOURCE}${policy.object_key.suffix}`;
const TILE_URL = `https://tiles.example.test/${SOURCE}/0/0/0`;

describe("local workerd/R2/cache proof", () => {
  let runtime: Miniflare;

  beforeEach(async () => {
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      bundle: true, format: "esm", platform: "browser", write: false,
      absWorkingDir: fileURLToPath(new URL("..", import.meta.url)),
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    runtime = new Miniflare({
      compatibilityDate: policy.compatibility_date,
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [policy.r2_binding],
      bindings: { [policy.allowed_origins_binding]: "https://app.example.test, https://admin.example.test" },
      cache: true,
    });
  });

  afterEach(async () => { await runtime?.dispose(); });

  it.each([1, 2])("serves cold and cached tile bytes without double encoding (compression %s)", async (tileCompression) => {
    const bucket = await runtime.getR2Bucket(policy.r2_binding);
    await bucket.put(KEY, syntheticArchive({ tileCompression, internalCompression: 2 }));
    const cold = await runtime.dispatchFetch(TILE_URL, {
      headers: { Origin: "https://app.example.test", "Accept-Encoding": "gzip" },
    });
    expect(cold.status).toBe(200);
    // dispatchFetch moves Content-Encoding to MF-Content-Encoding and decodes once.
    expect(cold.headers.get("MF-Content-Encoding")).toBe(tileCompression === 2 ? "gzip" : null);
    expect(new Uint8Array(await cold.arrayBuffer())).toEqual(TILE_BYTES[0]);
    const etag = cold.headers.get("ETag");
    expect(etag).not.toBeNull();
    const cache = (await runtime.getCaches()).default;
    await expect.poll(async () => (await cache.match(TILE_URL))?.status).toBe(200);
    const cached = await cache.match(TILE_URL);
    expect(cached?.headers.get("Access-Control-Allow-Origin")).toBeNull();
    expect(cached?.headers.get("Vary")).toBeNull();
    await bucket.delete(KEY);
    const warm = await runtime.dispatchFetch(TILE_URL, {
      headers: { Origin: "https://admin.example.test", "Accept-Encoding": "gzip" },
    });
    expect(warm.status).toBe(200);
    expect(warm.headers.get("Access-Control-Allow-Origin")).toBe("https://admin.example.test");
    expect(new Uint8Array(await warm.arrayBuffer())).toEqual(TILE_BYTES[0]);
    const head = await runtime.dispatchFetch(TILE_URL, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");
    const conditional = await runtime.dispatchFetch(TILE_URL, { headers: { "If-None-Match": etag ?? "" } });
    expect(conditional.status).toBe(304);
    expect(await conditional.text()).toBe("");
  });

  it("caches empty tiles as 204 with immutable headers", async () => {
    const bucket = await runtime.getR2Bucket(policy.r2_binding);
    await bucket.put(KEY, syntheticArchive());
    const url = `https://tiles.example.test/${SOURCE}/1/1/1`;
    const cold = await runtime.dispatchFetch(url);
    expect(cold.status).toBe(204);
    expect(await cold.text()).toBe("");
    // dispatchFetch can finish before ctx.waitUntil's cache write, especially for a null body.
    const cache = (await runtime.getCaches()).default;
    await expect.poll(async () => (await cache.match(url))?.headers.get("X-Foundation-Empty-Tile")).toBe("1");
    await bucket.delete(KEY);
    const warm = await runtime.dispatchFetch(url);
    expect(warm.status).toBe(204);
    expect(warm.headers.get("Cache-Control")).toBe(policy.cache_control);
    expect(warm.headers.has("X-Foundation-Empty-Tile")).toBe(false);
    expect(await warm.text()).toBe("");
  });
});
