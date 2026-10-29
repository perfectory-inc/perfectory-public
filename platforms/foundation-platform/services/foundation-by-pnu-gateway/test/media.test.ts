import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import type { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { startMiniflare } from "./miniflare";

// The parcel edge serves content-addressed auction media by its hash path (root ADR-0296 §5 /
// ADR-0297 §5): the URL path is the R2 key under the lakehouse bucket the lane already binds, the
// file name is the content hash, so it answers public and immutable with no new host. The hash is a
// synthetic 64-hex value (no real object); bytes are a stand-in — the read path never inspects them.
const MEDIA = connectionContract.parcel_media;
const BINDING = MEDIA.r2_binding;
const HASH = "a".repeat(64);
const KEY = `onbid/media/sha256/${HASH}.jpg`;
const BYTES = new Uint8Array([0xff, 0xd8, 0xff, 0x01]);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

async function worker(lane: string, bindings: Record<string, unknown> = {}): Promise<Miniflare> {
  const bundle = await build({
    entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
    define: { __FOUNDATION_BY_PNU_LANE__: JSON.stringify(lane) },
    bundle: true,
    format: "esm",
    platform: "browser",
    write: false,
    absWorkingDir: packageRoot,
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("esbuild emitted no Worker module");
  return startMiniflare({
    compatibilityDate: "2026-04-26",
    modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
    r2Buckets: [BINDING],
    bindings: { FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: "https://app.example.test", ...bindings },
    cache: true,
  });
}

describe("the parcel edge serves content-addressed media", () => {
  let runtime: Miniflare | undefined;

  beforeEach(async () => {
    runtime = await worker("parcel");
    const bucket = await runtime.getR2Bucket(BINDING);
    await bucket.put(KEY, BYTES, {
      httpMetadata: { contentType: "image/jpeg", cacheControl: "public, max-age=31536000, immutable" },
    });
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  async function get(path: string, init?: Parameters<Miniflare["dispatchFetch"]>[1]) {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    return runtime.dispatchFetch(`https://catalog.example.test/${path}`, init);
  }

  it("serves a stored blob by its hash path, immutable, with its stored type", async () => {
    const response = await get(KEY);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("image/jpeg");
    expect(response.headers.get("cache-control")).toBe("public, max-age=31536000, immutable");
    expect(response.headers.get("etag")).not.toBeNull();
    expect(response.headers.get("x-content-type-options")).toBe("nosniff");
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(BYTES);
  });

  it("404s a hash with no stored object", async () => {
    expect((await get(`onbid/media/sha256/${"b".repeat(64)}.jpg`)).status).toBe(404);
  });

  it("405s a write method on a media path", async () => {
    const response = await get(KEY, { method: "POST" });
    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET, HEAD");
  });

  it("does not treat a malformed media path as media — it falls through to a 404", async () => {
    // A short hash, an unknown extension, and a query string each fail the one hash rule, so they are
    // not served as a blob; the by-PNU lanes do not match the path either, so the answer is 404.
    expect((await get(`onbid/media/sha256/${"a".repeat(63)}.jpg`)).status).toBe(404);
    expect((await get(`onbid/media/sha256/${HASH}.svg`)).status).toBe(404);
    expect((await get(`${KEY}?x=1`)).status).toBe(404);
  });
});

it("serves a cached blob again with the Worker version header (cache hits have immutable headers)", async () => {
  // 2026-10-28: the second GET of every photo answered 500 — the cache hit's headers are immutable and
  // the version header was set on them. Production binds version metadata, so this test does too.
  const runtime = await worker("parcel", {
    FOUNDATION_PLATFORM_WORKER_VERSION: { id: "v-test", tag: "", timestamp: "" },
  });
  try {
    const bucket = await runtime.getR2Bucket(BINDING);
    await bucket.put(KEY, BYTES, { httpMetadata: { contentType: "image/jpeg" } });
    for (const pass of ["miss", "hit"]) {
      const response = await runtime.dispatchFetch(`https://catalog.example.test/${KEY}`);
      expect(response.status, pass).toBe(200);
      expect(response.headers.get("foundation-worker-version"), pass).toBe("v-test");
      expect(new Uint8Array(await response.arrayBuffer()), pass).toEqual(BYTES);
    }
  } finally {
    await runtime.dispose();
  }
});

it("the building lane does not serve media (only the parcel edge does)", async () => {
  const runtime = await worker("building");
  try {
    const bucket = await runtime.getR2Bucket(BINDING);
    await bucket.put(KEY, BYTES, { httpMetadata: { contentType: "image/jpeg" } });
    const response = await runtime.dispatchFetch(`https://buildings.example.test/${KEY}`);
    expect(response.status).toBe(404);
  } finally {
    await runtime.dispose();
  }
});
