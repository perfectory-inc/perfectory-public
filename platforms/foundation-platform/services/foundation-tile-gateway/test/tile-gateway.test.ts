import { gunzipSync } from "node:zlib";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { FakeBucket, syntheticArchive, TILE_BYTES } from "./helpers/pmtiles";

const policy = connectionContract.vector_tile_gateway;
const root = connectionContract.connections.tile_derivatives.expected_values.FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX;
const SOURCE = "synthetic_layer-00000000-0000-4000-8000-000000000001";
const KEY = `${root}/${SOURCE}${policy.object_key.suffix}`;
const URL_ROOT = `https://tiles.example.test/${SOURCE}`;
const TILE_URL = `${URL_ROOT}/0/0/0`;
const ORIGIN = "https://app.example.test";
const OTHER_ORIGIN = "https://admin.example.test";

describe("foundation tile gateway", () => {
  let worker: typeof import("../src/index");
  let bucket: FakeBucket;
  let cached: Map<string, Response>;
  let pending: Promise<unknown>[];
  let env: Record<string, string | Pick<R2Bucket, "get">>;

  async function request(url = TILE_URL, init?: RequestInit): Promise<Response> {
    const response = await worker.default.fetch(new Request(url, init), env, {
      waitUntil(promise: Promise<unknown>) { pending.push(promise); },
    } as ExecutionContext);
    await Promise.all(pending);
    return response;
  }

  beforeEach(async () => {
    vi.resetModules();
    worker = await import("../src/index");
    bucket = new FakeBucket();
    bucket.objects.set(KEY, syntheticArchive());
    cached = new Map();
    pending = [];
    env = { [policy.r2_binding]: bucket.binding(), [policy.allowed_origins_binding]: `${ORIGIN}, ${OTHER_ORIGIN}` };
    vi.stubGlobal("caches", { default: {
      async match(req: Request) { return cached.get(req.url)?.clone(); },
      async put(req: Request, response: Response) { cached.set(req.url, response.clone()); },
    } });
  });

  afterEach(() => vi.unstubAllGlobals());

  it("serves MVT through bounded ranges with contract headers", async () => {
    const response = await request(TILE_URL, { headers: { Origin: ORIGIN } });
    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(TILE_BYTES[0]);
    expect(response.headers.get("Content-Type")).toBe(policy.content_type);
    expect(response.headers.get("Cache-Control")).toBe(policy.cache_control);
    expect(response.headers.get("Content-Length")).toBe("3");
    expect(response.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe(ORIGIN);
    expect(response.headers.get("Access-Control-Expose-Headers")).toBe("ETag");
    expect(response.headers.get("ETag")).toBe(`"${bucket.etag}-0-0-0"`);
    expect(bucket.reads.every((read) => read.key === KEY)).toBe(true);
    expect(bucket.reads[0]).toEqual({ key: KEY, offset: 0, length: 16384 });
  });

  it("retains the library header/directory cache per source across requests", async () => {
    const first = await request();
    const next = await request(`${URL_ROOT}/1/0/0`);
    expect(next.status).toBe(200);
    expect(new Uint8Array(await next.arrayBuffer())).toEqual(TILE_BYTES[1]);
    expect(next.headers.get("ETag")).not.toBe(first.headers.get("ETag"));
    expect(bucket.reads.filter((read) => read.offset === 0)).toHaveLength(1);
  });

  it("returns and caches an empty tile as 204, including zooms outside the archive", async () => {
    for (const path of ["1/1/1", "24/16777215/16777215"]) {
      const url = `${URL_ROOT}/${path}`;
      const response = await request(url);
      expect(response.status).toBe(204);
      expect(await response.text()).toBe("");
      expect(response.headers.get("Cache-Control")).toBe(policy.cache_control);
      expect(cached.has(url)).toBe(true);
      const warm = await request(url);
      expect(warm.status).toBe(204);
      expect(warm.headers.has("X-Foundation-Empty-Tile")).toBe(false);
      expect(response.headers.has("Content-Encoding")).toBe(false);
    }
  });

  it("returns 404 for an absent archive and permits a later retry", async () => {
    bucket.objects.clear();
    expect((await request()).status).toBe(404);
    expect(cached.size).toBe(0);
    bucket.objects.set(KEY, syntheticArchive());
    expect((await request()).status).toBe(200);
  });

  it.each([
    "/0/0/0.pbf", "/0/0/0.mvt", "/0/0/0?download=1", "/0/0/0?", "/0/1/0", "/0/0/1",
    "/25/0/0", "/01/0/0", "/1/00/0", "/1/0/00", "/-1/0/0", "/1/-1/0",
    "/1/0/2", "/1/2/0", "/1/0/0/", "/1/0", "/1/0/0/extra", "/1/0/0.0", "/1/%30/0",
  ])("rejects noncanonical coordinates without R2 access: %s", async (path) => {
    expect((await request(`${URL_ROOT}${path}`)).status).toBe(404);
    expect(bucket.reads).toHaveLength(0);
  });

  it.each(["bad", SOURCE.toUpperCase(), `_${SOURCE}`, `a-${SOURCE}`, `${"a".repeat(65)}-00000000-0000-4000-8000-000000000001`])(
    "rejects malformed source ids: %s", async (source) => {
      expect((await request(`https://tiles.example.test/${source}/0/0/0`)).status).toBe(404);
      expect(bucket.reads).toHaveLength(0);
    },
  );

  it.each(["POST", "PUT", "PATCH", "DELETE"])("rejects method %s", async (method) => {
    const response = await request(TILE_URL, { method });
    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("GET, HEAD, OPTIONS");
    expect(bucket.reads).toHaveLength(0);
  });

  it("keeps cache entries origin-neutral and checks Origin even on cache hits", async () => {
    await request(TILE_URL, { headers: { Origin: ORIGIN } });
    expect(cached.get(TILE_URL)?.headers.has("Access-Control-Allow-Origin")).toBe(false);
    expect(cached.get(TILE_URL)?.headers.has("Vary")).toBe(false);
    bucket.failure = new Error("must use edge cache");
    const hit = await request(TILE_URL, { headers: { Origin: OTHER_ORIGIN } });
    expect(hit.status).toBe(200);
    expect(hit.headers.get("Access-Control-Allow-Origin")).toBe(OTHER_ORIGIN);
    expect(hit.headers.get("Vary")).toBe("Origin");
    expect((await request()).headers.has("Access-Control-Allow-Origin")).toBe(false);
    expect((await request(TILE_URL, { headers: { Origin: "https://denied.example.test" } })).status).toBe(403);
  });

  it("accepts only the configured CORS grammar", async () => {
    for (const value of policy.cors.accepted) {
      env[policy.allowed_origins_binding] = value;
      expect((await request()).status).toBe(200);
    }
    for (const value of [...policy.cors.rejected, "https://user:password@app.example.test", "null"]) {
      env[policy.allowed_origins_binding] = value;
      expect((await request()).status).toBe(500);
    }
    delete env[policy.allowed_origins_binding];
    expect((await request()).status).toBe(500);
  });

  it.each(["GET", "HEAD"])("accepts %s preflight without R2 reads", async (method) => {
    const response = await request(TILE_URL, { method: "OPTIONS", headers: {
      Origin: ORIGIN, "Access-Control-Request-Method": method, "Access-Control-Request-Headers": "If-None-Match",
    } });
    expect(response.status).toBe(204);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe(ORIGIN);
    expect(response.headers.get("Access-Control-Allow-Methods")).toBe("GET, HEAD, OPTIONS");
    expect(response.headers.get("Access-Control-Allow-Headers")).toBe("If-None-Match");
    expect(response.headers.get("Access-Control-Max-Age")).toBe("86400");
    expect(bucket.reads).toHaveLength(0);
  });

  it.each([
    {}, { Origin: ORIGIN }, { Origin: ORIGIN, "Access-Control-Request-Method": "POST" },
    { Origin: ORIGIN, "Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "Authorization" },
    { Origin: "https://denied.example.test", "Access-Control-Request-Method": "GET" },
  ])("rejects invalid preflight %j", async (headers) => {
    expect((await request(TILE_URL, { method: "OPTIONS", headers: headers as Record<string, string> })).status).toBe(403);
    expect(bucket.reads).toHaveLength(0);
  });

  it("HEAD has the GET headers and no body on cold and warm cache", async () => {
    const cold = await request(TILE_URL, { method: "HEAD" });
    expect(cold.status).toBe(200);
    expect(cold.headers.get("Content-Length")).toBe("3");
    expect(await cold.text()).toBe("");
    expect(cached.size).toBe(0);
    const get = await request();
    bucket.failure = new Error("must use edge cache");
    const warm = await request(TILE_URL, { method: "HEAD" });
    expect(warm.headers.get("ETag")).toBe(get.headers.get("ETag"));
    expect(await warm.text()).toBe("");
  });

  it.each([`"synthetic-object-etag-0-0-0"`, `W/"synthetic-object-etag-0-0-0"`, `"other", W/"synthetic-object-etag-0-0-0"`, "*"])(
    "honours conditional requests cold and warm: %s", async (etag) => {
      for (const method of ["GET", "HEAD"]) {
        const cold = await request(TILE_URL, { method, headers: { "If-None-Match": etag, Origin: ORIGIN } });
        expect(cold.status).toBe(304);
        expect(await cold.text()).toBe("");
        expect(cold.headers.get("Cache-Control")).toBe(policy.cache_control);
        expect(cold.headers.get("Access-Control-Allow-Origin")).toBe(ORIGIN);
      }
      const miss = await request(TILE_URL, { headers: { "If-None-Match": '"other"' } });
      expect(miss.status).toBe(200);
      bucket.failure = new Error("must use edge cache");
      expect((await request(TILE_URL, { headers: { "If-None-Match": etag } })).status).toBe(304);
    },
  );

  it.each([1, 2])("preserves gzip tile bytes with directory compression %s", async (internalCompression) => {
    bucket.objects.set(KEY, syntheticArchive({ tileCompression: 2, internalCompression }));
    const response = await request();
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Encoding")).toBe("gzip");
    expect(new Uint8Array(gunzipSync(Buffer.from(await response.arrayBuffer())))).toEqual(TILE_BYTES[0]);
    const hit = await request();
    expect(new Uint8Array(gunzipSync(Buffer.from(await hit.arrayBuffer())))).toEqual(TILE_BYTES[0]);
  });

  it.each([{ tileType: 2 }, { tileCompression: 0 }, { tileCompression: 3 }, { tileCompression: 4 }])(
    "fails closed for unsupported archive format %j", async (options) => {
      bucket.objects.set(KEY, syntheticArchive(options));
      const response = await request();
      expect(response.status).toBe(500);
      expect(await response.text()).toBe("");
      expect(cached.size).toBe(0);
    },
  );

  it("does not disclose R2 failures or cache them", async () => {
    bucket.failure = new Error("synthetic storage diagnostic that must stay private");
    const response = await request();
    expect(response.status).toBe(500);
    expect(await response.text()).toBe("");
    expect(cached.size).toBe(0);
  });

  it("fails closed on a malformed archive without poisoning retries", async () => {
    bucket.objects.set(KEY, new Uint8Array([0, 1, 2]));
    const malformed = await request();
    expect(malformed.status).toBe(500);
    expect(await malformed.text()).toBe("");
    bucket.objects.set(KEY, syntheticArchive());
    expect((await request()).status).toBe(200);
  });

  it("separates library cache entries for distinct release source keys", async () => {
    await request();
    const otherSource = "future_unit-00000000-0000-4000-8000-000000000003";
    const otherKey = `${root}/${otherSource}${policy.object_key.suffix}`;
    bucket.objects.set(otherKey, syntheticArchive({ tileCompression: 2 }));
    const response = await request(`https://tiles.example.test/${otherSource}/0/0/0`);
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Encoding")).toBe("gzip");
    expect(bucket.reads.filter((read) => read.offset === 0).map((read) => read.key)).toEqual([KEY, otherKey]);
  });
});
