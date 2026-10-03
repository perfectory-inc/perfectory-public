import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { MANIFEST_SCHEMA_VERSIONS, parseAllowedOrigins, parseManifest } from "../src/index";

// PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
// (`scripts/guard/public-fixture-safety.py`).
const PNU = "9999900000100000000";
const UNBAKED_PNU = "9999900000200000000";
const SERVED_GENERATION = 2;
const GATEWAY = connectionContract.parcel_by_pnu_gateway;
const OBJECT_KEY = `${GATEWAY.object_key.root}/v${SERVED_GENERATION}/${PNU}${GATEWAY.object_key.suffix}`;
const STALE_OBJECT_KEY = `${GATEWAY.object_key.root}/v1/${PNU}${GATEWAY.object_key.suffix}`;
const MANIFEST_KEY = GATEWAY.object_key.manifest_object;
const PARCEL_URL = `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}`;
const ALLOWED_ORIGIN = "https://app.example.test";
const SECOND_ALLOWED_ORIGIN = "https://admin.example.test";
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const R2_BINDING = GATEWAY.r2_binding;
const GATEWAY_PATCHES = connectionContract.by_pnu_serving_patches;

function manifestBody(generation: number): string {
  return `${JSON.stringify({
    schema_version: 1,
    unit: "parcel-by-pnu",
    current_generation: generation,
    gold_table: "gold.parcel_panel",
    gold_iceberg_snapshot_id: "999990000000000001",
    object_count: 1,
    published_at_utc: "2026-01-01T00:00:00Z",
  })}\n`;
}

describe("foundation parcel gateway", () => {
  let runtime: Miniflare | undefined;
  let parcelBody: string;

  beforeEach(async () => {
    parcelBody = await readFile(new URL("fixtures/parcel.json", import.meta.url), "utf8");
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      absWorkingDir: packageRoot,
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    runtime = new Miniflare({
      compatibilityDate: "2026-04-26",
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [R2_BINDING],
      bindings: {
        FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: `${ALLOWED_ORIGIN}, ${SECOND_ALLOWED_ORIGIN}`,
      },
      cache: true,
    });
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.put(MANIFEST_KEY, manifestBody(SERVED_GENERATION), {
      httpMetadata: { contentType: "application/json; charset=utf-8" },
    });
    await bucket.put(OBJECT_KEY, parcelBody, {
      httpMetadata: { contentType: "application/json; charset=utf-8" },
    });
    await bucket.put(STALE_OBJECT_KEY, `{"pnu":"${PNU}","generation":1}\n`, {
      httpMetadata: { contentType: "application/json; charset=utf-8" },
    });
    await bucket.put("bronze/vworld/2026/raw.jsonl", "{}\n");
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  it("canonical GET serves the manifest-pinned generation", async () => {
    const response = await runtime?.dispatchFetch(PARCEL_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    if (response === undefined) throw new Error("Miniflare did not start");

    expect(response.status).toBe(200);
    expect(await response.text()).toBe(parcelBody);
    expect(response.headers.get("content-type")).toBe("application/json; charset=utf-8");
    expect(response.headers.get("cache-control")).toBe(GATEWAY.cache_control);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]+"$/);
  });

  it("a parcel absent from the served generation is 404 even when a stale generation has it", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.delete(OBJECT_KEY);

    const response = await runtime.dispatchFetch(PARCEL_URL);
    expect(response.status).toBe(404);
  });

  it("an unbaked parcel is 404, not an outage", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(
      `https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`,
    );
    expect(response.status).toBe(404);
  });

  it.each([
    ["absent", null],
    ["malformed JSON", "not json"],
    ["wrong unit", `${JSON.stringify({ schema_version: 1, unit: "tiles", current_generation: 1 })}\n`],
    ["wrong schema", `${JSON.stringify({ schema_version: 2, unit: "parcel-by-pnu", current_generation: 1 })}\n`],
    ["zero generation", `${JSON.stringify({ schema_version: 1, unit: "parcel-by-pnu", current_generation: 0 })}\n`],
  ])("a manifest the gateway cannot trust is an outage, not a 404: %s", async (_label, body) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    if (body === null) {
      await bucket.delete(MANIFEST_KEY);
    } else {
      await bucket.put(MANIFEST_KEY, body);
    }

    const response = await runtime.dispatchFetch(PARCEL_URL);
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("the manifest lookup is edge-cached so a burst does not re-read the pointer", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const first = await runtime.dispatchFetch(PARCEL_URL);
    expect(first.status).toBe(200);
    await first.arrayBuffer();

    // With the pointer gone, a fresh resolution would 503; the cached manifest keeps serving.
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.delete(MANIFEST_KEY);
    const second = await runtime.dispatchFetch(
      `https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`,
    );
    expect(second.status).toBe(404);
  });

  it.each([
    `https://catalog.example.test/${OBJECT_KEY}`,
    `https://catalog.example.test/${MANIFEST_KEY}`,
    "https://catalog.example.test/bronze/vworld/2026/raw.jsonl",
    `https://catalog.example.test${GATEWAY.request_path.prefix}999990000010000000`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}99999000001000000001`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}9999900000000000000`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}not-a-pnu`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}.json`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}nested/${PNU}`,
    `${PARCEL_URL}?download=1`,
  ])("non-canonical paths return 404: %s", async (url) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
  });

  it.each([
    `https://catalog.example.test${GATEWAY.request_path.prefix}../../bronze/x`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}%2e%2e/%2e%2e/bronze/x`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}%252e%252e/%252e%252e/bronze/x`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}%2f..%2fbronze.json`,
    `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}%252f..%252fbronze.json`,
  ])("traversal forms return 404: %s", async (url) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
  });

  it.each(["PUT", "POST", "DELETE", "PATCH"])(
    "write methods return 405 and preserve the object: %s",
    async (method) => {
      if (runtime === undefined) throw new Error("Miniflare did not start");
      const response = await runtime.dispatchFetch(PARCEL_URL, {
        method,
        ...(method === "DELETE" ? {} : { body: "mutated" }),
      });
      expect(response.status).toBe(405);
      expect(response.headers.get("allow")).toBe("GET, HEAD, OPTIONS");
      expect(await (await runtime.dispatchFetch(PARCEL_URL)).text()).toBe(parcelBody);
    },
  );

  it("origin gate and cache isolation never emit wildcard CORS", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");

    const noOrigin = await runtime.dispatchFetch(PARCEL_URL);
    expect(noOrigin.status).toBe(200);
    expect(noOrigin.headers.get("access-control-allow-origin")).toBeNull();

    const first = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(first.status).toBe(200);
    expect(first.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(first.headers.get("vary")).toBe("Origin");

    const second = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(second.status).toBe(200);
    expect(second.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
    expect(second.headers.get("access-control-allow-origin")).not.toBe("*");

    const denied = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { Origin: "https://attacker.example.test" },
    });
    expect(denied.status).toBe(403);
    expect(denied.headers.get("access-control-allow-origin")).toBeNull();
  });

  it("cached object is origin-neutral", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    await response.arrayBuffer();
    // The edge cache key carries the served state's fingerprint (base v2, no patch).
    const cached = await (await runtime.getCaches()).default.match(`${PARCEL_URL}?serving=v2`);
    expect(cached).toBeDefined();
    expect(cached?.headers.get("access-control-allow-origin")).toBeNull();
    expect(cached?.headers.get("vary")).toBeNull();
  });

  it("CORS preflight allows only configured application origins", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const allowed = await runtime.dispatchFetch(PARCEL_URL, {
      method: "OPTIONS",
      headers: {
        Origin: ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "If-None-Match",
      },
    });
    expect(allowed.status).toBe(204);
    expect(allowed.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(allowed.headers.get("access-control-allow-methods")).toBe("GET, HEAD, OPTIONS");
    expect(allowed.headers.get("access-control-allow-headers")).toBe("If-None-Match");
    expect(allowed.headers.get("access-control-expose-headers")).toBe("ETag");

    const denied = await runtime.dispatchFetch(PARCEL_URL, {
      method: "OPTIONS",
      headers: {
        Origin: "https://attacker.example.test",
        "Access-Control-Request-Method": "GET",
      },
    });
    expect(denied.status).toBe(403);
  });

  it("conditional GET returns 304 on cold R2 and warm Cache API matches", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const stored = await bucket.head(OBJECT_KEY);
    if (stored === null) throw new Error("parcel fixture is missing");

    const cold = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: ALLOWED_ORIGIN },
    });
    expect(cold.status).toBe(304);
    expect(cold.headers.get("etag")).toBe(stored.httpEtag);
    expect(await cold.text()).toBe("");

    const nonMatch = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { "If-None-Match": '"not-this-object"' },
    });
    expect(nonMatch.status).toBe(200);
    expect(await nonMatch.text()).toBe(parcelBody);

    await bucket.delete(OBJECT_KEY);
    const warm = await runtime.dispatchFetch(PARCEL_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(warm.status).toBe(304);
    expect(warm.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
  });

  it("conditional HEAD returns headers without a body", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const stored = await bucket.head(OBJECT_KEY);
    if (stored === null) throw new Error("parcel fixture is missing");

    const response = await runtime.dispatchFetch(PARCEL_URL, { method: "HEAD" });
    expect(response.status).toBe(200);
    expect(response.headers.get("etag")).toBe(stored.httpEtag);
    expect(response.headers.get("content-length")).toBe(String(stored.size));
    expect(await response.text()).toBe("");

    const conditional = await runtime.dispatchFetch(PARCEL_URL, {
      method: "HEAD",
      headers: { "If-None-Match": stored.httpEtag },
    });
    expect(conditional.status).toBe(304);
    expect(conditional.headers.get("etag")).toBe(stored.httpEtag);
    expect(await conditional.text()).toBe("");
  });

  it("CORS grammar matches the shared contract corpus", () => {
    for (const accepted of GATEWAY.cors.accepted) {
      expect(parseAllowedOrigins(accepted), accepted).not.toBeNull();
    }
    for (const rejected of GATEWAY.cors.rejected) {
      expect(parseAllowedOrigins(rejected), rejected).toBeNull();
    }
  });

  it("manifest validation accepts only the lane's own pointer shapes", () => {
    expect(parseManifest({ schema_version: 1, unit: "parcel-by-pnu", current_generation: 7 })).toEqual({
      base: 7,
      patches: [],
      fingerprint: "v7",
    });
    const v2 = parseManifest({
      schema_version: 2,
      unit: "parcel-by-pnu",
      base_generation: 7,
      patches: [
        { generation: 3, prefixes: ["99999"] },
        { generation: 1, prefixes: ["99998", "99999"] },
      ],
    });
    expect(v2?.base).toBe(7);
    expect(v2?.fingerprint).toBe("v7p3");
    expect(v2?.patches.map((patch) => patch.generation)).toEqual([3, 1]);
    const patch = (generation: unknown, prefixes: unknown) => ({ generation, prefixes });
    for (const invalid of [
      null,
      "text",
      {},
      { schema_version: 1, unit: "parcel-by-pnu", current_generation: 0 },
      { schema_version: 1, unit: "parcel-by-pnu", current_generation: 1.5 },
      { schema_version: 1, unit: "parcel-by-pnu", current_generation: "1" },
      { schema_version: 2, unit: "parcel-by-pnu", current_generation: 1 },
      { schema_version: 3, unit: "parcel-by-pnu", base_generation: 1, patches: [] },
      { schema_version: 1, unit: "tiles", current_generation: 1 },
      { schema_version: 2, unit: "parcel-by-pnu", base_generation: 1 },
      { schema_version: 2, unit: "parcel-by-pnu", base_generation: 1, patches: [patch(1, ["9999"])] },
      { schema_version: 2, unit: "parcel-by-pnu", base_generation: 1, patches: [patch(1, [])] },
      { schema_version: 2, unit: "parcel-by-pnu", base_generation: 1, patches: [patch(0, ["99999"])] },
      {
        schema_version: 2,
        unit: "parcel-by-pnu",
        base_generation: 1,
        patches: [patch(1, ["99999"]), patch(2, ["99999"])],
      },
      {
        schema_version: 2,
        unit: "parcel-by-pnu",
        base_generation: 1,
        patches: Array.from({ length: GATEWAY_PATCHES.max_patches + 1 }, (_, i) => patch(99 - i, ["99999"])),
      },
    ]) {
      expect(parseManifest(invalid), JSON.stringify(invalid)).toBeNull();
    }
  });

  it("production source exposes no R2 list or write capability", async () => {
    const source = await readFile(new URL("../src/index.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/LAKEHOUSE\s*\.\s*(?:list|put|delete)\s*\(/);
    expect(source).not.toMatch(/(?:ACCESS_KEY|SECRET_ACCESS|ACCOUNT_ID)/);
    expect(source).not.toContain("foundation-platform-lakehouse-prod");
    expect(source).not.toMatch(/Access-Control-Allow-Origin[\s\S]{0,80}["']\*["']/);
    expect(source).toContain('Pick<R2Bucket, "get">');
  });
});

describe("foundation parcel gateway patch generations (root ADR-0141)", () => {
  let runtime: Miniflare | undefined;
  let baseBody: string;
  const PATCHED_PNU = PNU;
  const patchKey = (patch: number, pnu: string) =>
    `${GATEWAY.object_key.root}/v${SERVED_GENERATION}/p${patch}/${pnu}${GATEWAY.object_key.suffix}`;
  const json = { httpMetadata: { contentType: "application/json; charset=utf-8" } };

  function manifestV2(patches: { generation: number; prefixes: string[] }[]): string {
    return `${JSON.stringify({
      schema_version: 2,
      unit: "parcel-by-pnu",
      base_generation: SERVED_GENERATION,
      base_object_count: 1,
      document_schema_version: "doc.v1",
      gold_table: "gold.panel",
      gold_iceberg_snapshot_id: "999990000000000001",
      reflected_gold_iceberg_snapshot_id: "999990000000000002",
      patches: patches.map((patch) => ({
        ...patch,
        gold_iceberg_snapshot_id: "999990000000000002",
        upserted: 1,
        deleted: 0,
      })),
      object_count: 1,
      published_at_utc: "2026-01-01T00:00:00Z",
    })}\n`;
  }

  function tombstone(pnu: string): string {
    return `${JSON.stringify({
      schema_version: GATEWAY_PATCHES.tombstone_schema_version,
      pnu,
      deleted: true,
      source: { table: "gold.panel", iceberg_snapshot_id: "999990000000000002" },
    })}\n`;
  }

  async function publish(manifest: string): Promise<void> {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await (await runtime.getR2Bucket(R2_BINDING)).put(MANIFEST_KEY, manifest, json);
    // The manifest is edge-cached for a minute; a test does not wait for it to expire.
    await (await runtime.getCaches()).default.delete(
      "https://foundation-parcel-gateway.invalid/serving-manifest",
    );
  }

  beforeEach(async () => {
    baseBody = await readFile(new URL("fixtures/parcel.json", import.meta.url), "utf8");
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      absWorkingDir: packageRoot,
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    runtime = new Miniflare({
      compatibilityDate: "2026-04-26",
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [R2_BINDING],
      bindings: { FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: ALLOWED_ORIGIN },
      cache: true,
    });
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.put(OBJECT_KEY, baseBody, json);
    await bucket.put(MANIFEST_KEY, manifestBody(SERVED_GENERATION), json);
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  it("a v1 manifest still serves its generation", async () => {
    const response = await runtime?.dispatchFetch(PARCEL_URL);
    expect(response?.status).toBe(200);
    expect(await response?.text()).toBe(baseBody);
  });

  it("the newest patch holding the PNU answers before older patches and the base", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const older = `{"pnu":"${PATCHED_PNU}","patch":1,"padding":"${"x".repeat(600)}"}\n`;
    const newer = `{"pnu":"${PATCHED_PNU}","patch":3,"padding":"${"y".repeat(600)}"}\n`;
    await bucket.put(patchKey(1, PATCHED_PNU), older, json);
    await bucket.put(patchKey(3, PATCHED_PNU), newer, json);
    await publish(
      manifestV2([
        { generation: 3, prefixes: ["99999"] },
        { generation: 1, prefixes: ["99999"] },
      ]),
    );
    const response = await runtime.dispatchFetch(PARCEL_URL, { headers: { Origin: ALLOWED_ORIGIN } });
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(newer);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    // A PNU no patch holds falls through to the base, and to 404 when the base lacks it too.
    expect((await runtime.dispatchFetch(`https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`)).status).toBe(404);
  });

  it("a patch whose prefix list lacks the PNU's prefix is skipped", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    // The object exists, but the manifest says this patch holds nothing under 99999: the Worker
    // must not read it, so the base answers.
    await bucket.put(patchKey(2, PATCHED_PNU), `{"pnu":"${PATCHED_PNU}","patch":2}\n`, json);
    await publish(manifestV2([{ generation: 2, prefixes: ["99998"] }]));
    const response = await runtime.dispatchFetch(PARCEL_URL);
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(baseBody);
  });

  it("a tombstone hides the base object with a typed 404", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.put(patchKey(1, PATCHED_PNU), tombstone(PATCHED_PNU), json);
    await publish(manifestV2([{ generation: 1, prefixes: ["99999"] }]));
    const response = await runtime.dispatchFetch(PARCEL_URL, { headers: { Origin: ALLOWED_ORIGIN } });
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "deleted", pnu: PATCHED_PNU });
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
  });

  it("a small patch document that is not a tombstone is served", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const small = `{"pnu":"${PATCHED_PNU}","deleted":false}\n`;
    await bucket.put(patchKey(1, PATCHED_PNU), small, json);
    await publish(manifestV2([{ generation: 1, prefixes: ["99999"] }]));
    const response = await runtime.dispatchFetch(PARCEL_URL);
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(small);
  });

  it("a new patch is answered at once, not from the previous state's edge cache", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const before = await runtime.dispatchFetch(PARCEL_URL);
    expect(await before.text()).toBe(baseBody);
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const changed = `{"pnu":"${PATCHED_PNU}","patch":1,"padding":"${"z".repeat(600)}"}\n`;
    await bucket.put(patchKey(1, PATCHED_PNU), changed, json);
    await publish(manifestV2([{ generation: 1, prefixes: ["99999"] }]));
    const after = await runtime.dispatchFetch(PARCEL_URL);
    expect(await after.text()).toBe(changed);
    // Rolling the manifest back serves the base again: each state has its own cache entries.
    await publish(manifestBody(SERVED_GENERATION));
    expect(await (await runtime.dispatchFetch(PARCEL_URL)).text()).toBe(baseBody);
  });

  it("a conditional GET of a patched document answers 304", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const changed = `{"pnu":"${PATCHED_PNU}","patch":1,"padding":"${"w".repeat(600)}"}\n`;
    await bucket.put(patchKey(1, PATCHED_PNU), changed, json);
    await publish(manifestV2([{ generation: 1, prefixes: ["99999"] }]));
    const stored = await bucket.head(patchKey(1, PATCHED_PNU));
    if (stored === null) throw new Error("patch fixture is missing");
    const response = await runtime.dispatchFetch(PARCEL_URL, { headers: { "If-None-Match": stored.httpEtag } });
    expect(response.status).toBe(304);
  });

  it("the capabilities path names the manifest schemas this Worker reads", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(
      `https://catalog.example.test${GATEWAY.request_path.capabilities}`,
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    const body = (await response.json()) as { unit: string; manifest_schema_versions: number[] };
    expect(body.unit).toBe("parcel-by-pnu");
    expect(body.manifest_schema_versions).toEqual([...MANIFEST_SCHEMA_VERSIONS]);
    // The publisher writes this schema; the Worker it publishes for must read it.
    expect(body.manifest_schema_versions).toContain(GATEWAY_PATCHES.manifest_schema_version);
  });
});
