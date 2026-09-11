import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { parseAllowedOrigins, parseManifestGeneration } from "../src/index";

// PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
// (`scripts/guard/public-fixture-safety.py`).
const PNU = "9999900000100000000";
const UNBAKED_PNU = "9999900000200000000";
const SERVED_GENERATION = 2;
const GATEWAY = connectionContract.building_by_pnu_gateway;
const OBJECT_KEY = `${GATEWAY.object_key.root}/v${SERVED_GENERATION}/${PNU}${GATEWAY.object_key.suffix}`;
const STALE_OBJECT_KEY = `${GATEWAY.object_key.root}/v1/${PNU}${GATEWAY.object_key.suffix}`;
const MANIFEST_KEY = GATEWAY.object_key.manifest_object;
const BUILDING_URL = `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}`;
const ALLOWED_ORIGIN = "https://app.example.test";
const SECOND_ALLOWED_ORIGIN = "https://admin.example.test";
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const R2_BINDING = GATEWAY.r2_binding;

function manifestBody(generation: number): string {
  return `${JSON.stringify({
    schema_version: 1,
    unit: "building-by-pnu",
    current_generation: generation,
    gold_table: "gold.building_panel",
    gold_iceberg_snapshot_id: "999990000000000001",
    object_count: 1,
    published_at_utc: "2026-01-01T00:00:00Z",
  })}\n`;
}

describe("foundation building gateway", () => {
  let runtime: Miniflare | undefined;
  let buildingBody: string;

  beforeEach(async () => {
    buildingBody = await readFile(new URL("fixtures/building.json", import.meta.url), "utf8");
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
    await bucket.put(OBJECT_KEY, buildingBody, {
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
    const response = await runtime?.dispatchFetch(BUILDING_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    if (response === undefined) throw new Error("Miniflare did not start");

    expect(response.status).toBe(200);
    expect(await response.text()).toBe(buildingBody);
    expect(response.headers.get("content-type")).toBe("application/json; charset=utf-8");
    expect(response.headers.get("cache-control")).toBe(GATEWAY.cache_control);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]+"$/);
  });

  it("a building absent from the served generation is 404 even when a stale generation has it", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.delete(OBJECT_KEY);

    const response = await runtime.dispatchFetch(BUILDING_URL);
    expect(response.status).toBe(404);
  });

  it("the v2 request does not reuse a cached year-only document", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const legacy = '{"schema_version":"foundation-platform.building_by_pnu_profile.v1"}';
    await bucket.put(OBJECT_KEY, legacy);
    expect(await (await runtime.dispatchFetch(BUILDING_URL)).text()).toBe(legacy);
    await bucket.put(OBJECT_KEY, buildingBody);
    const response = await runtime.dispatchFetch(`${BUILDING_URL}?schema=2`);
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(buildingBody);
  });

  it("an unbaked building is 404, not an outage", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(
      `https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`,
      { headers: { Origin: ALLOWED_ORIGIN } },
    );
    expect(response.status).toBe(404);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it.each([
    ["absent", null],
    ["malformed JSON", "not json"],
    ["wrong unit", `${JSON.stringify({ schema_version: 1, unit: "tiles", current_generation: 1 })}\n`],
    ["wrong schema", `${JSON.stringify({ schema_version: 2, unit: "building-by-pnu", current_generation: 1 })}\n`],
    ["zero generation", `${JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 0 })}\n`],
  ])("a manifest the gateway cannot trust is an outage, not a 404: %s", async (_label, body) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    if (body === null) {
      await bucket.delete(MANIFEST_KEY);
    } else {
      await bucket.put(MANIFEST_KEY, body);
    }

    const response = await runtime.dispatchFetch(BUILDING_URL);
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("the manifest lookup is edge-cached so a burst does not re-read the pointer", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const first = await runtime.dispatchFetch(BUILDING_URL);
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
    `${BUILDING_URL}?download=1`,
    `${BUILDING_URL}?schema=1`,
    `${BUILDING_URL}?schema=2&download=1`,
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
      const response = await runtime.dispatchFetch(BUILDING_URL, {
        method,
        ...(method === "DELETE" ? {} : { body: "mutated" }),
      });
      expect(response.status).toBe(405);
      expect(response.headers.get("allow")).toBe("GET, HEAD, OPTIONS");
      expect(await (await runtime.dispatchFetch(BUILDING_URL)).text()).toBe(buildingBody);
    },
  );

  it("origin gate and cache isolation never emit wildcard CORS", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");

    const noOrigin = await runtime.dispatchFetch(BUILDING_URL);
    expect(noOrigin.status).toBe(200);
    expect(noOrigin.headers.get("access-control-allow-origin")).toBeNull();

    const first = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(first.status).toBe(200);
    expect(first.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(first.headers.get("vary")).toBe("Origin");

    const second = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(second.status).toBe(200);
    expect(second.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
    expect(second.headers.get("access-control-allow-origin")).not.toBe("*");

    const denied = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { Origin: "https://attacker.example.test" },
    });
    expect(denied.status).toBe(403);
    expect(denied.headers.get("access-control-allow-origin")).toBeNull();
  });

  it("cached object is origin-neutral", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    await response.arrayBuffer();
    const cached = await (await runtime.getCaches()).default.match(BUILDING_URL);
    expect(cached).toBeDefined();
    expect(cached?.headers.get("access-control-allow-origin")).toBeNull();
    expect(cached?.headers.get("vary")).toBeNull();
  });

  it("CORS preflight allows only configured application origins", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const allowed = await runtime.dispatchFetch(BUILDING_URL, {
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

    const denied = await runtime.dispatchFetch(BUILDING_URL, {
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
    if (stored === null) throw new Error("building fixture is missing");

    const cold = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: ALLOWED_ORIGIN },
    });
    expect(cold.status).toBe(304);
    expect(cold.headers.get("etag")).toBe(stored.httpEtag);
    expect(await cold.text()).toBe("");

    const nonMatch = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { "If-None-Match": '"not-this-object"' },
    });
    expect(nonMatch.status).toBe(200);
    expect(await nonMatch.text()).toBe(buildingBody);

    await bucket.delete(OBJECT_KEY);
    const warm = await runtime.dispatchFetch(BUILDING_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(warm.status).toBe(304);
    expect(warm.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
  });

  it("conditional HEAD returns headers without a body", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const stored = await bucket.head(OBJECT_KEY);
    if (stored === null) throw new Error("building fixture is missing");

    const response = await runtime.dispatchFetch(BUILDING_URL, { method: "HEAD" });
    expect(response.status).toBe(200);
    expect(response.headers.get("etag")).toBe(stored.httpEtag);
    expect(response.headers.get("content-length")).toBe(String(stored.size));
    expect(await response.text()).toBe("");

    const conditional = await runtime.dispatchFetch(BUILDING_URL, {
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

  it("manifest validation accepts only the lane's own pointer shape", () => {
    expect(
      parseManifestGeneration({
        schema_version: 1,
        unit: "building-by-pnu",
        current_generation: 7,
      }),
    ).toBe(7);
    for (const invalid of [
      null,
      "text",
      {},
      { schema_version: 1, unit: "building-by-pnu", current_generation: 0 },
      { schema_version: 1, unit: "building-by-pnu", current_generation: 1.5 },
      { schema_version: 1, unit: "building-by-pnu", current_generation: "1" },
      { schema_version: 2, unit: "building-by-pnu", current_generation: 1 },
      { schema_version: 1, unit: "tiles", current_generation: 1 },
    ]) {
      expect(parseManifestGeneration(invalid), JSON.stringify(invalid)).toBeNull();
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
