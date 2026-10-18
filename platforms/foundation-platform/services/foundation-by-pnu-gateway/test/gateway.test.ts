import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { MANIFEST_SCHEMA_VERSIONS, parseAllowedOrigins, parseManifest } from "../src/index";

// The HTTP surface every lane's Worker shares, held for the building and the parcel lane on the
// pack path, the only one they have (root ADR-0274). The packs are the publisher's golden packs
// (section-packs.test.ts): the read path never looks at a member's content. PNUs and snapshot ids
// sit in the repository-reserved synthetic namespaces (`scripts/guard/public-fixture-safety.py`).
const PNU = "9999900000100000000";
const UNBAKED_PNU = "9999900000200000000";
const UNIT = "9999900000";
const ALLOWED_ORIGIN = "https://app.example.test";
const SECOND_ALLOWED_ORIGIN = "https://admin.example.test";
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const GATEWAY_PATCHES = connectionContract.by_pnu_serving_patches;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

const LANES = [
  {
    lane: "building",
    gateway: connectionContract.building_by_pnu_gateway,
    documentSchema: "foundation-platform.building_by_pnu_profile.v2",
    goldTable: "gold.building_panel",
  },
  {
    lane: "parcel",
    gateway: connectionContract.parcel_by_pnu_gateway,
    documentSchema: "foundation-platform.parcel_by_pnu_profile.v1",
    goldTable: "gold.parcel_panel",
  },
] as const;

describe.each(LANES)("foundation $lane gateway", ({ lane, gateway: GATEWAY, documentSchema, goldTable }) => {
  const UNIT_NAME = `${lane}-by-pnu`;
  const MANIFEST_KEY = GATEWAY.object_key.manifest_object;
  const R2_BINDING = GATEWAY.r2_binding;
  const URL_OF_PNU = `https://catalog.example.test${GATEWAY.request_path.prefix}${PNU}`;
  const OBJECT_KEY = `${GATEWAY.object_key.root}/v7/${PNU}${GATEWAY.object_key.suffix}`;
  let runtime: Miniflare | undefined;
  let body: string;

  function manifest(withPacks = true): string {
    return `${JSON.stringify({
      schema_version: 2,
      unit: UNIT_NAME,
      base_generation: 7,
      base_object_count: 2,
      document_schema_version: documentSchema,
      gold_table: goldTable,
      gold_iceberg_snapshot_id: "999990000000000001",
      reflected_gold_iceberg_snapshot_id: "999990000000000001",
      pnu_prefix_length: 5,
      patches: [],
      object_count: 2,
      published_at_utc: "2026-01-01T00:00:00Z",
      ...(withPacks
        ? {
            section_packs: {
              schema_version: PACK_POLICY.manifest_section_packs_schema_version,
              format_version: PACK_POLICY.format_version,
              unit_prefix_length: PACK_POLICY.unit_prefix_length,
              document_schema_version: documentSchema,
              gold_table: goldTable,
              reflected_gold_iceberg_snapshot_id: "999990000000000001",
              document_count: 2,
              sections: GATEWAY.section_packs.sections.map((name) => ({
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

  beforeEach(async () => {
    const documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as {
      base: Record<string, string>;
    };
    body = documents.base[PNU] ?? "";
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
    await bucket.put(MANIFEST_KEY, manifest());
    for (const section of GATEWAY.section_packs.sections) {
      await bucket.put(
        `${GATEWAY.section_packs.root}/${section}/g1/${UNIT}${PACK_POLICY.suffix}`,
        new Uint8Array(await readFile(new URL(`g1-${section}.pack`, fixtures))),
      );
    }
    await bucket.put("bronze/vworld/2026/raw.jsonl", "{}\n");
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  async function get(url = URL_OF_PNU, init?: Parameters<Miniflare["dispatchFetch"]>[1]) {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    return runtime.dispatchFetch(url, init);
  }

  it("canonical GET serves the PNU's document from the manifest's packs", async () => {
    const response = await get(URL_OF_PNU, { headers: { Origin: ALLOWED_ORIGIN } });
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(body);
    expect(response.headers.get("content-type")).toBe(GATEWAY.content_type);
    expect(response.headers.get("cache-control")).toBe(GATEWAY.cache_control);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]+-[0-9]+"$/);
  });

  it("an unbaked PNU is 404, not an outage", async () => {
    const response = await get(`https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(response.status).toBe(404);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it.each([
    ["absent", null],
    ["malformed JSON", "not json"],
    ["wrong unit", `${JSON.stringify({ schema_version: 1, unit: "tiles", current_generation: 1 })}\n`],
    ["wrong schema", `${JSON.stringify({ schema_version: 2, unit: `${lane}-by-pnu`, current_generation: 1 })}\n`],
    ["zero generation", `${JSON.stringify({ schema_version: 1, unit: `${lane}-by-pnu`, current_generation: 0 })}\n`],
    ["no packs, with the object still in R2", "no-packs"],
  ])("a manifest the gateway cannot serve is an outage, not a 404: %s", async (_label, manifestBody) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    if (manifestBody === null) {
      await bucket.delete(MANIFEST_KEY);
    } else if (manifestBody === "no-packs") {
      // Root ADR-0274: the per-PNU objects stay in R2 but are never read again.
      await bucket.put(OBJECT_KEY, `{"pnu":"${PNU}","served_by":"objects"}\n`);
      await bucket.put(MANIFEST_KEY, manifest(false));
    } else {
      await bucket.put(MANIFEST_KEY, manifestBody);
    }
    const response = await get();
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("the manifest lookup is edge-cached so a burst does not re-read the pointer", async () => {
    const first = await get();
    expect(first.status).toBe(200);
    await first.arrayBuffer();
    // With the pointer gone, a fresh resolution would 503; the cached manifest keeps serving.
    await (await runtime?.getR2Bucket(R2_BINDING))?.delete(MANIFEST_KEY);
    expect((await get(`https://catalog.example.test${GATEWAY.request_path.prefix}${UNBAKED_PNU}`)).status).toBe(404);
  });

  it("non-canonical paths return 404", async () => {
    const base = `https://catalog.example.test${GATEWAY.request_path.prefix}`;
    const urls = [
      `https://catalog.example.test/${OBJECT_KEY}`,
      `https://catalog.example.test/${MANIFEST_KEY}`,
      "https://catalog.example.test/bronze/vworld/2026/raw.jsonl",
      `${base}999990000010000000`,
      `${base}99999000001000000001`,
      `${base}9999900000000000000`,
      `${base}not-a-pnu`,
      `${base}${PNU}.json`,
      `${base}nested/${PNU}`,
      `${URL_OF_PNU}?download=1`,
      `${URL_OF_PNU}?schema=1`,
      // Traversal forms.
      `${base}../../bronze/x`,
      `${base}%2e%2e/%2e%2e/bronze/x`,
      `${base}%252e%252e/%252e%252e/bronze/x`,
      `${base}${PNU}%2f..%2fbronze.json`,
      `${base}${PNU}%252f..%252fbronze.json`,
    ];
    for (const accepted of GATEWAY.request_path.accepted_queries) urls.push(`${URL_OF_PNU}?${accepted}&download=1`);
    for (const url of urls) expect((await get(url)).status, url).toBe(404);
  });

  it.each(["PUT", "POST", "DELETE", "PATCH"])("write methods return 405 and change nothing: %s", async (method) => {
    const response = await get(URL_OF_PNU, { method, ...(method === "DELETE" ? {} : { body: "mutated" }) });
    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET, HEAD, OPTIONS");
    expect(await (await get()).text()).toBe(body);
  });

  it("origin gate never emits wildcard CORS", async () => {
    const noOrigin = await get();
    expect(noOrigin.status).toBe(200);
    expect(noOrigin.headers.get("access-control-allow-origin")).toBeNull();
    const first = await get(URL_OF_PNU, { headers: { Origin: ALLOWED_ORIGIN } });
    expect(first.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(first.headers.get("vary")).toContain("Origin");
    const second = await get(URL_OF_PNU, { headers: { Origin: SECOND_ALLOWED_ORIGIN } });
    expect(second.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
    const denied = await get(URL_OF_PNU, { headers: { Origin: "https://attacker.example.test" } });
    expect(denied.status).toBe(403);
    expect(denied.headers.get("access-control-allow-origin")).toBeNull();
  });

  it("CORS preflight allows only configured application origins", async () => {
    const allowed = await get(URL_OF_PNU, {
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
    const denied = await get(URL_OF_PNU, {
      method: "OPTIONS",
      headers: { Origin: "https://attacker.example.test", "Access-Control-Request-Method": "GET" },
    });
    expect(denied.status).toBe(403);
  });

  it("CORS grammar matches the shared contract corpus", () => {
    for (const accepted of GATEWAY.cors.accepted) expect(parseAllowedOrigins(accepted), accepted).not.toBeNull();
    for (const rejected of GATEWAY.cors.rejected) expect(parseAllowedOrigins(rejected), rejected).toBeNull();
  });

  it("the capabilities path names the manifest schemas this Worker reads", async () => {
    const response = await get(`https://catalog.example.test${GATEWAY.request_path.capabilities}`);
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    const answer = (await response.json()) as { unit: string; manifest_schema_versions: number[] };
    expect(answer.unit).toBe(UNIT_NAME);
    expect(answer.manifest_schema_versions).toEqual([...MANIFEST_SCHEMA_VERSIONS]);
    // The publisher writes this schema; the Worker it publishes for must read it.
    expect(answer.manifest_schema_versions).toContain(GATEWAY_PATCHES.manifest_schema_version);
  });
});

describe("the served manifest", () => {
  const GATEWAY = connectionContract.building_by_pnu_gateway;

  it("validation accepts only the lane's own pointer shapes", () => {
    expect(parseManifest({ schema_version: 1, unit: "building-by-pnu", current_generation: 7 })).toEqual({
      base: 7,
      prefixLength: 0,
      patches: [],
      fingerprint: "v7",
    });
    const v2 = parseManifest({
      schema_version: 2,
      unit: "building-by-pnu",
      base_generation: 7,
      pnu_prefix_length: 5,
      patches: [
        { generation: 3, prefixes: ["99999"] },
        { generation: 1, prefixes: ["99998", "99999"] },
      ],
    });
    expect(v2?.base).toBe(7);
    expect(v2?.fingerprint).toBe("v7p3");
    expect(v2?.patches.map((patch) => patch.generation)).toEqual([3, 1]);
    const patch = (generation: unknown, prefixes: unknown) => ({ generation, prefixes });
    const v2Head = { schema_version: 2, unit: "building-by-pnu", base_generation: 1, pnu_prefix_length: 5 };
    for (const invalid of [
      null,
      "text",
      {},
      { schema_version: 1, unit: "building-by-pnu", current_generation: 0 },
      { schema_version: 1, unit: "building-by-pnu", current_generation: 1.5 },
      { schema_version: 1, unit: "building-by-pnu", current_generation: "1" },
      { schema_version: 2, unit: "building-by-pnu", current_generation: 1 },
      { schema_version: 3, unit: "building-by-pnu", base_generation: 1, patches: [] },
      { schema_version: 1, unit: "tiles", current_generation: 1 },
      { ...v2Head },
      { ...v2Head, pnu_prefix_length: undefined, patches: [] },
      { ...v2Head, pnu_prefix_length: 0, patches: [] },
      { ...v2Head, pnu_prefix_length: 20, patches: [] },
      { ...v2Head, pnu_prefix_length: "5", patches: [] },
      { ...v2Head, patches: [patch(1, ["9999"])] },
      { ...v2Head, pnu_prefix_length: 4, patches: [patch(1, ["99999"])] },
      { ...v2Head, patches: [patch(1, [])] },
      { ...v2Head, patches: [patch(0, ["99999"])] },
      { ...v2Head, patches: [patch(1, ["99999"]), patch(2, ["99999"])] },
      {
        ...v2Head,
        patches: Array.from({ length: GATEWAY_PATCHES.manifest_patch_ceiling + 1 }, (_, i) => patch(999 - i, ["99999"])),
      },
    ]) {
      expect(parseManifest(invalid), JSON.stringify(invalid)).toBeNull();
    }
    expect(GATEWAY.pack_only).toBe(true);
  });

  it("production source exposes no R2 list or write capability, and reads no per-PNU object", async () => {
    const source = await readFile(new URL("../src/index.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/LAKEHOUSE\s*\.\s*(?:list|put|delete)\s*\(/);
    expect(source).not.toMatch(/(?:ACCESS_KEY|SECRET_ACCESS|ACCOUNT_ID)/);
    expect(source).not.toContain("foundation-platform-lakehouse-prod");
    expect(source).not.toMatch(/Access-Control-Allow-Origin[\s\S]{0,80}["']\*["']/);
    expect(source).toContain('Pick<R2Bucket, "get">');
    // Root ADR-0274: the object lane's keys are not built anywhere in the Worker.
    expect(source).not.toMatch(/object_key\s*;|object_key\.(?:root|suffix)/);
  });
});
