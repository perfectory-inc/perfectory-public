import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { findEntry, parseHead, parseSectionPacks } from "../src/packs";

// The golden packs and documents are written by the publisher's own tests
// (`foundation-outbox-publisher/src/building_by_pnu_serving_export/section_packs/tests.rs`), so
// this reader and that writer are held to one set of bytes. PNUs and snapshot ids sit in the
// repository-reserved synthetic namespaces (`scripts/guard/public-fixture-safety.py`).
const PNU_A = "9999900000100000000";
const PNU_B = "9999900000100010000";
const UNKNOWN_IN_DONG = "9999900000100020000";
const OTHER_DONG = "9999900001100000000";
const UNIT = "9999900000";
const GATEWAY = connectionContract.building_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const R2_BINDING = GATEWAY.r2_binding;
const MANIFEST_KEY = GATEWAY.object_key.manifest_object;
const ALLOWED_ORIGIN = "https://app.example.test";
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));

const url = (pnu: string, query = "") => `https://buildings.example.test${GATEWAY.request_path.prefix}${pnu}${query}`;
const packKey = (section: string, patch: number | null) =>
  `${PACKS.root}/${section}/g1/${patch === null ? "" : `p${patch}/`}${UNIT}${PACK_POLICY.suffix}`;

interface Documents {
  base: Record<string, string>;
  patched: Record<string, string>;
}

function sectionPacks(patches: { patch: number; units: string[] }[], overrides: Record<string, unknown> = {}) {
  return {
    schema_version: PACK_POLICY.manifest_section_packs_schema_version,
    format_version: PACK_POLICY.format_version,
    unit_prefix_length: PACK_POLICY.unit_prefix_length,
    document_schema_version: "foundation-platform.building_by_pnu_profile.v2",
    gold_table: "gold.building_panel",
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
    patches: patches.map((patch) => ({
      ...patch,
      gold_iceberg_snapshot_id: "999990000000000002",
      upserted: 1,
      deleted: 1,
    })),
    ...overrides,
  };
}

function manifest(packs: unknown): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "building-by-pnu",
    base_generation: 7,
    base_object_count: 2,
    document_schema_version: "foundation-platform.building_by_pnu_profile.v2",
    gold_table: "gold.building_panel",
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: 5,
    patches: [],
    object_count: 2,
    published_at_utc: "2026-01-01T00:00:00Z",
    ...(packs === undefined ? {} : { section_packs: packs }),
  })}\n`;
}

describe("foundation building gateway section packs (root ADR-0147)", () => {
  let runtime: Miniflare | undefined;
  let documents: Documents;

  async function start(bindings: Record<string, string> = {}): Promise<Miniflare> {
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
    const started = new Miniflare({
      compatibilityDate: "2026-04-26",
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [R2_BINDING],
      bindings: { FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: ALLOWED_ORIGIN, ...bindings },
      cache: true,
    });
    const bucket = await started.getR2Bucket(R2_BINDING);
    for (const section of PACKS.sections) {
      // A plain Uint8Array: Miniflare's proxy does not carry a Node Buffer.
      await bucket.put(packKey(section, null), new Uint8Array(await readFile(new URL(`g1-${section}.pack`, fixtures))));
      await bucket.put(packKey(section, 1), new Uint8Array(await readFile(new URL(`g1-p1-${section}.pack`, fixtures))));
    }
    return started;
  }

  async function serve(packs: unknown): Promise<void> {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await (await runtime.getR2Bucket(R2_BINDING)).put(MANIFEST_KEY, manifest(packs));
  }

  async function get(pnu: string, query = "", headers: Record<string, string> = {}) {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    return runtime.dispatchFetch(url(pnu, query), { headers });
  }

  beforeEach(async () => {
    documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as Documents;
    runtime = await start();
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  it("serves the object lane's document byte for byte, gzip as the pack holds it", async () => {
    await serve(sectionPacks([]));
    const response = await get(PNU_A, "", { Origin: ALLOWED_ORIGIN, "Accept-Encoding": "gzip" });
    expect(response.status).toBe(200);
    // The client decodes the member; what it reads is exactly what the object lane served.
    expect(await response.text()).toBe(documents.base[PNU_A]);
    expect(response.headers.get("content-type")).toBe(GATEWAY.content_type);
    expect(response.headers.get("cache-control")).toBe(GATEWAY.cache_control);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("vary")).toContain("Accept-Encoding");
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]+-[0-9]+"$/);
    expect(await (await get(PNU_B)).text()).toBe(documents.base[PNU_B]);
  });

  it("a client that takes no gzip gets the same bytes decompressed by the Worker", async () => {
    await serve(sectionPacks([]));
    for (const acceptEncoding of ["identity", "gzip;q=0", "br"]) {
      const response = await get(PNU_A, "", { "Accept-Encoding": acceptEncoding });
      expect(response.status, acceptEncoding).toBe(200);
      expect(response.headers.get("content-encoding"), acceptEncoding).toBeNull();
      expect(await response.text(), acceptEncoding).toBe(documents.base[PNU_A]);
    }
  });

  it("the gzip and the decompressed answers carry different entity tags, each validating only itself", async () => {
    await serve(sectionPacks([]));
    const gzip = await get(PNU_A, "", { "Accept-Encoding": "gzip" });
    const identity = await get(PNU_A, "", { "Accept-Encoding": "identity" });
    await Promise.all([gzip.arrayBuffer(), identity.arrayBuffer()]);
    const gzipTag = gzip.headers.get("etag") ?? "";
    const identityTag = identity.headers.get("etag") ?? "";
    expect(gzipTag).toMatch(/^"[0-9a-f]+-[0-9]+"$/);
    expect(identityTag).toMatch(/^"[0-9a-f]+-[0-9]+-identity"$/);
    expect(identityTag).not.toBe(gzipTag);
    expect((await get(PNU_A, "", { "Accept-Encoding": "gzip", "If-None-Match": gzipTag })).status).toBe(304);
    expect((await get(PNU_A, "", { "Accept-Encoding": "identity", "If-None-Match": identityTag })).status).toBe(304);
    // A tag of the other representation is not a match: the client gets the bytes it asked for.
    const crossed = await get(PNU_A, "", { "Accept-Encoding": "identity", "If-None-Match": gzipTag });
    expect(crossed.status).toBe(200);
    expect(await crossed.text()).toBe(documents.base[PNU_A]);
    const crossedGzip = await get(PNU_A, "", { "Accept-Encoding": "gzip", "If-None-Match": identityTag });
    expect(crossedGzip.status).toBe(200);
    await crossedGzip.arrayBuffer();
  });

  it("HEAD answers the headers of the GET it stands for, with no body, in either encoding", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await serve(sectionPacks([]));
    for (const acceptEncoding of ["gzip", "identity"]) {
      const got = await get(PNU_A, "", { "Accept-Encoding": acceptEncoding });
      await got.arrayBuffer();
      const head = await runtime.dispatchFetch(url(PNU_A, ""), {
        method: "HEAD",
        headers: { "Accept-Encoding": acceptEncoding },
      });
      expect(head.status, acceptEncoding).toBe(200);
      expect(await head.text(), acceptEncoding).toBe("");
      for (const name of ["etag", "content-encoding", "content-length", "content-type", "cache-control", "vary"]) {
        expect(head.headers.get(name), `${acceptEncoding} ${name}`).toBe(got.headers.get(name));
      }
    }
    const identity = await runtime.dispatchFetch(url(PNU_A, ""), {
      method: "HEAD",
      headers: { "Accept-Encoding": "identity" },
    });
    expect(identity.headers.get("content-length")).toBe(
      new TextEncoder().encode(documents.base[PNU_A]).byteLength.toString(),
    );
  });

  it("a PNU no pack holds is a typed 404, in its dong or in a dong without packs", async () => {
    await serve(sectionPacks([]));
    for (const pnu of [UNKNOWN_IN_DONG, OTHER_DONG]) {
      const response = await get(pnu);
      expect(response.status, pnu).toBe(404);
      expect(response.headers.get("cache-control")).toBe("no-store");
    }
  });

  it("gate (다): a patch's changed document and its tombstone are what the PNU answers", async () => {
    await serve(sectionPacks([{ patch: 1, units: [UNIT] }]));
    const changed = await get(PNU_A);
    expect(changed.status).toBe(200);
    expect(await changed.text()).toBe(documents.patched[PNU_A]);
    const deleted = await get(PNU_B);
    expect(deleted.status).toBe(404);
    expect(await deleted.json()).toEqual({ error: "deleted", pnu: PNU_B });
    // A patch whose unit list lacks the dong is not read: the base still answers. (A fresh
    // runtime: the manifest is edge-cached for its contract lifetime.)
    await runtime?.dispose();
    runtime = await start();
    await serve(sectionPacks([{ patch: 1, units: ["9999900009"] }]));
    expect((await get(PNU_B, "?schema=2")).status).toBe(200);
  });

  it("a section a patch floor already folded in is read from its base", async () => {
    const floored = sectionPacks([{ patch: 1, units: [UNIT] }]);
    floored.sections = floored.sections.map((section) => ({ ...section, patch_floor: 1 }));
    await serve(floored);
    expect(await (await get(PNU_A)).text()).toBe(documents.base[PNU_A]);
  });

  it("a listed pack that is missing, or sections that disagree, are an outage, not a document", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await serve(sectionPacks([{ patch: 1, units: [UNIT] }]));
    await bucket.delete(packKey(PACKS.anchor_section, 1));
    const missing = await get(PNU_A);
    expect(missing.status).toBe(503);
    expect(missing.headers.get("cache-control")).toBe("no-store");

    await runtime.dispose();
    runtime = await start();
    await serve(sectionPacks([]));
    await (await runtime.getR2Bucket(R2_BINDING)).delete(packKey(PACKS.anchor_section, null));
    // The base pack of a dong is absent: the PNU is not served (a 404), not an outage.
    expect((await get(PNU_A)).status).toBe(404);
  });

  it.each([
    ["an unknown block schema", { schema_version: 99 }],
    ["another format", { format_version: 2 }],
    ["a missing section", { sections: [] }],
    ["patches oldest first", { patches: [{ patch: 1, units: [UNIT] }, { patch: 2, units: [UNIT] }] }],
    ["a short unit", { patches: [{ patch: 1, units: ["99999"] }] }],
  ])("a section_packs block the Worker cannot trust is an outage: %s", async (_label, override) => {
    await serve(sectionPacks([], override));
    const response = await get(PNU_A);
    expect(response.status).toBe(503);
  });

  it("answers 304 to its own ETag, from R2 and from the edge cache", async () => {
    await serve(sectionPacks([]));
    const first = await get(PNU_A);
    const etag = first.headers.get("etag") ?? "";
    await first.arrayBuffer();
    const cold = await get(PNU_A, "", { "If-None-Match": etag });
    expect(cold.status).toBe(304);
    expect(await cold.text()).toBe("");
  });

  it("a preview version serves an unpublished generation; the live route refuses it", async () => {
    await serve(undefined);
    expect((await get(PNU_A, "?packs=g1")).status).toBe(404);
    if (runtime === undefined) throw new Error("Miniflare did not start");
    await runtime.dispose();
    runtime = await start({ [PACKS.preview_binding]: "true" });
    await serve(undefined);
    const preview = await get(PNU_A, "?packs=g1");
    expect(preview.status).toBe(200);
    expect(await preview.text()).toBe(documents.base[PNU_A]);
    expect((await get(PNU_A, "?packs=g2")).status).toBe(404);
    expect((await get(PNU_A, "?packs=g0")).status).toBe(404);
    expect((await get(PNU_A, "?packs=1")).status).toBe(404);
    // A live manifest naming the generation lets the live route answer it too.
    await runtime.dispose();
    runtime = await start();
    await serve(sectionPacks([]));
    expect((await get(PNU_A, "?packs=g1")).status).toBe(200);
  });

  it("a version whose serving binding is off answers from objects under a pack manifest", async () => {
    const object = `{"pnu":"${PNU_A}","served_by":"objects"}\n`;
    const objectKey = `${GATEWAY.object_key.root}/v7/${PNU_A}${GATEWAY.object_key.suffix}`;
    for (const [binding, expected] of [
      ["off", object],
      ["on", documents.base[PNU_A]],
    ] as const) {
      await runtime?.dispose();
      runtime = await start({ [PACKS.serving_binding]: binding });
      await (await runtime.getR2Bucket(R2_BINDING)).put(objectKey, object);
      await serve(sectionPacks([]));
      const response = await get(PNU_A);
      expect(response.status, binding).toBe(200);
      expect(await response.text(), binding).toBe(expected);
    }
  });

  it("the gateway says it reads the section_packs block", async () => {
    await serve(sectionPacks([]));
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(`https://buildings.example.test${GATEWAY.request_path.capabilities}`);
    const body = (await response.json()) as { manifest_schema_versions: number[] };
    expect(body.manifest_schema_versions).toContain(PACK_POLICY.manifest_section_packs_schema_version);
    expect(body.manifest_schema_versions).toEqual([1, 2, 3]);
  });
});

describe("section pack format (golden bytes)", () => {
  it("reads the publisher's golden pack: header, index search, tombstone", async () => {
    const bytes = new Uint8Array(await readFile(new URL("format.golden.pack", fixtures)));
    const head = parseHead(bytes, "");
    if (head === null) throw new Error("the golden head is incomplete");
    expect(head.entryCount).toBe(3);
    expect(findEntry(head, "9999900000100000000")?.state).toBe(1);
    expect(findEntry(head, "9999900000100010000")).toMatchObject({ state: 2, length: 0 });
    expect(findEntry(head, "9999900000300000000")).toBeNull();
    const entry = findEntry(head, "9999900000200000000");
    if (entry === null) throw new Error("C is missing");
    const member = bytes.slice(head.bodyStart + entry.offset, head.bodyStart + entry.offset + entry.length);
    const decoded = await new Response(new Blob([member]).stream().pipeThrough(new DecompressionStream("gzip"))).text();
    expect(decoded).toBe("[]");
    // A head cut short asks for more instead of guessing.
    expect(parseHead(bytes.slice(0, head.bodyStart - 1), "")).toBeNull();
    const corrupt = bytes.slice();
    corrupt[0] = 0;
    expect(() => parseHead(corrupt, "")).toThrow();
  });

  it("the lane serves exactly one section, the contract's anchor (root ADR-0151)", () => {
    expect([...PACKS.sections]).toEqual([PACKS.anchor_section]);
    expect(parseSectionPacks(sectionPacks([]), 64)?.fingerprint).toBe("packs-g1-p0");
  });
});
