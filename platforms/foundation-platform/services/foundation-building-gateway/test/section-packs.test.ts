import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import {
  JOINED_SECTIONS,
  findEntry,
  forgetHeads,
  headCacheResponse,
  headFromCache,
  parseHead,
  parseSectionPacks,
  previewPlan,
  readHead,
  resolvePacks,
  unquotedEtag,
} from "../src/packs";

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

  it("serves the joined document the object held, with the object lane's headers", async () => {
    await serve(sectionPacks([]));
    const response = await get(PNU_A, "", { Origin: ALLOWED_ORIGIN });
    expect(response.status).toBe(200);
    const text = await response.text();
    expect(JSON.parse(text)).toEqual(JSON.parse(documents.base[PNU_A] ?? ""));
    expect(text.endsWith("}\n")).toBe(true);
    expect(response.headers.get("content-type")).toBe(GATEWAY.content_type);
    expect(response.headers.get("cache-control")).toBe(GATEWAY.cache_control);
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]{32}"$/);
    const empty = await get(PNU_B);
    expect(JSON.parse(await empty.text())).toEqual(JSON.parse(documents.base[PNU_B] ?? ""));
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
    expect(JSON.parse(await changed.text())).toEqual(JSON.parse(documents.patched[PNU_A] ?? ""));
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
    expect(JSON.parse(await (await get(PNU_A)).text())).toEqual(JSON.parse(documents.base[PNU_A] ?? ""));
  });

  it("a listed pack that is missing, or sections that disagree, are an outage, not a document", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await serve(sectionPacks([{ patch: 1, units: [UNIT] }]));
    await bucket.delete(packKey("unit_prices", 1));
    const missing = await get(PNU_A);
    expect(missing.status).toBe(503);
    expect(missing.headers.get("cache-control")).toBe("no-store");

    await runtime.dispose();
    runtime = await start();
    await serve(sectionPacks([]));
    await (await runtime.getR2Bucket(R2_BINDING)).delete(packKey("floors", null));
    expect((await get(PNU_A)).status).toBe(503);
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
    expect(JSON.parse(await preview.text())).toEqual(JSON.parse(documents.base[PNU_A] ?? ""));
    expect((await get(PNU_A, "?packs=g2")).status).toBe(404);
    expect((await get(PNU_A, "?packs=g0")).status).toBe(404);
    expect((await get(PNU_A, "?packs=1")).status).toBe(404);
    // A live manifest naming the generation lets the live route answer it too.
    await runtime.dispose();
    runtime = await start();
    await serve(sectionPacks([]));
    expect((await get(PNU_A, "?packs=g1")).status).toBe(200);
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

  it("the join knows exactly the sections the contract lists", () => {
    expect([...PACKS.sections]).toEqual([...JOINED_SECTIONS]);
    expect(PACKS.sections).toContain(PACKS.anchor_section);
    expect(parseSectionPacks(sectionPacks([]), 64)?.fingerprint).toBe("packs-g1.1.1.1-p0");
  });
});

describe("pack heads: one unranged read, an entity tag that survives the edge cache", () => {
  const ETAG = "0123456789abcdef0123456789abcdef";
  let pending: Promise<unknown>[];
  let edge: Map<string, Response>;
  const ctx = {
    waitUntil: (promise: Promise<unknown>) => {
      pending.push(promise);
    },
    passThroughOnException: () => undefined,
  } as unknown as ExecutionContext;

  beforeEach(() => {
    forgetHeads();
    pending = [];
    edge = new Map();
    // The Cache API, as the Worker sees it: what was put is matched, header for header.
    (globalThis as Record<string, unknown>).caches = {
      default: {
        match: async (url: string) => edge.get(url)?.clone(),
        put: async (url: string, response: Response) => {
          edge.set(url, response);
        },
      },
    };
  });

  afterEach(() => {
    delete (globalThis as Record<string, unknown>).caches;
  });

  interface Asked {
    key: string;
    range: { offset?: number; length?: number } | undefined;
  }

  /// R2 as a stricter bucket than the real one: a range past the object's end is an error (so no
  /// read may rely on R2 clamping it), and an unranged body streams in 7-byte chunks, counting
  /// what was pulled and whether the reader cancelled it.
  function strictBucket(objects: Record<string, Uint8Array>) {
    const asked: Asked[] = [];
    const streamed = { pulled: 0, cancelled: 0 };
    const bucket = {
      async get(key: string, options?: { range?: { offset?: number; length?: number } }) {
        asked.push({ key, range: options?.range });
        const bytes = objects[key];
        if (bytes === undefined) return null;
        const range = options?.range;
        if (range !== undefined) {
          const offset = range.offset ?? 0;
          const length = range.length ?? bytes.length - offset;
          if (offset + length > bytes.length) throw new Error(`range ${offset}+${length} is past ${bytes.length}`);
          const slice = bytes.slice(offset, offset + length);
          return { etag: ETAG, body: new Blob([slice]).stream(), arrayBuffer: async () => slice.buffer };
        }
        let at = 0;
        const body = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (at >= bytes.length) {
              controller.close();
              return;
            }
            const chunk = bytes.slice(at, at + 7);
            at += chunk.length;
            streamed.pulled += chunk.length;
            controller.enqueue(chunk);
          },
          cancel() {
            streamed.cancelled += 1;
          },
        });
        return { etag: ETAG, body, arrayBuffer: async () => bytes.buffer };
      },
    };
    return { bucket: bucket as unknown as Pick<R2Bucket, "get">, asked, streamed };
  }

  async function packs(): Promise<Record<string, Uint8Array>> {
    const objects: Record<string, Uint8Array> = {};
    for (const section of PACKS.sections) {
      objects[packKey(section, null)] = new Uint8Array(await readFile(new URL(`g1-${section}.pack`, fixtures)));
    }
    return objects;
  }

  it("reads a pack far smaller than any fixed first read with one unranged GET, and stops at its head", async () => {
    const objects = await packs();
    const key = packKey("buildings", null);
    const whole = objects[key];
    if (whole === undefined) throw new Error("no fixture");
    const { bucket, asked, streamed } = strictBucket(objects);
    const head = await readHead(bucket, key, ctx);
    if (head === null) throw new Error("the pack was not found");
    expect(asked).toEqual([{ key, range: undefined }]);
    expect(head.etag).toBe(ETAG);
    const expected = parseHead(whole, ETAG);
    expect(head.entryCount).toBe(expected?.entryCount);
    expect(head.bodyStart).toBe(expected?.bodyStart);
    // The head spans several chunks; the body after it is cancelled, not read.
    expect(head.bodyStart).toBeGreaterThan(7);
    expect(streamed.cancelled).toBe(1);
    expect(streamed.pulled).toBeLessThan(whole.length);
  });

  it("a head the edge cache holds carries the pack's own entity tag, so its documents still read", async () => {
    const objects = await packs();
    const { bucket, asked } = strictBucket(objects);
    const plan = previewPlan(1);
    const first = await resolvePacks(bucket, ctx, plan, PNU_A);
    expect(first.kind).toBe("document");
    await Promise.all(pending);
    const cached = edge.values().next().value;
    expect(cached?.headers.get("ETag")).toBe(`"${ETAG}"`);
    // A new isolate: the heads come from the edge cache, the documents from R2, and the two
    // entity tags agree (one quoted in the cache, one unquoted from R2).
    forgetHeads();
    const heads = asked.filter((read) => read.range === undefined).length;
    const second = await resolvePacks(bucket, ctx, plan, PNU_A);
    expect(second).toEqual(first);
    expect(asked.filter((read) => read.range === undefined).length).toBe(heads);
    expect(unquotedEtag(`W/"${ETAG}"`)).toBe(ETAG);
    expect(unquotedEtag("")).toBeNull();
  });

  it("a cached head without an entity tag is read from R2 again", async () => {
    const objects = await packs();
    const key = packKey("floors", null);
    const { bucket, asked } = strictBucket(objects);
    const whole = objects[key];
    const parsed = whole === undefined ? null : parseHead(whole, ETAG);
    if (parsed === null) throw new Error("no fixture head");
    const tagless = headCacheResponse(parsed);
    tagless.headers.delete("ETag");
    edge.set(`https://foundation-building-gateway.invalid/pack-head/${key}`, tagless);
    expect(await headFromCache(key)).toBeNull();
    const head = await readHead(bucket, key, ctx);
    expect(head?.etag).toBe(ETAG);
    expect(asked).toEqual([{ key, range: undefined }]);
  });

  it("a truncated pack is a format error, not a guess", async () => {
    const objects = await packs();
    const key = packKey("units", null);
    const whole = objects[key];
    if (whole === undefined) throw new Error("no fixture");
    const { bucket } = strictBucket({ [key]: whole.slice(0, 30) });
    await expect(readHead(bucket, key, ctx)).rejects.toThrow("shorter than declared");
  });
});
