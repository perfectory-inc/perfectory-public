import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { fetchByPnu } from "../src/index";
import { fnv1a32, forgetPacks, parseSectionPacks, partsKey, partUnit } from "../src/packs";

// Parts of a large dong (root ADR-0163): the Worker reads a section generation's parts index once,
// checked against the sha256 the manifest names, and looks a PNU up in the pack of its part. The
// pack bytes are the publisher's golden packs (section-packs.test.ts): the read path never looks
// at which unit a pack was written for, so a golden pack placed under a part's key stands for that
// part. The Worker is driven in Node over an in-memory bucket and edge cache (as in
// pack-read-path.test.ts). PNUs sit in the repository-reserved synthetic namespace.
const PNU_A = "9999900000100000000";
const PNU_B = "9999900000100010000";
const DONG = "9999900000";
const GATEWAY = connectionContract.building_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const PARTS = PACK_POLICY.parts;
const R2_BINDING = GATEWAY.r2_binding;
const MANIFEST_KEY = GATEWAY.object_key.manifest_object;
const SECTION = PACKS.anchor_section;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);

/// The dong's part count in these tests, and each PNU's part from the contract's own vectors: the
/// expectations never come from the function under test.
const K = 2;
function vectorPart(pnu: string): number {
  const vector = PARTS.hash_test_vectors.find((entry) => entry.input === pnu);
  if (vector === undefined) throw new Error(`the contract has no hash vector for ${pnu}`);
  return vector.fnv1a32 % K;
}
const PART_A = `${DONG}-${vectorPart(PNU_A)}`;
const PART_B = `${DONG}-${vectorPart(PNU_B)}`;

const url = (pnu: string, query = "") => `https://buildings.example.test${GATEWAY.request_path.prefix}${pnu}${query}`;
const packKey = (unit: string, patch: number | null = null, generation = 1) =>
  `${PACKS.root}/${SECTION}/g${generation}/${patch === null ? "" : `p${patch}/`}${unit}${PACK_POLICY.suffix}`;

function partsIndex(overrides: Record<string, unknown> = {}, generation = 1): string {
  return JSON.stringify({
    schema_version: PARTS.index_schema_version,
    unit: "building-by-pnu",
    section: SECTION,
    generation,
    hash: PARTS.hash,
    parts: { [DONG]: K },
    ...overrides,
  });
}

const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

function sectionPacks(
  options: { index?: string; patches?: { patch: number; units: string[] }[]; parts?: unknown; schema?: number } = {},
) {
  const index = options.index ?? partsIndex();
  const parts =
    options.parts === undefined
      ? {
          key: partsKey({ name: SECTION, generation: 1 }),
          sha256: sha256(index),
          parted_units: Object.keys((JSON.parse(index) as { parts: object }).parts).length,
        }
      : options.parts;
  return {
    schema_version: options.schema ?? PACK_POLICY.manifest_section_packs_parted_schema_version,
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
      ...(parts === null ? {} : { parts }),
    })),
    patches: (options.patches ?? []).map((patch) => ({
      ...patch,
      gold_iceberg_snapshot_id: "999990000000000002",
      upserted: 1,
      deleted: 1,
    })),
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

describe("fnv1a32 (root ADR-0163 §5)", () => {
  it("answers every hash test vector of the contract", () => {
    expect(PARTS.hash).toBe("fnv1a32");
    expect(PARTS.hash_test_vectors.length).toBeGreaterThan(0);
    for (const vector of PARTS.hash_test_vectors) {
      expect(fnv1a32(vector.input), vector.input).toBe(vector.fnv1a32);
    }
  });

  it("places a PNU in its dong when the dong has one part, else in {dong}-{part}", () => {
    expect(partUnit(new Map(), PACK_POLICY.unit_prefix_length, PNU_A)).toBe(DONG);
    expect(partUnit(new Map([[DONG, 1]]), PACK_POLICY.unit_prefix_length, PNU_A)).toBe(DONG);
    expect(partUnit(new Map([[DONG, K]]), PACK_POLICY.unit_prefix_length, PNU_A)).toBe(PART_A);
    expect(partUnit(new Map([[DONG, K]]), PACK_POLICY.unit_prefix_length, PNU_B)).toBe(PART_B);
    expect(PART_A).not.toBe(PART_B);
  });
});

describe("section_packs schema 4: a section may name its parts index", () => {
  it("a version 3 block is read as before and names no parts", () => {
    const plan = parseSectionPacks(sectionPacks({ schema: PACK_POLICY.manifest_section_packs_schema_version, parts: null }), 64);
    expect(plan?.fingerprint).toBe("packs-g1-p0");
    expect(plan?.sections.every((section) => section.parts === undefined)).toBe(true);
  });

  it("a version 4 block carries its index's key, sha256 and parted dongs, and parted patch units", () => {
    const plan = parseSectionPacks(sectionPacks({ patches: [{ patch: 1, units: [PART_A, "9999900001"] }] }), 64);
    expect(plan?.sections[0]?.parts).toEqual({
      kind: "named",
      key: `${PACKS.root}/${SECTION}/g1/${PARTS.index_file_name}`,
      sha256: sha256(partsIndex()),
      partedUnits: 1,
    });
    expect([...(plan?.patches[0]?.units ?? [])]).toEqual([PART_A, "9999900001"]);
    expect(plan?.fingerprint).toBe("packs-g1-p1");
  });

  const goodParts = { key: partsKey({ name: SECTION, generation: 1 }), sha256: sha256(partsIndex()), parted_units: 1 };
  it.each([
    ["a version 3 block naming parts", sectionPacks({ schema: PACK_POLICY.manifest_section_packs_schema_version })],
    ["a version 4 block naming none", sectionPacks({ parts: null })],
    ["parts that are not an object", sectionPacks({ parts: "parts.json" })],
    ["an index key outside the generation", sectionPacks({ parts: { ...goodParts, key: partsKey({ name: SECTION, generation: 2 }) } })],
    ["a sha256 that is not lowercase hex", sectionPacks({ parts: { ...goodParts, sha256: goodParts.sha256.toUpperCase() } })],
    ["a short sha256", sectionPacks({ parts: { ...goodParts, sha256: "abc" } })],
    ["parted_units that is not a count", sectionPacks({ parts: { ...goodParts, parted_units: -1 } })],
    ["a missing parted_units", sectionPacks({ parts: { key: goodParts.key, sha256: goodParts.sha256 } })],
    ["a patch unit with an empty part", sectionPacks({ patches: [{ patch: 1, units: [`${DONG}-`] }] })],
    ["a patch unit with a five-digit part", sectionPacks({ patches: [{ patch: 1, units: [`${DONG}-10000`] }] })],
    ["a parted block of another unit length", { ...sectionPacks(), unit_prefix_length: 5 }],
  ])("refuses %s", (_label, block) => {
    expect(parseSectionPacks(block, 64)).toBeNull();
  });
});

describe("a parted section generation, served (root ADR-0163)", () => {
  let documents: { base: Record<string, string>; patched: Record<string, string> };
  let objects: Map<string, Uint8Array>;
  let asked: string[];
  let edge: Map<string, Response>;
  let pending: Promise<unknown>[];
  const ctx = {
    waitUntil: (promise: Promise<unknown>) => {
      pending.push(promise);
    },
    passThroughOnException: () => undefined,
  } as unknown as ExecutionContext;

  /// R2 as the Worker reads it: every key it was given, by its bytes, under one entity tag.
  const bucket = {
    async get(key: string) {
      asked.push(key);
      const bytes = objects.get(key);
      if (bytes === undefined) return null;
      return {
        key,
        size: bytes.byteLength,
        etag: "0123456789abcdef0123456789abcdef",
        httpEtag: '"0123456789abcdef0123456789abcdef"',
        body: new Blob([bytes]).stream(),
        arrayBuffer: async () => bytes.slice().buffer,
        text: async () => new TextDecoder().decode(bytes),
      };
    },
  } as unknown as Pick<R2Bucket, "get">;

  async function golden(name: string): Promise<Uint8Array> {
    return new Uint8Array(await readFile(new URL(name, fixtures)));
  }

  function put(key: string, value: Uint8Array | string): void {
    objects.set(key, typeof value === "string" ? new TextEncoder().encode(value) : value);
  }

  function serve(packs: unknown, index: string | null = partsIndex()): void {
    if (index !== null) put(partsKey({ name: SECTION, generation: 1 }), index);
    put(MANIFEST_KEY, manifest(packs));
  }

  async function get(pnu: string, query = "", bindings: Record<string, string> = {}): Promise<Response> {
    const response = await fetchByPnu(
      new Request(url(pnu, query)),
      { [R2_BINDING]: bucket, [GATEWAY.allowed_origins_binding]: "https://app.example.test", ...bindings },
      ctx,
    );
    await Promise.all(pending);
    return response;
  }

  const preview = { [PACKS.preview_binding]: "true" };

  beforeEach(async () => {
    documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as typeof documents;
    objects = new Map();
    asked = [];
    edge = new Map();
    pending = [];
    // A new isolate, with an empty edge cache.
    forgetPacks();
    vi.stubGlobal("caches", {
      default: {
        match: async (at: string | Request) => edge.get(typeof at === "string" ? at : at.url)?.clone(),
        put: async (at: string | Request, response: Response) => {
          edge.set(typeof at === "string" ? at : at.url, response);
        },
      },
    });
    // Only A's part holds a base pack: B's part, and the dong's unparted key, hold none.
    put(packKey(PART_A), await golden("g1-documents.pack"));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("answers a PNU from the pack of its part, and nothing from another part's", async () => {
    serve(sectionPacks());
    const answer = await get(PNU_A);
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_A]);
    // B's part has no pack: the golden pack under A's part is never asked for B.
    expect((await get(PNU_B)).status).toBe(404);
    expect(asked).toContain(packKey(PART_B));
    expect(asked).not.toContain(packKey(DONG));
  });

  it("an unparted dong of a parted generation is read under its dong", async () => {
    const empty = partsIndex({ parts: {} });
    serve(sectionPacks({ index: empty }), empty);
    put(packKey(DONG), await golden("g1-documents.pack"));
    const answer = await get(PNU_B);
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_B]);
  });

  it("reads the parts index once per isolate, and says so apart from the pack reads", async () => {
    serve(sectionPacks());
    const first = await get(PNU_A, "", preview);
    expect(first.status).toBe(200);
    const timing = first.headers.get("server-timing") ?? "";
    expect(timing).toMatch(new RegExp(`parts-${SECTION};dur=\\d+;desc="r2 gets=1"`));
    // The pack read is still one GET: the index is not a cost of the PNU's first read.
    expect(timing).toMatch(/r2;dur=\d+;desc="gets=1 retries=0"/);
    const second = await get(PNU_B, "", preview);
    expect(second.status).toBe(404);
    expect(second.headers.get("server-timing")).toContain(`parts-${SECTION};dur=0;desc="memory gets=0"`);
    expect(asked.filter((key) => key === partsKey({ name: SECTION, generation: 1 }))).toHaveLength(1);
  });

  it.each([
    ["bytes that do not hash to the manifest's sha256", partsIndex(), sha256(`${partsIndex()} `), 1],
    ["an index of another generation", partsIndex({}, 2), null, 1],
    ["an index of another section", partsIndex({ section: "other" }), null, 1],
    ["an index of another lane", partsIndex({ unit: "parcel-by-pnu" }), null, 1],
    ["an index of another schema", partsIndex({ schema_version: "foundation-platform.by_pnu_pack_parts.v0" }), null, 1],
    ["an index of another hash", partsIndex({ hash: "xxhash32" }), null, 1],
    ["a count below 2", partsIndex({ parts: { [DONG]: 1 } }), null, 1],
    ["a count that is not an integer", partsIndex({ parts: { [DONG]: 2.5 } }), null, 1],
    ["a count past the unit pattern", partsIndex({ parts: { [DONG]: 10001 } }), null, 1],
    ["a key that is not a dong", partsIndex({ parts: { "99999": 2 } }), null, 1],
    ["another number of dongs than the manifest names", partsIndex(), null, 2],
    ["bytes that are not JSON", "{", null, 1],
  ])("an index with %s is an outage", async (_label, index, digest, partedUnits) => {
    serve(
      sectionPacks({
        index,
        parts: { key: partsKey({ name: SECTION, generation: 1 }), sha256: digest ?? sha256(index), parted_units: partedUnits },
      }),
      index,
    );
    const response = await get(PNU_A, "", preview);
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("server-timing")).toContain('outcome;desc="pack-inconsistent"');
  });

  it("a named index that is missing is an outage, not an unparted generation", async () => {
    put(packKey(DONG), await golden("g1-documents.pack"));
    serve(sectionPacks(), null);
    expect((await get(PNU_A)).status).toBe(503);
  });

  it("a patch over a parted generation is read under the same part", async () => {
    put(packKey(PART_A, 1), await golden("g1-p1-documents.pack"));
    put(packKey(PART_B, 1), await golden("g1-p1-documents.pack"));
    serve(sectionPacks({ patches: [{ patch: 1, units: [PART_A, PART_B] }] }));
    expect(await (await get(PNU_A)).text()).toBe(documents.patched[PNU_A]);
    const deleted = await get(PNU_B);
    expect(deleted.status).toBe(404);
    expect(await deleted.json()).toEqual({ error: "deleted", pnu: PNU_B });
  });

  it("a patch naming only another part leaves a PNU to its base", async () => {
    put(packKey(PART_B, 1), await golden("g1-p1-documents.pack"));
    serve(sectionPacks({ patches: [{ patch: 1, units: [PART_B] }] }));
    expect(await (await get(PNU_A)).text()).toBe(documents.base[PNU_A]);
    expect(asked).not.toContain(packKey(PART_B, 1));
  });

  it("a patch naming a parted dong unparted is an outage", async () => {
    put(packKey(DONG, 1), await golden("g1-p1-documents.pack"));
    serve(sectionPacks({ patches: [{ patch: 1, units: [DONG] }] }));
    expect((await get(PNU_A)).status).toBe(503);
  });

  it("a preview reads a parted generation by the index at its conventional key", async () => {
    // No manifest block names the generation: the preview finds its index by convention.
    serve(undefined);
    const answer = await get(PNU_A, "?packs=g1", preview);
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_A]);
    expect(answer.headers.get("server-timing")).toContain('desc="r2 gets=1"');
    expect((await get(PNU_B, "?packs=g1", preview)).status).toBe(404);
  });

  it("a preview of a generation with no index reads it unparted", async () => {
    put(packKey(DONG), await golden("g1-documents.pack"));
    serve(undefined, null);
    const answer = await get(PNU_B, "?packs=g1", preview);
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_B]);
    expect(answer.headers.get("server-timing")).toContain('desc="absent gets=1"');
  });

  it("the gateway says it reads the parted block before any is published", async () => {
    const response = await fetchByPnu(
      new Request(`https://buildings.example.test${GATEWAY.request_path.capabilities}`),
      { [R2_BINDING]: bucket, [GATEWAY.allowed_origins_binding]: "https://app.example.test" },
      ctx,
    );
    const body = (await response.json()) as { manifest_schema_versions: number[] };
    expect(body.manifest_schema_versions).toContain(PACK_POLICY.manifest_section_packs_parted_schema_version);
  });
});
