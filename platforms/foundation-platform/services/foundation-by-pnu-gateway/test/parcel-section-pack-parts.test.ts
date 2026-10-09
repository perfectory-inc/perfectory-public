import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { fetchByPnu } from "../src/index";
import { fnv1a32, forgetPacks, partsKey } from "../src/packs";

// The parcel lane reads parts with the same code as the building lane (root ADR-0160, ADR-0163);
// these hold it to the parcel block: its pack root and its manifest unit, in the index too. The
// pack bytes are the publisher's golden packs (section-packs.test.ts). PNUs sit in the
// repository-reserved synthetic namespace.
const PNU_A = "9999900000100000000";
const DONG = "9999900000";
const GATEWAY = connectionContract.parcel_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const PARTS = PACK_POLICY.parts;
const SECTION = PACKS.anchor_section;
const K = 3;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);

function partsIndex(unit: string): string {
  return JSON.stringify({
    schema_version: PARTS.index_schema_version,
    unit,
    section: SECTION,
    generation: 1,
    hash: PARTS.hash,
    parts: { [DONG]: K },
  });
}

function manifest(index: string): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "parcel-by-pnu",
    base_generation: 7,
    base_object_count: 1,
    pnu_prefix_length: 5,
    patches: [],
    object_count: 1,
    section_packs: {
      schema_version: PACK_POLICY.manifest_section_packs_parted_schema_version,
      format_version: PACK_POLICY.format_version,
      unit_prefix_length: PACK_POLICY.unit_prefix_length,
      sections: PACKS.sections.map((name) => ({
        name,
        generation: 1,
        patch_floor: 0,
        parts: {
          key: partsKey({ name, generation: 1 }),
          sha256: createHash("sha256").update(index).digest("hex"),
          parted_units: 1,
        },
      })),
      patches: [],
    },
  })}\n`;
}

describe("foundation parcel gateway parts (root ADR-0163, ADR-0160)", () => {
  let objects: Map<string, Uint8Array>;
  let documents: { base: Record<string, string> };
  const ctx = { waitUntil: () => undefined, passThroughOnException: () => undefined } as unknown as ExecutionContext;
  const bucket = {
    async get(key: string) {
      const bytes = objects.get(key);
      if (bytes === undefined) return null;
      return {
        size: bytes.byteLength,
        etag: "0123456789abcdef0123456789abcdef",
        body: new Blob([bytes]).stream(),
        arrayBuffer: async () => bytes.slice().buffer,
        text: async () => new TextDecoder().decode(bytes),
      };
    },
  } as unknown as Pick<R2Bucket, "get">;

  function serve(index: string): void {
    const encode = (text: string) => new TextEncoder().encode(text);
    objects.set(partsKey({ name: SECTION, generation: 1 }), encode(index));
    objects.set(GATEWAY.object_key.manifest_object, encode(manifest(index)));
  }

  const get = () =>
    fetchByPnu(
      new Request(`https://catalog.example.test${GATEWAY.request_path.prefix}${PNU_A}`),
      { [GATEWAY.r2_binding]: bucket, [GATEWAY.allowed_origins_binding]: "https://app.example.test" },
      ctx,
    );

  beforeEach(async () => {
    forgetPacks();
    vi.stubGlobal("caches", { default: { match: async () => undefined, put: async () => undefined } });
    documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as typeof documents;
    const part = `${DONG}-${fnv1a32(PNU_A) % K}`;
    objects = new Map([
      [`${PACKS.root}/${SECTION}/g1/${part}${PACK_POLICY.suffix}`, new Uint8Array(await readFile(new URL("g1-documents.pack", fixtures)))],
    ]);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reads a parted generation under the parcel pack root", async () => {
    expect(partsKey({ name: SECTION, generation: 1 }).startsWith(`${PACKS.root}/`)).toBe(true);
    serve(partsIndex("parcel-by-pnu"));
    const answer = await get();
    expect(answer.status).toBe(200);
    expect(await answer.text()).toBe(documents.base[PNU_A]);
  });

  it("refuses the building lane's index", async () => {
    serve(partsIndex("building-by-pnu"));
    expect((await get()).status).toBe(503);
  });
});
