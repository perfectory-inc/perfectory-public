import { readFile } from "node:fs/promises";
import { gunzipSync, gzipSync } from "node:zlib";

import { beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { canonicalRequest } from "../src/index";
import {
  PackFormatError,
  findEntry,
  forgetPacks,
  packKey,
  packReads,
  parseHead,
  previewPlan,
  resolvePacks,
  type PackPlan,
} from "../src/packs";

// Root ADR-0325: a document past what one request may copy is served as pages. Page 1 is the
// entry's own member and answers the PNU's path; page n follows it in the body, found by the pack
// header's pages table, and answers `<pnu>/<n>`. The 2026-10-28 stop it ends: one building
// document of 268,404 compressed bytes refused the whole building lane. PNUs sit in the
// repository-reserved synthetic namespace (scripts/guard/public-fixture-safety.py).
const UNIT = "9999900000";
const PAGED = "9999900000100010000";
const WHOLE = "9999900000100000000";
const LATER = "9999900000200000000";
const POLICY = connectionContract.by_pnu_section_packs;
const PAGES = POLICY.read_path.request_cpu_budget.pages;
const GATEWAY = connectionContract.building_by_pnu_gateway;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);

interface Written {
  key: string;
  pages: string[];
  /// A pages table to write instead of the true one (a corrupt pack).
  table?: number[];
}

/// A format 1 pack as the publisher's `PackWriter` lays it out: prefix, header, index, then each
/// key's members back to back; a key of more than one page is named in the header's pages table.
function writePack(entries: Written[], patch: number | null = null): Uint8Array {
  const encoder = new TextEncoder();
  const members = entries.map((entry) => entry.pages.map((page) => new Uint8Array(gzipSync(encoder.encode(page)))));
  const index = new Uint8Array(entries.length * 28);
  const view = new DataView(index.buffer);
  const pages: Record<string, number[]> = {};
  let offset = 0;
  for (const [i, entry] of entries.entries()) {
    const own = members[i] ?? [];
    index.set(encoder.encode(entry.key), i * 28);
    view.setUint32(i * 28 + 19, offset, true);
    view.setUint32(i * 28 + 23, own[0]?.length ?? 0, true);
    index[i * 28 + 27] = 1;
    if (own.length > 1 || entry.table !== undefined) pages[entry.key] = entry.table ?? own.map((member) => member.length);
    offset += own.reduce((total, member) => total + member.length, 0);
  }
  const header = encoder.encode(
    JSON.stringify({
      format_version: POLICY.format_version,
      lane: "building-by-pnu",
      section: "documents",
      generation: 1,
      patch,
      unit: UNIT,
      gold_table: "gold.building_panel",
      gold_iceberg_snapshot_id: "999990000000000001",
      compression: "gzip",
      entry_count: entries.length,
      document_count: entries.length,
      tombstone_count: 0,
      index_length: index.length,
      body_length: offset,
      body_sha256: "0".repeat(64),
      ...(Object.keys(pages).length === 0 ? {} : { pages }),
    }),
  );
  const pack = new Uint8Array(20 + header.length + index.length + offset);
  pack.set(encoder.encode(POLICY.magic), 0);
  const prefix = new DataView(pack.buffer);
  prefix.setUint16(8, POLICY.format_version, true);
  prefix.setUint32(12, header.length, true);
  prefix.setUint32(16, index.length, true);
  pack.set(header, 20);
  pack.set(index, 20 + header.length);
  let at = 20 + header.length + index.length;
  for (const member of members.flat()) {
    pack.set(member, at);
    at += member.length;
  }
  return pack;
}

/// R2 as the read path uses it: a head read by `Range` header, a member by offset and length.
function bucketOf(objects: Record<string, Uint8Array>) {
  return {
    async get(key: string, options?: { range?: Headers | { offset?: number; length?: number } }) {
      const bytes = objects[key];
      if (bytes === undefined) return null;
      let slice = bytes;
      const range = options?.range;
      if (range instanceof Headers) {
        const [, from = "0", to = "0"] = (range.get("Range") ?? "").match(/^bytes=(\d+)-(\d+)$/) ?? [];
        slice = bytes.slice(Number(from), Math.min(Number(to) + 1, bytes.length));
      } else if (range !== undefined) {
        const start = range.offset ?? 0;
        const length = range.length ?? bytes.length - start;
        if (start + length > bytes.length) throw new Error(`range ${start}+${length} is past ${bytes.length}`);
        slice = bytes.slice(start, start + length);
      }
      return { etag: "0123456789abcdef", size: bytes.length, body: new Blob([slice]).stream(), arrayBuffer: async () => slice.buffer };
    },
  } as unknown as Pick<R2Bucket, "get">;
}

const ctx = { waitUntil: () => undefined, passThroughOnException: () => undefined } as unknown as ExecutionContext;

function plan(patches: number[] = []): PackPlan {
  const preview = previewPlan(1, null);
  return {
    ...preview,
    sections: preview.sections.map(({ name, generation, patchFloor }) => ({ name, generation, patchFloor })),
    patches: patches.map((patch) => ({ patch, units: new Set([UNIT]) })),
  };
}

async function answer(objects: Record<string, Uint8Array>, pnu: string, page: number, patches: number[] = []): Promise<string | number> {
  const resolved = await resolvePacks(packReads(bucketOf(objects), ctx), plan(patches), pnu, page);
  return resolved.kind === "document" ? gunzipSync(resolved.member).toString("utf8") : resolved.kind === "tombstone" ? 410 : 404;
}

const base = packKey({ name: "documents", generation: 1 }, null, UNIT);
const patchOne = packKey({ name: "documents", generation: 1 }, 1, UNIT);

describe("a document past one request is served as pages (root ADR-0325)", () => {
  beforeEach(() => forgetPacks());

  it("answers page 1 on the PNU's own path and page n from the pages table, one member each", async () => {
    const objects = {
      [base]: writePack([
        { key: WHOLE, pages: ['{"whole":true}'] },
        { key: PAGED, pages: ['{"page":1}', '{"page":2}', '{"page":3}'] },
        { key: LATER, pages: ['{"later":true}'] },
      ]),
    };
    expect(await answer(objects, PAGED, 1)).toBe('{"page":1}');
    expect(await answer(objects, PAGED, 2)).toBe('{"page":2}');
    expect(await answer(objects, PAGED, 3)).toBe('{"page":3}');
    expect(await answer(objects, PAGED, 4)).toBe(404);
    // The entries beside a paged one read as before, and a whole document has no page 2.
    expect(await answer(objects, WHOLE, 1)).toBe('{"whole":true}');
    expect(await answer(objects, LATER, 1)).toBe('{"later":true}');
    expect(await answer(objects, WHOLE, 2)).toBe(404);
  });

  it("takes every page from the pack that holds the entry, so pages of two bakes never mix", async () => {
    const objects = {
      [base]: writePack([{ key: PAGED, pages: ['{"old":1}', '{"old":2}'] }]),
      // The newer patch holds the document whole: its old page 2 is no longer an answer.
      [patchOne]: writePack([{ key: PAGED, pages: ['{"new":"whole"}'] }], 1),
    };
    expect(await answer(objects, PAGED, 1, [1])).toBe('{"new":"whole"}');
    expect(await answer(objects, PAGED, 2, [1])).toBe(404);
  });

  it("refuses a pages table that does not describe the entry's members", async () => {
    const disagrees = { [base]: writePack([{ key: PAGED, pages: ['{"page":1}', '{"page":2}'], table: [5, 10] }]) };
    await expect(answer(disagrees, PAGED, 2)).rejects.toBeInstanceOf(PackFormatError);
    const past = writePack([{ key: PAGED, pages: ['{"page":1}', '{"page":2}'] }]);
    const head = parseHead(past, "tag");
    const lengths = head?.pages.get(PAGED) ?? [];
    const overlong = { [base]: writePack([{ key: PAGED, pages: ['{"page":1}', '{"page":2}'], table: [lengths[0] ?? 0, 4096] }]) };
    await expect(answer(overlong, PAGED, 2)).rejects.toBeInstanceOf(PackFormatError);
    for (const table of [[1], Array.from({ length: PAGES.max_pages + 1 }, () => 1), [1, 0], [1, POLICY.read_path.request_cpu_budget.document_max_bytes + 1]]) {
      expect(() => parseHead(writePack([{ key: PAGED, pages: ['{"page":1}'], table }]), "tag"), JSON.stringify(table.length)).toThrow(PackFormatError);
    }
  });

  it("names page n as <pnu>/<n> for 2 to max_pages, one path a page", () => {
    const at = (path: string) => canonicalRequest(new URL(`https://buildings.example.test${GATEWAY.request_path.prefix}${path}`));
    expect(at(PAGED)).toMatchObject({ pnu: PAGED, page: 1 });
    expect(at(`${PAGED}/2`)).toMatchObject({ pnu: PAGED, page: 2 });
    expect(at(`${PAGED}/${PAGES.max_pages}`)).toMatchObject({ pnu: PAGED, page: PAGES.max_pages });
    for (const refused of [`${PAGED}/1`, `${PAGED}/02`, `${PAGED}/0`, `${PAGED}/${PAGES.max_pages + 1}`, `${PAGED}/`, `${PAGED}/2/3`, `${PAGED}/x`]) {
      expect(at(refused), refused).toBeNull();
    }
  });

  it("reads the publisher's paged golden pack (the Rust writer's bytes)", async () => {
    const golden = new Uint8Array(await readFile(new URL("format-paged.golden.pack", fixtures)));
    const head = parseHead(golden, "tag");
    if (head === null) throw new Error("the golden pack has no whole head");
    expect(head.pages.get(PAGED)?.length).toBe(3);
    expect(findEntry(head, PAGED)?.length).toBe(head.pages.get(PAGED)?.[0]);
    const objects = { [base]: golden };
    for (const page of [1, 2, 3]) {
      expect(await answer(objects, PAGED, page)).toBe(`{"page":${page}}`);
    }
    expect(await answer(objects, WHOLE, 1)).toBe('{"page":"whole a"}');
    expect(await answer(objects, LATER, 1)).toBe('{"page":"whole c"}');
  });
});
