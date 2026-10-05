import { readFile } from "node:fs/promises";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { fetchBuilding } from "../src/index";
import {
  answerCacheUrl,
  PackFormatError,
  PackReadUnavailable,
  ReadTrace,
  forgetPacks,
  packCacheResponse,
  packCacheUrl,
  packFromCache,
  packKey,
  packReads,
  parseHead,
  previewPlan,
  readPack,
  resolvePacks,
  unquotedEtag,
  type PackReads,
} from "../src/packs";

// The read path of root ADR-0147's Revision: a pack within the contract's whole-pack bound is read
// whole by the GET that finds its head (one R2 hop), a larger one as head + range; copies live in
// isolate memory and the edge cache; R2 is retried within a deadline. The golden packs are the
// publisher's (see section-packs.test.ts); PNUs sit in the synthetic 99999 namespace.
const PNU_A = "9999900000100000000";
const UNIT = "9999900000";
const ETAG = "0123456789abcdef0123456789abcdef";
const GATEWAY = connectionContract.building_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const READ = connectionContract.by_pnu_section_packs.read_path;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const key = (section: string) => packKey({ name: section, generation: 1 }, null, UNIT);

interface Asked {
  key: string;
  range: { offset?: number; length?: number } | undefined;
  etagMatches: string | undefined;
}

interface BucketOptions {
  /// The size R2 reports for every pack; a test makes small fixtures look large with it.
  reportedSize?: number | undefined;
  /// The entity tag R2 answers with (a pack rewritten under its key, which create-only forbids).
  etag?: string;
  /// Thrown by the first `failures` calls, then the bucket answers.
  failures?: number;
  failure?: () => Error;
  hang?: boolean;
}

/// R2 as a stricter bucket than the real one: a range past the object's end is an error (so no
/// read may rely on R2 clamping it), a conditional read under another tag returns no body (as R2
/// does), and an unranged body streams in 7-byte chunks, counting what was pulled and cancelled.
function strictBucket(objects: Record<string, Uint8Array>, options: BucketOptions = {}) {
  const asked: Asked[] = [];
  const streamed = { pulled: 0, cancelled: 0 };
  let failed = 0;
  const etag = options.etag ?? ETAG;
  const bucket = {
    async get(
      wanted: string,
      opts?: { range?: { offset?: number; length?: number }; onlyIf?: { etagMatches?: string } },
    ) {
      asked.push({ key: wanted, range: opts?.range, etagMatches: opts?.onlyIf?.etagMatches });
      if (options.hang === true) return new Promise(() => undefined);
      if (failed < (options.failures ?? 0)) {
        failed += 1;
        throw (options.failure ?? (() => new Error("get: We encountered an internal error. Please try again. (10001)")))();
      }
      const bytes = objects[wanted];
      if (bytes === undefined) return null;
      const size = options.reportedSize ?? bytes.length;
      const condition = opts?.onlyIf?.etagMatches;
      if (condition !== undefined && condition !== etag) return { etag, size };
      const range = opts?.range;
      if (range !== undefined) {
        const offset = range.offset ?? 0;
        const length = range.length ?? bytes.length - offset;
        if (offset + length > bytes.length) throw new Error(`range ${offset}+${length} is past ${bytes.length}`);
        const slice = bytes.slice(offset, offset + length);
        return { etag, size, body: new Blob([slice]).stream(), arrayBuffer: async () => slice.buffer };
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
      return { etag, size, body, arrayBuffer: async () => bytes.slice().buffer };
    },
  };
  return { bucket: bucket as unknown as Pick<R2Bucket, "get">, asked, streamed };
}

async function goldenPacks(): Promise<Record<string, Uint8Array>> {
  const objects: Record<string, Uint8Array> = {};
  for (const section of PACKS.sections) {
    objects[key(section)] = new Uint8Array(await readFile(new URL(`g1-${section}.pack`, fixtures)));
  }
  return objects;
}

let pending: Promise<unknown>[];
let edge: Map<string, Response>;
let edgeAsked: string[];
const ctx = {
  waitUntil: (promise: Promise<unknown>) => {
    pending.push(promise);
  },
  passThroughOnException: () => undefined,
} as unknown as ExecutionContext;

function reads(bucket: Pick<R2Bucket, "get">, overrides: Partial<PackReads> = {}): PackReads {
  return { ...packReads(bucket, ctx), sleep: async () => undefined, random: () => 0.5, ...overrides };
}

beforeEach(() => {
  forgetPacks();
  pending = [];
  edge = new Map();
  edgeAsked = [];
  // The Cache API, as the Worker sees it: what was put is matched, header for header.
  vi.stubGlobal("caches", {
    default: {
      match: async (url: string | Request) => {
        const at = typeof url === "string" ? url : url.url;
        edgeAsked.push(at);
        return edge.get(at)?.clone();
      },
      put: async (url: string | Request, response: Response) => {
        edge.set(typeof url === "string" ? url : url.url, response);
      },
    },
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("one-hop small packs and head + range large packs", () => {
  it("a pack within the bound is read whole by one GET and answers from those bytes", async () => {
    expect(READ.whole_pack_max_bytes).toBeGreaterThan(0);
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects);
    const resolved = await resolvePacks(reads(bucket), previewPlan(1), PNU_A);
    expect(resolved.kind).toBe("document");
    // One unranged GET per section, no range reads.
    expect(asked).toHaveLength(PACKS.sections.length);
    expect(asked.every((read) => read.range === undefined)).toBe(true);
  });

  it("whole-pack and head + range answer the same bytes (golden documents unchanged)", async () => {
    const objects = await goldenPacks();
    const documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as {
      base: Record<string, string>;
    };
    const small = strictBucket(objects);
    const whole = await resolvePacks(reads(small.bucket), previewPlan(1), PNU_A);
    forgetPacks();
    edge.clear();
    const large = strictBucket(objects, { reportedSize: READ.whole_pack_max_bytes + 1 });
    const ranged = await resolvePacks(reads(large.bucket), previewPlan(1), PNU_A);
    // The large path read each head, cancelled the rest, and read each document by a range that
    // R2 answers only under the head's entity tag.
    expect(large.asked.filter((read) => read.range === undefined)).toHaveLength(PACKS.sections.length);
    const ranges = large.asked.filter((read) => read.range !== undefined);
    expect(ranges).toHaveLength(PACKS.sections.length);
    expect(ranges.every((read) => read.etagMatches === ETAG)).toBe(true);
    expect(large.streamed.cancelled).toBe(PACKS.sections.length);
    if (whole.kind !== "document" || ranged.kind !== "document") throw new Error("no document");
    // The same member bytes and the same entity tag, decoding to the object lane's document.
    expect([...whole.member]).toEqual([...ranged.member]);
    expect(whole.etag).toBe(ranged.etag);
    const decoded = await new Response(
      new Blob([whole.member]).stream().pipeThrough(new DecompressionStream("gzip")),
    ).text();
    expect(decoded).toBe(documents.base[PNU_A]);
  });

  it("a whole pack whose length disagrees with its header is a format error", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const whole = objects[name];
    if (whole === undefined) throw new Error("no fixture");
    const { bucket } = strictBucket({ [name]: whole.slice(0, whole.length - 1) });
    await expect(readPack(reads(bucket), PACKS.anchor_section, name)).rejects.toBeInstanceOf(PackFormatError);
  });

  it("a truncated large pack is a format error, not a guess", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const whole = objects[name];
    if (whole === undefined) throw new Error("no fixture");
    const { bucket } = strictBucket({ [name]: whole.slice(0, 30) }, { reportedSize: READ.whole_pack_max_bytes + 1 });
    await expect(readPack(reads(bucket), PACKS.anchor_section, name)).rejects.toThrow("shorter than declared");
  });
});

describe("copies: isolate memory, then the edge cache, entity tags held", () => {
  it("never reads a pack twice within an isolate", async () => {
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects);
    const plan = previewPlan(1);
    await resolvePacks(reads(bucket), plan, PNU_A);
    const r2 = asked.length;
    const edgeLookups = edgeAsked.length;
    const trace = new ReadTrace();
    await resolvePacks(reads(bucket, { trace }), plan, PNU_A);
    expect(asked).toHaveLength(r2);
    expect(edgeAsked).toHaveLength(edgeLookups);
    expect([...trace.sections.values()].every((source) => source === "memory-whole")).toBe(true);
  });

  it("a new isolate reads the edge copy, whole or head, with the pack's own entity tag", async () => {
    const objects = await goldenPacks();
    for (const reportedSize of [undefined, READ.whole_pack_max_bytes + 1]) {
      forgetPacks();
      edge.clear();
      const { bucket, asked } = strictBucket(objects, { reportedSize });
      const first = await resolvePacks(reads(bucket), previewPlan(1), PNU_A);
      await Promise.all(pending);
      const cached = edge.get(packCacheUrl(key(PACKS.anchor_section)));
      expect(cached?.headers.get("ETag")).toBe(`"${ETAG}"`);
      expect(cached?.headers.get("Cache-Control")).toBe(connectionContract.by_pnu_section_packs.cache_control);
      forgetPacks();
      const before = asked.filter((read) => read.range === undefined).length;
      const trace = new ReadTrace();
      const second = await resolvePacks(reads(bucket, { trace }), previewPlan(1), PNU_A);
      expect(second).toEqual(first);
      // No pack GET again: only a large pack's documents still need their range.
      expect(asked.filter((read) => read.range === undefined)).toHaveLength(before);
      expect([...trace.sections.values()].every((source) => source.startsWith("edge-"))).toBe(true);
    }
    expect(unquotedEtag(`W/"${ETAG}"`)).toBe(ETAG);
    expect(unquotedEtag("")).toBeNull();
  });

  it("a head copy is never combined with bytes R2 holds under another entity tag", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const whole = objects[name];
    const head = whole === undefined ? null : parseHead(whole, "an-older-tag");
    if (head === null) throw new Error("no fixture head");
    edge.set(packCacheUrl(name), packCacheResponse({ head, bytes: null }));
    const { bucket, asked } = strictBucket(objects, { reportedSize: READ.whole_pack_max_bytes + 1 });
    await expect(resolvePacks(reads(bucket), previewPlan(1), PNU_A)).rejects.toBeInstanceOf(PackFormatError);
    // Asked under the copy's tag, refused, and not retried: retrying cannot make it right.
    const ranged = asked.filter((read) => read.key === name && read.range !== undefined);
    expect(ranged).toEqual([expect.objectContaining({ etagMatches: "an-older-tag" })]);
  });

  it("an edge copy without an entity tag, or of a form this Worker does not write, is read again", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const whole = objects[name];
    if (whole === undefined) throw new Error("no fixture");
    const parsed = parseHead(whole, ETAG);
    if (parsed === null) throw new Error("no fixture head");
    const tagless = packCacheResponse({ head: parsed, bytes: whole });
    tagless.headers.delete("ETag");
    edge.set(packCacheUrl(name), tagless);
    expect(await packFromCache(name)).toBeNull();
    const formless = packCacheResponse({ head: parsed, bytes: whole });
    formless.headers.delete("X-Pack-Form");
    edge.set(packCacheUrl(name), formless);
    expect(await packFromCache(name)).toBeNull();
    const { bucket, asked } = strictBucket(objects);
    const copy = await readPack(reads(bucket), PACKS.anchor_section, name);
    expect(copy?.head.etag).toBe(ETAG);
    expect(asked).toHaveLength(1);
  });
});

describe("R2 retries within a deadline", () => {
  it("a transient R2 error is retried with jitter and the read recovers", async () => {
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects, { failures: 2 });
    const slept: number[] = [];
    const trace = new ReadTrace();
    const resolved = await resolvePacks(
      reads(bucket, { trace, sleep: async (ms) => void slept.push(ms) }),
      previewPlan(1),
      PNU_A,
    );
    expect(resolved.kind).toBe("document");
    expect(trace.retries).toBe(2);
    expect(trace.failures).toHaveLength(2);
    expect(trace.failures.every((failure) => failure.endsWith("r2:10001"))).toBe(true);
    expect(asked).toHaveLength(PACKS.sections.length + 2);
    // Full jitter: the planted random 0.5 of an exponentially growing cap.
    expect(slept.every((ms) => ms <= READ.r2_retry_base_ms * 2 ** (READ.r2_attempts - 1))).toBe(true);
  });

  it("a persistent R2 error ends as unavailable after the bounded attempts", async () => {
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects, { failures: Number.POSITIVE_INFINITY });
    await expect(resolvePacks(reads(bucket), previewPlan(1), PNU_A)).rejects.toBeInstanceOf(PackReadUnavailable);
    expect(asked.length).toBeLessThanOrEqual(PACKS.sections.length * READ.r2_attempts);
  });

  it("a read that never answers ends at the deadline, not after it", async () => {
    const objects = await goldenPacks();
    const { bucket } = strictBucket(objects, { hang: true });
    const started = Date.now();
    await expect(
      readPack(reads(bucket, { deadline: Date.now() + 150 }), PACKS.anchor_section, key(PACKS.anchor_section)),
    ).rejects.toBeInstanceOf(PackReadUnavailable);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it("the gateway answers a persistent R2 failure with 503 within the contract deadline, and says why", async () => {
    const policy = GATEWAY;
    const objects = await goldenPacks();
    const { bucket: packs } = strictBucket(objects);
    const bucket = {
      get: vi.fn(async (wanted: string, options?: unknown) => {
        if (wanted === policy.object_key.manifest_object) {
          return { body: null, text: async () => JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 1 }) };
        }
        if (wanted === key(PACKS.anchor_section)) throw new Error("get: We encountered an internal error. Please try again. (10001)");
        return packs.get(wanted, options as R2GetOptions);
      }),
    };
    const started = Date.now();
    const response = await fetchBuilding(
      new Request(`https://buildings.example.test${policy.request_path.prefix}${PNU_A}?packs=g1`),
      {
        [policy.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">,
        [policy.allowed_origins_binding]: "https://app.example.test",
        [PACKS.preview_binding]: "true",
      },
      ctx,
    );
    expect(response.status).toBe(503);
    expect(Date.now() - started).toBeLessThan(READ.r2_deadline_ms);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("server-timing")).toContain('outcome;desc="r2-unavailable"');
    expect(bucket.get.mock.calls.filter(([wanted]) => wanted === key(PACKS.anchor_section))).toHaveLength(READ.r2_attempts);
  });

  it("a PNU read before under the same served state answers from its edge copy, with no pack read", async () => {
    const policy = GATEWAY;
    const objects = await goldenPacks();
    const { bucket: packs, asked } = strictBucket(objects);
    const bucket = {
      get: async (wanted: string, options?: unknown) =>
        wanted === policy.object_key.manifest_object
          ? { body: null, text: async () => JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 1 }) }
          : packs.get(wanted, options as R2GetOptions),
    };
    const env = {
      [policy.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">,
      [policy.allowed_origins_binding]: "https://app.example.test",
      [PACKS.preview_binding]: "true",
    };
    const url = `https://buildings.example.test${policy.request_path.prefix}${PNU_A}?packs=g1`;
    const get = () => fetchBuilding(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env, ctx);
    const cold = await get();
    const coldBody = new Uint8Array(await cold.arrayBuffer());
    expect(cold.headers.get("server-timing")).toContain('desc="r2-whole"');
    await Promise.all(pending);
    // The copy sits under the plan's fingerprint: another generation names another entry.
    expect(edge.has(answerCacheUrl(previewPlan(1).fingerprint, PNU_A))).toBe(true);
    expect(answerCacheUrl(previewPlan(2).fingerprint, PNU_A)).not.toBe(answerCacheUrl(previewPlan(1).fingerprint, PNU_A));
    // A new isolate, the pack copies gone from it and from the edge: only the answer copy is left.
    forgetPacks();
    edge.delete(packCacheUrl(key(PACKS.anchor_section)));
    const before = asked.length;
    const warm = await get();
    expect(warm.status).toBe(200);
    expect(asked).toHaveLength(before);
    const timing = warm.headers.get("server-timing") ?? "";
    expect(timing).toContain('desc="edge-answer"');
    expect(timing).toContain('desc="gets=0 retries=0"');
    expect(warm.headers.get("etag")).toBe(cold.headers.get("etag"));
    expect(warm.headers.get("content-encoding")).toBe("gzip");
    expect(new Uint8Array(await warm.arrayBuffer())).toEqual(coldBody);
  });

  it("a preview answer carries Server-Timing; the live route's answer does not", async () => {
    const policy = GATEWAY;
    const objects = await goldenPacks();
    const { bucket: packs } = strictBucket(objects);
    const bucket = {
      get: async (wanted: string, options?: unknown) =>
        wanted === policy.object_key.manifest_object
          ? { body: null, text: async () => JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 1 }) }
          : packs.get(wanted, options as R2GetOptions),
    };
    const env = (preview: boolean) => ({
      [policy.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">,
      [policy.allowed_origins_binding]: "https://app.example.test",
      ...(preview ? { [PACKS.preview_binding]: "true" } : {}),
    });
    const url = `https://buildings.example.test${policy.request_path.prefix}${PNU_A}?packs=g1`;
    const preview = await fetchBuilding(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env(true), ctx);
    expect(preview.headers.get("content-encoding")).toBe("gzip");
    expect(preview.status).toBe(200);
    const timing = preview.headers.get("server-timing") ?? "";
    expect(timing).toContain('outcome;desc="document"');
    expect(timing).toMatch(/r2;dur=\d+;desc="gets=1 retries=0"/);
    for (const section of PACKS.sections) expect(timing).toContain(`pack-${section};dur=`);
    expect(timing).toMatch(/total;dur=\d+/);
    const live = await fetchBuilding(new Request(url), env(false), ctx);
    expect(live.status).toBe(404);
    expect(live.headers.get("server-timing")).toBeNull();
  });
});
