import { readFile } from "node:fs/promises";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { fetchByPnu, forgetPlans, warmRequestPath } from "../src/index";
import {
  answerCacheUrl,
  PackFormatError,
  PackReadUnavailable,
  ReadTrace,
  forgetPacks,
  packKey,
  packReads,
  previewPlan,
  readPack,
  resolvePacks,
  type PackPlan,
  type PackReads,
} from "../src/packs";

// The read path of root ADR-0233: every pack, whatever its size, is read by its head (kept in
// isolate memory) and then the one document by range; the answer of a PNU lives in the edge cache
// (ADR-0154); R2 is retried within a deadline. The golden packs are the
// publisher's (see section-packs.test.ts); PNUs sit in the synthetic 99999 namespace.
const PNU_A = "9999900000100000000";
const UNIT = "9999900000";
const ETAG = "0123456789abcdef0123456789abcdef";
const GATEWAY = connectionContract.building_by_pnu_gateway;
const PACKS = GATEWAY.section_packs;
const READ = connectionContract.by_pnu_section_packs.read_path;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);
const key = (section: string) => packKey({ name: section, generation: 1 }, null, UNIT);
/// Generation 1 as a manifest without parts names it: these tests count the pack reads, and a
/// preview's parts index probe (root ADR-0163, section-pack-parts.test.ts) is a read of its own.
function unpartedPlan(): PackPlan {
  const plan = previewPlan(1, null);
  return { ...plan, sections: plan.sections.map(({ name, generation, patchFloor }) => ({ name, generation, patchFloor })) };
}

interface Asked {
  key: string;
  /// An offset and length, or `{ header }` for a `Range` header.
  range: { offset?: number; length?: number; header?: string } | undefined;
  etagMatches: string | undefined;
}

interface BucketOptions {
  /// The size R2 reports for every pack; a test makes small fixtures look large with it.
  reportedSize?: number | undefined;
  /// The entity tag R2 answers with (a pack rewritten under its key, which create-only forbids).
  etag?: string;
  /// The entity tag R2 answers range reads with, when it differs from the first read's.
  rangeEtag?: string;
  /// Thrown by the first `failures` calls, then the bucket answers.
  failures?: number;
  failure?: () => Error;
  hang?: boolean;
  /// The first `stalls` calls never answer, then the bucket answers (a stalled R2 read).
  stalls?: number;
  /// A `Range` header read returns at most this many bytes, as a pack whose head is longer than
  /// the first read would.
  headerBytes?: number;
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
      opts?: { range?: { offset?: number; length?: number } | Headers; onlyIf?: { etagMatches?: string } },
    ) {
      const header = opts?.range instanceof Headers ? (opts.range.get("Range") ?? "") : undefined;
      asked.push({
        key: wanted,
        range: header === undefined ? (opts?.range as Asked["range"]) : { header },
        etagMatches: opts?.onlyIf?.etagMatches,
      });
      if (options.hang === true) return new Promise(() => undefined);
      if (failed < (options.stalls ?? 0)) {
        failed += 1;
        return new Promise(() => undefined);
      }
      if (failed < (options.failures ?? 0)) {
        failed += 1;
        throw (options.failure ?? (() => new Error("get: We encountered an internal error. Please try again. (10001)")))();
      }
      const bytes = objects[wanted];
      if (bytes === undefined) return null;
      const size = options.reportedSize ?? bytes.length;
      const condition = opts?.onlyIf?.etagMatches;
      if (condition !== undefined && condition !== etag) return { etag, size };
      if (header !== undefined) {
        // HTTP serves a range past the end clamped to the object.
        const [, from = "0", to = "0"] = header.match(/^bytes=(\d+)-(\d+)$/) ?? [];
        const end = Math.min(Number(to) + 1, bytes.length, Number(from) + (options.headerBytes ?? Number.MAX_SAFE_INTEGER));
        const slice = bytes.slice(Number(from), end);
        return { etag, size, body: new Blob([slice]).stream(), arrayBuffer: async () => slice.buffer };
      }
      const range = opts?.range as { offset?: number; length?: number } | undefined;
      if (range !== undefined && options.rangeEtag !== undefined) {
        const slice = bytes.slice(range.offset ?? 0, (range.offset ?? 0) + (range.length ?? bytes.length));
        return { etag: options.rangeEtag, size, body: new Blob([slice]).stream(), arrayBuffer: async () => slice.buffer };
      }
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

describe("one read rule: head, then the document by range", () => {
  it("every pack is read by one ranged head read and the document by range, never streamed", async () => {
    const objects = await goldenPacks();
    const { bucket, asked, streamed } = strictBucket(objects);
    const resolved = await resolvePacks(reads(bucket), unpartedPlan(), PNU_A);
    expect(resolved.kind).toBe("document");
    // Per section: the head by one `Range` header read of the pack's first bytes (root ADR-0233),
    // then the document by one offset range whose entity tag is compared on the answer (ADR-0154).
    const heads = asked.filter((read) => read.range?.header !== undefined);
    expect(heads).toHaveLength(PACKS.sections.length);
    expect(heads.every((read) => read.range?.header === "bytes=0-262143")).toBe(true);
    const documents = asked.filter((read) => read.range?.offset !== undefined);
    expect(documents).toHaveLength(PACKS.sections.length);
    expect(documents.every((read) => read.etagMatches === undefined)).toBe(true);
    expect(asked.filter((read) => read.range === undefined)).toEqual([]);
    expect(streamed.pulled).toBe(0);
  });

  it("a head longer than the first read is completed by one more range, then answers", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const { bucket, asked } = strictBucket(objects, { headerBytes: 25 });
    const resolved = await resolvePacks(reads(bucket), unpartedPlan(), PNU_A);
    expect(resolved.kind).toBe("document");
    const ranges = asked.filter((read) => read.key === name).map((read) => read.range);
    expect(ranges[0]).toEqual({ header: "bytes=0-262143" });
    expect(ranges[1]).toMatchObject({ offset: 25 });
    expect(ranges).toHaveLength(3);
  });

  it("a head completed under another entity tag is a format error, never joined", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const { bucket } = strictBucket(objects, { headerBytes: 25, rangeEtag: "a-newer-tag" });
    await expect(readPack(reads(bucket), PACKS.anchor_section, name)).rejects.toThrow("changed or vanished");
  });

  it("the ranged member is the publisher's document (golden documents unchanged)", async () => {
    const objects = await goldenPacks();
    const documents = JSON.parse(await readFile(new URL("documents.json", fixtures), "utf8")) as {
      base: Record<string, string>;
    };
    const { bucket } = strictBucket(objects);
    const resolved = await resolvePacks(reads(bucket), unpartedPlan(), PNU_A);
    if (resolved.kind !== "document") throw new Error("no document");
    const decoded = await new Response(
      new Blob([resolved.member]).stream().pipeThrough(new DecompressionStream("gzip")),
    ).text();
    expect(decoded).toBe(documents.base[PNU_A]);
  });

  it("a truncated large pack is a format error, not a guess", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const whole = objects[name];
    if (whole === undefined) throw new Error("no fixture");
    const { bucket } = strictBucket({ [name]: whole.slice(0, 30) });
    await expect(readPack(reads(bucket), PACKS.anchor_section, name)).rejects.toThrow("shorter than declared");
  });
});

describe("copies: isolate memory for packs, the edge cache only for answers", () => {
  it("never reads a pack twice within an isolate", async () => {
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects);
    const plan = unpartedPlan();
    await resolvePacks(reads(bucket), plan, PNU_A);
    const heads = asked.filter((read) => read.range === undefined).length;
    const trace = new ReadTrace();
    await resolvePacks(reads(bucket, { trace }), plan, PNU_A);
    // The head is remembered; only the document is read again.
    expect(asked.filter((read) => read.range === undefined)).toHaveLength(heads);
    expect([...trace.sections.values()].every((source) => source === "memory-head+range")).toBe(true);
  });

  it("no pack is put in or looked up from the edge cache: a new isolate reads R2 again", async () => {
    const objects = await goldenPacks();
    {
      forgetPacks();
      edge.clear();
      edgeAsked.length = 0;
      const { bucket, asked } = strictBucket(objects);
      const first = await resolvePacks(reads(bucket), unpartedPlan(), PNU_A);
      await Promise.all(pending);
      expect(edge.size).toBe(0);
      forgetPacks();
      const before = asked.length;
      const trace = new ReadTrace();
      const second = await resolvePacks(reads(bucket, { trace }), unpartedPlan(), PNU_A);
      expect(second).toEqual(first);
      expect(asked.length).toBeGreaterThan(before);
      expect([...trace.sections.values()].every((source) => source.startsWith("r2-"))).toBe(true);
      expect(edgeAsked).toEqual([]);
    }
  });

  it("a remembered head is never combined with bytes R2 holds under another entity tag", async () => {
    const objects = await goldenPacks();
    const name = key(PACKS.anchor_section);
    const { bucket, asked } = strictBucket(objects, { rangeEtag: "a-newer-tag" });
    await expect(resolvePacks(reads(bucket), unpartedPlan(), PNU_A)).rejects.toBeInstanceOf(PackFormatError);
    // Read once, refused, and not retried: retrying cannot make it right.
    const ranged = asked.filter((read) => read.key === name && read.range?.offset !== undefined);
    expect(ranged).toHaveLength(1);
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
      unpartedPlan(),
      PNU_A,
    );
    expect(resolved.kind).toBe("document");
    expect(trace.retries).toBe(2);
    expect(trace.failures).toHaveLength(2);
    expect(trace.failures.every((failure) => failure.endsWith("r2:10001"))).toBe(true);
    expect(asked).toHaveLength(2 * PACKS.sections.length + 2);
    // Full jitter: the planted random 0.5 of an exponentially growing cap.
    expect(slept.every((ms) => ms <= READ.r2_retry_base_ms * 2 ** (READ.r2_attempts - 1))).toBe(true);
  });

  it("a persistent R2 error ends as unavailable after the bounded attempts", async () => {
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects, { failures: Number.POSITIVE_INFINITY });
    await expect(resolvePacks(reads(bucket), unpartedPlan(), PNU_A)).rejects.toBeInstanceOf(PackReadUnavailable);
    expect(asked.length).toBeLessThanOrEqual(PACKS.sections.length * READ.r2_attempts);
  });

  it("a stalled attempt is abandoned at its own bound and asked again, not left to spend the deadline", async () => {
    expect(READ.r2_attempt_timeout_ms).toBeGreaterThan(0);
    // Room for a second attempt after a stalled first one.
    expect(READ.r2_attempt_timeout_ms * 2).toBeLessThan(READ.r2_deadline_ms);
    const objects = await goldenPacks();
    const { bucket, asked } = strictBucket(objects, { stalls: 1 });
    const trace = new ReadTrace();
    const started = Date.now();
    const resolved = await resolvePacks(reads(bucket, { trace }), unpartedPlan(), PNU_A);
    const elapsed = Date.now() - started;
    expect(resolved.kind).toBe("document");
    expect(trace.failures).toEqual([`${PACKS.anchor_section}:pack:timeout`]);
    expect(trace.retries).toBe(1);
    expect(asked).toHaveLength(3);
    expect(elapsed).toBeGreaterThanOrEqual(READ.r2_attempt_timeout_ms - 50);
    expect(elapsed).toBeLessThan(READ.r2_deadline_ms);
  }, 10_000);

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
    const response = await fetchByPnu(
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
    expect(response.headers.get(READ.outage_header)).toBe("r2-unavailable");
    expect(bucket.get.mock.calls.filter(([wanted]) => wanted === key(PACKS.anchor_section))).toHaveLength(READ.r2_attempts);
  });

  // 2026-10-18 18:19Z: the live parcel-transfer-history route answered one monitor read 503 and the
  // same read 200 a minute later; the manifest read was the one R2 read with no retry.
  describe("the live route's manifest read", () => {
    const LIVE = `https://buildings.example.test${GATEWAY.request_path.prefix}${PNU_A}`;
    const servedManifest = JSON.stringify({
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
      section_packs: {
        schema_version: connectionContract.by_pnu_section_packs.manifest_section_packs_schema_version,
        format_version: connectionContract.by_pnu_section_packs.format_version,
        unit_prefix_length: connectionContract.by_pnu_section_packs.unit_prefix_length,
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
        patches: [],
      },
    });

    async function live(manifestFailures: number, manifestBody = servedManifest) {
      forgetPlans();
      const { bucket: packs } = strictBucket(await goldenPacks());
      let manifestReads = 0;
      const bucket = {
        get: vi.fn(async (wanted: string, options?: unknown) => {
          if (wanted !== GATEWAY.object_key.manifest_object) return packs.get(wanted, options as R2GetOptions);
          manifestReads += 1;
          if (manifestReads <= manifestFailures) throw new Error("get: We encountered an internal error. Please try again. (10001)");
          return { body: null, etag: "manifest-etag", text: async () => manifestBody };
        }),
      };
      const response = await fetchByPnu(
        new Request(LIVE),
        {
          [GATEWAY.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">,
          [GATEWAY.allowed_origins_binding]: "https://app.example.test",
        },
        ctx,
      );
      return { response, manifestReads: () => manifestReads };
    }

    it("retries a transient R2 error and answers", async () => {
      const { response, manifestReads } = await live(1);
      expect(response.status).toBe(200);
      expect(manifestReads()).toBe(2);
    });

    it("answers a persistent R2 error with 503 naming the manifest, within the deadline", async () => {
      const started = Date.now();
      const { response, manifestReads } = await live(Number.POSITIVE_INFINITY);
      expect(response.status).toBe(503);
      expect(response.headers.get(READ.outage_header)).toBe("manifest-unavailable");
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(manifestReads()).toBe(READ.r2_attempts);
      expect(Date.now() - started).toBeLessThan(READ.r2_deadline_ms);
    });

    it("names an unreadable manifest and one without section packs apart from R2", async () => {
      expect((await live(0, "not json")).response.headers.get(READ.outage_header)).toBe("manifest-invalid");
      const withoutPacks = JSON.stringify({ ...JSON.parse(servedManifest), section_packs: undefined });
      const { response } = await live(0, withoutPacks);
      expect(response.status).toBe(503);
      expect(response.headers.get(READ.outage_header)).toBe("no-section-packs");
    });
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
    const get = () => fetchByPnu(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env, ctx);
    const cold = await get();
    const coldBody = new Uint8Array(await cold.arrayBuffer());
    expect(cold.headers.get("server-timing")).toContain('desc="r2-head+range"');
    await Promise.all(pending);
    // The copy sits under the plan's fingerprint: another generation names another entry.
    expect(edge.has(answerCacheUrl(previewPlan(1, null).fingerprint, PNU_A))).toBe(true);
    expect(answerCacheUrl(previewPlan(2, null).fingerprint, PNU_A)).not.toBe(answerCacheUrl(previewPlan(1, null).fingerprint, PNU_A));
    // A new isolate, the pack copy gone from it: only the answer's edge copy is left.
    forgetPacks();
    expect([...edge.keys()].filter((url) => url.includes("/pack/"))).toEqual([]);
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

  // 2026-10-07: gate (b) passed with 9,952 of its 10,000 cold reads answered from edge copies an
  // earlier probe of the same sample had left, so a fresh preview upload must start cold.
  it("a new preview version reads R2 for a PNU an earlier version left an edge copy of", async () => {
    const policy = GATEWAY;
    const objects = await goldenPacks();
    const { bucket: packs, asked } = strictBucket(objects);
    const bucket = {
      get: async (wanted: string, options?: unknown) =>
        wanted === policy.object_key.manifest_object
          ? { body: null, text: async () => JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 1 }) }
          : packs.get(wanted, options as R2GetOptions),
    };
    const env = (version: string) => ({
      [policy.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">,
      [policy.allowed_origins_binding]: "https://app.example.test",
      [PACKS.preview_binding]: "true",
      [policy.version_metadata_binding]: { id: version, tag: "" },
    });
    const url = `https://buildings.example.test${policy.request_path.prefix}${PNU_A}?packs=g1`;
    const get = (version: string) =>
      fetchByPnu(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env(version), ctx);
    const first = await get("11111111-1111-4111-8111-111111111111");
    expect(first.headers.get("server-timing")).toContain('desc="r2-head+range"');
    await Promise.all(pending);
    forgetPacks();
    // The same version answers from its copy; a new upload of the preview does not see it.
    expect((await get("11111111-1111-4111-8111-111111111111")).headers.get("server-timing")).toContain('desc="edge-answer"');
    const before = asked.length;
    const next = await get("22222222-2222-4222-8222-222222222222");
    expect(next.status).toBe(200);
    const timing = next.headers.get("server-timing") ?? "";
    expect(timing).not.toContain("edge-answer");
    expect(timing).toContain('desc="r2-head+range"');
    expect(asked.length).toBeGreaterThan(before);
  });

  // Root ADR-0233: gunzip costs the Worker 5.8 ms per decompressed MB and a building document
  // reaches 11.5 MB, so a client that takes no gzip is answered by the edge's conversion.
  it("a client that takes no gzip still gets the stored member from the Worker, never a decoded body", async () => {
    const policy = GATEWAY;
    const objects = await goldenPacks();
    const { bucket } = strictBucket(objects);
    const env = {
      [policy.r2_binding]: bucket,
      [policy.allowed_origins_binding]: "https://app.example.test",
      [PACKS.preview_binding]: "true",
    };
    const url = `https://buildings.example.test${policy.request_path.prefix}${PNU_A}?packs=g1`;
    const gzip = await fetchByPnu(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env, ctx);
    for (const acceptEncoding of ["identity", "gzip;q=0", "br"]) {
      forgetPacks();
      edge.clear();
      const plain = await fetchByPnu(new Request(url, { headers: { "Accept-Encoding": acceptEncoding } }), env, ctx);
      expect(plain.headers.get("content-encoding"), acceptEncoding).toBe("gzip");
      expect(plain.headers.get("etag"), acceptEncoding).toBe(gzip.headers.get("etag"));
      expect(plain.headers.get("server-timing"), acceptEncoding).toContain('outcome;desc="document"');
    }
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
    const preview = await fetchByPnu(new Request(url, { headers: { "Accept-Encoding": "gzip" } }), env(true), ctx);
    expect(preview.headers.get("content-encoding")).toBe("gzip");
    expect(preview.status).toBe(200);
    const timing = preview.headers.get("server-timing") ?? "";
    expect(timing).toContain('outcome;desc="document"');
    expect(timing).toMatch(/r2;dur=\d+;desc="gets=2 retries=0"/);
    for (const section of PACKS.sections) expect(timing).toContain(`pack-${section};dur=`);
    expect(timing).toMatch(/total;dur=\d+/);
    const live = await fetchByPnu(new Request(url), env(false), ctx);
    expect(live.status).toBe(404);
    expect(live.headers.get("server-timing")).toBeNull();
  });
});

describe("an isolate's startup runs the request path once (root ADR-0233)", () => {
  it("every pure step answers at startup as it does for a request", () => {
    expect(warmRequestPath()).toBe(true);
  });
});
