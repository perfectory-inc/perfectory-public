import { zxyToTileId } from "pmtiles";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { archiveOf, FakeBucket, TILE_BYTES } from "./helpers/pmtiles";

// Root ADR-0323: a patched unit serves patch, tombstone or base under one immutable base URL.
const policy = connectionContract.vector_tile_gateway;
const patches = policy.patches;
const root = connectionContract.connections.tile_derivatives.expected_values.FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX;
const UNIT = patches.units[0]!;
const SOURCE = `${UNIT}-00000000-0000-4000-8000-000000000004`;
const BASE = `${root}/${SOURCE}${policy.object_key.suffix}`;
const MANIFEST = `${root}/${SOURCE}${patches.manifest_suffix}`;
const patchKey = (generation: number) => `${root}/${SOURCE}${patches.patch_infix}${generation}${policy.object_key.suffix}`;
const URL_ROOT = `https://tiles.example.test/${SOURCE}`;
const PATCHED = new Uint8Array([0x1a, 0x01, 0x07]);
const REPATCHED = new Uint8Array([0x1a, 0x01, 0x08]);
const ID = { z0: zxyToTileId(0, 0, 0), patched: zxyToTileId(1, 0, 0), gone: zxyToTileId(1, 0, 1) };

function manifest(generation: number, patch: number | null, base = SOURCE): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({
    schema_version: patches.manifest_schema_version, base_source_id: base, generation,
    gold_snapshot: "1", folded_through_change_seq: 0,
    patch: patch === null ? null : { generation: patch, sha256: "0".repeat(64), bytes: 1, tiles: 2, empty_tiles: 1 },
  }));
}

describe("foundation tile gateway patches", () => {
  let worker: typeof import("../src/index");
  let bucket: FakeBucket;
  let cached: Map<string, Response>;
  let pending: Promise<unknown>[];
  let env: Record<string, string | Pick<R2Bucket, "get">>;

  async function request(path: string): Promise<Response> {
    const response = await worker.default.fetch(new Request(`${URL_ROOT}/${path}`), env, {
      waitUntil(promise: Promise<unknown>) { pending.push(promise); },
    } as ExecutionContext);
    await Promise.all(pending);
    return response;
  }

  beforeEach(async () => {
    vi.resetModules();
    vi.useFakeTimers({ toFake: ["Date"] });
    worker = await import("../src/index");
    bucket = new FakeBucket();
    bucket.objects.set(BASE, archiveOf(TILE_BYTES.map((bytes, id): [number, Uint8Array] => [id, bytes]), {
      tileCompression: 2,
    }));
    cached = new Map();
    pending = [];
    env = { [policy.r2_binding]: bucket.binding(), [policy.allowed_origins_binding]: "https://app.example.test" };
    vi.stubGlobal("caches", { default: {
      async match(req: Request) { return cached.get(req.url)?.clone(); },
      async put(req: Request, response: Response) { cached.set(req.url, response.clone()); },
    } });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  async function body(response: Response): Promise<Uint8Array> {
    const bytes = new Uint8Array(await response.arrayBuffer());
    return new Uint8Array(await new Response(new Response(bytes).body!.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer());
  }

  it("serves the base with revalidating caching while no patch is published", async () => {
    const response = await request("1/0/0");
    expect(response.status).toBe(200);
    expect(await body(response)).toEqual(TILE_BYTES[1]);
    expect(response.headers.get("Cache-Control")).toBe(patches.cache_control);
    expect(bucket.reads.some((read) => read.key === MANIFEST)).toBe(true);
  });

  it("serves a patched tile from the patch, a tombstone as 204, and every other tile from the base", async () => {
    bucket.objects.set(patchKey(2), archiveOf([[ID.patched, PATCHED], [ID.gone, new Uint8Array()]], { tileCompression: 2 }));
    bucket.objects.set(MANIFEST, manifest(2, 2));
    const patched = await request("1/0/0");
    expect(patched.status).toBe(200);
    expect(await body(patched)).toEqual(PATCHED);
    expect(cached.has(`${URL_ROOT}/1/0/0?patch=2`)).toBe(true);
    const gone = await request("1/0/1");
    expect(gone.status).toBe(204);
    const base = await request("0/0/0");
    expect(await body(base)).toEqual(TILE_BYTES[0]);
    expect(cached.has(`${URL_ROOT}/0/0/0`)).toBe(true);
    // A tile in neither archive stays empty.
    expect((await request("1/1/1")).status).toBe(204);
  });

  it("moves a patched tile to the next generation once the manifest is reread", async () => {
    bucket.objects.set(patchKey(2), archiveOf([[ID.patched, PATCHED]], { tileCompression: 2 }));
    bucket.objects.set(MANIFEST, manifest(2, 2));
    expect(await body(await request("1/0/0"))).toEqual(PATCHED);
    bucket.objects.set(patchKey(3), archiveOf([[ID.patched, REPATCHED]], { tileCompression: 2 }));
    bucket.objects.set(MANIFEST, manifest(3, 3));
    expect(await body(await request("1/0/0"))).toEqual(PATCHED);
    vi.setSystemTime(Date.now() + (patches.manifest_cache_seconds + 1) * 1000);
    expect(await body(await request("1/0/0"))).toEqual(REPATCHED);
    // A manifest with no tile change keeps serving the same patch generation.
    bucket.objects.set(MANIFEST, manifest(4, 3));
    vi.setSystemTime(Date.now() + (patches.manifest_cache_seconds + 1) * 1000);
    expect(await body(await request("1/0/0"))).toEqual(REPATCHED);
  });

  it.each([
    ["a manifest for another base", () => manifest(2, 2, `${UNIT}-00000000-0000-4000-8000-000000000005`)],
    ["a patch ahead of its manifest", () => manifest(1, 2)],
    ["a manifest that is not JSON", () => new TextEncoder().encode("{")],
    ["a manifest naming an absent patch", () => manifest(5, 5)],
  ])("fails closed on %s without caching", async (_, bytes) => {
    bucket.objects.set(patchKey(2), archiveOf([[ID.patched, PATCHED]], { tileCompression: 2 }));
    bucket.objects.set(MANIFEST, bytes());
    const response = await request("1/0/0");
    expect(response.status).toBe(500);
    expect(cached.size).toBe(0);
  });

  it("leaves a unit the contract does not patch immutable and never reads a manifest", async () => {
    const other = "synthetic_layer-00000000-0000-4000-8000-000000000001";
    bucket.objects.set(`${root}/${other}${policy.object_key.suffix}`, archiveOf([[0, TILE_BYTES[0]!]]));
    const response = await worker.default.fetch(new Request(`https://tiles.example.test/${other}/0/0/0`), env, {
      waitUntil(promise: Promise<unknown>) { pending.push(promise); },
    } as ExecutionContext);
    expect(response.status).toBe(200);
    expect(response.headers.get("Cache-Control")).toBe(policy.cache_control);
    expect(bucket.reads.some((read) => read.key.endsWith(patches.manifest_suffix))).toBe(false);
  });
});
