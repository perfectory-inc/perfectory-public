import {
  Compression, findTile, PMTiles, ResolvedValueCache, TileType, zxyToTileId, type RangeResponse, type Source,
} from "pmtiles";
import connectionContract from "../../../config/r2-connections.contract.json";

const policy = connectionContract.vector_tile_gateway;
// Root ADR-0323: a unit listed here serves its base archive plus the patch its manifest names.
const patches = policy.patches;
const connection = connectionContract.connections[policy.connection as keyof typeof connectionContract.connections];
const expectedValues: Readonly<Record<string, string>> = connection.expected_values;
const objectRoot = expectedValues[policy.object_key.root.expected_value];
const sourcePattern = new RegExp(`^(?:${policy.object_key.source_id_pattern})$`);
// The official cache owns bounded header/directory eviction, keyed by Source.getKey().
// Resolved values are safe across Worker requests; failed reads cannot poison later requests.
const archiveCache = new ResolvedValueCache(64);
const emptyTileCacheHeader = "X-Foundation-Empty-Tile";

interface Env {
  [binding: string]: string | Pick<R2Bucket, "get">;
}

interface TileAddress {
  sourceId: string;
  z: number;
  x: number;
  y: number;
}

class MissingArchive extends Error {}

class R2Source implements Source {
  constructor(private readonly bucket: Pick<R2Bucket, "get">, private readonly key: string) {}

  getKey(): string {
    return this.key;
  }

  async getBytes(offset: number, length: number, _signal?: AbortSignal, etag?: string): Promise<RangeResponse> {
    const object = await this.bucket.get(this.key, { range: { offset, length } });
    if (object === null) throw new MissingArchive();
    if (!("body" in object) || (etag !== undefined && object.etag !== etag)) {
      // Release keys are immutable: never combine cached directories with changed tile bytes.
      throw new Error("Immutable archive changed");
    }
    return { data: await object.arrayBuffer(), etag: object.etag };
  }
}

interface PatchState {
  generation: number;
  patchGeneration: number | null;
}

// The manifest is the one mutable object; each isolate rereads it after manifest_cache_seconds.
const manifestCache = new Map<string, { at: number; state: PatchState | null }>();

function positiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

async function patchState(bucket: Pick<R2Bucket, "get">, sourceId: string): Promise<PatchState | null> {
  const key = `${objectRoot}/${sourceId}${patches.manifest_suffix}`;
  const now = Date.now();
  const hit = manifestCache.get(key);
  if (hit !== undefined && now - hit.at < patches.manifest_cache_seconds * 1000) return hit.state;
  const object = await bucket.get(key);
  let state: PatchState | null = null;
  if (object !== null) {
    if (!("body" in object)) throw new Error("Patch manifest has no body");
    const manifest = JSON.parse(new TextDecoder().decode(await object.arrayBuffer())) as Record<string, unknown>;
    const patch = manifest.patch as Record<string, unknown> | null;
    if (
      manifest.schema_version !== patches.manifest_schema_version || manifest.base_source_id !== sourceId ||
      !positiveInteger(manifest.generation) ||
      (patch !== null && (typeof patch !== "object" || !positiveInteger(patch.generation) ||
        patch.generation > manifest.generation))
    ) {
      throw new Error("Patch manifest is not one the bake publishes");
    }
    state = { generation: manifest.generation, patchGeneration: patch === null ? null : patch.generation as number };
  }
  manifestCache.delete(key);
  manifestCache.set(key, { at: now, state });
  if (manifestCache.size > 64) manifestCache.delete(manifestCache.keys().next().value as string);
  return state;
}

// Whether the patch archive holds z/x/y, read from its (cached) directories without the tile bytes,
// so an edge-cache hit costs no tile read.
async function patchHolds(source: R2Source, z: number, x: number, y: number): Promise<boolean> {
  try {
    const header = await archiveCache.getHeader(source);
    if (z < header.minZoom || z > header.maxZoom) return false;
    const tileId = zxyToTileId(z, x, y);
    let [offset, length] = [header.rootDirectoryOffset, header.rootDirectoryLength];
    for (let depth = 0; depth < 4; depth++) {
      const entry = findTile(await archiveCache.getDirectory(source, offset, length, header), tileId);
      if (entry === null) return false;
      if (entry.runLength > 0) return true;
      [offset, length] = [header.leafDirectoryOffset + entry.offset, entry.length];
    }
  } catch (error) {
    // A manifest naming an absent patch is a broken publish, not an absent tile.
    throw error instanceof MissingArchive ? new Error("Patch archive missing") : error;
  }
  throw new Error("Patch directories nest too deep");
}

// The tile tombstone (ADR-0111 §3): an empty MVT, which a patch writes where a dirty tile lost every feature.
async function isEmptyTile(bytes: ArrayBuffer, compression: Compression): Promise<boolean> {
  if (bytes.byteLength === 0) return true;
  if (compression !== Compression.Gzip || bytes.byteLength > 64) return false;
  const body = new Response(bytes).body;
  if (body === null) return true;
  return (await new Response(body.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer()).byteLength === 0;
}

function canonicalTile(url: URL): TileAddress | null {
  // URL.search is empty for a bare '?', which is still a noncanonical request.
  if (url.href.includes("?") || url.hash !== "") return null;
  const parts = url.pathname.split("/");
  if (parts.length !== 5) return null;
  const [, sourceId, zText, xText, yText] = parts;
  if (sourceId === undefined || !sourcePattern.test(sourceId)) return null;
  if (![zText, xText, yText].every((part) => part !== undefined && /^(0|[1-9][0-9]*)$/.test(part))) {
    return null;
  }
  const z = Number(zText);
  const x = Number(xText);
  const y = Number(yText);
  if (!Number.isSafeInteger(z) || z > policy.max_zoom ||
    !Number.isSafeInteger(x) || !Number.isSafeInteger(y) || x >= 2 ** z || y >= 2 ** z) {
    return null;
  }
  return url.pathname === `/${sourceId}/${z}/${x}/${y}` ? { sourceId, z, x, y } : null;
}

function parseAllowedOrigins(raw: string): ReadonlySet<string> | null {
  const origins = new Set<string>();
  for (const field of raw.split(",")) {
    const value = field.trim();
    if (value === "") continue;
    try {
      const parsed = new URL(value);
      if (
        !["http:", "https:"].includes(parsed.protocol) ||
        parsed.origin !== value ||
        parsed.username !== "" ||
        parsed.password !== ""
      ) {
        return null;
      }
      origins.add(value);
    } catch {
      return null;
    }
  }
  return origins;
}

function corsHeaders(origin: string | null, allowed: ReadonlySet<string>): Headers {
  const headers = new Headers({ Vary: "Origin" });
  if (origin !== null && allowed.has(origin)) {
    headers.set("Access-Control-Allow-Origin", origin);
    headers.set("Access-Control-Expose-Headers", "ETag");
  }
  return headers;
}

function notModified(request: Request, etag: string | null): boolean {
  if (etag === null) return false;
  return (request.headers.get("If-None-Match") ?? "").split(",").some((entry) => {
    const candidate = entry.trim();
    return candidate === "*" || candidate.replace(/^W\//, "") === etag;
  });
}

function clientResponse(response: Response, request: Request, origin: string | null, allowed: ReadonlySet<string>): Response {
  const headers = new Headers(response.headers);
  for (const [name, value] of corsHeaders(origin, allowed)) headers.set(name, value);
  const tileStatus = headers.get(emptyTileCacheHeader) === "1" ? 204 : response.status;
  headers.delete(emptyTileCacheHeader);
  if (tileStatus === 204) headers.delete("Content-Length");
  const status = notModified(request, headers.get("ETag")) ? 304 : tileStatus;
  const noBody = request.method === "HEAD" || status === 304 || status === 204;
  if (noBody) void response.body?.cancel();
  return new Response(noBody ? null : response.body, {
    status,
    headers,
    // Both fresh and cache-hit gzip bodies are already encoded.
    encodeBody: "manual",
  });
}

async function fetchTile(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const tile = canonicalTile(new URL(request.url));
  if (tile === null) return new Response(null, { status: 404 });
  if (!["GET", "HEAD", "OPTIONS"].includes(request.method)) {
    return new Response(null, { status: 405, headers: { Allow: "GET, HEAD, OPTIONS" } });
  }

  const bucket = env[policy.r2_binding];
  const rawAllowedOrigins = env[policy.allowed_origins_binding];
  if (
    typeof rawAllowedOrigins !== "string" ||
    typeof bucket !== "object" || bucket === null || !("get" in bucket) ||
    typeof objectRoot !== "string" || objectRoot === ""
  ) {
    return new Response(null, { status: 500 });
  }
  const allowed = parseAllowedOrigins(rawAllowedOrigins);
  if (allowed === null) return new Response(null, { status: 500 });
  const origin = request.headers.get("Origin");
  if (origin !== null && !allowed.has(origin)) return new Response(null, { status: 403 });
  if (request.method === "OPTIONS") {
    const requestedMethod = request.headers.get("Access-Control-Request-Method");
    const requestedHeaders = request.headers.get("Access-Control-Request-Headers");
    if (
      origin === null || !["GET", "HEAD"].includes(requestedMethod ?? "") ||
      (requestedHeaders !== null && requestedHeaders.toLowerCase() !== "if-none-match")
    ) {
      return new Response(null, { status: 403 });
    }
    const headers = corsHeaders(origin, allowed);
    headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "If-None-Match");
    headers.set("Access-Control-Max-Age", "86400");
    return new Response(null, { status: 204, headers });
  }

  let key = `${objectRoot}/${tile.sourceId}${policy.object_key.suffix}`;
  const patched = patches.units.includes(tile.sourceId.slice(0, tile.sourceId.indexOf("-")));
  let cacheUrl = request.url;
  if (patched) {
    const state = await patchState(bucket, tile.sourceId);
    if (state?.patchGeneration != null) {
      const patchKey = `${objectRoot}/${tile.sourceId}${patches.patch_infix}${state.patchGeneration}${policy.object_key.suffix}`;
      if (await patchHolds(new R2Source(bucket, patchKey), tile.z, tile.x, tile.y)) {
        key = patchKey;
        // A patched tile is cached per patch generation; base tiles keep their cache entries.
        cacheUrl = `${request.url}?patch=${state.patchGeneration}`;
      }
    }
  }
  const cacheRequest = new Request(cacheUrl);
  const cached = await caches.default.match(cacheRequest);
  if (cached !== undefined) return clientResponse(cached, request, origin, allowed);

  // Directory decompression remains in archiveCache. Only tile bytes pass through unchanged.
  const archive = new PMTiles(new R2Source(bucket, key), archiveCache, async (bytes) => bytes);
  const header = await archive.getHeader();
  if (header.specVersion !== 3 || header.tileType !== TileType.Mvt ||
    ![Compression.None, Compression.Gzip].includes(header.tileCompression) || header.etag === undefined) {
    return new Response(null, { status: 500 });
  }
  let data = await archive.getZxy(tile.z, tile.x, tile.y);
  if (data !== undefined && cacheUrl !== request.url && await isEmptyTile(data.data, header.tileCompression)) {
    data = undefined;
  }
  const headers = new Headers({
    // A patched unit's tiles change under the same URL, so browsers revalidate (ADR-0323 §5).
    "Cache-Control": patched ? patches.cache_control : policy.cache_control,
    "Content-Type": policy.content_type,
    ETag: `"${header.etag}-${tile.z}-${tile.x}-${tile.y}"`,
    "X-Content-Type-Options": "nosniff",
  });
  if (data !== undefined) {
    headers.set("Content-Length", String(data.data.byteLength));
    if (header.tileCompression === Compression.Gzip) headers.set("Content-Encoding", "gzip");
  }
  const response = new Response(data?.data ?? null, {
    status: data === undefined ? 204 : 200,
    headers,
    encodeBody: "manual",
  });
  if (request.method === "GET") {
    // Cache backends do not reliably store null-body status 204 (including pinned Miniflare).
    // Store an empty 200 with a private marker; clientResponse restores 204 and strips it.
    const cacheHeaders = new Headers(headers);
    if (response.status === 204) cacheHeaders.set(emptyTileCacheHeader, "1");
    // workerd Response.clone() resets encodeBody to automatic. Reassert manual encoding on
    // the cache copy too, or a gzip archive becomes double-compressed on cache hits.
    ctx.waitUntil(caches.default.put(cacheRequest, new Response(response.clone().body, {
      status: 200,
      headers: cacheHeaders,
      encodeBody: "manual",
    })));
  }
  return clientResponse(response, request, origin, allowed);
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      return await fetchTile(request, env, ctx);
    } catch (error) {
      return new Response(null, { status: error instanceof MissingArchive ? 404 : 500 });
    }
  },
};

export { canonicalTile, corsHeaders, parseAllowedOrigins };
