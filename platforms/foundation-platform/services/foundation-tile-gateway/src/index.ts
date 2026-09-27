import { Compression, PMTiles, ResolvedValueCache, TileType, type RangeResponse, type Source } from "pmtiles";
import connectionContract from "../../../config/r2-connections.contract.json";

const policy = connectionContract.vector_tile_gateway;
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

  const cacheRequest = new Request(request.url);
  const cached = await caches.default.match(cacheRequest);
  if (cached !== undefined) return clientResponse(cached, request, origin, allowed);

  const key = `${objectRoot}/${tile.sourceId}${policy.object_key.suffix}`;
  // Directory decompression remains in archiveCache. Only tile bytes pass through unchanged.
  const archive = new PMTiles(new R2Source(bucket, key), archiveCache, async (bytes) => bytes);
  const header = await archive.getHeader();
  if (header.specVersion !== 3 || header.tileType !== TileType.Mvt ||
    ![Compression.None, Compression.Gzip].includes(header.tileCompression) || header.etag === undefined) {
    return new Response(null, { status: 500 });
  }
  const data = await archive.getZxy(tile.z, tile.x, tile.y);
  const headers = new Headers({
    "Cache-Control": policy.cache_control,
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
