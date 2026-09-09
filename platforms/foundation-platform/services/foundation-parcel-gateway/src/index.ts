import connectionContract from "../../../config/r2-connections.contract.json";

const policy = connectionContract.parcel_by_pnu_gateway;
const pnuPattern = new RegExp(`^(?:${policy.object_key.pnu_pattern})$`);
// A synthetic cache identity for the parsed manifest: the manifest's own R2 key is not a public
// URL of this Worker, and caching under a request URL would let a client shape the cache.
const manifestCacheUrl = "https://foundation-parcel-gateway.invalid/serving-manifest";

interface Env {
  [binding: string]: string | Pick<R2Bucket, "get">;
}

function canonicalPnu(url: URL): string | null {
  if (url.search !== "") return null;
  const prefix = policy.request_path.prefix;
  if (!url.pathname.startsWith(prefix)) return null;
  const candidate = url.pathname.slice(prefix.length);
  if (!pnuPattern.test(candidate)) return null;
  return url.pathname === `${prefix}${candidate}` ? candidate : null;
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
  }
  return headers;
}

function withCors(
  response: Response,
  origin: string | null,
  allowed: ReadonlySet<string>,
): Response {
  const headers = new Headers(response.headers);
  for (const [name, value] of corsHeaders(origin, allowed)) headers.set(name, value);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function objectHeaders(object: R2Object): Headers {
  return new Headers({
    "Cache-Control": policy.cache_control,
    "Content-Length": object.size.toString(),
    "Content-Type": policy.content_type,
    ETag: object.httpEtag,
    "X-Content-Type-Options": "nosniff",
  });
}

/// The pointer is an outage boundary, not a data boundary: a parcel that was never baked is a
/// 404, but a manifest that cannot be read or does not validate means the whole lane is not
/// serving, and that must surface as 503 rather than as forty million spurious 404s.
function parseManifestGeneration(raw: unknown): number | null {
  if (typeof raw !== "object" || raw === null) return null;
  const manifest = raw as Record<string, unknown>;
  if (manifest.schema_version !== 1 || manifest.unit !== "parcel-by-pnu") return null;
  const generation = manifest.current_generation;
  if (typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 1) {
    return null;
  }
  return generation;
}

async function resolveGeneration(
  bucket: Pick<R2Bucket, "get">,
  ctx: ExecutionContext,
): Promise<number | null> {
  const cached = await caches.default.match(manifestCacheUrl);
  if (cached !== undefined) {
    const generation = parseManifestGeneration(await cached.json().catch(() => null));
    if (generation !== null) return generation;
  }
  const object = await bucket.get(policy.object_key.manifest_object);
  if (object === null || !("body" in object)) return null;
  const text = await object.text();
  let generation: number | null = null;
  try {
    generation = parseManifestGeneration(JSON.parse(text));
  } catch {
    generation = null;
  }
  if (generation === null) return null;
  const response = new Response(text, {
    headers: {
      "Cache-Control": `max-age=${policy.manifest_edge_cache_seconds}`,
      "Content-Type": policy.content_type,
    },
  });
  ctx.waitUntil(caches.default.put(manifestCacheUrl, response));
  return generation;
}

async function fetchParcel(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const pnu = canonicalPnu(new URL(request.url));
  if (pnu === null) return new Response(null, { status: 404 });
  if (!["GET", "HEAD", "OPTIONS"].includes(request.method)) {
    return new Response(null, {
      status: 405,
      headers: { Allow: "GET, HEAD, OPTIONS" },
    });
  }

  const bucket = env[policy.r2_binding];
  const rawAllowedOrigins = env[policy.allowed_origins_binding];
  if (
    typeof rawAllowedOrigins !== "string" ||
    typeof bucket !== "object" ||
    bucket === null ||
    !("get" in bucket)
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
      origin === null ||
      !["GET", "HEAD"].includes(requestedMethod ?? "") ||
      (requestedHeaders !== null && requestedHeaders.toLowerCase() !== "if-none-match")
    ) {
      return new Response(null, { status: 403 });
    }
    const headers = corsHeaders(origin, allowed);
    headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "If-None-Match");
    headers.set("Access-Control-Expose-Headers", "ETag");
    headers.set("Access-Control-Max-Age", "86400");
    return new Response(null, { status: 204, headers });
  }

  if (request.method === "GET") {
    const ifNoneMatch = request.headers.get("If-None-Match");
    const cacheRequest =
      ifNoneMatch === null
        ? new Request(request.url)
        : new Request(request.url, { headers: { "If-None-Match": ifNoneMatch } });
    const cached = await caches.default.match(cacheRequest);
    if (cached !== undefined) return withCors(cached, origin, allowed);
  }

  const generation = await resolveGeneration(bucket, ctx);
  if (generation === null) {
    return withCors(
      new Response(null, { status: 503, headers: { "Cache-Control": "no-store" } }),
      origin,
      allowed,
    );
  }

  const key = `${policy.object_key.root}/v${generation}/${pnu}${policy.object_key.suffix}`;
  const object = await bucket.get(key, { onlyIf: request.headers });
  if (object === null) return new Response(null, { status: 404 });
  const headers = objectHeaders(object);
  if (!("body" in object)) {
    return withCors(new Response(null, { status: 304, headers }), origin, allowed);
  }
  const response = new Response(object.body, { status: 200, headers });
  if (request.method === "GET") {
    const cacheRequest = new Request(request.url);
    ctx.waitUntil(caches.default.put(cacheRequest, response.clone()));
  }
  return withCors(response, origin, allowed);
}

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return fetchParcel(request, env, ctx);
  },
};

export { canonicalPnu, corsHeaders, fetchParcel, parseAllowedOrigins, parseManifestGeneration };
