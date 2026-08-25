import connectionContract from "../../../config/r2-connections.contract.json";

const policy = connectionContract.profile_gateway;
const artifactId = new RegExp(`^(?:${policy.object_key.artifact_id_pattern})$`);

interface Env {
  [binding: string]: string | Pick<R2Bucket, "get">;
}

function canonicalObjectKey(url: URL): string | null {
  if (url.search !== "") return null;
  const prefix = `/${policy.object_key.root}/`;
  if (!url.pathname.startsWith(prefix) || !url.pathname.endsWith(policy.object_key.suffix)) {
    return null;
  }
  const candidate = url.pathname.slice(prefix.length, -policy.object_key.suffix.length);
  if (!artifactId.test(candidate)) return null;
  const key = `${policy.object_key.root}/${candidate}${policy.object_key.suffix}`;
  return url.pathname === `/${key}` ? key : null;
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

async function fetchProfile(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const key = canonicalObjectKey(new URL(request.url));
  if (key === null) return new Response(null, { status: 404 });
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
    return fetchProfile(request, env, ctx);
  },
};

export { canonicalObjectKey, corsHeaders, fetchProfile, parseAllowedOrigins };
