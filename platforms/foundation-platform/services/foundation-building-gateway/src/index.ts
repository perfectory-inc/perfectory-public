import connectionContract from "../../../config/r2-connections.contract.json";

const policy = connectionContract.building_by_pnu_gateway;
const patchPolicy = connectionContract.by_pnu_serving_patches;
const UNIT = "building-by-pnu";
/// The manifest schemas this Worker resolves (root ADR-0141 §4). The publisher asks for this list
/// at `request_path.capabilities` before it writes the first manifest of a newer schema.
const MANIFEST_SCHEMA_VERSIONS = [1, 2] as const;
const pnuPattern = new RegExp(`^(?:${policy.object_key.pnu_pattern})$`);
/// The prefix lengths a 19-digit PNU can have. A manifest declares its own; the contract's
/// `pnu_prefix_length` and `max_patches` bind only the publisher writing the next manifest, so
/// tuning them never makes the live manifest unreadable (root ADR-0141 §5). What the Worker holds
/// every manifest to is the fixed `manifest_patch_ceiling`.
const PNU_PREFIX_LENGTHS = { min: 1, max: 19 } as const;
// A synthetic cache identity for the parsed manifest: the manifest's own R2 key is not a public
// URL of this Worker, and caching under a request URL would let a client shape the cache.
const manifestCacheUrl = "https://foundation-building-gateway.invalid/serving-manifest";

interface Env {
  [binding: string]: string | Pick<R2Bucket, "get">;
}

interface ServingPatch {
  generation: number;
  prefixes: ReadonlySet<string>;
}

/// What one manifest serves: a base generation and its patches, newest first.
interface ServingPlan {
  base: number;
  /// The length of every prefix in `patches`, as the manifest declares it.
  prefixLength: number;
  patches: readonly ServingPatch[];
  /// Names the served state in the per-PNU edge cache key, so a new patch or base never answers
  /// from a response cached under the previous one.
  fingerprint: string;
}

function canonicalPnu(url: URL): string | null {
  // The exact schema query separates v2 browser/edge caches from year-only documents.
  if (url.search !== "" && url.search !== "?schema=2") return null;
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

function isGeneration(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
}

/// The pointer is an outage boundary, not a data boundary: a building that was never baked is a
/// 404, but a manifest that cannot be read or does not validate means the whole lane is not
/// serving, and that must surface as 503 rather than as millions of spurious 404s. A v1
/// manifest is a base with no patches.
function parseManifest(raw: unknown): ServingPlan | null {
  if (typeof raw !== "object" || raw === null) return null;
  const manifest = raw as Record<string, unknown>;
  if (manifest.unit !== UNIT) return null;
  if (manifest.schema_version === 1) {
    if (!isGeneration(manifest.current_generation)) return null;
    return {
      base: manifest.current_generation,
      prefixLength: 0,
      patches: [],
      fingerprint: `v${manifest.current_generation}`,
    };
  }
  if (manifest.schema_version !== 2 || !isGeneration(manifest.base_generation)) return null;
  const prefixLength = manifest.pnu_prefix_length;
  if (
    typeof prefixLength !== "number" ||
    !Number.isSafeInteger(prefixLength) ||
    prefixLength < PNU_PREFIX_LENGTHS.min ||
    prefixLength > PNU_PREFIX_LENGTHS.max
  ) {
    return null;
  }
  const prefixPattern = new RegExp(`^[0-9]{${prefixLength}}$`);
  if (!Array.isArray(manifest.patches) || manifest.patches.length > patchPolicy.manifest_patch_ceiling) {
    return null;
  }
  const patches: ServingPatch[] = [];
  for (const entry of manifest.patches as unknown[]) {
    if (typeof entry !== "object" || entry === null) return null;
    const { generation, prefixes } = entry as Record<string, unknown>;
    if (!isGeneration(generation)) return null;
    const previous = patches.at(-1);
    if (previous !== undefined && previous.generation <= generation) return null;
    if (!Array.isArray(prefixes) || prefixes.length === 0) return null;
    if (!prefixes.every((prefix) => typeof prefix === "string" && prefixPattern.test(prefix))) {
      return null;
    }
    patches.push({ generation, prefixes: new Set(prefixes as string[]) });
  }
  const newest = patches[0];
  return {
    base: manifest.base_generation,
    prefixLength,
    patches,
    fingerprint: newest === undefined ? `v${manifest.base_generation}` : `v${manifest.base_generation}p${newest.generation}`,
  };
}

async function resolvePlan(
  bucket: Pick<R2Bucket, "get">,
  ctx: ExecutionContext,
): Promise<ServingPlan | null> {
  const cached = await caches.default.match(manifestCacheUrl);
  if (cached !== undefined) {
    const plan = parseManifest(await cached.json().catch(() => null));
    if (plan !== null) return plan;
  }
  const object = await bucket.get(policy.object_key.manifest_object);
  if (object === null || !("body" in object)) return null;
  const text = await object.text();
  let plan: ServingPlan | null = null;
  try {
    plan = parseManifest(JSON.parse(text));
  } catch {
    plan = null;
  }
  if (plan === null) return null;
  const response = new Response(text, {
    headers: {
      "Cache-Control": `max-age=${policy.manifest_edge_cache_seconds}`,
      "Content-Type": policy.content_type,
    },
  });
  ctx.waitUntil(caches.default.put(manifestCacheUrl, response));
  return plan;
}

/// The edge cache identity of one PNU's answer under one served state.
function servingCacheUrl(requestUrl: string, plan: ServingPlan): string {
  const url = new URL(requestUrl);
  url.searchParams.set("serving", plan.fingerprint);
  return url.toString();
}

type Found =
  | { kind: "object"; object: R2Object | R2ObjectBody; body?: string }
  | { kind: "deleted" }
  | { kind: "absent" };

function isTombstone(body: string): boolean {
  try {
    const parsed = JSON.parse(body) as Record<string, unknown> | null;
    return parsed?.deleted === true && parsed.schema_version === patchPolicy.tombstone_schema_version;
  } catch {
    return false;
  }
}

/// Newest patch first, then the base. A patch whose prefix list lacks the PNU's prefix holds
/// nothing for it and costs no read. A tombstone ends the search: the PNU is gone (ADR-0141 §2).
///
/// The client's validators (If-None-Match, If-Modified-Since) answer only for a document. A small
/// object may be a tombstone, and whether it is one is read from its stored body before any
/// validator can turn the deletion into a 304.
async function findObject(
  bucket: Pick<R2Bucket, "get">,
  plan: ServingPlan,
  pnu: string,
  conditions: Headers,
): Promise<Found> {
  const { root, suffix } = policy.object_key;
  const pnuPrefix = pnu.slice(0, plan.prefixLength);
  for (const patch of plan.patches) {
    if (!patch.prefixes.has(pnuPrefix)) continue;
    const key = `${root}/v${plan.base}/p${patch.generation}/${pnu}${suffix}`;
    const object = await bucket.get(key, { onlyIf: conditions });
    if (object === null) continue;
    // A document is never as small as a tombstone and is answered as the validators say.
    if (object.size > patchPolicy.tombstone_max_bytes) return { kind: "object", object };
    const stored = "body" in object ? object : await bucket.get(key);
    if (stored === null || !("body" in stored)) continue;
    const body = await stored.text();
    if (isTombstone(body)) return { kind: "deleted" };
    return "body" in object ? { kind: "object", object, body } : { kind: "object", object };
  }
  const object = await bucket.get(`${root}/v${plan.base}/${pnu}${suffix}`, { onlyIf: conditions });
  return object === null ? { kind: "absent" } : { kind: "object", object };
}

function capabilities(): Response {
  return new Response(
    `${JSON.stringify({ unit: UNIT, manifest_schema_versions: MANIFEST_SCHEMA_VERSIONS })}\n`,
    {
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": policy.content_type,
        "X-Content-Type-Options": "nosniff",
      },
    },
  );
}

async function fetchBuilding(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url);
  if (
    url.pathname === policy.request_path.capabilities &&
    url.search === "" &&
    ["GET", "HEAD"].includes(request.method)
  ) {
    return capabilities();
  }
  const pnu = canonicalPnu(url);
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

  const plan = await resolvePlan(bucket, ctx).catch(() => null);
  if (plan === null) {
    return withCors(
      new Response(null, { status: 503, headers: { "Cache-Control": "no-store" } }),
      origin,
      allowed,
    );
  }

  const cacheUrl = servingCacheUrl(request.url, plan);
  if (request.method === "GET") {
    const ifNoneMatch = request.headers.get("If-None-Match");
    const cacheRequest =
      ifNoneMatch === null
        ? new Request(cacheUrl)
        : new Request(cacheUrl, { headers: { "If-None-Match": ifNoneMatch } });
    const cached = await caches.default.match(cacheRequest);
    if (cached !== undefined) return withCors(cached, origin, allowed);
  }

  let found: Found;
  try {
    found = await findObject(bucket, plan, pnu, request.headers);
  } catch {
    return withCors(
      new Response(null, { status: 503, headers: { "Cache-Control": "no-store" } }),
      origin,
      allowed,
    );
  }
  if (found.kind === "absent") {
    return withCors(
      new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } }),
      origin,
      allowed,
    );
  }
  if (found.kind === "deleted") {
    return withCors(
      new Response(`${JSON.stringify({ error: "deleted", pnu })}\n`, {
        status: 404,
        headers: { "Cache-Control": "no-store", "Content-Type": policy.content_type },
      }),
      origin,
      allowed,
    );
  }
  const { object } = found;
  const headers = objectHeaders(object);
  if (!("body" in object)) {
    return withCors(new Response(null, { status: 304, headers }), origin, allowed);
  }
  const response = new Response(found.body ?? object.body, { status: 200, headers });
  if (request.method === "GET") {
    ctx.waitUntil(caches.default.put(new Request(cacheUrl), response.clone()));
  }
  return withCors(response, origin, allowed);
}

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return fetchBuilding(request, env, ctx);
  },
};

export {
  canonicalPnu,
  corsHeaders,
  fetchBuilding,
  MANIFEST_SCHEMA_VERSIONS,
  parseAllowedOrigins,
  parseManifest,
  servingCacheUrl,
};
