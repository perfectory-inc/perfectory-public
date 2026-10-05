import connectionContract from "../../../config/r2-connections.contract.json";
import {
  PackFormatError,
  PackReadUnavailable,
  packReads,
  parseSectionPacks,
  previewPlan,
  resolvePacks,
  type PackPlan,
  type ReadTrace,
} from "./packs";

const policy = connectionContract.building_by_pnu_gateway;
const patchPolicy = connectionContract.by_pnu_serving_patches;
const packPolicy = connectionContract.by_pnu_section_packs;
const lanePacks = policy.section_packs;
const UNIT = "building-by-pnu";
/// The manifest schemas this Worker resolves (root ADR-0141 §4): the v1 and v2 envelopes, and 3,
/// the `section_packs` block a v2 envelope can carry (root ADR-0147). The publisher asks for this
/// list at `request_path.capabilities` before it writes the first manifest of a newer schema.
const MANIFEST_SCHEMA_VERSIONS = [1, 2, packPolicy.manifest_section_packs_schema_version] as const;
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
  /// The section packs the lane serves from; absent while it serves objects (root ADR-0147).
  packs?: PackPlan;
}

/// A canonical request: the PNU, and the unpublished pack generation a preview asks for.
interface CanonicalRequest {
  pnu: string;
  previewGeneration: number | null;
}

const previewPattern = new RegExp(`^\\?${packPolicy.preview_query_parameter}=g([1-9][0-9]{0,15})$`);

function canonicalRequest(url: URL): CanonicalRequest | null {
  // The exact schema query separates v2 browser/edge caches from year-only documents; the preview
  // query names one unpublished pack generation (the cut-over gate's latency probe).
  const preview = url.search.match(previewPattern);
  if (url.search !== "" && url.search !== "?schema=2" && preview === null) return null;
  const prefix = policy.request_path.prefix;
  if (!url.pathname.startsWith(prefix)) return null;
  const candidate = url.pathname.slice(prefix.length);
  if (!pnuPattern.test(candidate)) return null;
  if (url.pathname !== `${prefix}${candidate}`) return null;
  return { pnu: candidate, previewGeneration: preview === null ? null : Number(preview[1]) };
}

function canonicalPnu(url: URL): string | null {
  return canonicalRequest(url)?.pnu ?? null;
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
  const plan: ServingPlan = {
    base: manifest.base_generation,
    prefixLength,
    patches,
    fingerprint: newest === undefined ? `v${manifest.base_generation}` : `v${manifest.base_generation}p${newest.generation}`,
  };
  if (manifest.section_packs === undefined) return plan;
  // A block this Worker cannot trust is an outage, never a silent fall back to objects.
  const packs = parseSectionPacks(manifest.section_packs, patchPolicy.manifest_patch_ceiling);
  if (packs === null) return null;
  return { ...plan, packs, fingerprint: `${plan.fingerprint}-${packs.fingerprint}` };
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

/// The served state's fingerprint without its packs: what an object-only version caches under.
function objectFingerprint(plan: ServingPlan): string {
  const newest = plan.patches[0];
  return newest === undefined ? `v${plan.base}` : `v${plan.base}p${newest.generation}`;
}

/// The edge cache identity of one PNU's answer under one served state.
function servingCacheUrl(requestUrl: string, plan: Pick<ServingPlan, "fingerprint">): string {
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

function matchesEtag(request: Request, etag: string): boolean {
  return (request.headers.get("If-None-Match") ?? "").split(",").some((entry) => {
    const candidate = entry.trim();
    return candidate === "*" || candidate.replace(/^W\//, "") === etag;
  });
}

/// Why a pack read became an outage, in the words the logs and `Server-Timing` use: an
/// inconsistency the data itself shows, or R2 not answering within the read policy.
function outageClass(error: unknown): string {
  if (error instanceof PackReadUnavailable) return "r2-unavailable";
  if (error instanceof PackFormatError) return "pack-inconsistent";
  return "unexpected";
}

/// `Server-Timing` (W3C) for a pack answer: R2 reads and the time spent waiting on them, where
/// each section's pack came from, and the whole request. Only a preview version says it.
function serverTiming(trace: ReadTrace | null, started: number, outcome: string): string {
  const parts = [`outcome;desc="${outcome}"`];
  if (trace !== null) {
    parts.push(`r2;dur=${trace.r2Ms};desc="gets=${trace.r2Gets} retries=${trace.retries}"`);
    for (const [section, source] of trace.sections) {
      parts.push(`pack-${section};dur=${trace.sectionR2Ms.get(section) ?? 0};desc="${source}"`);
    }
  }
  parts.push(`total;dur=${Date.now() - started}`);
  return parts.join(", ");
}

/// The joined document of a PNU from section packs (root ADR-0147 §3): the same JSON the object
/// held, with the same headers, 404 semantics and edge caching under the served-state fingerprint.
async function packResponse(
  request: Request,
  bucket: Pick<R2Bucket, "get">,
  ctx: ExecutionContext,
  plan: PackPlan,
  pnu: string,
  origin: string | null,
  allowed: ReadonlySet<string>,
  timing: { started: number } | null,
): Promise<Response> {
  const reads = packReads(bucket, ctx);
  const timed = (response: Response, outcome: string): Response => {
    if (timing !== null) response.headers.set("Server-Timing", serverTiming(reads.trace, timing.started, outcome));
    return response;
  };
  let resolved;
  try {
    resolved = await resolvePacks(reads, plan, pnu);
  } catch (error) {
    const outage = outageClass(error);
    console.log(
      JSON.stringify({
        event: "pack_outage",
        error_class: outage,
        message: (error instanceof Error ? error.message : String(error)).slice(0, 200),
        sections: Object.fromEntries(reads.trace.sections),
        failures: reads.trace.failures,
        r2_gets: reads.trace.r2Gets,
        r2_ms: reads.trace.r2Ms,
      }),
    );
    return withCors(
      timed(new Response(null, { status: 503, headers: { "Cache-Control": "no-store" } }), outage),
      origin,
      allowed,
    );
  }
  if (resolved.kind === "absent") {
    return withCors(
      timed(new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } }), "absent"),
      origin,
      allowed,
    );
  }
  if (resolved.kind === "tombstone") {
    return withCors(
      timed(
        new Response(`${JSON.stringify({ error: "deleted", pnu })}\n`, {
          status: 404,
          headers: { "Cache-Control": "no-store", "Content-Type": policy.content_type },
        }),
        "tombstone",
      ),
      origin,
      allowed,
    );
  }
  // The member is the served document's gzip, exactly as the object lane served it uncompressed:
  // it goes out as it is, nothing decompressed or parsed (root ADR-0151). A client that does not
  // accept gzip gets it decompressed here, the one path that spends CPU on the body.
  // The two are different representations (RFC 9110 §8.8.3): each has its own strong tag, so a
  // cache or client never takes the gzip bytes for the identity ones under one validator.
  const gzip = acceptsGzip(request);
  const etag = gzip ? `"${resolved.etag}"` : `"${resolved.etag}-identity"`;
  const headers = corsHeaders(origin, allowed);
  headers.set("Cache-Control", policy.cache_control);
  headers.set("Content-Type", policy.content_type);
  headers.set("ETag", etag);
  headers.set("X-Content-Type-Options", "nosniff");
  headers.append("Vary", "Accept-Encoding");
  if (matchesEtag(request, etag)) {
    return timed(new Response(null, { status: 304, headers }), "not-modified");
  }
  if (gzip) {
    headers.set("Content-Encoding", "gzip");
    headers.set("Content-Length", resolved.member.byteLength.toString());
    const body = request.method === "HEAD" ? null : resolved.member;
    return timed(new Response(body, { status: 200, headers, encodeBody: "manual" }), "document");
  }
  const plain = new Response(new Blob([resolved.member]).stream().pipeThrough(new DecompressionStream("gzip")));
  const bytes = new Uint8Array(await plain.arrayBuffer());
  headers.set("Content-Length", bytes.byteLength.toString());
  return timed(
    new Response(request.method === "HEAD" ? null : bytes, { status: 200, headers }),
    "document-decompressed",
  );
}

/// Whether the client takes `Content-Encoding: gzip` (RFC 9110 §12.5.3): named with a nonzero
/// weight, or covered by `*`. The edge rewrites a Worker's incoming `Accept-Encoding` to
/// `br, gzip` and keeps the client's own in `cf.clientAcceptEncoding`; that is the one asked, so
/// the representation (and its entity tag) is the one the client gets, not one the edge converts.
function acceptsGzip(request: Request): boolean {
  const client = (request as { cf?: { clientAcceptEncoding?: unknown } }).cf?.clientAcceptEncoding;
  const raw = typeof client === "string" ? client : (request.headers.get("Accept-Encoding") ?? "");
  const accepted = new Map<string, number>();
  for (const field of raw.split(",")) {
    const [coding, ...parameters] = field.trim().toLowerCase().split(";");
    if (coding === undefined || coding === "") continue;
    const weight = parameters.map((parameter) => parameter.trim()).find((parameter) => parameter.startsWith("q="));
    accepted.set(coding, weight === undefined ? 1 : Number(weight.slice(2)));
  }
  const gzip = accepted.get("gzip") ?? accepted.get("x-gzip") ?? accepted.get("*");
  return gzip !== undefined && gzip > 0;
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
  const started = Date.now();
  const url = new URL(request.url);
  if (
    url.pathname === policy.request_path.capabilities &&
    url.search === "" &&
    ["GET", "HEAD"].includes(request.method)
  ) {
    return capabilities();
  }
  const canonical = canonicalRequest(url);
  if (canonical === null) return new Response(null, { status: 404 });
  const { pnu, previewGeneration } = canonical;
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

  // A preview names an unpublished pack generation: only a preview version (its binding set)
  // serves one, and the live route serves a generation only when the manifest already names it.
  // A version whose serving binding is off answers from objects while the manifest already names
  // packs: the pack path is rolled out and back by version percentage (root ADR-0151 §7).
  const servesPacks = env[lanePacks.serving_binding] !== "off";
  let packs = servesPacks ? plan.packs : undefined;
  let fingerprint = servesPacks ? plan.fingerprint : objectFingerprint(plan);
  // A preview version says how it answered (`Server-Timing`); the live route does not.
  const timing = env[lanePacks.preview_binding] === "true" ? { started } : null;
  if (previewGeneration !== null) {
    if (timing !== null) {
      packs = previewPlan(previewGeneration);
      fingerprint = packs.fingerprint;
    } else if (packs === undefined || !packs.sections.every((section) => section.generation === previewGeneration)) {
      return withCors(new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } }), origin, allowed);
    }
  }
  if (packs !== undefined) {
    // No per-PNU edge copy: the pack copies already make a warm read free of R2, and the answer
    // is the member as it is, so a second copy would only spend CPU putting it.
    return packResponse(request, bucket, ctx, packs, pnu, origin, allowed, timing);
  }
  const cacheUrl = servingCacheUrl(request.url, { fingerprint });
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
  canonicalRequest,
  corsHeaders,
  fetchBuilding,
  MANIFEST_SCHEMA_VERSIONS,
  parseAllowedOrigins,
  parseManifest,
  servingCacheUrl,
};
