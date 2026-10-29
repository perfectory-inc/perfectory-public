import connectionContract from "../../../config/r2-connections.contract.json";
import {
  answerCacheResponse,
  answerCacheUrl,
  answerFromCache,
  PackFormatError,
  PackReadUnavailable,
  type PackReads,
  packReads,
  parseSectionPacks,
  previewPlan,
  resolvePacks,
  type PackPlan,
  packKey,
  ReadTrace,
  warmPackPath,
  type Resolved,
  withRetry,
} from "./packs";
import { cacheOrigin, LANE, laneOfPath, LANES, lanePacks, OWN, policy, type ServedLane } from "./lane";

const patchPolicy = connectionContract.by_pnu_serving_patches;
const packPolicy = connectionContract.by_pnu_section_packs;
const pagesPolicy = packPolicy.read_path.request_cpu_budget.pages;
// Content-addressed media served on the parcel edge only (root ADR-0296 §5 / ADR-0297 §5): the URL
// path is the R2 key and the file name is the content hash, so it answers public and immutable. This
// one source serves several lanes; only the lane the media policy names (parcel) answers it, off the
// lakehouse bucket the lane already binds.
const mediaPolicy = connectionContract.parcel_media;
const mediaPath = new RegExp(`^/(?:${mediaPolicy.path_pattern})$`);
/// The manifest schemas this Worker resolves (root ADR-0141 §4): the v1 and v2 envelopes, 3, the
/// `section_packs` block a v2 envelope can carry (root ADR-0147), and the block whose sections name
/// a parts index (root ADR-0163). The publisher asks for this list at `request_path.capabilities`
/// before it writes the first manifest of a newer schema.
const MANIFEST_SCHEMA_VERSIONS = [
  1,
  2,
  packPolicy.manifest_section_packs_schema_version,
  packPolicy.manifest_section_packs_parted_schema_version,
] as const;
const pnuPattern = new RegExp(`^(?:${policy.object_key.pnu_pattern})$`);
/// The prefix lengths a 19-digit PNU can have. A manifest declares its own; the contract's
/// `pnu_prefix_length` and `max_patches` bind only the publisher writing the next manifest, so
/// tuning them never makes the live manifest unreadable (root ADR-0141 §5). What the Worker holds
/// every manifest to is the fixed `manifest_patch_ceiling`.
const PNU_PREFIX_LENGTHS = { min: 1, max: 19 } as const;
// A synthetic cache identity for the parsed manifest: the manifest's own R2 key is not a public
// URL of this Worker, and caching under a request URL would let a client shape the cache. Each
// lane the Worker serves has its own (root ADR-0193 §4); its own lane keeps the key it had.
function manifestCacheUrl(lane: ServedLane): string {
  return lane === OWN ? `${cacheOrigin}/serving-manifest` : `${cacheOrigin}/serving-manifest/${lane.name}`;
}

interface Env {
  [binding: string]: string | Pick<R2Bucket, "get"> | Pick<WorkerVersionMetadata, "id">;
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
  /// The section packs the lane serves from (root ADR-0147); a manifest without them is an outage.
  packs?: PackPlan;
}

/// A canonical request: the key (a PNU, or a PNU and the lane's key suffix, root ADR-0204), and
/// the unpublished pack generation a preview asks for, with the probe run it is read in
/// (`g{N}-r{run}`): a preview keys its edge copies by the run, so each gate run's first read of a
/// document is an R2 read on both sides (2026-10-13: a baseline answered from edge copies an
/// earlier run left was timed against R2 reads).
interface CanonicalRequest {
  pnu: string;
  /// The page of a document too large for one request (root ADR-0325): 1 for the PNU's own path,
  /// `n` for `<pnu>/<n>`.
  page: number;
  previewGeneration: number | null;
  previewRun: string | null;
  /// A manifest-history file name (`<published>-<sha256>`): a preview version serves that recorded
  /// state exactly, its generations and pack patches (the rollback rehearsal, root ADR-0274 §5).
  previewState: string | null;
}

const previewPattern = new RegExp(
  `^\\?${packPolicy.preview_query_parameter}=(?:g([1-9][0-9]{0,15})(?:-r([0-9a-z]{1,16}))?|h([0-9]{8}T[0-9]{6}Z-[0-9a-f]{64}))$`,
);

function canonicalRequest(url: URL, lane: ServedLane = OWN): CanonicalRequest | null {
  // The exact schema query separates v2 browser/edge caches from year-only documents; the preview
  // query names one unpublished pack generation (the cut-over gate's latency probe).
  const preview = url.search.match(previewPattern);
  // A lane may accept a query that names a document variant for browser and edge caches
  // (contract `request_path.accepted_queries`, the building lane's `schema=2`).
  const accepted = lane.acceptedQueries.some((query) => url.search === `?${query}`);
  if (url.search !== "" && !accepted && preview === null) return null;
  const prefix = lane.prefix;
  if (!url.pathname.startsWith(prefix)) return null;
  const candidate = url.pathname.slice(prefix.length);
  const previewGeneration = preview?.[1] === undefined ? null : Number(preview[1]);
  const previewRun = preview?.[2] ?? null;
  const previewState = preview?.[3] ?? null;
  if (lane.keySuffix !== null) {
    // `<pnu>/<suffix>`: the key is the two joined, as the bake writes it (root ADR-0204).
    const [pnu, suffix, ...rest] = candidate.split("/");
    if (pnu === undefined || suffix === undefined || rest.length > 0) return null;
    if (!pnuPattern.test(pnu) || !lane.keySuffix.test(suffix)) return null;
    return { pnu: `${pnu}${suffix}`, page: 1, previewGeneration, previewRun, previewState };
  }
  // `<pnu>/<page>`: a later page of a document served as pages (root ADR-0325); 2 to max_pages,
  // without a leading zero, so one page has one path.
  const [pnu = "", page, ...rest] = candidate.split("/");
  if (!pnuPattern.test(pnu) || rest.length > 0) return null;
  if (page === undefined) return { pnu, page: 1, previewGeneration, previewRun, previewState };
  const number = Number(page);
  if (!/^[1-9][0-9]?$/.test(page) || number < 2 || number > pagesPolicy.max_pages) return null;
  return { pnu, page: number, previewGeneration, previewRun, previewState };
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

function isGeneration(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
}

/// The pointer is an outage boundary, not a data boundary: a PNU that was never baked is a
/// 404, but a manifest that cannot be read or does not validate means the whole lane is not
/// serving, and that must surface as 503 rather than as millions of spurious 404s. A v1
/// manifest is a base with no patches.
function parseManifest(raw: unknown, lane: ServedLane = OWN): ServingPlan | null {
  if (typeof raw !== "object" || raw === null) return null;
  const manifest = raw as Record<string, unknown>;
  if (manifest.unit !== lane.unit) return null;
  if (manifest.schema_version === 1) {
    if (!isGeneration(manifest.current_generation)) return null;
    return {
      base: manifest.current_generation,
      prefixLength: 0,
      patches: [],
      fingerprint: `${lane.cacheScope}v${manifest.current_generation}`,
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
    fingerprint: `${lane.cacheScope}${newest === undefined ? `v${manifest.base_generation}` : `v${manifest.base_generation}p${newest.generation}`}`,
  };
  if (manifest.section_packs === undefined) return plan;
  // A block this Worker cannot trust is an outage, never a silent fall back to objects.
  const packs = parseSectionPacks(manifest.section_packs, patchPolicy.manifest_patch_ceiling, lane);
  if (packs === null) return null;
  return { ...plan, packs, fingerprint: `${plan.fingerprint}-${packs.fingerprint}` };
}

/// The plan this isolate last parsed for each lane, by the R2 entity tag of the manifest bytes it
/// was parsed from (root ADR-0233): a request whose manifest copy carries the same tag reuses it
/// instead of parsing the manifest again, whose patch unit lists grow with the data (2026-10-14:
/// 1.5-5.7 ms per request for the parcel manifest's 16,000 units). Freshness is still the edge
/// copy's: a new manifest arrives with a new tag.
const planMemory = new Map<string, { version: string; plan: ServingPlan }>();
const MANIFEST_VERSION_HEADER = "X-Manifest-Etag";

/// Forgets every parsed plan (tests, to stand for a new isolate).
function forgetPlans(): void {
  planMemory.clear();
}

async function resolvePlan(reads: PackReads, lane: ServedLane): Promise<ServingPlan> {
  const known = planMemory.get(lane.manifestObject);
  const cached = await caches.default.match(manifestCacheUrl(lane));
  if (cached !== undefined) {
    const version = cached.headers.get(MANIFEST_VERSION_HEADER);
    if (version !== null && version === known?.version) {
      // The copy's bytes are not read; cancelling them is not waited on (a teed body waits for its twin).
      void cached.body?.cancel().catch(() => undefined);
      return known.plan;
    }
    const plan = parseManifest(await cached.json().catch(() => null), lane);
    if (plan !== null) {
      if (version !== null) planMemory.set(lane.manifestObject, { version, plan });
      return plan;
    }
  }
  // The manifest is read under the same bounded retries as the packs (contract
  // `read_path.r2_retry_scope`): one transient R2 error on it answered 503 at once (2026-10-18).
  const object = await withRetry(reads, { section: "manifest", key: lane.manifestObject, phase: "manifest" }, async () => {
    const got = await reads.bucket.get(lane.manifestObject);
    return got === null || !("body" in got) ? null : { text: await got.text(), etag: got.etag };
  }).catch((error: unknown) => {
    throw new LaneOutage(error instanceof PackReadUnavailable ? "manifest-unavailable" : "unexpected");
  });
  if (object === null) throw new LaneOutage("manifest-invalid");
  let plan: ServingPlan | null = null;
  try {
    plan = parseManifest(JSON.parse(object.text), lane);
  } catch {
    plan = null;
  }
  if (plan === null) throw new LaneOutage("manifest-invalid");
  planMemory.set(lane.manifestObject, { version: object.etag, plan });
  const response = new Response(object.text, {
    headers: {
      "Cache-Control": `max-age=${policy.manifest_edge_cache_seconds}`,
      "Content-Type": policy.content_type,
      [MANIFEST_VERSION_HEADER]: object.etag,
    },
  });
  reads.ctx.waitUntil(caches.default.put(manifestCacheUrl(lane), response));
  return plan;
}

/// A lane that cannot answer at all, in the class its 503 names (contract `read_path.outage_header`).
class LaneOutage extends Error {
  constructor(readonly outage: string) {
    super(outage);
  }
}

/// A 503 that says why (contract `read_path.outage_header`), live and preview alike.
function outageResponse(outage: string): Response {
  return new Response(null, {
    status: 503,
    headers: { "Cache-Control": "no-store", [packPolicy.read_path.outage_header]: outage },
  });
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
    // A parted section's index (root ADR-0163): read once per isolate, so named on its own.
    for (const [section, source] of trace.parts) {
      parts.push(`parts-${section};dur=${trace.partsR2Ms.get(section) ?? 0};desc="${source} gets=${trace.partsGets}"`);
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
  { pnu, page }: Pick<CanonicalRequest, "pnu" | "page">,
  origin: string | null,
  allowed: ReadonlySet<string>,
  timing: { started: number } | null,
  reads: PackReads = packReads(bucket, ctx),
): Promise<Response> {
  const timed = (response: Response, outcome: string): Response => {
    if (timing !== null) response.headers.set("Server-Timing", serverTiming(reads.trace, timing.started, outcome));
    return response;
  };
  let resolved: Resolved;
  try {
    // A PNU read before under this served state is one edge lookup, as on the object path; a
    // tombstone or an absence is never copied, so only documents answer from here.
    const cached = await answerFromCache(plan.fingerprint, pnu, page);
    if (cached !== null) {
      reads.trace.note(lanePacks.anchor_section, "edge-answer");
      resolved = { kind: "document", ...cached };
    } else {
      resolved = await resolvePacks(reads, plan, pnu, page);
      if (resolved.kind === "document") {
        ctx.waitUntil(caches.default.put(answerCacheUrl(plan.fingerprint, pnu, page), answerCacheResponse(resolved)));
      }
    }
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
      timed(outageResponse(outage), outage),
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
  // it goes out as it is, nothing decompressed or parsed (root ADR-0151), to every client. One that
  // does not accept gzip is answered by the edge's own conversion, which weakens the tag of what
  // it converts: the Worker spends no CPU on a body, whose size it does not bound (root ADR-0233;
  // a building document reaches 11.5 MB decompressed, 5.8 ms of gunzip per MB).
  const etag = `"${resolved.etag}"`;
  const headers = corsHeaders(origin, allowed);
  headers.set("Cache-Control", policy.cache_control);
  headers.set("Content-Type", policy.content_type);
  headers.set("ETag", etag);
  headers.set("X-Content-Type-Options", "nosniff");
  headers.append("Vary", "Accept-Encoding");
  if (matchesEtag(request, etag)) {
    return timed(new Response(null, { status: 304, headers }), "not-modified");
  }
  headers.set("Content-Encoding", "gzip");
  headers.set("Content-Length", resolved.member.byteLength.toString());
  const body = request.method === "HEAD" ? null : resolved.member;
  return timed(new Response(body, { status: 200, headers, encodeBody: "manual" }), "document");
}

function capabilities(lane: ServedLane): Response {
  return new Response(
    `${JSON.stringify({ unit: lane.unit, manifest_schema_versions: MANIFEST_SCHEMA_VERSIONS })}\n`,
    {
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": policy.content_type,
        "X-Content-Type-Options": "nosniff",
      },
    },
  );
}

/// Every answer names the Worker version that produced it (`version_header`, root ADR-0157), so a
/// canary step counts its pinned reads by the version that served them. Set on the answer as it
/// leaves: an edge copy is shared by every version and never carries one.
function withVersion(response: Response, env: Env): Response {
  const version = workerVersion(env);
  if (version === null) return response;
  // A copy: a cache hit's headers are immutable, and setting one threw (500 on every cached photo).
  const versioned = new Response(response.body, response);
  versioned.headers.set(policy.version_header, version);
  return versioned;
}

/// This Worker version's id from the version metadata binding, `null` when it holds none.
function workerVersion(env: Env): string | null {
  const metadata = env[policy.version_metadata_binding];
  if (typeof metadata === "object" && "id" in metadata && typeof metadata.id === "string" && metadata.id !== "") {
    return metadata.id;
  }
  return null;
}

/// A content-addressed media blob served on the parcel edge (root ADR-0296 §5, ADR-0297 §5): the
/// request path is the R2 key under the lakehouse bucket this Worker already binds, the file name is
/// the content hash, so it answers public and immutable with no new host. `null` when the path is
/// not a media path (the request then falls through to the by-PNU lanes). Read only — never
/// put/list/delete — so the Worker keeps its pack-only stance (root ADR-0274).
async function serveMedia(request: Request, env: Env, ctx: ExecutionContext, url: URL): Promise<Response | null> {
  if (LANE !== mediaPolicy.lane || url.search !== "" || !mediaPath.test(url.pathname)) return null;
  if (!["GET", "HEAD"].includes(request.method)) {
    return new Response(null, { status: 405, headers: { Allow: "GET, HEAD" } });
  }
  const bucket = env[mediaPolicy.r2_binding];
  if (typeof bucket !== "object" || bucket === null || !("get" in bucket)) {
    return new Response(null, { status: 500 });
  }
  const key = url.pathname.slice(1);
  const cacheRequest = new Request(url.toString());
  if (request.method === "GET") {
    const cached = await caches.default.match(cacheRequest);
    if (cached !== undefined) return cached;
  }
  const object = await bucket.get(key, { onlyIf: request.headers });
  if (object === null) return new Response(null, { status: 404 });
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("Cache-Control", mediaPolicy.cache_control);
  headers.set("ETag", object.httpEtag);
  headers.set("X-Content-Type-Options", "nosniff");
  if (!("body" in object)) return new Response(null, { status: 304, headers });
  headers.set("Content-Length", object.size.toString());
  const response = new Response(object.body, { status: 200, headers });
  if (request.method === "GET") ctx.waitUntil(caches.default.put(cacheRequest, response.clone()));
  return response;
}

async function fetchByPnu(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  return withVersion(await answerByPnu(request, env, ctx), env);
}

async function answerByPnu(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const started = Date.now();
  const url = new URL(request.url);
  // Content-addressed media (root ADR-0296 §5 / ADR-0297 §5) answers before the by-PNU lanes; a
  // path that is not a media path falls through to them.
  const media = await serveMedia(request, env, ctx, url);
  if (media !== null) return media;
  // One Worker serves its own lane and the lanes derived from its block (root ADR-0193 §4).
  const lane = laneOfPath(url.pathname);
  if (lane === null) return new Response(null, { status: 404 });
  if (url.pathname === lane.capabilities && url.search === "" && ["GET", "HEAD"].includes(request.method)) {
    return capabilities(lane);
  }
  const canonical = canonicalRequest(url, lane);
  if (canonical === null) return new Response(null, { status: 404 });
  const { previewGeneration, previewRun, previewState } = canonical;
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

  // A preview version's read of an unpublished generation (the cut-over gate's probe, root
  // ADR-0147 §6) is planned from the generation alone, never from the manifest: a pack-only lane
  // (root ADR-0190, ADR-0193) has no manifest until its first publish, which that very gate opens.
  // 2026-10-13 every zoning probe read answered 503 here, the manifest read failing first.
  const timing = env[lanePacks.preview_binding] === "true" ? { started } : null;
  if (previewGeneration !== null && timing !== null) {
    const preview = previewPlan(previewGeneration, workerVersion(env), lane, previewRun);
    return packResponse(request, bucket, ctx, preview, canonical, origin, allowed, timing);
  }
  // Root ADR-0274 §5: one recorded manifest state, served exactly as its packs and patches say, so the
  // rollback rehearsal reads the state a rollback would serve. Only a preview version reads the
  // history; the live route never does and answers 404.
  if (previewState !== null) {
    const missing = () => withCors(new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } }), origin, allowed);
    if (timing === null) return missing();
    const stored = await bucket.get(`${lane.manifestHistoryDir}/${previewState}.json`).catch(() => null);
    const recorded = stored === null || !("body" in stored) ? null : parseManifest(await stored.json().catch(() => null), lane);
    if (recorded?.packs === undefined) return missing();
    const state = { ...recorded.packs, fingerprint: `${recorded.packs.fingerprint}-preview-h-${workerVersion(env) ?? "local"}` };
    return packResponse(request, bucket, ctx, state, canonical, origin, allowed, timing);
  }

  // One request, one R2 deadline: the manifest read and the pack reads share it.
  const reads = packReads(bucket, ctx);
  let plan: ServingPlan;
  try {
    plan = await resolvePlan(reads, lane);
  } catch (error) {
    const outage = error instanceof LaneOutage ? error.outage : "unexpected";
    console.log(JSON.stringify({ event: "lane_outage", error_class: outage, failures: reads.trace.failures }));
    return withCors(outageResponse(outage), origin, allowed);
  }

  // Every lane answers from section packs or not at all (root ADR-0190, ADR-0274): the pack bake
  // no longer writes the per-PNU objects, so a manifest without packs is an outage, never an
  // object read. The rollback lever is the previous pack state, published as a manifest.
  const packs = plan.packs;
  // The live route serves a named generation only when the manifest already names it.
  if (previewGeneration !== null) {
    if (packs === undefined || !packs.sections.every((section) => section.generation === previewGeneration)) {
      return withCors(new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } }), origin, allowed);
    }
  }
  if (packs === undefined) return withCors(outageResponse("no-section-packs"), origin, allowed);
  // The per-PNU edge copy is the member itself under the pack plan's fingerprint (packResponse).
  return packResponse(request, bucket, ctx, packs, canonical, origin, allowed, timing, reads);
}

/// The request path's pure steps, run once per isolate at startup (root ADR-0233): global scope
/// has its own startup CPU limit, so a request's CPU no longer carries their first calls. A
/// measuring Worker, same hour, about 600 fresh isolates a side (2026-10-15): the isolate's first
/// document request p50/p90/p99 9/14/19 ms before, 7/13/17 ms after, mean 9.8 -> 8.3 ms; warm
/// requests unchanged. The I/O a first request also pays (first R2 and cache use) cannot run here.
/// `true` when every step answered as it does for a request.
function warmRequestPath(): boolean {
  // The reserved region (99999): never a real parcel.
  const pnu = "9999910100100010000";
  let answered = warmPackPath(pnu);
  for (const lane of LANES) {
    const url = new URL(`${cacheOrigin}${lane.prefix}${pnu}?${packPolicy.preview_query_parameter}=g1-rwarm`);
    answered &&= laneOfPath(url.pathname) === lane && canonicalRequest(url, lane)?.pnu === pnu;
    const plan = previewPlan(1, "warm-up", lane, "warm");
    answerCacheUrl(plan.fingerprint, pnu);
    for (const section of plan.sections) packKey(section, null, pnu.slice(0, plan.unitLength), lane.packRoot);
  }
  const allowed = parseAllowedOrigins("https://warm-up.invalid");
  answered &&= allowed !== null && corsHeaders("https://warm-up.invalid", allowed).has("Access-Control-Allow-Origin");
  serverTiming(new ReadTrace(), Date.now(), "document");
  new Response(new Uint8Array(1), { headers: { "Content-Encoding": "gzip", ETag: '"warm-up"' } });
  return answered;
}

try {
  warmRequestPath();
} catch {
  // A warm-up that fails never stops the Worker: its first request pays the first calls instead.
}

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return fetchByPnu(request, env, ctx);
  },
};

export {
  canonicalPnu,
  forgetPlans,
  canonicalRequest,
  corsHeaders,
  fetchByPnu,
  MANIFEST_SCHEMA_VERSIONS,
  parseAllowedOrigins,
  parseManifest,
  warmRequestPath,
};
