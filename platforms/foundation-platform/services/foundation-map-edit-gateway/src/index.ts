import connectionContract from "../../../config/r2-connections.contract.json";

// ADR-0112: edits not yet folded into the R2 base tiles live here. Customers read them as an
// overlay (ids to hide + fresh polygons); only the trusted writer (foundation-api, after its own
// authorization and topology checks) may append edits, read them for the ledger export, or record
// a fold. The tile path never reads this store.
const policy = connectionContract.map_edit_gateway;
type UnitKey = keyof typeof policy.units;
interface UnitPolicy {
  feature_id_property: string;
  feature_id_pattern: string;
  properties: Readonly<Record<string, string>>;
}
const units: Readonly<Record<string, UnitPolicy>> = policy.units;
const featureIdPatterns = new Map(
  Object.entries(units).map(([unit, spec]) => [unit, new RegExp(`^(?:${spec.feature_id_pattern})$`)]),
);
const [minLon, minLat, maxLon, maxLat] = policy.geometry.bounds_wgs84 as [number, number, number, number];
const keyPattern = /^[A-Za-z0-9._:-]{1,128}$/;
const releasePattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const minWriteTokenLength = 32;
const maxEditsPage = 1000;

interface Env {
  [binding: string]: string | D1Database;
}

type Position = [number, number];
type Ring = Position[];
type Geometry =
  | { type: "Polygon"; coordinates: Ring[] }
  | { type: "MultiPolygon"; coordinates: Ring[][] };

interface EditRow {
  change_seq: number;
  feature_id: string;
  op: "upsert" | "delete";
  geometry: string | null;
  properties: string;
  editor: string;
  edited_at: string;
}

class Rejected extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(code);
  }
}

function json(status: number, body: unknown, headers?: Headers): Response {
  const out = headers ?? new Headers();
  out.set("Content-Type", policy.content_type);
  out.set("X-Content-Type-Options", "nosniff");
  if (!out.has("Cache-Control")) out.set("Cache-Control", "no-store");
  return new Response(JSON.stringify(body), { status, headers: out });
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

function binding(env: Env): D1Database {
  const db = env[policy.d1_binding];
  if (typeof db !== "object" || db === null || !("prepare" in db)) throw new Rejected(500, "misconfigured");
  return db;
}

function unitOf(name: string | undefined): UnitKey | null {
  return name !== undefined && Object.hasOwn(units, name) ? (name as UnitKey) : null;
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function authorizedWriter(request: Request, env: Env): Promise<boolean> {
  const expected = env[policy.write_token_binding];
  if (typeof expected !== "string" || expected.length < minWriteTokenLength) throw new Rejected(500, "misconfigured");
  const header = request.headers.get("Authorization") ?? "";
  const presented = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : "";
  // Hash both sides so the comparison runs over equal lengths whatever was presented, then compare
  // every character so the time taken does not reveal where the first difference is.
  const [a, b] = await Promise.all([sha256Hex(presented), sha256Hex(expected)]);
  let difference = 0;
  for (let index = 0; index < a.length; index += 1) difference |= a.charCodeAt(index) ^ b.charCodeAt(index);
  return difference === 0;
}

function position(value: unknown, counter: { vertices: number }): Position {
  if (!Array.isArray(value) || value.length !== 2) throw new Rejected(422, "invalid_position");
  const [lon, lat] = value as unknown[];
  if (typeof lon !== "number" || typeof lat !== "number" || !Number.isFinite(lon) || !Number.isFinite(lat)) {
    throw new Rejected(422, "invalid_position");
  }
  if (lon < minLon || lon > maxLon || lat < minLat || lat > maxLat) throw new Rejected(422, "out_of_bounds");
  counter.vertices += 1;
  if (counter.vertices > policy.geometry.max_vertices) throw new Rejected(422, "too_many_vertices");
  return [lon, lat];
}

function ring(value: unknown, counter: { vertices: number }): Ring {
  if (!Array.isArray(value) || value.length < 4) throw new Rejected(422, "invalid_ring");
  const positions = value.map((entry) => position(entry, counter));
  const first = positions[0];
  const last = positions[positions.length - 1];
  if (first === undefined || last === undefined || first[0] !== last[0] || first[1] !== last[1]) {
    throw new Rejected(422, "ring_not_closed");
  }
  return positions;
}

function polygon(value: unknown, counter: { vertices: number }): Ring[] {
  if (!Array.isArray(value) || value.length === 0) throw new Rejected(422, "invalid_polygon");
  return value.map((entry) => ring(entry, counter));
}

// Structural checks only. Topology (self-intersection, ring orientation, holes inside shells) is
// the writer's job before it calls this store — one validator, not two.
function geometryOf(value: unknown): Geometry {
  if (typeof value !== "object" || value === null) throw new Rejected(422, "invalid_geometry");
  const { type, coordinates } = value as { type?: unknown; coordinates?: unknown };
  const counter = { vertices: 0 };
  if (!(policy.geometry.types as string[]).includes(String(type))) throw new Rejected(422, "unsupported_geometry");
  if (type === "Polygon") return { type, coordinates: polygon(coordinates, counter) };
  if (!Array.isArray(coordinates) || coordinates.length === 0) throw new Rejected(422, "invalid_geometry");
  return { type: "MultiPolygon", coordinates: coordinates.map((entry) => polygon(entry, counter)) };
}

function propertiesOf(unit: UnitKey, value: unknown): Record<string, string> {
  const declared = units[unit]?.properties ?? {};
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Rejected(422, "invalid_properties");
  const given = value as Record<string, unknown>;
  const keys = Object.keys(given);
  if (keys.some((key) => !Object.hasOwn(declared, key))) throw new Rejected(422, "unknown_property");
  const out: Record<string, string> = {};
  for (const key of Object.keys(declared).sort()) {
    const field = given[key];
    if (typeof field !== "string" || field.length === 0 || field.length > 256) throw new Rejected(422, "invalid_properties");
    out[key] = field;
  }
  return out;
}

async function readJson(request: Request): Promise<Record<string, unknown>> {
  const declared = Number(request.headers.get("Content-Length") ?? "0");
  if (declared > policy.geometry.max_body_bytes) throw new Rejected(413, "body_too_large");
  const text = await request.text();
  if (new TextEncoder().encode(text).byteLength > policy.geometry.max_body_bytes) throw new Rejected(413, "body_too_large");
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Rejected(400, "invalid_json");
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) throw new Rejected(400, "invalid_json");
  return body as Record<string, unknown>;
}

async function appendEdit(request: Request, env: Env, unit: UnitKey): Promise<Response> {
  const body = await readJson(request);
  const { feature_id: featureId, op, editor, idempotency_key: key } = body;
  if (typeof featureId !== "string" || !(featureIdPatterns.get(unit)?.test(featureId) ?? false)) {
    throw new Rejected(422, "invalid_feature_id");
  }
  if (op !== "upsert" && op !== "delete") throw new Rejected(422, "invalid_op");
  if (typeof editor !== "string" || editor.length === 0 || editor.length > 128) throw new Rejected(422, "invalid_editor");
  if (typeof key !== "string" || !keyPattern.test(key)) throw new Rejected(422, "invalid_idempotency_key");
  let geometry: string | null = null;
  let properties: Record<string, string> = {};
  if (op === "upsert") {
    geometry = JSON.stringify(geometryOf(body.geometry));
    properties = propertiesOf(unit, body.properties);
  } else if (body.geometry !== undefined || body.properties !== undefined) {
    throw new Rejected(422, "delete_carries_no_geometry");
  }
  const propertiesText = JSON.stringify(properties);
  const requestSha = await sha256Hex(JSON.stringify([unit, featureId, op, geometry, propertiesText, editor]));
  const db = binding(env);
  const inserted = await db
    .prepare(
      `INSERT INTO map_edit (unit, feature_id, op, geometry, properties, editor, edited_at, idempotency_key, request_sha256)
       SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
       WHERE (SELECT COUNT(*) FROM map_edit WHERE unit = ?1 AND change_seq > COALESCE(
         (SELECT folded_through_change_seq FROM map_edit_fold WHERE unit = ?1), 0)) < ?10
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING change_seq`,
    )
    .bind(unit, featureId, op, geometry, propertiesText, editor, new Date().toISOString(), key, requestSha,
      policy.max_pending_edits_per_unit)
    .first<{ change_seq: number }>();
  if (inserted !== null) return json(201, { unit, change_seq: inserted.change_seq });
  const prior = await db
    .prepare("SELECT change_seq, request_sha256 FROM map_edit WHERE idempotency_key = ?1")
    .bind(key)
    .first<{ change_seq: number; request_sha256: string }>();
  if (prior === null) throw new Rejected(409, "fold_required");
  if (prior.request_sha256 !== requestSha) throw new Rejected(409, "idempotency_key_reused");
  return json(200, { unit, change_seq: prior.change_seq });
}

async function listEdits(url: URL, env: Env, unit: UnitKey): Promise<Response> {
  const keys = [...url.searchParams.keys()];
  if (keys.some((key) => key !== "after" && key !== "limit") || new Set(keys).size !== keys.length) {
    throw new Rejected(400, "invalid_query");
  }
  const after = Number(url.searchParams.get("after") ?? "0");
  const limit = Number(url.searchParams.get("limit") ?? String(maxEditsPage));
  if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > maxEditsPage) {
    throw new Rejected(400, "invalid_query");
  }
  const { results } = await binding(env)
    .prepare(
      `SELECT change_seq, feature_id, op, geometry, properties, editor, edited_at
       FROM map_edit WHERE unit = ?1 AND change_seq > ?2 ORDER BY change_seq LIMIT ?3`,
    )
    .bind(unit, after, limit)
    .all<EditRow>();
  return json(200, {
    unit,
    edits: results.map((row) => ({
      ...row,
      geometry: row.geometry === null ? null : JSON.parse(row.geometry),
      properties: JSON.parse(row.properties),
    })),
  });
}

async function recordFold(request: Request, env: Env, unit: UnitKey): Promise<Response> {
  const body = await readJson(request);
  const through = body.folded_through_change_seq;
  const releaseId = body.release_id;
  if (typeof through !== "number" || !Number.isSafeInteger(through) || through < 0) throw new Rejected(422, "invalid_change_seq");
  if (typeof releaseId !== "string" || !releasePattern.test(releaseId)) throw new Rejected(422, "invalid_release_id");
  const db = binding(env);
  const state = await db
    .prepare(
      `SELECT COALESCE((SELECT MAX(change_seq) FROM map_edit WHERE unit = ?1), 0) AS latest,
              (SELECT folded_through_change_seq FROM map_edit_fold WHERE unit = ?1) AS folded`,
    )
    .bind(unit)
    .first<{ latest: number; folded: number | null }>();
  if (state === null) throw new Rejected(500, "state_unreadable");
  // A fold can only claim edits the store has handed out, and never un-fold what is served.
  if (through < (state.folded ?? 0)) throw new Rejected(409, "fold_moves_backwards");
  if (through > Math.max(state.latest, state.folded ?? 0)) throw new Rejected(409, "fold_beyond_latest_edit");
  // One transaction: the fold becomes visible and the rows it covers retire together.
  await db.batch([
    db
      .prepare(
        `INSERT INTO map_edit_fold (unit, folded_through_change_seq, release_id, folded_at) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (unit) DO UPDATE SET folded_through_change_seq = excluded.folded_through_change_seq,
           release_id = excluded.release_id, folded_at = excluded.folded_at`,
      )
      .bind(unit, through, releaseId, new Date().toISOString()),
    db.prepare("DELETE FROM map_edit WHERE unit = ?1 AND change_seq <= ?2").bind(unit, through),
  ]);
  return json(200, { unit, folded_through_change_seq: through, release_id: releaseId });
}

async function overlay(request: Request, env: Env, unit: UnitKey, headers: Headers): Promise<Response> {
  const db = binding(env);
  const pending = `change_seq > COALESCE((SELECT folded_through_change_seq FROM map_edit_fold WHERE unit = ?1), 0)`;
  // One batch is one transaction, so the version and the rows describe the same instant.
  const [versionResult, rowsResult] = await db.batch([
    db
      .prepare(
        `SELECT COALESCE((SELECT folded_through_change_seq FROM map_edit_fold WHERE unit = ?1), 0) AS folded,
                COALESCE((SELECT MAX(change_seq) FROM map_edit WHERE unit = ?1), 0) AS latest`,
      )
      .bind(unit),
    db
      .prepare(
        `SELECT edit.change_seq, edit.feature_id, edit.op, edit.geometry, edit.properties
         FROM map_edit AS edit
         JOIN (SELECT feature_id, MAX(change_seq) AS change_seq FROM map_edit
               WHERE unit = ?1 AND ${pending} GROUP BY feature_id) AS newest
           ON newest.change_seq = edit.change_seq
         ORDER BY edit.change_seq`,
      )
      .bind(unit),
  ]);
  const version = versionResult?.results[0] as { folded: number; latest: number } | undefined;
  const rows = (rowsResult?.results ?? []) as EditRow[];
  if (version === undefined) throw new Rejected(500, "state_unreadable");
  const latest = Math.max(version.latest, version.folded);
  const etag = `"${unit}-${version.folded}-${latest}"`;
  headers.set("ETag", etag);
  headers.set("Cache-Control", policy.overlay_cache_control);
  const match = request.headers.get("If-None-Match") ?? "";
  if (match.split(",").some((entry) => entry.trim().replace(/^W\//, "") === etag)) {
    return new Response(null, { status: 304, headers });
  }
  const idProperty = units[unit]?.feature_id_property ?? "";
  const body = {
    unit,
    feature_id_property: idProperty,
    folded_through_change_seq: version.folded,
    latest_change_seq: latest,
    hidden_feature_ids: rows.map((row) => row.feature_id),
    features: {
      type: "FeatureCollection",
      features: rows
        .filter((row) => row.op === "upsert" && row.geometry !== null)
        .map((row) => ({
          type: "Feature",
          properties: { ...JSON.parse(row.properties), [idProperty]: row.feature_id },
          geometry: JSON.parse(row.geometry ?? "null"),
        })),
    },
  };
  const response = json(200, body, headers);
  return request.method === "HEAD" ? new Response(null, { status: 200, headers: response.headers }) : response;
}

async function route(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const [, resource, unitName, ...rest] = url.pathname.split("/");
  const unit = unitOf(unitName);
  if (unit === null || rest.length !== 0 || url.hash !== "") return json(404, { error: "not_found" });
  const origin = request.headers.get("Origin");

  if (resource === "overlay") {
    if (url.href.includes("?")) return json(404, { error: "not_found" });
    const rawAllowed = env[policy.allowed_origins_binding];
    const allowed = typeof rawAllowed === "string" ? parseAllowedOrigins(rawAllowed) : null;
    if (allowed === null) throw new Rejected(500, "misconfigured");
    if (origin !== null && !allowed.has(origin)) return new Response(null, { status: 403 });
    const headers = corsHeaders(origin, allowed);
    if (request.method === "OPTIONS") {
      const method = request.headers.get("Access-Control-Request-Method");
      const requested = request.headers.get("Access-Control-Request-Headers");
      if (origin === null || !["GET", "HEAD"].includes(method ?? "") ||
        (requested !== null && requested.toLowerCase() !== "if-none-match")) {
        return new Response(null, { status: 403 });
      }
      headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
      headers.set("Access-Control-Allow-Headers", "If-None-Match");
      headers.set("Access-Control-Max-Age", "86400");
      return new Response(null, { status: 204, headers });
    }
    if (!["GET", "HEAD"].includes(request.method)) {
      headers.set("Allow", "GET, HEAD, OPTIONS");
      return new Response(null, { status: 405, headers });
    }
    return overlay(request, env, unit, headers);
  }

  if (resource !== "edits" && resource !== "folds") return json(404, { error: "not_found" });
  // The write side is server-to-server only; a browser origin never reaches it.
  if (origin !== null) return new Response(null, { status: 403 });
  const allowedMethods = resource === "edits" ? ["GET", "POST"] : ["POST"];
  if (!allowedMethods.includes(request.method)) {
    return new Response(null, { status: 405, headers: { Allow: allowedMethods.join(", ") } });
  }
  if (!(await authorizedWriter(request, env))) return json(401, { error: "unauthorized" });
  if (resource === "folds") return recordFold(request, env, unit);
  if (request.method === "GET") return listEdits(url, env, unit);
  if (url.href.includes("?")) return json(404, { error: "not_found" });
  return appendEdit(request, env, unit);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await route(request, env);
    } catch (error) {
      if (error instanceof Rejected) return json(error.status, { error: error.code });
      return json(500, { error: "internal" });
    }
  },
};

export { corsHeaders, geometryOf, parseAllowedOrigins };
