import { readFile } from "node:fs/promises";

import { Miniflare } from "miniflare";
import { afterAll, beforeEach, describe, expect, it } from "vitest";

import connectionContract from "../../../config/r2-connections.contract.json";
import worker from "../src/index";

const policy = connectionContract.map_edit_gateway;
const token = "synthetic-writer-token-0123456789abcdef";
const origin = "https://app.example.test";
const base = "https://map-edits.example.test";
const featureA = "00000000-0000-5000-8000-000000000001";
const featureB = "00000000-0000-5000-8000-000000000002";
const releaseId = "00000000-0000-7000-8000-000000000001";
// The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py.
const square = {
  type: "Polygon",
  coordinates: [[[127.123, 36.123], [127.124, 36.123], [127.124, 36.124], [127.123, 36.124], [127.123, 36.123]]],
};

const mf = new Miniflare({
  modules: true,
  script: "export default { fetch() { return new Response(null); } }",
  d1Databases: { [policy.d1_binding]: "map-edit-test" },
});
let db: D1Database;
let env: Record<string, string | D1Database>;

async function applyMigrations(database: D1Database): Promise<void> {
  const sql = await readFile(new URL("../migrations/0001_map_edit.sql", import.meta.url), "utf8");
  for (const chunk of sql.split(/\n\s*\n/)) {
    const statement = chunk.split("\n").filter((line) => !line.startsWith("--")).join("\n").trim();
    if (statement !== "") await database.prepare(statement).run();
  }
}

beforeEach(async () => {
  db = (await mf.getD1Database(policy.d1_binding)) as unknown as D1Database;
  for (const name of ["map_edit", "map_edit_fold"]) await db.prepare(`DROP TABLE IF EXISTS ${name}`).run();
  await applyMigrations(db);
  env = {
    [policy.d1_binding]: db,
    [policy.allowed_origins_binding]: origin,
    [policy.write_token_binding]: token,
  };
});

afterAll(async () => {
  await mf.dispose();
});

function call(path: string, init: RequestInit = {}): Promise<Response> {
  return worker.fetch(new Request(`${base}${path}`, init), env as never);
}

function write(path: string, body: unknown, headers: Record<string, string> = {}): Promise<Response> {
  return call(path, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

function upsert(featureId: string, key: string, code = "SYN-0001") {
  return write("/edits/complex", {
    feature_id: featureId, op: "upsert", geometry: square,
    properties: { official_complex_code: code }, editor: "synthetic-staff", idempotency_key: key,
  });
}

async function overlay(headers: Record<string, string> = {}) {
  const response = await call("/overlay/complex", { headers: { Origin: origin, ...headers } });
  return { response, body: response.status === 200 ? await response.json() as Record<string, any> : null };
}

describe("overlay of unfolded edits", () => {
  it("is empty before any edit and names the unit's id property", async () => {
    const { response, body } = await overlay();
    expect(response.status).toBe(200);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe(origin);
    expect(response.headers.get("Cache-Control")).toBe(policy.overlay_cache_control);
    expect(body).toEqual({
      unit: "complex", feature_id_property: "complex_id", folded_through_change_seq: 0, latest_change_seq: 0,
      hidden_feature_ids: [], features: { type: "FeatureCollection", features: [] },
    });
  });

  it("hides every edited id, draws only upserts, and uses each feature's newest edit", async () => {
    expect((await upsert(featureA, "k1", "SYN-OLD")).status).toBe(201);
    expect((await upsert(featureA, "k2", "SYN-NEW")).status).toBe(201);
    expect((await write("/edits/complex", {
      feature_id: featureB, op: "delete", editor: "synthetic-staff", idempotency_key: "k3",
    })).status).toBe(201);
    const { body } = await overlay();
    expect(body?.hidden_feature_ids).toEqual([featureA, featureB]);
    expect(body?.features.features).toEqual([{
      type: "Feature",
      properties: { official_complex_code: "SYN-NEW", complex_id: featureA },
      geometry: square,
    }]);
    expect(body?.latest_change_seq).toBe(3);
  });

  it("answers 304 to the current version and a new version after an edit", async () => {
    const first = await overlay();
    const etag = first.response.headers.get("ETag") ?? "";
    expect((await overlay({ "If-None-Match": etag })).response.status).toBe(304);
    await upsert(featureA, "k1");
    expect((await overlay({ "If-None-Match": etag })).response.status).toBe(200);
  });

  it("refuses foreign origins, unknown units, queries and writes", async () => {
    expect((await call("/overlay/complex", { headers: { Origin: "https://evil.example.test" } })).status).toBe(403);
    expect((await call("/overlay/unknown")).status).toBe(404);
    expect((await call("/overlay/complex?x=1")).status).toBe(404);
    expect((await call("/overlay/complex", { method: "POST" })).status).toBe(405);
  });
});

describe("appending edits", () => {
  it("requires the writer token and refuses browser origins", async () => {
    const body = { feature_id: featureA, op: "delete", editor: "synthetic-staff", idempotency_key: "k1" };
    expect((await write("/edits/complex", body, { Authorization: "Bearer wrong" })).status).toBe(401);
    expect((await write("/edits/complex", body, { Authorization: "" })).status).toBe(401);
    expect((await write("/edits/complex", body, { Origin: origin })).status).toBe(403);
    const { body: after } = await overlay();
    expect(after?.hidden_feature_ids).toEqual([]);
  });

  it("fails closed when the token is not configured", async () => {
    env[policy.write_token_binding] = "short";
    expect((await upsert(featureA, "k1")).status).toBe(500);
  });

  it("replays an idempotency key and refuses its reuse for a different edit", async () => {
    const first = await upsert(featureA, "k1");
    const replay = await upsert(featureA, "k1");
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual(await first.json());
    expect((await upsert(featureA, "k1", "SYN-OTHER")).status).toBe(409);
  });

  it.each([
    ["an id outside the unit's grammar", { feature_id: "not-a-uuid" }, "invalid_feature_id"],
    ["an open ring", { geometry: { type: "Polygon", coordinates: [square.coordinates[0]!.slice(0, 4)] } }, "ring_not_closed"],
    ["a point outside the national bounds", { geometry: { type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] } }, "out_of_bounds"],
    ["a third dimension", { geometry: { type: "Polygon", coordinates: [square.coordinates[0]!.map(([x, y]) => [x, y, 0])] } }, "invalid_position"],
    ["a line", { geometry: { type: "LineString", coordinates: [[127.123, 36.123], [127.124, 36.124]] } }, "unsupported_geometry"],
    ["an undeclared property", { properties: { official_complex_code: "SYN", name: "x" } }, "unknown_property"],
    ["a missing property", { properties: {} }, "invalid_properties"],
  ])("refuses %s", async (_label, patch, code) => {
    const response = await write("/edits/complex", {
      feature_id: featureA, op: "upsert", geometry: square, properties: { official_complex_code: "SYN" },
      editor: "synthetic-staff", idempotency_key: "k1", ...patch,
    });
    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({ error: code });
  });

  it("refuses a delete that carries geometry", async () => {
    const response = await write("/edits/complex", {
      feature_id: featureA, op: "delete", geometry: square, editor: "synthetic-staff", idempotency_key: "k1",
    });
    expect(response.status).toBe(422);
  });

  it("refuses a vertex count above the contract limit", async () => {
    const ring = Array.from({ length: policy.geometry.max_vertices + 1 }, (_, index) =>
      [127.123 + (index % 1000) / 1_000_000, 36.123]);
    ring.push(ring[0]!);
    const response = await write("/edits/complex", {
      feature_id: featureA, op: "upsert", geometry: { type: "Polygon", coordinates: [ring] },
      properties: { official_complex_code: "SYN" }, editor: "synthetic-staff", idempotency_key: "k1",
    });
    expect([413, 422]).toContain(response.status);
  });
});

describe("folding", () => {
  it("lists edits for the ledger export in change order", async () => {
    await upsert(featureA, "k1");
    await upsert(featureB, "k2");
    const response = await call("/edits/complex?after=1", { headers: { Authorization: `Bearer ${token}` } });
    const body = await response.json() as { edits: Array<{ change_seq: number; feature_id: string; geometry: unknown }> };
    expect(body.edits.map((edit) => [edit.change_seq, edit.feature_id])).toEqual([[2, featureB]]);
    expect(body.edits[0]?.geometry).toEqual(square);
    expect((await call("/edits/complex?after=-1", { headers: { Authorization: `Bearer ${token}` } })).status).toBe(400);
  });

  it("retires folded rows so the overlay shows only what the tiles do not contain yet", async () => {
    await upsert(featureA, "k1");
    await upsert(featureB, "k2");
    const fold = await write("/folds/complex", { folded_through_change_seq: 1, release_id: releaseId });
    expect(fold.status).toBe(200);
    const { body } = await overlay();
    expect(body?.hidden_feature_ids).toEqual([featureB]);
    expect(body?.folded_through_change_seq).toBe(1);
    const rows = await db.prepare("SELECT change_seq FROM map_edit ORDER BY change_seq").all<{ change_seq: number }>();
    expect(rows.results.map((row) => row.change_seq)).toEqual([2]);
  });

  it("never shows a row the recorded fold covers, even if it was not retired", async () => {
    await upsert(featureA, "k1");
    await upsert(featureB, "k2");
    await db.prepare(
      "INSERT INTO map_edit_fold (unit, folded_through_change_seq, release_id, folded_at) VALUES ('complex', 1, ?1, '2099-01-01T00:00:00Z')",
    ).bind(releaseId).run();
    const { body } = await overlay();
    expect(body?.hidden_feature_ids).toEqual([featureB]);
  });

  it("refuses a fold that moves backwards or claims an edit that does not exist", async () => {
    await upsert(featureA, "k1");
    await upsert(featureB, "k2");
    expect((await write("/folds/complex", { folded_through_change_seq: 3, release_id: releaseId })).status).toBe(409);
    expect((await write("/folds/complex", { folded_through_change_seq: 2, release_id: releaseId })).status).toBe(200);
    expect((await write("/folds/complex", { folded_through_change_seq: 1, release_id: releaseId })).status).toBe(409);
    // Re-recording the same fold is a no-op, so a retried fold job is safe.
    expect((await write("/folds/complex", { folded_through_change_seq: 2, release_id: releaseId })).status).toBe(200);
  });

  it("stops accepting edits at the pending limit until a fold catches up", async () => {
    await db.prepare(
      `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?1)
       INSERT INTO map_edit (unit, feature_id, op, geometry, properties, editor, edited_at, idempotency_key, request_sha256)
       SELECT 'complex', ?2, 'delete', NULL, '{}', 'synthetic-staff', '2099-01-01T00:00:00Z', 'bulk-' || i, printf('%064d', 0) FROM n`,
    ).bind(policy.max_pending_edits_per_unit, featureA).run();
    const refused = await upsert(featureB, "over-limit");
    expect(refused.status).toBe(409);
    expect(await refused.json()).toEqual({ error: "fold_required" });
    await write("/folds/complex", { folded_through_change_seq: 1, release_id: releaseId });
    expect((await upsert(featureB, "over-limit")).status).toBe(201);
  });
});

describe("the store itself refuses what the worker must never do", () => {
  it("rejects rewriting an edit and deleting one the tiles do not contain", async () => {
    await upsert(featureA, "k1");
    await expect(db.prepare("UPDATE map_edit SET editor = 'x'").run()).rejects.toThrow(/append-only/);
    await expect(db.prepare("DELETE FROM map_edit").run()).rejects.toThrow(/not folded/);
  });
});
