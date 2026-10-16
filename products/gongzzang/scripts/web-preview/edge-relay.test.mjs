import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { EDGES, RUM, relayRequest } from "./edge-relay.mjs";

const ORIGIN = "https://preview.example.test";

test("an edge path goes to its edge with the query, and without the browser's Origin or cookies", () => {
  const upstream = relayRequest(
    new Request(`${ORIGIN}/edge/buildings/buildings/by-pnu/0000000000000000000?schema=2`, {
      headers: {
        origin: ORIGIN,
        cookie: "a=b",
        "if-none-match": '"v1"',
        accept: "application/json",
      },
    }),
  );
  assert.ok(upstream instanceof Request);
  assert.equal(
    upstream.url,
    "https://buildings.perfectory.io/buildings/by-pnu/0000000000000000000?schema=2",
  );
  assert.equal(upstream.headers.get("origin"), null);
  assert.equal(upstream.headers.get("cookie"), null);
  assert.equal(upstream.headers.get("if-none-match"), '"v1"');
  assert.equal(upstream.headers.get("accept"), "application/json");
});

test("every edge name relays to its own host", () => {
  for (const [name, host] of Object.entries(EDGES)) {
    const upstream = relayRequest(new Request(`${ORIGIN}/edge/${name}/x/1`));
    assert.equal(upstream.url, `https://${host}/x/1`);
  }
});

test("anything else is not relayed", () => {
  for (const path of [
    "/listings",
    "/edge/tiles",
    "/edge/unknown/x",
    "/edge/constructor/x",
    "/x/edge/tiles/1",
  ]) {
    assert.equal(relayRequest(new Request(`${ORIGIN}${path}`)), null, path);
  }
});

test("only reads are relayed", () => {
  const answer = relayRequest(
    new Request(`${ORIGIN}/edge/map-edits/overlay/admin`, { method: "POST" }),
  );
  assert.equal(answer.status, 405);
});

test("the real-user beacon is the one write relayed, with the browser's country and nothing else", async () => {
  const request = new Request(`${ORIGIN}/edge/map-edits/rum`, {
    method: "POST",
    headers: { origin: ORIGIN, cookie: "a=b", "content-type": "text/plain;charset=UTF-8" },
    body: '{"v":1,"m":[{"n":"LCP","v":1}]}',
  });
  Object.defineProperty(request, "cf", { value: { country: "KR" } });
  const upstream = relayRequest(request);
  assert.ok(upstream instanceof Request);
  assert.equal(upstream.url, "https://map-edits.perfectory.io/rum");
  assert.equal(upstream.method, "POST");
  assert.equal(upstream.headers.get("x-rum-viewer-country"), "KR");
  assert.equal(upstream.headers.get("origin"), null);
  assert.equal(upstream.headers.get("cookie"), null);
  assert.equal(await upstream.text(), '{"v":1,"m":[{"n":"LCP","v":1}]}');
  for (const path of ["/edge/map-edits/rum?x=1", "/edge/parcels/rum", "/edge/map-edits/rum/x"]) {
    assert.equal(
      relayRequest(new Request(`${ORIGIN}${path}`, { method: "POST" })).status,
      405,
      path,
    );
  }
});

test("the relayed hosts are the contract's public hostnames", () => {
  const contract = JSON.parse(
    readFileSync(
      new URL(
        "../../../../platforms/foundation-platform/config/r2-connections.contract.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  assert.deepEqual(EDGES, {
    tiles: contract.vector_tile_gateway.public_hostname,
    parcels: contract.parcel_by_pnu_gateway.public_hostname,
    buildings: contract.building_by_pnu_gateway.public_hostname,
    "unit-prices": contract.by_pnu_lanes["unit-price"].public_hostname,
    "map-edits": contract.map_edit_gateway.public_hostname,
  });
  assert.equal(RUM.path, contract.map_edit_gateway.real_user_metrics.path);
});
