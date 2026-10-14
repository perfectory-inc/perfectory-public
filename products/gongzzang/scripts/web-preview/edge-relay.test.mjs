import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { EDGES, relayRequest } from "./edge-relay.mjs";

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
});
