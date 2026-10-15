// The preview's origin is not on the public edges' CORS allowlists, and widening those production
// lists is exactly what the preview must not do. So the browser asks this Worker same-origin and
// the Worker asks the edge without an Origin header, the way any server does.
//
// The hostnames are the `public_hostname`s of platforms/foundation-platform/config/
// r2-connections.contract.json; edge-relay.test.mjs fails when they drift.
export const EDGES = {
  tiles: "tiles.perfectory.io",
  parcels: "catalog.perfectory.io",
  buildings: "buildings.perfectory.io",
  "unit-prices": "unit-prices.perfectory.io",
  "map-edits": "map-edits.perfectory.io",
};

const RELAY = /^\/edge\/([a-z-]+)(\/.*)$/;
const FORWARDED_HEADERS = ["accept", "if-none-match"];

/** The upstream request for a relayed path, or null when the path is not a relayed edge. */
export function relayRequest(request) {
  const url = new URL(request.url);
  const match = RELAY.exec(url.pathname);
  if (!match || !Object.hasOwn(EDGES, match[1])) return null;
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response(null, { status: 405, headers: { allow: "GET, HEAD" } });
  }
  const headers = new Headers();
  for (const name of FORWARDED_HEADERS) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  return new Request(`https://${EDGES[match[1]]}${match[2]}${url.search}`, {
    method: request.method,
    headers,
  });
}
