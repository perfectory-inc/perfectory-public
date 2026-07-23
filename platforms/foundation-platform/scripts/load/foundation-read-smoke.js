import http from "k6/http";
import { check, sleep } from "k6";

const baseUrl = __ENV.FOUNDATION_PLATFORM_API_URL || "http://localhost:8080";
const complexId = __ENV.FOUNDATION_PLATFORM_TEST_COMPLEX_ID || "";
const markerTilePath =
  __ENV.FOUNDATION_PLATFORM_MARKER_TILE_PATH ||
  "/map/v1/marker-tiles/parcel_anchor/12/3494/1591.pbf?filter_hash=all-active-v1";

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    health: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.FOUNDATION_PLATFORM_LOAD_HEALTH_RPS || 5),
      timeUnit: "1s",
      duration: __ENV.FOUNDATION_PLATFORM_LOAD_DURATION || "5m",
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: "health",
    },
    hot_reads: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.FOUNDATION_PLATFORM_LOAD_READ_RPS || 20),
      timeUnit: "1s",
      duration: __ENV.FOUNDATION_PLATFORM_LOAD_DURATION || "5m",
      preAllocatedVUs: 25,
      maxVUs: 150,
      exec: "hotReads",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500", "p(99)<1500"],
  },
};

export function health() {
  const res = http.get(`${baseUrl}/healthz`, { tags: { route: "health" } });
  check(res, { "health 200": (r) => r.status === 200 });
}

export function hotReads() {
  const manifest = http.get(`${baseUrl}/catalog/v1/vector-tiles/manifest`, {
    tags: { route: "vector_tile_manifest" },
  });
  check(manifest, {
    "manifest ok or missing fixture": (r) => r.status === 200 || r.status === 404,
  });

  const graph = http.get(`${baseUrl}/catalog/v1/pipeline-graph`, {
    tags: { route: "pipeline_graph" },
  });
  check(graph, { "pipeline graph 200": (r) => r.status === 200 });

  const markerContract = http.get(`${baseUrl}/map/v1/marker-tiles/contract`, {
    tags: { route: "marker_tile_contract" },
  });
  check(markerContract, {
    "marker contract 200": (r) => r.status === 200,
    "marker contract uses PNU anchor": (r) =>
      r.body.includes('"position_source":"pnu_anchor"') &&
      r.body.includes('"response_format":"mvt_pbf"'),
  });

  const markerTile = http.get(`${baseUrl}${markerTilePath}`, {
    responseType: "binary",
    tags: { route: "parcel_anchor_marker_tile" },
  });
  check(markerTile, {
    "marker tile 200": (r) => r.status === 200,
    "marker tile protobuf content type": (r) =>
      String(r.headers["Content-Type"] || "").includes("application/x-protobuf"),
    "marker tile cache-control": (r) =>
      String(r.headers["Cache-Control"] || "").includes("public") &&
      String(r.headers["Cache-Control"] || "").includes("max-age"),
    "marker tile non-empty": (r) => r.body && r.body.byteLength > 0,
  });

  if (complexId) {
    const complex = http.get(`${baseUrl}/catalog/v1/complexes/${complexId}`, {
      tags: { route: "complex_detail" },
    });
    check(complex, { "complex detail 200": (r) => r.status === 200 });
  }

  sleep(0.1);
}
