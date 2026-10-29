
import type { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { startGateway } from "./miniflare";

// A pack-only lane of the contract's `by_pnu_lanes` (root ADR-0190): it never had objects, so a
// manifest without section packs is an outage and an object at the base key is never read, whatever
// the serving binding says. PNUs and snapshot ids sit in the repository-reserved synthetic
// namespaces (`scripts/guard/public-fixture-safety.py`).
const PNU = "9999900000100000000";
const GATEWAY = connectionContract.by_pnu_lanes["unit-price"];
// Root ADR-0204: a unit price document is one building of one PNU, requested as <pnu>/<building id>.
const BUILDING = "00000000-0000-0000-0000-00000000000b";
const URL_OF_PNU = `https://unit-prices.example.test${GATEWAY.request_path.prefix}${PNU}/${BUILDING}`;

function envelope(): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "unit-price-by-pnu",
    base_generation: 1,
    base_object_count: 0,
    document_schema_version: GATEWAY.document_schema_version,
    gold_table: GATEWAY.gold_table,
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: connectionContract.by_pnu_serving_patches.pnu_prefix_length,
    patches: [],
    object_count: 0,
    published_at_utc: "2026-01-01T00:00:00Z",
  })}\n`;
}

describe("foundation unit price gateway (pack-only)", () => {
  let runtime: Miniflare | undefined;

  async function start(): Promise<Miniflare> {
    const started = await startGateway("unit-price");
    const bucket = await started.getR2Bucket(GATEWAY.r2_binding);
    await bucket.put(GATEWAY.object_key.manifest_object, envelope());
    // An object where an object lane's base would hold one: a pack-only lane must never read it.
    await bucket.put(`${GATEWAY.object_key.root}/v1/${PNU}${GATEWAY.object_key.suffix}`, "{}\n");
    return started;
  }

  afterEach(async () => {
    await runtime?.dispose();
    runtime = undefined;
  });

  beforeEach(() => {
    runtime = undefined;
  });

  it("a manifest without packs is an outage, never an object read", async () => {
    runtime = await start();
    const response = await runtime.dispatchFetch(URL_OF_PNU);
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("takes a request only as <pnu>/<building id> of the contract's suffix pattern (root ADR-0204)", async () => {
    runtime = await start();
    const prefix = `https://unit-prices.example.test${GATEWAY.request_path.prefix}`;
    for (const path of [PNU, `${PNU}/`, `${PNU}/not-a-building`, `${PNU}/${BUILDING}/x`, `${PNU}${BUILDING}`, `x/${BUILDING}`]) {
      const response = await runtime.dispatchFetch(`${prefix}${path}`);
      expect(response.status, path).toBe(404);
    }
    // The well-formed key reaches the pack path (an outage here: the manifest has no packs).
    expect((await runtime.dispatchFetch(`${prefix}${PNU}/${BUILDING}`)).status).toBe(503);
  });

  it("answers its own capabilities under its own path", async () => {
    runtime = await start();
    const response = await runtime.dispatchFetch(
      `https://unit-prices.example.test${GATEWAY.request_path.capabilities}`,
    );
    expect(response.status).toBe(200);
    expect(((await response.json()) as { unit: string }).unit).toBe("unit-price-by-pnu");
  });
});
