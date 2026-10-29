import { readdirSync, readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { productionBindings, startGateway, TEST_WORKER_VERSION, wranglerConfig } from "./miniflare";

// Root ADR-0327 §3. 2026-10-29: every cached onbid photo answered 500 on catalog.perfectory.io
// ("Can't modify immutable headers" in withVersion). Production binds version metadata; every test
// started Miniflare without it, so no test ever set the version header on a cache hit. A test now
// starts the gateway only through startGateway, which binds what the Wrangler config declares.

const testDirectory = new URL("./", import.meta.url);
const lanes = readdirSync(new URL("../", import.meta.url))
  .map((name) => name.match(/^wrangler\.(.+)\.jsonc$/)?.[1])
  .filter((lane): lane is string => lane !== undefined);

describe("the test harness binds what production binds (root ADR-0327 §3)", () => {
  it("finds every lane's Wrangler config", () => {
    expect(lanes.sort()).toEqual(["building", "parcel", "unit-price"]);
  });

  it.each(lanes)("every binding of wrangler.%s.jsonc, production and each env", (lane) => {
    const config = wranglerConfig(lane);
    for (const env of [undefined, ...Object.keys(config.env ?? {})]) {
      const section = env === undefined ? config : config.env?.[env];
      const bound = productionBindings(config, env);
      expect(bound.r2Buckets, `${lane} ${env ?? "production"}`).toEqual(
        (section?.r2_buckets ?? []).map((bucket) => bucket.binding),
      );
      for (const name of Object.keys(section?.vars ?? {})) expect(bound.bindings).toHaveProperty(name);
      const metadata = section?.version_metadata?.binding;
      expect(metadata, `${lane} ${env ?? "production"} declares version metadata`).toBeDefined();
      expect(bound.bindings[metadata as string]).toMatchObject({ id: TEST_WORKER_VERSION });
    }
  });

  it("refuses a config that declares a binding kind the harness does not bind", () => {
    const config = { ...wranglerConfig("parcel"), kv_namespaces: [{ binding: "SOMETHING", id: "x" }] };
    expect(() => productionBindings(config)).toThrow(/kv_namespaces.*2026-10-29/);
  });

  it("leaves out only a production binding named on purpose", async () => {
    await expect(startGateway("parcel", { without: ["NOT_A_PRODUCTION_BINDING"] })).rejects.toThrow(
      "is not a binding production gives",
    );
  });

  it("no test file starts Miniflare without the harness", () => {
    // miniflare-port.test.ts starts a one-line Worker to pin the port retry, not the gateway.
    const allowed = new Set(["miniflare.ts", "miniflare-port.test.ts"]);
    const offenders = readdirSync(testDirectory)
      .filter((name) => name.endsWith(".ts") && !allowed.has(name))
      .filter((name) => /\bstartMiniflare\(|\bnew Miniflare\(/.test(readFileSync(new URL(name, testDirectory), "utf8")));
    expect(offenders, "start the gateway with startGateway (test/miniflare.ts)").toEqual([]);
  });
});
