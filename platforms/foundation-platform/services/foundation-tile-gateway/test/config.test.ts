import { spawnSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";

const gateway = connectionContract.vector_tile_gateway;

describe("generated Wrangler configuration", () => {
  it("projects exactly the contract's private bucket and custom domains without credentials", async () => {
    const text = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    const config = JSON.parse(text);
    expect(config).toEqual({
      $schema: "node_modules/wrangler/config-schema.json",
      name: gateway.worker_name,
      main: "src/index.ts",
      compatibility_date: gateway.compatibility_date,
      workers_dev: false,
      keep_vars: true,
      routes: [gateway.public_hostname, ...gateway.public_hostname_aliases].map((pattern) => ({
        pattern, custom_domain: true,
      })),
      r2_buckets: [{
        binding: gateway.r2_binding,
        bucket_name: connectionContract.connections.tile_derivatives.expected_values.FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET,
      }],
    });
    expect(gateway.r2_binding).toMatch(/^FOUNDATION_PLATFORM_(?!R2_)[A-Z0-9_]+$/);
    expect(text).not.toMatch(/remote|account_id|access_key|secret/i);
    expect(text).not.toContain('"*"');
  });

  it("references the connection prefix and accepts catalog-defined units", () => {
    const values: Record<string, string> = connectionContract.connections.tile_derivatives.expected_values;
    expect(values[gateway.object_key.root.expected_value]).toBeDefined();
    expect(gateway.object_key.suffix).toBe(".pmtiles");
    expect(gateway.max_zoom).toBe(24);
    const source = new RegExp(`^(?:${gateway.object_key.source_id_pattern})$`);
    expect(source.test("future_unit-00000000-0000-4000-8000-000000000001")).toBe(true);
    expect(gateway.cors).toEqual(connectionContract.parcel_by_pnu_gateway.cors);
  });

  it("the render/check CLI rejects a drifted config and an unresolved prefix reference", async () => {
    const parent = fileURLToPath(new URL("../node_modules/.cache/", import.meta.url));
    await mkdir(parent, { recursive: true });
    const fixture = await mkdtemp(join(parent, "tile-config-"));
    try {
      const script = join(fixture, "services/gateway/scripts/render-wrangler-config.mjs");
      const contract = join(fixture, "config/r2-connections.contract.json");
      const output = join(fixture, "services/gateway/wrangler.jsonc");
      await mkdir(dirname(script), { recursive: true });
      await mkdir(dirname(contract), { recursive: true });
      await copyFile(new URL("../scripts/render-wrangler-config.mjs", import.meta.url), script);
      await writeFile(contract, JSON.stringify(connectionContract));
      const run = (mode: string) => spawnSync(process.execPath, [script, mode], { encoding: "utf8" });
      expect(run("--write").status).toBe(0);
      expect(run("--check").status).toBe(0);
      const generated = await readFile(output, "utf8");
      expect(generated).toBe(await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8"));
      await writeFile(output, generated.replace(gateway.worker_name, "drifted-worker"));
      const drift = run("--check");
      expect(drift.status).toBe(1);
      expect(drift.stderr).toContain("drifted; run pnpm run config:render");
      await writeFile(contract, JSON.stringify({
        ...connectionContract,
        vector_tile_gateway: { ...gateway, object_key: { ...gateway.object_key, root: { expected_value: "MISSING" } } },
      }));
      expect(run("--write").status).toBe(1);
    } finally {
      const resolved = resolve(fixture);
      if (!resolved.startsWith(`${resolve(parent)}${sep}tile-config-`)) throw new Error("Unsafe fixture path");
      await rm(resolved, { recursive: true, force: true });
    }
  });
});
