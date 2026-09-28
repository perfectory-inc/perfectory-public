import { spawnSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";

const gateway = connectionContract.map_edit_gateway;

describe("generated Wrangler configuration", () => {
  it("projects exactly the contract's database and custom domain without account identifiers or secrets", async () => {
    const text = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    expect(JSON.parse(text)).toEqual({
      $schema: "node_modules/wrangler/config-schema.json",
      name: gateway.worker_name,
      main: "src/index.ts",
      compatibility_date: gateway.compatibility_date,
      workers_dev: false,
      keep_vars: true,
      routes: [gateway.public_hostname, ...gateway.public_hostname_aliases].map((pattern) => ({
        pattern, custom_domain: true,
      })),
      d1_databases: [{
        binding: gateway.d1_binding, database_name: gateway.d1_database_name, migrations_dir: "migrations",
      }],
    });
    expect(text).not.toMatch(/database_id|account_id|secret|token/i);
    expect(gateway.cors).toEqual(connectionContract.vector_tile_gateway.cors);
  });

  it("the render/check CLI rejects a drifted config and a contract without the gateway", async () => {
    const parent = fileURLToPath(new URL("../node_modules/.cache/", import.meta.url));
    await mkdir(parent, { recursive: true });
    const fixture = await mkdtemp(join(parent, "map-edit-config-"));
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
      await writeFile(output, generated.replace(gateway.worker_name, "drifted-worker"));
      const drift = run("--check");
      expect(drift.status).toBe(1);
      expect(drift.stderr).toContain("drifted; run pnpm run config:render");
      await writeFile(contract, JSON.stringify({ ...connectionContract, map_edit_gateway: undefined }));
      expect(run("--write").status).toBe(1);
    } finally {
      const resolved = resolve(fixture);
      if (!resolved.startsWith(`${resolve(parent)}${sep}map-edit-config-`)) throw new Error("Unsafe fixture path");
      await rm(resolved, { recursive: true, force: true });
    }
  });
});
