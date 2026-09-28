import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const contractUrl = new URL("../../../config/r2-connections.contract.json", import.meta.url);
const outputUrl = new URL("../wrangler.jsonc", import.meta.url);

export function render(contract) {
  const gateway = contract.map_edit_gateway;
  if (gateway === undefined) throw new Error("map_edit_gateway is missing from the connection contract");
  const hostnames = [gateway.public_hostname, ...(gateway.public_hostname_aliases ?? [])];
  if (hostnames.some((hostname) => typeof hostname !== "string" || hostname === "")) {
    throw new Error("map_edit_gateway.public_hostname(_aliases) must be non-empty strings");
  }
  for (const key of ["d1_binding", "d1_database_name", "write_token_binding", "allowed_origins_binding"]) {
    if (typeof gateway[key] !== "string" || gateway[key] === "") {
      throw new Error(`map_edit_gateway.${key} must be a non-empty string`);
    }
  }
  // No database_id: Wrangler provisions the database on first deploy and keeps it linked, so no
  // account-specific identifier is committed. The write token is a secret and never rendered.
  return `${JSON.stringify(
    {
      $schema: "node_modules/wrangler/config-schema.json",
      name: gateway.worker_name,
      main: "src/index.ts",
      compatibility_date: gateway.compatibility_date,
      workers_dev: false,
      keep_vars: true,
      routes: hostnames.map((hostname) => ({ pattern: hostname, custom_domain: true })),
      d1_databases: [
        {
          binding: gateway.d1_binding,
          database_name: gateway.d1_database_name,
          migrations_dir: "migrations",
        },
      ],
    },
    null,
    2,
  )}\n`;
}

async function main() {
  const mode = process.argv[2];
  if (!["--write", "--check"].includes(mode)) {
    throw new Error("usage: render-wrangler-config.mjs <--write|--check>");
  }
  const contract = JSON.parse(await readFile(contractUrl, "utf8"));
  const expected = render(contract);
  if (mode === "--write") {
    await writeFile(outputUrl, expected, "utf8");
    return;
  }
  const actual = await readFile(outputUrl, "utf8");
  if (actual !== expected) {
    throw new Error(`${fileURLToPath(outputUrl)} drifted; run pnpm run config:render`);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
