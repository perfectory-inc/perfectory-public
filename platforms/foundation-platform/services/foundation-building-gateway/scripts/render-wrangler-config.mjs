import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const contractUrl = new URL("../../../config/r2-connections.contract.json", import.meta.url);
const outputUrl = new URL("../wrangler.jsonc", import.meta.url);

export function render(contract) {
  const gateway = contract.building_by_pnu_gateway;
  const connection = contract.connections[gateway.connection];
  if (connection === undefined) {
    throw new Error(`building_by_pnu_gateway.connection does not exist: ${gateway.connection}`);
  }
  const bucket = connection.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET;
  if (typeof bucket !== "string" || bucket === "") {
    throw new Error("lakehouse expected bucket is missing from the R2 connection contract");
  }
  // The public hostname lives in the contract, nowhere else: the deploy attaches exactly the
  // domains the contract names, so moving the serving address is a one-line contract change.
  const hostnames = [gateway.public_hostname, ...(gateway.public_hostname_aliases ?? [])];
  if (hostnames.some((hostname) => typeof hostname !== "string" || hostname === "")) {
    throw new Error("building_by_pnu_gateway.public_hostname(_aliases) must be non-empty strings");
  }
  // The cut-over gate's preview (root ADR-0147 §6): its own Worker on its own hostname, so the
  // edge cache behaves as on the live custom domains, never a live route, and the binding that
  // lets it serve an unpublished pack generation set on it alone.
  // An explicit CPU limit (Workers Paid, root ADR-0151 Revision): the contract's value, never the
  // plan's 30 s default, and inherited by the preview so the gate measures what the live Worker runs.
  const cpuMs = gateway.cpu_limit_ms;
  if (!Number.isSafeInteger(cpuMs) || cpuMs < 1 || typeof gateway.cpu_limit_reason !== "string") {
    throw new Error("building_by_pnu_gateway.cpu_limit_ms must be a positive integer with a reason");
  }
  const packs = gateway.section_packs;
  const preview = packs?.preview_worker;
  if (preview !== undefined) {
    if (preview.worker_name === gateway.worker_name || hostnames.includes(preview.public_hostname)) {
      throw new Error("the preview Worker must not share the live Worker's name or hostnames");
    }
  }
  return `${JSON.stringify(
    {
      $schema: "node_modules/wrangler/config-schema.json",
      name: gateway.worker_name,
      main: "src/index.ts",
      compatibility_date: gateway.compatibility_date,
      workers_dev: false,
      keep_vars: true,
      limits: { cpu_ms: cpuMs },
      routes: hostnames.map((hostname) => ({ pattern: hostname, custom_domain: true })),
      r2_buckets: [{ binding: gateway.r2_binding, bucket_name: bucket }],
      ...(preview === undefined
        ? {}
        : {
            env: {
              [preview.wrangler_env]: {
                name: preview.worker_name,
                workers_dev: false,
                routes: [{ pattern: preview.public_hostname, custom_domain: true }],
                r2_buckets: [{ binding: gateway.r2_binding, bucket_name: bucket }],
                vars: { [packs.preview_binding]: "true" },
              },
            },
          }),
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
