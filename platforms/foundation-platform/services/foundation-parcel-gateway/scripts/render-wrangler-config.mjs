import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const contractUrl = new URL("../../../config/r2-connections.contract.json", import.meta.url);
const outputUrl = new URL("../wrangler.jsonc", import.meta.url);

export function render(contract) {
  const gateway = contract.parcel_by_pnu_gateway;
  const connection = contract.connections[gateway.connection];
  if (connection === undefined) {
    throw new Error(`parcel_by_pnu_gateway.connection does not exist: ${gateway.connection}`);
  }
  const bucket = connection.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET;
  if (typeof bucket !== "string" || bucket === "") {
    throw new Error("lakehouse expected bucket is missing from the R2 connection contract");
  }
  // The public hostname lives in the contract, nowhere else: the deploy attaches exactly the
  // domains the contract names, so moving the serving address is a one-line contract change.
  const hostnames = [gateway.public_hostname, ...(gateway.public_hostname_aliases ?? [])];
  if (hostnames.some((hostname) => typeof hostname !== "string" || hostname === "")) {
    throw new Error("parcel_by_pnu_gateway.public_hostname(_aliases) must be non-empty strings");
  }
  return `${JSON.stringify(
    {
      $schema: "node_modules/wrangler/config-schema.json",
      name: gateway.worker_name,
      main: "src/index.ts",
      compatibility_date: gateway.compatibility_date,
      workers_dev: false,
      keep_vars: true,
      routes: hostnames.map((hostname) => ({ pattern: hostname, custom_domain: true })),
      r2_buckets: [{ binding: gateway.r2_binding, bucket_name: bucket }],
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
