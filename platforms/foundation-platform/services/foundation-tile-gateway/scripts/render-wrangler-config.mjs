import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const contractUrl = new URL("../../../config/r2-connections.contract.json", import.meta.url);
const frontDoorUrl = new URL("../../../config/cdn-front-door.contract.json", import.meta.url);
const outputUrl = new URL("../wrangler.jsonc", import.meta.url);

export function render(contract, frontDoor = { doors: {} }) {
  const gateway = contract.vector_tile_gateway;
  const connection = contract.connections[gateway.connection];
  if (connection === undefined) {
    throw new Error(`vector_tile_gateway.connection does not exist: ${gateway.connection}`);
  }
  const bucket = connection.expected_values.FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET;
  if (typeof bucket !== "string" || bucket === "") {
    throw new Error("tile_derivatives expected bucket is missing from the R2 connection contract");
  }
  const root = connection.expected_values[gateway.object_key.root.expected_value];
  if (typeof root !== "string" || root === "" || root.startsWith("/") || root.endsWith("/")) {
    throw new Error("vector_tile_gateway.object_key.root must reference a relative connection prefix");
  }
  // The public hostname lives in the contract, nowhere else: the deploy attaches exactly the
  // domains the contract names, so moving the serving address is a one-line contract change.
  // The Seoul front door (root ADR-0460): the Worker also answers the door's origin hostname, which
  // CloudFront reads; once the door is switched to CloudFront the public hostname is its CNAME and no
  // longer this Worker's custom domain (Wrangler drops a domain the config no longer names).
  const door = Object.values(frontDoor.doors ?? {}).find((entry) => entry.gateway === "vector_tile_gateway");
  if (door !== undefined && (!["cloudflare", "cloudfront"].includes(door.answered_by) ||
    typeof door.origin_hostname !== "string" || door.origin_hostname === gateway.public_hostname)) {
    throw new Error("the tiles front door needs answered_by cloudflare|cloudfront and its own origin_hostname");
  }
  const hostnames = [
    ...(door?.answered_by === "cloudfront" ? [] : [gateway.public_hostname]),
    ...(gateway.public_hostname_aliases ?? []),
    ...(door === undefined ? [] : [door.origin_hostname]),
  ];
  if (hostnames.some((hostname) => typeof hostname !== "string" || hostname === "")) {
    throw new Error("vector_tile_gateway.public_hostname(_aliases) must be non-empty strings");
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
  const expected = render(contract, JSON.parse(await readFile(frontDoorUrl, "utf8")));
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
