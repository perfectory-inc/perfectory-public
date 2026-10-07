import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

/// One source, one Wrangler config per by-PNU lane (root ADR-0160): `wrangler.building.jsonc` and
/// `wrangler.parcel.jsonc`, each the contract's `<lane>_by_pnu_gateway` block and a `define` that
/// fixes the lane the bundle serves (`src/lane.ts`). Deploy one with `wrangler deploy -c <file>`.
export const LANES = ["building", "parcel"];

const contractUrl = new URL("../../../config/r2-connections.contract.json", import.meta.url);

export function outputUrl(lane) {
  return new URL(`../wrangler.${lane}.jsonc`, import.meta.url);
}

export function render(contract, lane) {
  const block = `${lane}_by_pnu_gateway`;
  const gateway = contract[block];
  if (gateway === undefined) throw new Error(`the contract has no ${block}`);
  const connection = contract.connections[gateway.connection];
  if (connection === undefined) {
    throw new Error(`${block}.connection does not exist: ${gateway.connection}`);
  }
  const bucket = connection.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET;
  if (typeof bucket !== "string" || bucket === "") {
    throw new Error("lakehouse expected bucket is missing from the R2 connection contract");
  }
  // The public hostname lives in the contract, nowhere else: the deploy attaches exactly the
  // domains the contract names, so moving the serving address is a one-line contract change.
  const hostnames = [gateway.public_hostname, ...(gateway.public_hostname_aliases ?? [])];
  if (hostnames.some((hostname) => typeof hostname !== "string" || hostname === "")) {
    throw new Error(`${block}.public_hostname(_aliases) must be non-empty strings`);
  }
  // An explicit CPU limit (Workers Paid, root ADR-0151 Revision): the contract's value, never the
  // plan's 30 s default, and inherited by the preview so the gate measures what the live Worker runs.
  const cpuMs = gateway.cpu_limit_ms;
  if (!Number.isSafeInteger(cpuMs) || cpuMs < 1 || typeof gateway.cpu_limit_reason !== "string") {
    throw new Error(`${block}.cpu_limit_ms must be a positive integer with a reason`);
  }
  // Where the Worker runs (ADR-0154): beside the bucket, for the live Worker and the preview alike,
  // so the cut-over gate compares the two paths under one placement.
  const region = gateway.placement?.region;
  if (typeof region !== "string" || !/^(aws|gcp|azure):[a-z0-9-]+$/.test(region) || typeof gateway.placement_reason !== "string") {
    throw new Error(`${block}.placement must name a cloud region hint, with a reason`);
  }
  // Every answer names the version that produced it (ADR-0157), from Cloudflare's version metadata
  // binding; bindings are not inherited, so the preview gets its own.
  const versionBinding = gateway.version_metadata_binding;
  if (typeof versionBinding !== "string" || !/^FOUNDATION_PLATFORM_[A-Z0-9_]+$/.test(versionBinding)) {
    throw new Error(`${block}.version_metadata_binding must name a FOUNDATION_PLATFORM_ binding`);
  }
  const versionMetadata = { binding: versionBinding };
  // `define` is not inherited by an environment either: the preview bundles the same lane.
  const define = { __FOUNDATION_BY_PNU_LANE__: JSON.stringify(lane) };
  const packs = gateway.section_packs;
  // The cut-over gate's preview (root ADR-0147 §6): its own Worker on its own hostname, so the
  // edge cache behaves as on the live custom domains, never a live route, and the binding that
  // lets it serve an unpublished pack generation set on it alone.
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
      define,
      limits: { cpu_ms: cpuMs },
      placement: { region },
      routes: hostnames.map((hostname) => ({ pattern: hostname, custom_domain: true })),
      r2_buckets: [{ binding: gateway.r2_binding, bucket_name: bucket }],
      version_metadata: versionMetadata,
      ...(preview === undefined
        ? {}
        : {
            env: {
              [preview.wrangler_env]: {
                name: preview.worker_name,
                workers_dev: false,
                define,
                placement: { region },
                routes: [{ pattern: preview.public_hostname, custom_domain: true }],
                r2_buckets: [{ binding: gateway.r2_binding, bucket_name: bucket }],
                version_metadata: versionMetadata,
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
  for (const lane of LANES) {
    const expected = render(contract, lane);
    if (mode === "--write") {
      await writeFile(outputUrl(lane), expected, "utf8");
      continue;
    }
    const actual = await readFile(outputUrl(lane), "utf8");
    if (actual !== expected) {
      throw new Error(`${fileURLToPath(outputUrl(lane))} drifted; run pnpm run config:render`);
    }
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
