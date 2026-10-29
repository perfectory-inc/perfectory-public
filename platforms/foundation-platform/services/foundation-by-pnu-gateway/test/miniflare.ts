import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare, type MiniflareOptions } from "miniflare";

import connectionContract from "../../../config/r2-connections.contract.json";

/// The ports the fetch standard refuses to connect to (https://fetch.spec.whatwg.org/#bad-port);
/// undici, which `dispatchFetch` uses, refuses exactly these with "bad port".
const BAD_PORTS = new Set([
  1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101, 102, 103,
  104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137, 139, 143, 161, 179, 389, 427, 465, 512, 513,
  514, 515, 526, 530, 531, 532, 540, 548, 554, 556, 563, 587, 601, 636, 989, 990, 993, 995, 1719,
  1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679,
  6697, 10080,
]);

/// A Miniflare whose entry port `dispatchFetch` can reach. Miniflare listens on a port the OS picks,
/// and a Windows dynamic port range can start at 1024 (a workstation's: 1024-15000, 2026-10-18), so
/// 4 runs in 30 of the suite drew a bad port such as 6000 or 6665 and failed with "fetch failed:
/// bad port". A runtime on such a port is disposed and started again; Linux (32768-60999) never
/// draws one.
export async function startMiniflare(options: MiniflareOptions): Promise<Miniflare> {
  for (let attempt = 1; attempt <= 20; attempt += 1) {
    const runtime = new Miniflare(options);
    const entry = await runtime.ready;
    if (!BAD_PORTS.has(Number(entry.port))) return runtime;
    await runtime.dispose();
  }
  throw new Error(`Miniflare drew a port the fetch standard refuses 20 times in a row`);
}

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

/// The version id the harness binds as production's version metadata (root ADR-0327 §3).
export const TEST_WORKER_VERSION = "11111111-1111-4111-8111-111111111111";
/// The value of the CORS variable production sets outside the config (keep_vars), unless a test names its own.
export const TEST_ALLOWED_ORIGIN = "https://app.example.test";

/// Keys of a Wrangler config section that bind nothing into the Worker's `env`.
const NOT_BINDINGS = new Set([
  "$schema", "name", "main", "compatibility_date", "compatibility_flags", "workers_dev", "keep_vars",
  "define", "limits", "placement", "routes", "route", "env",
]);
/// The binding kinds the harness gives a Worker as production does. A config that declares any
/// other kind is refused until the harness binds it too.
const BOUND = new Set(["r2_buckets", "version_metadata", "vars"]);

type Section = Record<string, unknown> & {
  compatibility_date?: string;
  compatibility_flags?: string[];
  define?: Record<string, string>;
  r2_buckets?: { binding: string }[];
  version_metadata?: { binding: string };
  vars?: Record<string, string>;
  env?: Record<string, Section>;
};

/// What production gives the Worker of one Wrangler config section: derived from the config, never
/// listed by hand, plus the CORS variable the contract names, which production keeps outside the
/// config (keep_vars).
export interface ProductionBindings {
  compatibilityDate: string;
  compatibilityFlags: string[];
  define: Record<string, string>;
  r2Buckets: string[];
  bindings: Record<string, unknown>;
}

/// The Wrangler config of a lane (`wrangler.<lane>.jsonc`, rendered JSON).
export function wranglerConfig(lane: string): Section {
  return JSON.parse(readFileSync(new URL(`../wrangler.${lane}.jsonc`, import.meta.url), "utf8")) as Section;
}

/// Every binding the section `env` of `config` (production when `undefined`) declares.
///
/// 2026-10-29: every cached onbid photo answered 500 on catalog.perfectory.io. The Worker set its
/// version header on a cache hit's immutable headers; production binds version metadata, the tests
/// started Miniflare without it, so no test ever took that path (root ADR-0327).
export function productionBindings(config: Section, env?: string): ProductionBindings {
  const section = env === undefined ? config : config.env?.[env];
  if (section === undefined) throw new Error(`the Wrangler config has no env ${env}`);
  const unbound = Object.keys(section).filter((key) => !NOT_BINDINGS.has(key) && !BOUND.has(key));
  if (unbound.length > 0) {
    throw new Error(
      `the Wrangler config of ${String(section.name)} binds ${unbound.join(", ")}, which the test harness does not ` +
        `bind: a test would run without what production has (2026-10-29, root ADR-0327). Bind it in test/miniflare.ts`,
    );
  }
  const define = section.define ?? config.define ?? {};
  const lane = JSON.parse(define.__FOUNDATION_BY_PNU_LANE__ ?? "null") as string | null;
  const gateways: Record<string, { allowed_origins_binding?: string }> = {
    building: connectionContract.building_by_pnu_gateway,
    parcel: connectionContract.parcel_by_pnu_gateway,
    ...(connectionContract.by_pnu_lanes as Record<string, { allowed_origins_binding?: string }>),
  };
  const origins = lane === null ? undefined : gateways[lane]?.allowed_origins_binding;
  if (origins === undefined) throw new Error(`the contract names no CORS binding for the lane ${String(lane)}`);
  const bindings: Record<string, unknown> = { [origins]: TEST_ALLOWED_ORIGIN, ...(section.vars ?? {}) };
  if (section.version_metadata !== undefined) {
    bindings[section.version_metadata.binding] = { id: TEST_WORKER_VERSION, tag: "", timestamp: "" };
  }
  const compatibilityDate = section.compatibility_date ?? config.compatibility_date;
  if (compatibilityDate === undefined) throw new Error("the Wrangler config names no compatibility_date");
  return {
    compatibilityDate,
    compatibilityFlags: section.compatibility_flags ?? config.compatibility_flags ?? [],
    define,
    r2Buckets: (section.r2_buckets ?? []).map((bucket) => bucket.binding),
    bindings,
  };
}

export interface GatewayOptions {
  /// A Wrangler env of the config (`preview`); production when absent.
  env?: string;
  /// Test values, added to or replacing production's.
  bindings?: Record<string, unknown>;
  /// Production bindings this test leaves out on purpose, by name: never by omission.
  without?: string[];
  cachePersist?: string;
}

/// The lane's Worker, bundled as `wrangler.<lane>.jsonc` bundles it, in a Miniflare that binds
/// every binding production declares (root ADR-0327 §3). The one way a test starts the gateway:
/// test/harness-parity.test.ts refuses a test file that starts Miniflare itself.
export async function startGateway(lane: string, options: GatewayOptions = {}): Promise<Miniflare> {
  const production = productionBindings(wranglerConfig(lane), options.env);
  const bindings = { ...production.bindings };
  let r2Buckets = production.r2Buckets;
  for (const name of options.without ?? []) {
    if (!(name in bindings) && !r2Buckets.includes(name)) {
      throw new Error(`${name} is not a binding production gives the ${lane} Worker`);
    }
    delete bindings[name];
    r2Buckets = r2Buckets.filter((bucket) => bucket !== name);
  }
  const bundle = await build({
    entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
    define: production.define,
    bundle: true,
    format: "esm",
    platform: "browser",
    write: false,
    absWorkingDir: packageRoot,
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("esbuild emitted no Worker module");
  return startMiniflare({
    compatibilityDate: production.compatibilityDate,
    ...(production.compatibilityFlags.length > 0 ? { compatibilityFlags: production.compatibilityFlags } : {}),
    modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
    r2Buckets,
    bindings: { ...bindings, ...options.bindings } as MiniflareOptions["bindings"],
    cache: true,
    ...(options.cachePersist === undefined ? {} : { cachePersist: options.cachePersist }),
  });
}
