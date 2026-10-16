import connectionContract from "../../../config/r2-connections.contract.json";

/// The by-PNU lane this build serves, fixed when the Worker is bundled (root ADR-0160): Wrangler's
/// `define` in `wrangler.<lane>.jsonc`, vitest's in `vitest.config.ts`. One source serves every
/// lane; the Workers differ only in the contract block they read: the parcel and building blocks,
/// or a pack-only lane of the contract's `by_pnu_lanes` written whole (root ADR-0190).
declare const __FOUNDATION_BY_PNU_LANE__: string;

export const LANE = __FOUNDATION_BY_PNU_LANE__;

const gateways = {
  building: connectionContract.building_by_pnu_gateway,
  parcel: connectionContract.parcel_by_pnu_gateway,
  ...connectionContract.by_pnu_lanes,
} as const;

type Gateway = typeof connectionContract.parcel_by_pnu_gateway;

function laneBlock(lane: string): Gateway {
  const block = (gateways as Record<string, unknown>)[lane];
  if (typeof block !== "object" || block === null || "host" in block) {
    throw new Error(`the contract names no by-PNU lane ${lane} with a Worker of its own`);
  }
  return block as Gateway;
}

/// The lane's gateway block of `config/r2-connections.contract.json`: everything this Worker does
/// the same for every lane it serves (bindings, CORS, caching, version header).
export const policy = laneBlock(LANE);
/// The lane's section packs (root ADR-0147, ADR-0151).
export const lanePacks = policy.section_packs;
/// A synthetic origin for this Worker's own edge cache entries: no client URL can name or shape it.
export const cacheOrigin = `https://${policy.worker_name}.invalid`;

/// What differs between the lanes one Worker serves: its own, and every `by_pnu_lanes` entry that
/// names this Worker's block as its `host` (root ADR-0193 §4).
export interface ServedLane {
  name: string;
  /// The manifest's `unit`, as the publisher writes it (`ByPnuLane::unit`).
  unit: string;
  /// Answers from section packs or not at all.
  packOnly: boolean;
  prefix: string;
  capabilities: string;
  acceptedQueries: readonly string[];
  manifestObject: string;
  packRoot: string;
  /// Prefixed to every served-state fingerprint, so two lanes' edge copies never share a key. The
  /// Worker's own lane keeps the bare fingerprint (and the edge copies it already has).
  cacheScope: string;
  /// A lane keyed narrower than a PNU (root ADR-0204, contract `document_key_suffix`): what follows
  /// the PNU in its key, requested as `<prefix><pnu>/<suffix>`. `null` for a lane keyed by PNU.
  keySuffix: RegExp | null;
}

/// The contract's `document_key_suffix.pattern` of an entry, anchored; `null` when it names none.
export function keySuffixOf(entry: unknown): RegExp | null {
  const suffix = (entry as { document_key_suffix?: { pattern?: unknown } }).document_key_suffix;
  if (suffix === undefined) return null;
  if (typeof suffix.pattern !== "string") throw new Error("document_key_suffix names no pattern");
  return new RegExp(`^(?:${suffix.pattern})$`);
}

/// `path` with `{kind}/` inserted before its last `segment` segment (the publisher's `with_kind`).
function withKind(path: string, segment: string, kind: string): string {
  const parts = path.split("/");
  const at = parts.lastIndexOf(segment);
  if (at < 0) throw new Error(`${path} has no ${segment} segment to put a kind before`);
  parts.splice(at, 0, kind);
  return parts.join("/");
}

/// The paths of a lane derived from `host` (root ADR-0193 §4; the publisher's `resolve_lane_entry`).
export function resolveLane(name: string, host: Gateway, kind: string, keySuffix: RegExp | null = null): ServedLane {
  return {
    name,
    unit: `${name}-by-pnu`,
    packOnly: true,
    prefix: withKind(host.request_path.prefix, "by-pnu", kind),
    capabilities: withKind(host.request_path.capabilities, "by-pnu", kind),
    acceptedQueries: [],
    manifestObject: withKind(host.object_key.manifest_object, "by-pnu", kind),
    packRoot: withKind(host.section_packs.root, "packs", kind),
    cacheScope: `${name}:`,
    keySuffix,
  };
}

/// The lane this build is named for.
export const OWN: ServedLane = {
  name: LANE,
  unit: `${LANE}-by-pnu`,
  packOnly: "pack_only" in policy && policy.pack_only === true,
  prefix: policy.request_path.prefix,
  capabilities: policy.request_path.capabilities,
  acceptedQueries: policy.request_path.accepted_queries as readonly string[],
  manifestObject: policy.object_key.manifest_object,
  packRoot: lanePacks.root,
  cacheScope: "",
  keySuffix: keySuffixOf(policy),
};

const derived = Object.entries(connectionContract.by_pnu_lanes as Record<string, Record<string, unknown>>)
  .filter(([, entry]) => entry.host === `${LANE}_by_pnu_gateway`)
  .map(([name, entry]) => resolveLane(name, policy, String(entry.kind), keySuffixOf(entry)));

/// Every lane this Worker serves, its own first.
export const LANES: readonly ServedLane[] = [OWN, ...derived];

/// The lane a request path belongs to: the one whose prefix or capabilities path it is under.
export function laneOfPath(pathname: string): ServedLane | null {
  return LANES.find((lane) => pathname === lane.capabilities || pathname.startsWith(lane.prefix)) ?? null;
}
