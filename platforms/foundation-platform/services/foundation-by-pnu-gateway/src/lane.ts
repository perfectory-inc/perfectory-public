import connectionContract from "../../../config/r2-connections.contract.json";

/// The by-PNU lane this build serves, fixed when the Worker is bundled (root ADR-0160): Wrangler's
/// `define` in `wrangler.<lane>.jsonc`, vitest's in `vitest.config.ts`. One source serves every
/// lane; the Workers differ only in the contract block they read: the parcel and building blocks,
/// or a pack-only lane of the contract's `by_pnu_lanes` (root ADR-0190).
declare const __FOUNDATION_BY_PNU_LANE__: string;

export const LANE = __FOUNDATION_BY_PNU_LANE__;

const gateways = {
  building: connectionContract.building_by_pnu_gateway,
  parcel: connectionContract.parcel_by_pnu_gateway,
  ...connectionContract.by_pnu_lanes,
} as const;

function laneBlock(lane: string): (typeof gateways)[keyof typeof gateways] {
  const block = (gateways as Record<string, (typeof gateways)[keyof typeof gateways]>)[lane];
  if (block === undefined) throw new Error(`the contract names no by-PNU lane ${lane}`);
  return block;
}

/// The lane's gateway block of `config/r2-connections.contract.json`.
export const policy = laneBlock(LANE);
/// A pack-only lane has no object path: it answers from section packs or not at all.
export const packOnly = "pack_only" in policy && policy.pack_only === true;
/// The lane's section packs (root ADR-0147, ADR-0151).
export const lanePacks = policy.section_packs;
/// The manifest's `unit`, as the publisher writes it (`ByPnuLane::unit`).
export const UNIT = `${LANE}-by-pnu`;
/// A synthetic origin for this Worker's own edge cache entries: no client URL can name or shape it.
export const cacheOrigin = `https://${policy.worker_name}.invalid`;
