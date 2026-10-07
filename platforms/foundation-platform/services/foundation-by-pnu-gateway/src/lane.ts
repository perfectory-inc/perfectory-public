import connectionContract from "../../../config/r2-connections.contract.json";

/// The by-PNU lane this build serves, fixed when the Worker is bundled (root ADR-0160): Wrangler's
/// `define` in `wrangler.<lane>.jsonc`, vitest's in `vitest.config.ts`. One source serves both
/// lanes; the parcel and building Workers differ only in the contract block they read.
declare const __FOUNDATION_BY_PNU_LANE__: "building" | "parcel";

export const LANE = __FOUNDATION_BY_PNU_LANE__;

const gateways = {
  building: connectionContract.building_by_pnu_gateway,
  parcel: connectionContract.parcel_by_pnu_gateway,
} as const;

/// The lane's gateway block of `config/r2-connections.contract.json`.
export const policy = gateways[LANE];
/// The lane's section packs (root ADR-0147, ADR-0151).
export const lanePacks = policy.section_packs;
/// The manifest's `unit`, as the publisher writes it (`ByPnuLane::unit`).
export const UNIT = `${LANE}-by-pnu`;
/// A synthetic origin for this Worker's own edge cache entries: no client URL can name or shape it.
export const cacheOrigin = `https://${policy.worker_name}.invalid`;
