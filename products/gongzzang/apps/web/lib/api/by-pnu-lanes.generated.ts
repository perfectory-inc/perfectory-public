// Generated from platforms/foundation-platform/config/r2-connections.contract.json (by_pnu_lanes).
// Run `node scripts/generate-by-pnu-lanes.ts` from products/gongzzang after editing the contract.

/** Every by-PNU lane the web reads: the host serving it, its path there and its document. */
export const BY_PNU_LANES = {
  "parcel-zoning": {
    host: "parcel",
    path: "/parcels/zoning/by-pnu/",
    documentSchemaVersion: "foundation-platform.parcel_zoning_by_pnu.v1",
    goldTable: "gold.parcel_zoning",
  },
  "unit-price": {
    host: "unit-price",
    path: "/unit-prices/by-pnu/",
    documentSchemaVersion: "foundation-platform.unit_price_by_pnu.v1",
    goldTable: "gold.unit_price_panel",
  },
} as const;

export type ByPnuLaneName = keyof typeof BY_PNU_LANES;
export type ByPnuHost = (typeof BY_PNU_LANES)[ByPnuLaneName]["host"];
