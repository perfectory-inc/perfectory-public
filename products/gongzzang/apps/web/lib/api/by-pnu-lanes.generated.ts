// Generated from platforms/foundation-platform/config/r2-connections.contract.json (by_pnu_lanes)
// and the lanes' Gold tables in infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json.
// Run `node scripts/generate-by-pnu-lanes.ts` from products/gongzzang after editing the contract.

/** Every by-PNU lane the web reads: the host serving it, its path there and its document. */
export const BY_PNU_LANES = {
  "parcel-zoning": {
    host: "parcel",
    kind: "zoning",
    path: "/parcels/zoning/by-pnu/",
    sections: ["zonings"],
    documentSchemaVersion: "foundation-platform.parcel_zoning_by_pnu.v1",
    goldTable: "gold.parcel_zoning",
  },
  "unit-price": {
    host: "unit-price",
    kind: "unit-price",
    path: "/unit-prices/by-pnu/",
    sections: ["units"],
    documentSchemaVersion: "foundation-platform.unit_price_by_pnu.v1",
    goldTable: "gold.unit_price_panel",
  },
} as const;

export type ByPnuLaneName = keyof typeof BY_PNU_LANES;
export type ByPnuHost = (typeof BY_PNU_LANES)[ByPnuLaneName]["host"];
