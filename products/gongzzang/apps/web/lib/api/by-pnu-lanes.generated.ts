// Generated from platforms/foundation-platform/config/r2-connections.contract.json (by_pnu_lanes).
// Run `node scripts/generate-by-pnu-lanes.ts` from products/gongzzang after editing the contract.

/** Every by-PNU lane the web reads: the host serving it, its path there and its document. */
export const BY_PNU_LANES = {
  "building-floors": {
    host: "building",
    kind: "floors",
    path: "/buildings/floors/by-pnu/",
    section: "floors",
    fields: ["floors"],
    documentSchemaVersion: "foundation-platform.building_floors_by_pnu.v1",
    goldTable: "gold.building_floors",
  },
  "building-titles": {
    host: "building",
    kind: "titles",
    path: "/buildings/titles/by-pnu/",
    section: "titles",
    fields: ["buildings"],
    documentSchemaVersion: "foundation-platform.building_titles_by_pnu.v1",
    goldTable: "gold.building_titles",
  },
  "building-units": {
    host: "building",
    kind: "units",
    path: "/buildings/units/by-pnu/",
    section: "units",
    fields: ["units", "unlinked_units"],
    documentSchemaVersion: "foundation-platform.building_units_by_pnu.v1",
    goldTable: "gold.building_units",
  },
  "parcel-characteristics": {
    host: "parcel",
    kind: "characteristics",
    path: "/parcels/characteristics/by-pnu/",
    section: "characteristics",
    fields: ["characteristics"],
    documentSchemaVersion: "foundation-platform.parcel_characteristics_by_pnu.v1",
    goldTable: "gold.parcel_characteristics",
  },
  "parcel-forest-ledger": {
    host: "parcel",
    kind: "forest-ledger",
    path: "/parcels/forest-ledger/by-pnu/",
    section: "forest_ledger",
    fields: ["forest_ledger"],
    documentSchemaVersion: "foundation-platform.parcel_forest_ledger_by_pnu.v1",
    goldTable: "gold.parcel_forest_ledger",
  },
  "parcel-land-rights": {
    host: "parcel",
    kind: "land-rights",
    path: "/parcels/land-rights/by-pnu/",
    section: "land_rights",
    fields: ["land_rights", "land_right_total"],
    documentSchemaVersion: "foundation-platform.parcel_land_rights_by_pnu.v1",
    goldTable: "gold.parcel_land_rights",
  },
  "parcel-price": {
    host: "parcel",
    kind: "price",
    path: "/parcels/price/by-pnu/",
    section: "price",
    fields: ["price"],
    documentSchemaVersion: "foundation-platform.parcel_price_by_pnu.v1",
    goldTable: "gold.parcel_price",
  },
  "parcel-transfer-history": {
    host: "parcel",
    kind: "transfer-history",
    path: "/parcels/transfer-history/by-pnu/",
    section: "transfer_history",
    fields: ["transfer_history"],
    documentSchemaVersion: "foundation-platform.parcel_transfer_history_by_pnu.v1",
    goldTable: "gold.parcel_transfer_history",
  },
  "parcel-zoning": {
    host: "parcel",
    kind: "zoning",
    path: "/parcels/zoning/by-pnu/",
    section: "zonings",
    fields: ["zonings"],
    documentSchemaVersion: "foundation-platform.parcel_zoning_by_pnu.v1",
    goldTable: "gold.parcel_zoning",
  },
  "unit-price": {
    host: "unit-price",
    kind: "unit-price",
    path: "/unit-prices/by-pnu/",
    section: null,
    fields: null,
    documentSchemaVersion: "foundation-platform.unit_price_by_pnu.v1",
    goldTable: "gold.unit_price_panel",
  },
} as const;

export type ByPnuLaneName = keyof typeof BY_PNU_LANES;
export type ByPnuHost = (typeof BY_PNU_LANES)[ByPnuLaneName]["host"];
