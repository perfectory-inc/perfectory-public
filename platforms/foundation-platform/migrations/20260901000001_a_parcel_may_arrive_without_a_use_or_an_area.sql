-- A canonical column may only be required when something produces it (root ADR-0040, ADR-0070).
--
-- `catalog.parcel` is empty and has no producer, and four of the eight foreign keys pointing at it
-- are NOT NULL, so five of the twenty producerless canonical tables measured on 2026-09-01 wait on
-- this one. The upstream is no longer missing: `silver.parcel_boundaries` holds 39,861,511 rows
-- and its re-run guard is live (root ADR-0069). Two columns are what stops a loader.
--
-- `kind` is decided by a person, not collected. The only path that writes it is
-- `catalog-application/src/update_parcel_kind.rs`, which requires `applied_by: StaffId` and records
-- the attribution in the edit ledger (root ADR-0023). Its vocabulary — factory, support, public,
-- river — describes land use *inside* an industrial complex, and root ADR-0019 already removed
-- `complex_id NOT NULL` from this table because most parcels belong to no complex. Filling
-- 39,861,511 rows with 'other' would write down not-knowing as if it were knowing.
--
-- `area_m2` is not in the source. The converter reads two attributes from the cadastral shapefile,
-- `PNU` and `JIBUN`; the `silver.parcel_boundaries` contract has no area column among its twenty.
-- It could be computed from the polygon, but `catalog_domain::Parcel` documents this column as the
-- *official* parcel area, and a computed area is a different number in the same slot — which is
-- what root ADR-0020 forbids.
--
-- Both CHECKs are deliberately left in place. `kind = ANY (ARRAY[...])` and `area_m2 >= 0` evaluate
-- to NULL for a NULL value, and Postgres accepts a row whose CHECK is NULL, so each keeps rejecting
-- a wrong value without rejecting an absent one. "Not yet decided" and "wrong" stay different facts.
--
-- No data changes: the table holds no rows. This migration only widens what a future row may be.
--
-- Rollback: `ALTER TABLE catalog.parcel ALTER COLUMN kind SET NOT NULL;` (and the same for
-- `area_m2`) as a new forward migration (ADR-0001 §7 — migration files are immutable once merged).
-- Either statement fails while a NULL row exists, so reverting means deciding a use for every
-- loaded parcel and sourcing an official area for it. The revert cost is the source and the
-- judgment, not the schema.

ALTER TABLE catalog.parcel
    ALTER COLUMN kind DROP NOT NULL;

ALTER TABLE catalog.parcel
    ALTER COLUMN area_m2 DROP NOT NULL;

COMMENT ON COLUMN catalog.parcel.kind IS
    'Land use inside an industrial complex, decided by a person through update_parcel_kind and attributed in the edit ledger (ADR-0023). NULL means no one has decided yet, which is the state every loaded parcel starts in (ADR-0070). It is not optional for a person to supply when they do decide.';

COMMENT ON COLUMN catalog.parcel.area_m2 IS
    'Official parcel area in square meters, from the cadastral record. NULL until a source that carries it is collected — the boundary source carries only PNU and JIBUN (ADR-0070). Do not fill this from the polygon: a computed area is a different fact (ADR-0020).';
