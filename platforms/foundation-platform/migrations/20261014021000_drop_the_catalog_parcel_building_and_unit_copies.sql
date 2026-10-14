-- Root ADR-0194 §1, §6, §7: catalog.parcel, catalog.building and catalog.building_unit were
-- Postgres copies of facts the R2 edge serves. Nothing reads or writes them any more (#442, #461).
-- Every dependent is named, and the tables are dropped without CASCADE, so an object this file
-- did not foresee stops the migration instead of disappearing with them.

-- Approval gate (root ADR-0194 §6): the server applies migrations on its own when main moves, so
-- merging this file must not be what deletes production data. Where the tables still hold rows,
-- the drop waits for an operator to record the owner's approval on the database first:
--     ALTER DATABASE foundation SET foundation.adr_0194_drop_approved = 'yes';
-- A fresh or test database holds no rows in them and migrates without it.
DO $gate$
BEGIN
    IF current_setting('foundation.adr_0194_drop_approved', true) IS DISTINCT FROM 'yes'
       AND (EXISTS (SELECT 1 FROM catalog.parcel)
            OR EXISTS (SELECT 1 FROM catalog.building)
            OR EXISTS (SELECT 1 FROM catalog.building_unit)) THEN
        RAISE EXCEPTION 'ADR-0194: catalog.parcel, building and building_unit still hold rows and the drop is not approved'
            USING HINT = 'After the owner approves: ALTER DATABASE foundation SET foundation.adr_0194_drop_approved = ''yes''; then re-run migrations.';
    END IF;
END;
$gate$;

-- Foreign keys into the three tables. The referencing columns stay; the tables that hold them are
-- empty in production, and serving_postgis.parcel_boundary_mirror.parcel_id is NULL on every row.
ALTER TABLE catalog.digital_twin_asset
    DROP CONSTRAINT digital_twin_asset_building_id_fkey,
    DROP CONSTRAINT digital_twin_asset_parcel_id_fkey;
ALTER TABLE catalog.manufacturer DROP CONSTRAINT manufacturer_primary_parcel_id_fkey;
ALTER TABLE catalog.parcel_industry_assignment
    DROP CONSTRAINT parcel_industry_assignment_parcel_id_fkey;
ALTER TABLE catalog.parcel_marker_anchor DROP CONSTRAINT parcel_marker_anchor_parcel_id_fkey;
ALTER TABLE catalog.spatial_layer DROP CONSTRAINT spatial_layer_parcel_id_fkey;
ALTER TABLE catalog.parcel_identifier DROP CONSTRAINT parcel_identifier_parcel_fkey;
ALTER TABLE catalog.parcel_administrative_unit DROP CONSTRAINT parcel_administrative_unit_parcel_fkey;
ALTER TABLE catalog.parcel_complex_membership DROP CONSTRAINT parcel_complex_membership_parcel_fkey;
ALTER TABLE serving_postgis.parcel_boundary_mirror DROP CONSTRAINT parcel_boundary_mirror_parcel_id_fkey;

-- Views and the function that read catalog.parcel.
DROP VIEW catalog.parcels_missing_temporal_identifier;
DROP VIEW catalog.parcel_current_identifier;
DROP VIEW catalog.parcel_identifier_lookup;
DROP FUNCTION catalog.publish_parcel_identifier(uuid, text, date, uuid, uuid, text, uuid);

-- The tables; parcel_pnu_projection_guard goes with catalog.parcel, then its function.
DROP TABLE catalog.building_unit;
DROP TABLE catalog.building;
DROP TABLE catalog.parcel;
DROP FUNCTION catalog.protect_parcel_pnu_projection();
