-- Root ADR-0571: the PostGIS parcel boundary copy retires (112 GB on 2026-11-18, tablespace parcel_bulk).
--
-- Nothing reads it: the parcel map is baked on the lakehouse path and served from R2, the publisher
-- commands that wrote it are gone (ADR-0571 §2), and the only scans were the shape ledger's daily
-- `exists` probe. Its rows are kept by the pgBackRest full backup the weekly restore drill restored and
-- compared (ADR-0571 §3; the PR names the label), inside the 35-day retention.
--
-- Kept, and why (ADR-0571 §4): serving_postgis.spatial_projection_load holds the complex and admin
-- loads too; parcel_boundary_mirror_rebuild_run and catalog.parcel_publication_source_evidence are
-- history ledgers the shared load ledger points at (append-only for all data); the dynamic_postgis
-- parcel release rows of catalog.vector_tile_release are history.

-- 1. The order (ADR-0571 §1): only once no served parcel release is the PostGIS one. A parcel unit whose
-- active or fallback release still reads PostGIS stops this migration, and the deploy, until the first
-- lakehouse base edition is promoted.
DO $order$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_publication_unit AS unit
          JOIN catalog.vector_tile_release AS release
            ON release.id IN (unit.active_release_id, unit.fallback_release_id)
         WHERE unit.unit_key = 'parcels'
           AND release.source_kind = 'dynamic_postgis'
    ) THEN
        RAISE EXCEPTION 'ADR-0571 §1: the parcels unit still serves or falls back to a dynamic_postgis release; promote the lakehouse parcel edition first';
    END IF;
END;
$order$;

-- 2. The evidence ledger is closed: its INSERT check read the mirror, and no writer remains. It refuses
-- every write from here, by the function that already refused UPDATE and DELETE.
DROP TRIGGER parcel_publication_source_evidence_validate ON catalog.parcel_publication_source_evidence;
DROP FUNCTION catalog.validate_parcel_publication_source_evidence();
DROP TRIGGER parcel_publication_source_evidence_append_only ON catalog.parcel_publication_source_evidence;
CREATE TRIGGER parcel_publication_source_evidence_append_only
BEFORE INSERT OR UPDATE OR DELETE ON catalog.parcel_publication_source_evidence
FOR EACH ROW EXECUTE FUNCTION catalog.reject_parcel_publication_source_evidence_mutation();
CREATE TRIGGER parcel_publication_source_evidence_truncate_guard
BEFORE TRUNCATE ON catalog.parcel_publication_source_evidence
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_parcel_publication_source_evidence_mutation();
COMMENT ON TABLE catalog.parcel_publication_source_evidence IS
    'Closed history (root ADR-0571): one succeeded parcel mirror run bound to its Iceberg snapshot (ADR-0025). The mirror it described was dropped; no row is added or changed.';

-- 3. The view, then the two tables; their triggers, indexes and outgoing keys go with them. No key points
-- into either table.
DROP VIEW serving_postgis.parcel_boundary_current;
DROP TABLE serving_postgis.parcel_boundary_publication;
DROP TABLE serving_postgis.parcel_boundary_mirror;

-- 4. The guard functions only those tables' triggers called.
DROP FUNCTION serving_postgis.reject_parcel_boundary_publication_mutation();
DROP FUNCTION serving_postgis.protect_sealed_parcel_boundary_mirror_row();
DROP FUNCTION serving_postgis.reject_sealed_parcel_boundary_mirror_truncate();
