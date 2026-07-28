-- Versioned official administrative-boundary geometry projection.
--
-- The source snapshot/registry remain the evidence authority. This table is a
-- PostGIS serving projection of one validated administrative revision; it is
-- never updated in place and is visible to Martin only through the runtime
-- manifest pointer.

CREATE TABLE serving_postgis.administrative_unit_boundary_publication (
    administrative_unit_id uuid NOT NULL,
    data_revision uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    source_object_key text NOT NULL,
    scope_kind text NOT NULL,
    canonical_code text NOT NULL,
    display_name text,
    geometry_checksum_sha256 character(64) NOT NULL,
    geom public.geometry(MultiPolygon, 4326) NOT NULL,
    properties jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT administrative_boundary_publication_snapshot_check
        CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$'),
    CONSTRAINT administrative_boundary_publication_source_snapshot_check
        CHECK (source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'),
    CONSTRAINT administrative_boundary_publication_kind_check
        CHECK (scope_kind IN ('sido', 'sigungu', 'legal_dong')),
    CONSTRAINT administrative_boundary_publication_code_check
        CHECK (
            (scope_kind = 'sido' AND canonical_code ~ '^[0-9]{2}$')
            OR (scope_kind = 'sigungu' AND canonical_code ~ '^[0-9]{5}$')
            OR (scope_kind = 'legal_dong' AND canonical_code ~ '^[0-9]{10}$')
        ),
    CONSTRAINT administrative_boundary_publication_geometry_check
        CHECK (public.st_srid(geom) = 4326 AND public.st_isvalid(geom)),
    CONSTRAINT administrative_boundary_publication_checksum_check
        CHECK (geometry_checksum_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT administrative_boundary_publication_source_object_key_check
        CHECK (
            source_object_key <> ''
            AND source_object_key !~~ '/%'
            AND source_object_key !~~ '%//%'
            AND source_object_key !~~ '%/./%'
            AND source_object_key !~~ './%'
            AND source_object_key !~~ '%/.'
            AND source_object_key !~~ '%/../%'
            AND source_object_key !~~ '../%'
            AND source_object_key !~~ '%/..'
        ),
    CONSTRAINT administrative_boundary_publication_properties_check
        CHECK (jsonb_typeof(properties) = 'object'),
    CONSTRAINT administrative_boundary_publication_pkey
        PRIMARY KEY (data_revision, administrative_unit_id),
    CONSTRAINT administrative_boundary_publication_code_key
        UNIQUE (data_revision, scope_kind, canonical_code)
);

ALTER TABLE serving_postgis.administrative_unit_boundary_publication
    ADD CONSTRAINT administrative_boundary_publication_unit_fkey
        FOREIGN KEY (administrative_unit_id)
        REFERENCES catalog.administrative_unit(id) ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_boundary_publication_revision_fkey
        FOREIGN KEY (data_revision, canonical_iceberg_snapshot_id)
        REFERENCES catalog.administrative_boundary_revision(id, canonical_iceberg_snapshot_id)
        ON DELETE RESTRICT,
    ADD CONSTRAINT administrative_boundary_publication_source_record_fkey
        FOREIGN KEY (source_record_id)
        REFERENCES catalog.source_record(id) ON DELETE RESTRICT;

CREATE INDEX administrative_boundary_publication_geom_gix
    ON serving_postgis.administrative_unit_boundary_publication USING gist (geom);
CREATE INDEX administrative_boundary_publication_code_idx
    ON serving_postgis.administrative_unit_boundary_publication (scope_kind, canonical_code);

CREATE OR REPLACE FUNCTION serving_postgis.reject_administrative_boundary_publication_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, serving_postgis, catalog, pg_temp
AS $function$
BEGIN
    IF current_setting('foundation.temporal_publisher', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'administrative boundary publication is append-only; use the Foundation publisher'
            USING ERRCODE = '42501';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$function$;

CREATE TRIGGER administrative_boundary_publication_append_only
BEFORE UPDATE OR DELETE ON serving_postgis.administrative_unit_boundary_publication
FOR EACH ROW EXECUTE FUNCTION serving_postgis.reject_administrative_boundary_publication_mutation();

CREATE TRIGGER administrative_boundary_publication_revision_snapshot_guard
BEFORE INSERT ON serving_postgis.administrative_unit_boundary_publication
FOR EACH ROW EXECUTE FUNCTION catalog.validate_temporal_revision_snapshot();

-- The runtime manifest remains the only visibility switch. An unpromoted or
-- superseded revision can therefore never leak through Martin.
CREATE OR REPLACE VIEW serving_postgis.administrative_unit_boundary_current AS
SELECT
    publication.administrative_unit_id::text AS administrative_unit_id,
    publication.scope_kind,
    publication.canonical_code,
    publication.display_name,
    publication.geom::public.geometry(MultiPolygon, 4326) AS geom
FROM serving_postgis.administrative_unit_boundary_publication AS publication
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.data_revision = publication.data_revision
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
JOIN catalog.vector_tile_release AS release
  ON release.id = manifest_unit.release_id
WHERE unit.unit_key = 'admin'
  AND release.source_kind = 'dynamic_postgis';

COMMENT ON TABLE serving_postgis.administrative_unit_boundary_publication IS
    'Append-only PostGIS projection of official administrative polygons. Runtime visibility is CAS-manifest controlled.';
