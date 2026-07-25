-- Additive v2 spatial publication ledger.
--
-- The legacy catalog.vector_tile_manifest/vector_tile_artifact tables remain the frozen v1 flat
-- object contract. These normalized tables model one complete Martin source per publication unit.

CREATE TABLE catalog.vector_tile_publication_unit (
    id uuid PRIMARY KEY,
    unit_key text NOT NULL UNIQUE,
    active_release_id uuid,
    fallback_release_id uuid,
    active_data_revision uuid,
    fallback_data_revision uuid,
    serving_generation bigint NOT NULL DEFAULT 1,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT vector_tile_publication_unit_key_check CHECK (unit_key ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'),
    CONSTRAINT vector_tile_publication_unit_generation_check CHECK (serving_generation BETWEEN 1 AND 9007199254740991),
    CONSTRAINT vector_tile_publication_unit_fallback_distinct_check CHECK (fallback_release_id IS NULL OR fallback_release_id <> active_release_id),
    CONSTRAINT vector_tile_publication_unit_fallback_revision_check CHECK (fallback_release_id IS NULL OR fallback_data_revision = active_data_revision)
);

CREATE TABLE catalog.vector_tile_release (
    id uuid PRIMARY KEY,
    publication_unit_id uuid NOT NULL REFERENCES catalog.vector_tile_publication_unit(id),
    data_revision uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    source_file_asset_ids uuid[] NOT NULL DEFAULT '{}',
    source_kind text NOT NULL,
    martin_source_id text NOT NULL,
    tiles_url_template text NOT NULL,
    postgis_projection_revision uuid,
    pmtiles_object_key text,
    pmtiles_file_asset_id uuid,
    pmtiles_sha256 character(64),
    pmtiles_bytes bigint,
    validated_at timestamptz,
    validation_evidence_sha256 character(64),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT vector_tile_release_source_kind_check CHECK (source_kind IN ('dynamic_postgis', 'static_pmtiles')),
    CONSTRAINT vector_tile_release_snapshot_check CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$'),
    CONSTRAINT vector_tile_release_source_id_check CHECK (martin_source_id ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'),
    CONSTRAINT vector_tile_release_url_check CHECK (position('{z}' in tiles_url_template) > 0 AND position('{x}' in tiles_url_template) > 0 AND position('{y}' in tiles_url_template) > 0),
    CONSTRAINT vector_tile_release_route_check CHECK (
        right(tiles_url_template, length(format('/%s/{z}/{x}/{y}', martin_source_id)))
        = format('/%s/{z}/{x}/{y}', martin_source_id)
    ),
    CONSTRAINT vector_tile_release_source_fields_check CHECK (
        (source_kind = 'dynamic_postgis' AND postgis_projection_revision IS NOT NULL AND pmtiles_object_key IS NULL AND pmtiles_file_asset_id IS NULL AND pmtiles_sha256 IS NULL AND pmtiles_bytes IS NULL)
        OR
        (source_kind = 'static_pmtiles' AND postgis_projection_revision IS NULL AND pmtiles_object_key IS NOT NULL AND pmtiles_file_asset_id IS NOT NULL AND pmtiles_sha256 ~ '^[0-9a-f]{64}$' AND pmtiles_bytes > 0)
    ),
    CONSTRAINT vector_tile_release_validation_evidence_check CHECK ((source_kind = 'dynamic_postgis') OR (validated_at IS NOT NULL AND validation_evidence_sha256 ~ '^[0-9a-f]{64}$'))
);

ALTER TABLE catalog.vector_tile_release
    ADD CONSTRAINT vector_tile_release_unit_revision_snapshot_key
    UNIQUE (publication_unit_id, data_revision, canonical_iceberg_snapshot_id),
    ADD CONSTRAINT vector_tile_release_id_unit_revision_key UNIQUE (id, publication_unit_id, data_revision),
    ADD CONSTRAINT vector_tile_release_selection_binding_key UNIQUE (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id);

ALTER TABLE catalog.vector_tile_publication_unit
    ADD CONSTRAINT vector_tile_publication_unit_active_release_fkey
    FOREIGN KEY (active_release_id, id, active_data_revision) REFERENCES catalog.vector_tile_release(id, publication_unit_id, data_revision),
    ADD CONSTRAINT vector_tile_publication_unit_fallback_release_fkey
    FOREIGN KEY (fallback_release_id, id, fallback_data_revision) REFERENCES catalog.vector_tile_release(id, publication_unit_id, data_revision);

CREATE TABLE catalog.vector_tile_release_layer (
    release_id uuid NOT NULL REFERENCES catalog.vector_tile_release(id) ON DELETE CASCADE,
    layer_id text NOT NULL,
    source_layer text NOT NULL,
    feature_id_property text NOT NULL,
    tile_min_zoom smallint NOT NULL,
    tile_max_zoom smallint NOT NULL,
    render_min_zoom smallint NOT NULL,
    render_max_zoom smallint NOT NULL,
    feature_filter_properties jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (release_id, layer_id),
    CONSTRAINT vector_tile_release_layer_id_check CHECK (layer_id ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'),
    CONSTRAINT vector_tile_release_layer_source_layer_check CHECK (source_layer ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'),
    CONSTRAINT vector_tile_release_layer_zoom_check CHECK (tile_min_zoom BETWEEN 0 AND 24 AND tile_max_zoom BETWEEN 0 AND 24 AND render_min_zoom BETWEEN 0 AND 24 AND render_max_zoom BETWEEN 0 AND 24 AND tile_min_zoom <= tile_max_zoom AND render_min_zoom <= render_max_zoom),
    CONSTRAINT vector_tile_release_layer_feature_id_check CHECK (feature_id_property = lower(feature_id_property) AND btrim(feature_id_property) <> ''),
    CONSTRAINT vector_tile_release_layer_filter_check CHECK (jsonb_typeof(feature_filter_properties) = 'object')
);

ALTER TABLE catalog.vector_tile_release_layer
    ADD CONSTRAINT vector_tile_release_layer_release_source_layer_key UNIQUE (release_id, source_layer);

CREATE TABLE catalog.vector_tile_runtime_manifest (
    id uuid PRIMARY KEY,
    manifest_generation bigint NOT NULL UNIQUE,
    published_at timestamptz NOT NULL DEFAULT now(),
    manifest_file_asset_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT vector_tile_runtime_manifest_generation_check CHECK (manifest_generation BETWEEN 1 AND 9007199254740991)
);

CREATE TABLE catalog.vector_tile_runtime_manifest_unit (
    manifest_id uuid NOT NULL REFERENCES catalog.vector_tile_runtime_manifest(id) ON DELETE CASCADE,
    publication_unit_id uuid NOT NULL REFERENCES catalog.vector_tile_publication_unit(id),
    release_id uuid NOT NULL REFERENCES catalog.vector_tile_release(id),
    serving_generation bigint NOT NULL,
    data_revision uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    PRIMARY KEY (manifest_id, publication_unit_id),
    UNIQUE (manifest_id, release_id),
    CONSTRAINT vector_tile_runtime_manifest_unit_generation_check CHECK (serving_generation BETWEEN 1 AND 9007199254740991),
    CONSTRAINT vector_tile_runtime_manifest_unit_snapshot_check CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$')
);

ALTER TABLE catalog.vector_tile_runtime_manifest_unit
    ADD CONSTRAINT vector_tile_runtime_manifest_unit_release_binding_fkey
    FOREIGN KEY (release_id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id)
    REFERENCES catalog.vector_tile_release(id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id);

CREATE TABLE catalog.vector_tile_runtime_manifest_pointer (
    singleton boolean PRIMARY KEY DEFAULT true,
    manifest_id uuid NOT NULL REFERENCES catalog.vector_tile_runtime_manifest(id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT vector_tile_runtime_manifest_pointer_singleton_check CHECK (singleton)
);

-- The only runtime switch. Builders insert and validate a complete manifest first; this function
-- is the compare-and-swap gate that makes the immutable manifest pointer authoritative for both
-- dynamic PostGIS and static PMTiles units. Martin's current PostGIS views join this pointer, so
-- a URL query parameter can never select an uncommitted generation.
CREATE OR REPLACE FUNCTION catalog.promote_vector_tile_runtime_manifest(
    expected_manifest_id uuid,
    next_manifest_id uuid
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
    current_manifest_id uuid;
    current_generation bigint;
    next_generation bigint;
    next_unit_count bigint;
    publication_unit_count bigint;
    updated_unit_count bigint;
BEGIN
    -- Lock the singleton relation itself so two bootstrap CAS calls cannot both observe an empty
    -- table and race into a unique-key error without a deterministic compare-and-swap boundary.
    LOCK TABLE catalog.vector_tile_runtime_manifest_pointer IN SHARE ROW EXCLUSIVE MODE;

    SELECT manifest_id
      INTO current_manifest_id
      FROM catalog.vector_tile_runtime_manifest_pointer
     WHERE singleton = true
     FOR UPDATE;

    IF (expected_manifest_id IS NULL AND current_manifest_id IS NOT NULL)
       OR (expected_manifest_id IS NOT NULL AND current_manifest_id IS DISTINCT FROM expected_manifest_id) THEN
        RAISE EXCEPTION 'vector tile runtime manifest compare-and-swap conflict: expected %, current %',
            expected_manifest_id, current_manifest_id
            USING ERRCODE = '40001';
    END IF;

    SELECT manifest_generation
      INTO next_generation
      FROM catalog.vector_tile_runtime_manifest
     WHERE id = next_manifest_id;
    IF next_generation IS NULL THEN
        RAISE EXCEPTION 'vector tile runtime manifest % does not exist', next_manifest_id
            USING ERRCODE = '23503';
    END IF;

    SELECT count(*)
      INTO next_unit_count
      FROM catalog.vector_tile_runtime_manifest_unit
     WHERE manifest_id = next_manifest_id;
    SELECT count(*)
      INTO publication_unit_count
      FROM catalog.vector_tile_publication_unit;
    IF next_unit_count = 0 OR next_unit_count <> publication_unit_count THEN
        RAISE EXCEPTION 'runtime manifest % is not a complete publication', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    -- Martin discovers remote PMTiles source IDs from filename stems. Keep that derived identity
    -- deterministic at the database promotion boundary: one unit/release has exactly one route.
    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND release.source_kind = 'static_pmtiles'
           AND (
               release.martin_source_id <> format('%s-%s', unit.unit_key, release.id)
               OR right(release.pmtiles_object_key, length(format('%s.pmtiles', release.martin_source_id)))
                  <> format('%s.pmtiles', release.martin_source_id)
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % has a non-release-addressed static PMTiles source', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    -- The database gate repeats the domain state machine so a direct SQL caller cannot bypass it.
    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND unit.active_release_id IS NULL
           AND release.source_kind <> 'dynamic_postgis'
    ) THEN
        RAISE EXCEPTION 'the first runtime publication must be dynamic PostGIS'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
          JOIN catalog.vector_tile_release AS release
            ON release.id = manifest_unit.release_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND unit.active_release_id IS NOT NULL
           AND release.source_kind = 'static_pmtiles'
           AND manifest_unit.data_revision <> unit.active_data_revision
    ) THEN
        RAISE EXCEPTION 'static PMTiles must use the currently selected data revision'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          JOIN catalog.vector_tile_publication_unit AS unit
            ON unit.id = manifest_unit.publication_unit_id
         WHERE manifest_unit.manifest_id = next_manifest_id
           AND (
               (unit.active_release_id IS NULL AND manifest_unit.serving_generation <> 1)
               OR
               (unit.active_release_id IS NOT NULL
                AND manifest_unit.serving_generation <> unit.serving_generation + 1)
           )
    ) THEN
        RAISE EXCEPTION 'runtime manifest % has a serving-generation gap', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    IF current_manifest_id IS NOT NULL THEN
        SELECT manifest_generation
          INTO current_generation
          FROM catalog.vector_tile_runtime_manifest
         WHERE id = current_manifest_id;
        IF next_generation <= current_generation THEN
            RAISE EXCEPTION 'runtime manifest generation must increase: current %, next %',
                current_generation, next_generation
                USING ERRCODE = '40001';
        END IF;
    END IF;

    UPDATE catalog.vector_tile_publication_unit AS unit
       SET active_release_id = manifest_unit.release_id,
           active_data_revision = manifest_unit.data_revision,
           serving_generation = manifest_unit.serving_generation,
           fallback_release_id = CASE
               WHEN unit.fallback_data_revision = manifest_unit.data_revision
               THEN unit.fallback_release_id
               ELSE NULL
           END,
           fallback_data_revision = CASE
               WHEN unit.fallback_data_revision = manifest_unit.data_revision
               THEN unit.fallback_data_revision
               ELSE NULL
           END,
           version = unit.version + 1,
           updated_at = now()
      FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
     WHERE manifest_unit.manifest_id = next_manifest_id
       AND manifest_unit.publication_unit_id = unit.id;
    GET DIAGNOSTICS updated_unit_count = ROW_COUNT;
    IF updated_unit_count <> publication_unit_count THEN
        RAISE EXCEPTION 'runtime manifest % does not select every publication unit', next_manifest_id
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO catalog.vector_tile_runtime_manifest_pointer (singleton, manifest_id, updated_at)
    VALUES (true, next_manifest_id, now())
    ON CONFLICT (singleton) DO UPDATE
        SET manifest_id = EXCLUDED.manifest_id,
            updated_at = EXCLUDED.updated_at;

    RETURN next_generation;
END;
$function$;

CREATE TABLE catalog.vector_tile_build_job (
    id uuid PRIMARY KEY,
    publication_unit_id uuid NOT NULL REFERENCES catalog.vector_tile_publication_unit(id),
    input_release_id uuid NOT NULL REFERENCES catalog.vector_tile_release(id),
    input_data_revision uuid NOT NULL,
    frozen_source_snapshot_id text NOT NULL,
    status text NOT NULL,
    idempotency_key text NOT NULL,
    result_snapshot_id text,
    result_evidence_sha256 character(64),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (publication_unit_id, idempotency_key),
    CONSTRAINT vector_tile_build_job_status_check CHECK (status IN ('planned', 'running', 'validated', 'promoted', 'superseded', 'failed')),
    CONSTRAINT vector_tile_build_job_snapshot_check CHECK (frozen_source_snapshot_id ~ '^[1-9][0-9]*$'),
    CONSTRAINT vector_tile_build_job_result_snapshot_check CHECK (result_snapshot_id IS NULL OR result_snapshot_id = frozen_source_snapshot_id),
    CONSTRAINT vector_tile_build_job_result_evidence_check CHECK (status NOT IN ('validated', 'promoted') OR result_evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE catalog.vector_tile_refresh_observation (
    id uuid PRIMARY KEY,
    manifest_generation bigint NOT NULL,
    serving_generation bigint NOT NULL,
    outcome text NOT NULL,
    probe_environment text NOT NULL,
    evidence_sha256 character(64) NOT NULL,
    idempotency_key text NOT NULL UNIQUE,
    observed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT vector_tile_refresh_observation_generation_check CHECK (manifest_generation > 0 AND serving_generation > 0),
    CONSTRAINT vector_tile_refresh_observation_outcome_check CHECK (outcome IN ('commit', 'first_tile', 'timeout', 'error')),
    CONSTRAINT vector_tile_refresh_observation_evidence_check CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT vector_tile_refresh_observation_environment_check CHECK (btrim(probe_environment) <> '')
);

CREATE TABLE serving_postgis.parcel_boundary_publication (
    pnu character(19) NOT NULL,
    data_revision uuid NOT NULL,
    canonical_iceberg_snapshot_id text NOT NULL,
    source_record_id uuid,
    source_object_key text NOT NULL,
    complex_id uuid,
    parcel_id uuid,
    official_complex_code text NOT NULL,
    geometry_checksum_sha256 character(64) NOT NULL,
    geom public.geometry(MultiPolygon, 5179) NOT NULL,
    properties jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_boundary_publication_pnu_check CHECK (pnu ~ '^[0-9]{19}$'),
    CONSTRAINT parcel_boundary_publication_snapshot_check CHECK (canonical_iceberg_snapshot_id ~ '^[1-9][0-9]*$'),
    CONSTRAINT parcel_boundary_publication_geometry_check CHECK (public.st_srid(geom) = 5179 AND public.st_isvalid(geom)),
    CONSTRAINT parcel_boundary_publication_checksum_check CHECK (geometry_checksum_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT parcel_boundary_publication_properties_check CHECK (jsonb_typeof(properties) = 'object')
);

ALTER TABLE serving_postgis.parcel_boundary_publication
    ADD CONSTRAINT parcel_boundary_publication_pkey PRIMARY KEY (data_revision, pnu);

CREATE INDEX parcel_boundary_publication_data_revision_idx ON serving_postgis.parcel_boundary_publication (data_revision, pnu);
CREATE INDEX parcel_boundary_publication_complex_idx ON serving_postgis.parcel_boundary_publication (complex_id) WHERE complex_id IS NOT NULL;
CREATE INDEX parcel_boundary_publication_geom_gix ON serving_postgis.parcel_boundary_publication USING gist (geom);

-- Martin is configured against this stable view. Producers may stage multiple revisions in the
-- base table, but only the pointer-selected revision is visible to the dynamic tile source.
CREATE OR REPLACE VIEW serving_postgis.parcel_boundary_current AS
SELECT
    publication.pnu::text AS pnu,
    publication.official_complex_code,
    publication.geom::public.geometry(MultiPolygon, 5179) AS geom
FROM serving_postgis.parcel_boundary_publication AS publication
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.data_revision = publication.data_revision
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
JOIN catalog.vector_tile_release AS release
  ON release.id = manifest_unit.release_id
WHERE unit.unit_key = 'parcels'
  AND release.source_kind = 'dynamic_postgis';
