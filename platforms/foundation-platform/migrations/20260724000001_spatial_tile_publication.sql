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
    CONSTRAINT vector_tile_publication_unit_key_check CHECK (unit_key ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
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
    CONSTRAINT vector_tile_release_source_id_check CHECK (btrim(martin_source_id) <> ''),
    CONSTRAINT vector_tile_release_url_check CHECK (position('{z}' in tiles_url_template) > 0 AND position('{x}' in tiles_url_template) > 0 AND position('{y}' in tiles_url_template) > 0),
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
    CONSTRAINT vector_tile_release_layer_id_check CHECK (layer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT vector_tile_release_layer_zoom_check CHECK (tile_min_zoom BETWEEN 0 AND 24 AND tile_max_zoom BETWEEN 0 AND 24 AND render_min_zoom BETWEEN 0 AND 24 AND render_max_zoom BETWEEN 0 AND 24 AND tile_min_zoom <= tile_max_zoom AND render_min_zoom <= render_max_zoom),
    CONSTRAINT vector_tile_release_layer_feature_id_check CHECK (feature_id_property = lower(feature_id_property) AND btrim(feature_id_property) <> ''),
    CONSTRAINT vector_tile_release_layer_filter_check CHECK (jsonb_typeof(feature_filter_properties) = 'object')
);

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
    pnu character(19) PRIMARY KEY,
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

CREATE INDEX parcel_boundary_publication_data_revision_idx ON serving_postgis.parcel_boundary_publication (data_revision, pnu);
CREATE INDEX parcel_boundary_publication_complex_idx ON serving_postgis.parcel_boundary_publication (complex_id) WHERE complex_id IS NOT NULL;
CREATE INDEX parcel_boundary_publication_geom_gix ON serving_postgis.parcel_boundary_publication USING gist (geom);
