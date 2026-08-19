-- The industrial-complex designation boundary gets the serving projection its polygons had no road to.
--
-- `silver.industrial_complex_boundaries` holds the polygons and `serving_postgis` held no table for
-- them, so the boundary a client would draw existed only in the lakehouse. This adds the same two
-- relations the administrative boundary already has — an append-only publication table and a view
-- gated by the runtime manifest — for the `complex` publication unit.
--
-- **The runtime manifest stays the only visibility switch.** `20260727000002` states it and
-- `20260731000001` re-stated the view so the join runs through the *release's projection load*
-- rather than through a revision: the rows a tile is cut from are the exact rows the selected
-- release names. The view below is that shape, not the older one. An unpromoted load is invisible.
--
-- Root ADR-0045 records the shape below and the alternatives it rejects. Four deliberate
-- differences from `serving_postgis.administrative_unit_boundary_publication`, each of which is a
-- fact about this source rather than a preference:
--
--   * **No `data_revision`, no `canonical_iceberg_snapshot_id`.** The administrative table carries
--     both because it predates `serving_postgis.spatial_projection_load`, which now carries them —
--     with foreign keys into `catalog.publication_revision` that a copy on the geometry row cannot
--     have. `projection_load_id` is NOT NULL here, so every row reaches the revision and the
--     snapshot by join. Copying them onto each of 1,343 geometry rows would be the same knowledge in
--     two places, which the header of `20260731000002` argues against at length while removing
--     exactly such a duplicate (`spatial_projection_load.publication_unit_key`).
--   * **`source_snapshot_id` is not required to be `iceberg:`-shaped.** That prefix is the
--     administrative source-snapshot contract. This source's Silver rows carry the Bronze snapshot
--     lineage the boundary export minted (`vworldkr__sandan_boundary-<object stem>`), so asserting
--     the administrative shape here would refuse every real row. The Iceberg snapshot this
--     projection was built from is the load's `canonical_iceberg_snapshot_id`, which is where a
--     snapshot number belongs.
--   * **No `properties` jsonb.** The administrative table has one and nothing selects it — not its
--     own `_current` view, not the slice proof. A tile property has exactly one definition,
--     `catalog_domain::vector_tile_feature_filter_properties`, and the view below reads the typed
--     columns it names. A second, untyped copy of the same values would be the duplicate this
--     migration is otherwise at pains to avoid.
--   * **`area_sqm_calculated` is carried and `NOT NULL`.** `gold.complex_catalog.calculated_area_sqm`
--     is null for all 1,442 complexes; Silver computed the area while building the geometry and this
--     is the first consumer that can state it. `NOT NULL` is provable rather than hopeful:
--     `normalize_industrial_complex_boundary_silver_rows` refuses a boundary whose area is not
--     finite and positive, so no row this producer can emit has one to lose.
--
-- The `complex_id` shape CHECK is the same guard `catalog.industrial_complex_lakehouse_complex_id_is_uuid_v5`
-- applies, for the same reason (ADR-0043): two id spaces meet here. `catalog.industrial_complex.id`
-- is minted locally with `Uuid::now_v7()`; the lakehouse derives `complex_id` as a UUIDv5 in
-- `industrial_complex_bronze_to_silver.py`. They are not computable from each other, and a v7 value
-- in this column would join to nothing in the lakehouse while looking like a perfectly good uuid.
-- The version nibble is the one place the difference is visible, so it is where the refusal lives.
--
-- No foreign key backs `complex_id`. The only uniqueness on the lakehouse id is
-- `industrial_complex_lakehouse_complex_id_idx`, a *partial* unique index (`WHERE ... IS NOT NULL`),
-- and PostgreSQL cannot reference a partial index. Making it total would require every canonical
-- complex to already carry its lakehouse id, which is not true today.
--
-- The `complex` publication unit is deliberately NOT seeded here.
-- `catalog.promote_vector_tile_runtime_manifest` refuses a manifest whose unit count differs from
-- `count(*)` over `catalog.vector_tile_publication_unit`, so a unit row that exists before it has a
-- release makes the *next* promotion of every other unit fail — on a deployment that has already
-- promoted `admin`, and in the `administrative_boundary_publication` promote tests. The unit is
-- minted by `publish-industrial-complex-boundary-postgis` instead, which is where
-- `ensure_administrative_unit` mints `admin` and for the same reason: the load names its unit by
-- foreign key, so the unit has to exist by the time the load is opened, and that is the publisher's
-- transaction.
--
-- Rollback: `DROP VIEW` / `DROP TABLE` as a new forward migration (root ADR-0001 §7). Dropping them
-- discards a serving projection that `publish-industrial-complex-boundary-postgis` rebuilds from the
-- lakehouse; no evidence is lost, because the lakehouse is the record and this is a projection of it.

CREATE TABLE serving_postgis.industrial_complex_boundary_publication (
    -- The lakehouse id, which is what `silver.industrial_complex_boundaries` and every Gold artifact
    -- key on. Not `catalog.industrial_complex.id`.
    complex_id uuid NOT NULL,
    -- What `catalog_domain::vector_tile_feature_filter_properties` names as the public property of
    -- the `complex` layer, and the code the two id spaces join on.
    official_complex_code text NOT NULL,
    projection_load_id uuid NOT NULL,
    boundary_kind text NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    source_object_key text NOT NULL,
    area_sqm_calculated numeric(18, 2) NOT NULL,
    -- Silver's checksum of the source WKB, in the source CRS. The publisher recomputes it over the
    -- bytes it decoded before writing the row, so this column is a checked fact rather than a copied
    -- string. It is not a digest of `geom`: `geom` is the 4326 reprojection PostGIS produced, and a
    -- digest of that would be a number with no counterpart anywhere to compare against.
    source_geometry_checksum_sha256 character(64) NOT NULL,
    geom public.geometry(MultiPolygon, 4326) NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT complex_boundary_publication_complex_id_is_uuid_v5
        CHECK (complex_id::text ~
               '^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    CONSTRAINT complex_boundary_publication_official_code_non_empty
        CHECK (length(btrim(official_complex_code)) > 0),
    -- One value, because one value has a producer. `docs/catalog/industrial-complex-lakehouse-poc.md`
    -- §4.2 defines four (`official`, `derived`, `corrected`, `draft`) and only the designation
    -- boundary is published; admitting the other three here would be a column whose extra values
    -- nothing writes, which is the `catalog.vector_tile_build_job` failure `20260730000004` names.
    CONSTRAINT complex_boundary_publication_kind_check
        CHECK (boundary_kind = 'official'),
    CONSTRAINT complex_boundary_publication_source_snapshot_check
        CHECK (source_snapshot_id = btrim(source_snapshot_id) AND source_snapshot_id <> ''),
    CONSTRAINT complex_boundary_publication_geometry_check
        CHECK (public.st_srid(geom) = 4326 AND public.st_isvalid(geom)),
    CONSTRAINT complex_boundary_publication_checksum_check
        CHECK (source_geometry_checksum_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT complex_boundary_publication_area_check
        CHECK (area_sqm_calculated > 0),
    CONSTRAINT complex_boundary_publication_source_object_key_check
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
    -- Keyed on the load, not on a revision: a second load of one revision is a second row set that
    -- can be served, compared or discarded on its own, which is what `20260731000001` established.
    CONSTRAINT complex_boundary_publication_pkey
        PRIMARY KEY (projection_load_id, complex_id),
    -- Within one materialisation an official complex code names one boundary. Agreement *between*
    -- two loads is not a serving-projection invariant.
    CONSTRAINT complex_boundary_publication_code_key
        UNIQUE (projection_load_id, official_complex_code)
);

ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    ADD CONSTRAINT complex_boundary_publication_load_fkey
        FOREIGN KEY (projection_load_id)
        REFERENCES serving_postgis.spatial_projection_load(id) ON DELETE RESTRICT,
    ADD CONSTRAINT complex_boundary_publication_source_record_fkey
        FOREIGN KEY (source_record_id)
        REFERENCES catalog.source_record(id) ON DELETE RESTRICT;

CREATE INDEX complex_boundary_publication_geom_gix
    ON serving_postgis.industrial_complex_boundary_publication USING gist (geom);
CREATE INDEX complex_boundary_publication_code_idx
    ON serving_postgis.industrial_complex_boundary_publication (official_complex_code);

-- The shared guard, not a third copy of it. `catalog.reject_temporal_history_mutation` is what
-- `catalog.publication_revision` and the administrative identity tables already use; the
-- administrative boundary publication has its own byte-identical function only because it was
-- written first.
CREATE TRIGGER complex_boundary_publication_append_only
BEFORE UPDATE OR DELETE ON serving_postgis.industrial_complex_boundary_publication
FOR EACH ROW EXECUTE FUNCTION catalog.reject_temporal_history_mutation();

-- Martin reads this. Every join in it is a condition on visibility: the rows must belong to the load
-- the selected release names, that release must be selected by a unit of the manifest the runtime
-- pointer currently points at, and that unit must be `complex`. A load that has been written but not
-- promoted satisfies none of them and returns no rows.
--
-- `area_sqm_calculated` is deliberately absent. ADR-0024 removed `official_complex_code` from the
-- parcels view on the grounds that a view feeding tiles carries what the layer contract names and
-- nothing else; for the `complex` layer `catalog_domain::vector_tile_feature_filter_properties`
-- names `official_complex_code`. `complex_id` joins it, as the administrative view carries
-- `administrative_unit_id`. The area is a column of the table, for the Gold backfill that has no
-- other source for it.
CREATE VIEW serving_postgis.industrial_complex_boundary_current AS
SELECT
    publication.complex_id::text AS complex_id,
    publication.official_complex_code,
    publication.geom::public.geometry(MultiPolygon, 4326) AS geom
FROM serving_postgis.industrial_complex_boundary_publication AS publication
JOIN catalog.vector_tile_release AS release
  ON release.postgis_projection_revision = publication.projection_load_id
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.release_id = release.id
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
WHERE unit.unit_key = 'complex'
  AND release.source_kind = 'dynamic_postgis';

COMMENT ON TABLE serving_postgis.industrial_complex_boundary_publication IS
    'Append-only PostGIS projection of official industrial-complex designation polygons, reprojected to EPSG:4326 from the EPSG:5186 the lakehouse stores. Runtime visibility is CAS-manifest controlled; a complex with no polygon has no row.';
