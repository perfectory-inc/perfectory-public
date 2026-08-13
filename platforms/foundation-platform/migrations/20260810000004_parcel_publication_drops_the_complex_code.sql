-- The parcel serving projection stops requiring an industrial complex code.
--
-- ADR-0019 removed `catalog.parcel.complex_id NOT NULL` because a parcel outside every industrial
-- complex — most of the country — could not be represented. The same requirement sat in the table
-- tiles are actually cut from: `serving_postgis.parcel_boundary_publication.official_complex_code
-- text NOT NULL`, beside a nullable `complex_id` saying the same thing. Only one of the two was
-- mandatory, and it was the one nothing reads.
--
-- Nothing reads it. `catalog_domain::vector_tile_feature_filter_properties` owns the public feature
-- contract and names `pnu` for the `parcels` layer; `official_complex_code` belongs to the `complex`
-- layer. `scripts/tiles/martin-dynamic.yaml` declares `properties: pnu` for that source. Yet the
-- view below was shipping the column into tiles — a property the contract does not name. See
-- ADR-0024.
--
-- `complex_id` stays. It is nullable, so it blocks no loader, and whether a serving projection
-- should carry membership at all is an open question ADR-0021 left. `parcel_boundary_mirror` holds
-- the same column in the same state; dropping one and not the other would make that question harder
-- to answer, not easier.

-- The view first: it reads the column, so it must stop before the column can go.
--
-- `DROP` then `CREATE`, not `CREATE OR REPLACE`. Postgres refuses to replace a view with one that
-- has fewer columns (42P16, `cannot drop columns from view`) — replacement may only append. The
-- proof caught this on the first run of this migration.
DROP VIEW serving_postgis.parcel_boundary_current;

CREATE VIEW serving_postgis.parcel_boundary_current AS
SELECT
    publication.pnu::text AS pnu,
    publication.geom::public.geometry(MultiPolygon, 5179) AS geom
FROM serving_postgis.parcel_boundary_publication AS publication
JOIN catalog.vector_tile_release AS release
  ON release.postgis_projection_revision = publication.projection_load_id
JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
  ON manifest_unit.release_id = release.id
JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
  ON pointer.manifest_id = manifest_unit.manifest_id
JOIN catalog.vector_tile_publication_unit AS unit
  ON unit.id = manifest_unit.publication_unit_id
WHERE unit.unit_key = 'parcels'
  AND release.source_kind = 'dynamic_postgis';

ALTER TABLE serving_postgis.parcel_boundary_publication
    DROP COLUMN official_complex_code;

COMMENT ON TABLE serving_postgis.parcel_boundary_publication IS
    'Parcel geometry a selected release publishes for tiles. Carries only what the tile contract names for the parcels layer; membership in an industrial complex is a dated fact in catalog.parcel_complex_membership and most parcels have none (ADR-0024).';
