-- Makes the static promotion path executable. It was not.
--
-- `vector_tile_release` was `UNIQUE (publication_unit_id, data_revision,
-- canonical_iceberg_snapshot_id)`, which permits exactly one release per unit and revision. Three
-- other rules require two:
--
--   * `promote_vector_tile_runtime_manifest` raises when a static release's revision differs from
--     the unit's currently selected one ("static PMTiles must use the currently selected data
--     revision"), so the static release must carry the active revision;
--   * `vector_tile_publication_unit_fallback_revision_check` requires the preserved fallback to
--     share the active revision, so the dynamic release it replaces stays at that same revision;
--   * `catalog_domain::validate_build_snapshot_binding` requires a build's frozen snapshot to equal
--     its input release's, so the two releases also share the snapshot.
--
-- A dynamic and a static release for one unit and revision therefore had to coexist and could not.
-- A live migrated database refuses the second insert with 23505; that is how this was found rather
-- than reasoned about. It is also why Task 6 Step 6 has no implementation.
--
-- The key gains `source_kind`, which is what the model actually says: one release per publication
-- unit, data revision, snapshot, and complete source kind. Nothing is widened beyond that — two
-- static promotions of one revision still collide, and that is a genuine duplicate. A failed or
-- superseded build cannot collide either, because a release row is written only by a successful
-- promotion.
--
-- `DROP CONSTRAINT` has no precedent in this repository's migrations. It is used here because
-- replacing a uniqueness key destroys no data and the alternative — leaving the contradiction in
-- place — makes a documented, gate-enforced publication path permanently unreachable. The two
-- narrower keys that other constraints reference by name are untouched:
-- `vector_tile_release_id_unit_revision_key` and `vector_tile_release_selection_binding_key` both
-- include `id`, so the foreign keys in `vector_tile_publication_unit` and
-- `vector_tile_runtime_manifest_unit` keep the exact indexes they were declared against.

ALTER TABLE catalog.vector_tile_release
    DROP CONSTRAINT vector_tile_release_unit_revision_snapshot_key,
    ADD CONSTRAINT vector_tile_release_unit_revision_snapshot_kind_key
    UNIQUE (publication_unit_id, data_revision, canonical_iceberg_snapshot_id, source_kind);
