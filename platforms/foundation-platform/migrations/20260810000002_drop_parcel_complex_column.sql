-- A parcel stops belonging to an industrial complex.
--
-- ADR-0019 step 3, the last one. `catalog.parcel.complex_id NOT NULL` said every parcel is inside
-- exactly one complex forever; membership has been a dated fact since `20260809000001`, the only
-- production read moved to `catalog.parcel_current_complex` in `20260810000001`, and the column has
-- been frozen against UPDATE since then. Nothing reads it and nothing may change it, so it goes.
--
-- What this unlocks is the point: a parcel outside every industrial complex — most of the country —
-- becomes representable for the first time. While the column was NOT NULL, creating one required
-- naming a complex it is not in.
--
-- `serving_postgis.parcel_boundary_mirror.complex_id` is NOT touched. It was nullable from the day
-- it was written, no production code fills it, and whether that projection should carry a complex
-- at all is a separate question (ADR-0020 남은 부채, ADR-0021). Dropping this column does not
-- decide that one.

-- The guard installed one migration ago has nothing left to protect. Dropping the column would
-- leave the trigger attached to `catalog.parcel` and its function comparing a column that no longer
-- exists; the trigger is removed first so no window exists where it could fire against a missing
-- field.
DROP TRIGGER IF EXISTS parcel_complex_projection_guard ON catalog.parcel;
DROP FUNCTION IF EXISTS catalog.protect_parcel_complex_projection();

-- The index and the foreign key were declared in `20260719000003` and `20260719000004`. Postgres
-- drops both with the column, but naming them makes this migration a complete statement of what
-- disappears rather than a line whose consequences a reader has to know to expect.
ALTER TABLE catalog.parcel
    DROP CONSTRAINT IF EXISTS parcel_complex_id_fkey;
DROP INDEX IF EXISTS catalog.parcel_complex_id_idx;

ALTER TABLE catalog.parcel
    DROP COLUMN complex_id;

COMMENT ON TABLE catalog.parcel IS
    'A parcel, identified by PNU. Membership in an industrial complex is a dated fact in catalog.parcel_complex_membership, not a column here (ADR-0019); most parcels belong to no complex at all.';
